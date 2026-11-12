# ActivityCorrelator memory leak: problem and fix

Issue: [microsoft/mssql-jdbc#3075](https://github.com/microsoft/mssql-jdbc/issues/3075)

## Context: what is activity tracing?

Activity tracing is a **driver diagnostic feature** that adds a thread-associated identifier to client logs and connection prelogin data. It provides supplementary context for connection troubleshooting; it is not connection pooling, SQL audit logging, or an application request-tracing framework.

- **Activity ID:** a UUID plus a sequence number, displayed as `<UUID>-<sequence>`. The driver reuses the UUID on the same thread until cleanup and advances the sequence through `getNext()`.
- **Activity map:** the original `activityIdMap` stores `thread ID -> ActivityId`. It holds the current tracing object for each participating thread, not connections, query results, or a history of log messages. An activity ID is distinct from the per-connection `ClientConnectionId`.
- **Activity logging:** setting `com.microsoft.sqlserver.jdbc.traceactivity=on` in Java logging configuration enables activity-ID logging when the relevant driver loggers allow `FINER` messages. These IDs help identify related operations among interleaved logs from multiple threads.

During connection prelogin, the driver sends the activity UUID and sequence alongside `ClientConnectionId` in the TDS `TRACEID` field. A later borrower of a pooled connection can log a different activity ID; that does not establish server-side correlation for that borrower's queries. This fix preserves existing tracing behavior rather than adding request-level tracing.

## Problem: activity entries outlive their threads

The static activity map retains its entries until they are explicitly removed; a thread terminating does not automatically remove its numeric ID or tracing object from the map. However, the driver's connection-close cleanup only removes the **calling thread's** entry. If thread A opens a connection and thread B later closes it, cleanup removes B's entry, leaving A's entry behind even after A terminates.

With repeated creation of short-lived threads, these stale entries accumulate and remain reachable through the static map, preventing garbage collection. **This is the memory leak: retained tracing data, not necessarily unclosed connections.** The reporter observed approximately **3.42 million entries retaining 427 MB**; these are reported production figures, not measurements from our local tests.

**Disabling activity logging does not avoid this leak:** prelogin still creates the activity data for the TDS `TRACEID` field, whether or not the driver writes activity IDs to logs.

## Example: HikariCP connection replacement

1. Adder thread **A** opens connection **C1**. Driver prelogin creates an activity-map entry for **A**, even with activity logging disabled.
2. HikariCP keeps **C1** in its connection pool. After five seconds without work, its adder worker **A** can time out and terminate. The map entry remains.
3. Application threads borrow and return **C1**. Normal proxy `close()` returns it to the pool rather than closing the physical connection.
4. When **C1** is retired, closer thread **B** physically closes it. Driver cleanup removes **B's** activity entry, not **A's**.
5. A new adder worker opens a replacement connection and creates another entry. Repeating this cycle grows the map while the number of open connections can remain stable.

**Root cause:** per-thread tracing data is kept in global storage, but cleanup is tied to connection-close calls rather than thread lifetime.

## Solution and required changes

Replace the static map in `ActivityCorrelator.java` with:

```java
private static final ThreadLocal<ActivityId> activityId =
        ThreadLocal.withInitial(ActivityId::new);
```

- `getCurrent()` returns `activityId.get()`.
- `cleanupActivityId()` calls `activityId.remove()`.
- Remove unused map imports and update the class description.
- Keep `getNext()`, UUID/sequence behavior, and existing cleanup callers unchanged.
- Add regression coverage for thread isolation, sequence behavior, explicit cleanup, and terminated-thread reclamation, including connections opened and closed on different threads.

### When cleanup happens

Explicit `remove()` clears the calling thread's value. When a thread terminates, its thread-local storage is released, making the activity eligible for garbage collection if nothing else references it. Actual memory reclamation occurs during GC, not necessarily immediately.

A live, idle worker retains its activity for reuse; finishing a task does not clear it. No HikariCP configuration changes, public JDBC API changes, or TDS format changes are required.

## Verification

Both new leak tests failed before the fix and passed afterward, including the real SQL Server cross-thread open/close scenario.

Activity, pooling (including HikariCP), and BVT smoke tests produced **36 passed, 0 failed, 1 skipped per profile** for `jre8`, `jre11`, and `jre21`, all running on JDK 21. The skipped test requires a specific authentication configuration. This is targeted regression coverage, not the full driver suite or separate Java 8/11 runtime validation.
