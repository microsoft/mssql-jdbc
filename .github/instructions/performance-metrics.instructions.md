# Performance Metrics for Microsoft JDBC Driver for SQL Server

This document describes the performance metrics instrumentation in the JDBC driver.

## Overview

The driver provides fine-grained performance tracking for both connection and statement operations. Metrics are published via `PerformanceLogCallback` (if registered) and/or logged via Java logging.

## Configuration

Performance metrics can be enabled via two mechanisms:

### 1. Programmatic Callback Registration

Register a `PerformanceLogCallback` to receive performance data programmatically:

```java
SQLServerDriver.registerPerformanceLogCallback(new PerformanceLogCallback() {
    @Override
    public void publish(PerformanceActivity activity, int connectionId, long durationMs, 
            Exception exception) {
        // Handle connection-level metrics
        System.out.printf("Activity: %s, Connection: %d, Duration: %d ms%n",
                activity, connectionId, durationMs);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, 
            long durationMs, Exception exception) {
        // Handle statement-level metrics
        System.out.printf("Activity: %s, Connection: %d, Statement: %d, Duration: %d ms%n",
                activity, connectionId, statementId, durationMs);
    }
});
```

#### Nanosecond Granularity

By default, the `duration` parameter is reported in **milliseconds**. To receive
**nanosecond** granularity instead, override `useNanoseconds()` to return `true`:

```java
SQLServerDriver.registerPerformanceLogCallback(new PerformanceLogCallback() {
    @Override
    public boolean useNanoseconds() {
        return true; // duration values will be in nanoseconds
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, long durationNs, 
            Exception exception) {
        System.out.printf("Activity: %s, Connection: %d, Duration: %d ns%n",
                activity, connectionId, durationNs);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, 
            long durationNs, Exception exception) {
        System.out.printf("Activity: %s, Connection: %d, Statement: %d, Duration: %d ns%n",
                activity, connectionId, statementId, durationNs);
    }
});
```

The value of `useNanoseconds()` is captured once at registration time and remains fixed
for the lifetime of the callback. To change the duration unit, unregister and re-register
with the new setting.

When `useNanoseconds()` returns `true`:
- Timing uses `System.nanoTime()` instead of `System.currentTimeMillis()`
- Log output uses `ns` as the unit suffix instead of `ms`
- All publish calls (connection-level and statement-level) use nanosecond values

#### Accessing SQL Text and Statement Type

Inside a `publish()` callback, you can retrieve the SQL text and statement type for the
current performance event using the default methods `getCurrentUserSql()` and
`getCurrentStatementType()`:

```java
SQLServerDriver.registerPerformanceLogCallback(new PerformanceLogCallback() {
    @Override
    public void publish(PerformanceActivity activity, int connectionId, long durationMs, 
            Exception exception) {
        // Connection-level: getCurrentUserSql() returns null here
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, 
            long durationMs, Exception exception) {
        // Statement-level: SQL and type available for EXECUTE/PREPEXEC/PREPARE activities
        String sql = getCurrentUserSql();
        String type = getCurrentStatementType();
        System.out.printf("[%s] %s | %s | %d ms%n", type, activity, sql, durationMs);
    }
});
```

**`getCurrentUserSql()`** returns the SQL text submitted by the application:
- For `Statement`: the SQL string passed to `executeQuery(sql)`, `execute(sql)`, etc.
- For `PreparedStatement`/`CallableStatement`: the SQL passed to `prepareStatement(sql)` or `prepareCall(sql)`
- Returns `null` for connection-level activities and sub-activities (REQUEST_BUILD, FIRST_SERVER_RESPONSE)

**`getCurrentStatementType()`** returns one of:
- `"Statement"` — plain `java.sql.Statement`
- `"PreparedStatement"` — `java.sql.PreparedStatement`
- `"CallableStatement"` — `java.sql.CallableStatement`
- `null` — for connection-level activities and sub-activities

These methods are only valid **inside** a `publish()` invocation. Calling them outside
`publish()` (e.g., from another thread or after `publish()` returns) will return `null`.

> **Note**: Sub-activities like `STATEMENT_REQUEST_BUILD` and `STATEMENT_FIRST_SERVER_RESPONSE`
> do not provide SQL or statement type information. Only the top-level execution activities
> (`STATEMENT_EXECUTE`, `STATEMENT_PREPEXEC`, `STATEMENT_PREPARE`) populate these values.

### 2. Java Logging Configuration

Configure `java.util.logging` for the performance metrics loggers at `FINE` level:

```properties
# In logging.properties
com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection.level = FINE
com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement.level = FINE
handlers = java.util.logging.ConsoleHandler
java.util.logging.ConsoleHandler.level = FINE
```

Or programmatically:
```java
Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Connection")
      .setLevel(Level.FINE);
Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceMetrics.Statement")
      .setLevel(Level.FINE);
```

Both mechanisms can be used simultaneously.

---

## Connection-Level Activities

| Activity | Description |
|----------|-------------|
| `CONNECTION` | Total time for establishing a connection (includes PRELOGIN, LOGIN, TOKEN_ACQUISITION) |
| `PRELOGIN` | Time for TDS prelogin negotiation with the server |
| `LOGIN` | Time for the TDS login/authentication handshake |
| `TOKEN_ACQUISITION` | Time to acquire federated authentication tokens (Azure AD) |

### CONNECTION
- **Scope**: Entire `connectInternal()` method
- **Includes**: All sub-activities (PRELOGIN, LOGIN, TOKEN_ACQUISITION if applicable)
- **Exception Tracking**: Yes

### PRELOGIN
- **Scope**: `prelogin()` method
- **Measures**: TDS prelogin packet exchange, encryption negotiation, server capability detection
- **Exception Tracking**: Yes

### LOGIN
- **Scope**: `login()` method
- **Measures**: Authentication handshake, TDS login7 packet exchange
- **Exception Tracking**: Yes

### TOKEN_ACQUISITION
- **Scope**: `onFedAuthInfo()` method
- **Measures**: Time to acquire Azure AD access tokens
- **Triggered When**: Using Active Directory authentication methods
- **Exception Tracking**: Yes

---

## Statement-Level Activities

| Activity | Description |
|----------|-------------|
| `STATEMENT_REQUEST_BUILD` | Client-side time to build TDS request (parameter binding, SQL processing) |
| `STATEMENT_FIRST_SERVER_RESPONSE` | Time from packet sent to first response received |
| `STATEMENT_PREPARE` | Time to prepare a statement using `sp_prepare` |
| `STATEMENT_PREPEXEC` | Time for combined prepare+execute using `sp_prepexec` |
| `STATEMENT_EXECUTE` | Time to execute a statement |

### STATEMENT_REQUEST_BUILD
- **Scope**: From `executeStatement()` call to just before `startResponse()`
- **Measures**: Parameter binding, SQL processing, TDS packet construction
- **Exception Tracking**: No (timing-only metric)
- **Applies To**: All statement types (Statement, PreparedStatement, CallableStatement)
- **Note**: Restarts on retry to exclude retry backoff/sleep time from metrics

### STATEMENT_FIRST_SERVER_RESPONSE
- **Scope**: Around `startResponse()` which sends packet and reads response
- **Measures**: Network latency (both directions) + SQL Server query processing time
- **Exception Tracking**: No (timing-only metric)
- **Applies To**: All statement types

### STATEMENT_PREPARE
- **Scope**: `doPrep()` method in SQLServerPreparedStatement
- **Measures**: Time for `sp_prepare` stored procedure call
- **Triggered When**: `prepareMethod=prepare` (NOT the default)
- **Exception Tracking**: Yes
- **Note**: For default `sp_prepexec`, prepare and execute are combined

### STATEMENT_PREPEXEC
- **Scope**: Execution scope in `doExecutePreparedStatement()` when `isPrepExecUsed=true`
- **Measures**: Combined prepare+execute time using `sp_prepexec`
- **Triggered When**: Default behavior - first execution or re-preparation needed
- **Exception Tracking**: Yes
- **Note**: Time cannot be split into prepare vs execute components

### STATEMENT_EXECUTE
- **Scope**: Execution scope in various `doExecute*` methods
- **Measures**: Statement execution time after preparation (if any)
- **Triggered When**: 
  - Regular Statement execution
  - PreparedStatement using `sp_execute` (already prepared)
  - PreparedStatement using `sp_executesql` (first execution, optimizing for single-use)
  - PreparedStatement with `prepareMethod=none` (direct SQL, no preparation)
  - PreparedStatement with `prepareMethod=scopeTempTablesToConnection` (direct SQL for temp table operations)
  - Batch execution
- **Exception Tracking**: Yes

---

## Execution Methods & Activity Mapping

### Regular Statement (`SQLServerStatement`)

| Method | Activities Tracked |
|--------|-------------------|
| `doExecuteStatement()` | REQUEST_BUILD → SERVER_ROUNDTRIP → EXECUTE |
| `doExecuteStatementBatch()` | REQUEST_BUILD → SERVER_ROUNDTRIP → EXECUTE |
| `doExecuteCursored()` | REQUEST_BUILD → SERVER_ROUNDTRIP → EXECUTE |

### Prepared Statement (`SQLServerPreparedStatement`)

| Method | Activities Tracked |
|--------|-------------------|
| `doExecutePreparedStatement()` | REQUEST_BUILD → SERVER_ROUNDTRIP → (PREPEXEC or EXECUTE) |
| `doExecutePreparedStatementBatch()` | REQUEST_BUILD → SERVER_ROUNDTRIP → EXECUTE |
| `doExecuteExecMethodBatchCombined()` | REQUEST_BUILD → SERVER_ROUNDTRIP → EXECUTE |
| `doPrep()` | PREPARE |

#### Prepare Method Impact on Activities

| `prepareMethod` Setting | Execution Path | Activity |
|------------------------|----------------|----------|
| `prepexec` (default) | `sp_prepexec` (first re-use) | PREPEXEC |
| `prepexec` (default) | `sp_executesql` (first call) | EXECUTE |
| `prepexec` (default) | `sp_execute` (cached handle) | EXECUTE |
| `prepare` | `sp_prepare` + `sp_execute` | PREPARE, then EXECUTE |
| `none` | Direct SQL (PKT_QUERY) | EXECUTE |
| `scopeTempTablesToConnection` | Direct SQL for temp tables | EXECUTE |
| `scopeTempTablesToConnection` | `sp_prepexec` otherwise | PREPEXEC |

### PrepareMethod Impact

| prepareMethod Setting | 1st Execution | 2nd Execution | 3rd+ Execution |
|----------------------|---------------|---------------|----------------|
| `prepexec` (default) | STATEMENT_EXECUTE (sp_executesql) | STATEMENT_PREPEXEC (sp_prepexec) | STATEMENT_EXECUTE (sp_execute) |
| `prepare` | STATEMENT_PREPARE + STATEMENT_EXECUTE | STATEMENT_EXECUTE | STATEMENT_EXECUTE |
| `none` | STATEMENT_EXECUTE (direct SQL) | STATEMENT_EXECUTE (direct SQL) | STATEMENT_EXECUTE (direct SQL) |

> **Note**: With `prepexec` (default), the driver defers preparation assuming single use.
> Only on the 2nd execution does it use `sp_prepexec` (combined prepare+execute).
> From the 3rd execution onward, the cached handle is reused via `sp_execute`.
> This behavior can be overridden with `enablePrepareOnFirstPreparedStatementCall=true`,
> which forces `sp_prepexec` on the first call.

---

## Exception Tracking

| Activity | Tracks Exceptions |
|----------|-------------------|
| CONNECTION | ✅ Yes |
| PRELOGIN | ✅ Yes |
| LOGIN | ✅ Yes |
| TOKEN_ACQUISITION | ✅ Yes |
| STATEMENT_REQUEST_BUILD | ❌ No (timing-only) |
| STATEMENT_FIRST_SERVER_RESPONSE | ❌ No (timing-only) |
| STATEMENT_PREPARE | ✅ Yes |
| STATEMENT_PREPEXEC | ✅ Yes |
| STATEMENT_EXECUTE | ✅ Yes |

Activities with exception tracking use `scope.setException(e)` to record failures before the scope closes.

---

## Implementation Pattern

### For metrics with exception tracking:
```java
try (PerformanceLog.Scope scope = PerformanceLog.createScope(
        PerformanceLog.perfLoggerConnection,
        connectionID,
        PerformanceActivity.ACTIVITY_NAME)) {
    try {
        // All work here
    } catch (SQLException e) {
        scope.setException(e);
        throw e;
    }
}
```

### For timing-only metrics:
```java
startCreationToFirstPacketTracking();
try {
    // Work
} finally {
    endCreationToFirstPacketTracking();
}
```

---

## Files Modified

- `SQLServerConnection.java` - Connection-level activities
- `SQLServerStatement.java` - Base statement activities and tracking helper methods
- `SQLServerPreparedStatement.java` - Prepared statement activities
- `PerformanceActivity.java` - Activity enum definitions
- `PerformanceLog.java` - Logging infrastructure (ThreadLocal context for userSql/statementType)
- `PerformanceLogCallback.java` - Callback interface (includes `useNanoseconds()`, `getCurrentUserSql()`, `getCurrentStatementType()`)
