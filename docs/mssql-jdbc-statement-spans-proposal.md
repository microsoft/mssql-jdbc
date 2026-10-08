# OpenTelemetry for mssql-jdbc statement and prepared-statement execution

## Proposal summary

This document proposes OpenTelemetry lifecycle spans for JDBC `Statement`, `PreparedStatement`, and inherited `CallableStatement` execution. It follows the connection-span proposal's model: accurately bounded driver phases, native parent/child relationships, structured errors, a strict metadata allowlist, asynchronous export, and compatibility with the existing `PerformanceLogCallback` API.

The driver already measures five statement activities:

| Existing `PerformanceActivity` | Current meaning |
|---|---|
| `STATEMENT_REQUEST_BUILD` | Client-side SQL processing, parameter serialization, and TDS request construction. |
| `STATEMENT_FIRST_SERVER_RESPONSE` | `startResponse()` duration; packet send plus response acquisition. With full response buffering this can include buffering the complete response, not only first-byte latency. |
| `STATEMENT_PREPARE` | The `sp_prepare` server call used by `prepareMethod=prepare`. |
| `STATEMENT_PREPEXEC` | Combined `sp_prepexec` preparation and execution. The two parts cannot be separated from the client. |
| `STATEMENT_EXECUTE` | Direct SQL, `sp_executesql`, `sp_execute`, cursor, or batch execution, depending on the actual path. |

The definitions live in [PerformanceActivity](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceActivity.java), the legacy/lifecycle callback boundary lives in [PerformanceLog](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceLog.java), and the current public behavior is documented in [performance-metrics.md](performance-metrics.md). The central retry loop and request/response timing helpers are in [SQLServerStatement](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java); prepared execution and protocol selection are in [SQLServerPreparedStatement](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java).

These activities should be **blended into the lifecycle**, not replaced and not emitted as five unrelated peer spans. The execution activity is an enclosing server-call phase; `STATEMENT_FIRST_SERVER_RESPONSE` is nested within that call. `STATEMENT_REQUEST_BUILD` precedes it. `STATEMENT_PREPARE` is a separate server call only when the driver actually sends `sp_prepare` before `sp_execute`.

> **MVP decision:** emit one sampled execution tree per `execute*()` or `executeBatch()` invocation. Do not create spans for parameter setter calls, `addBatch()`, statement construction, prepared-handle close/unprepare, or ordinary `ResultSet.next()` consumption. The application sampler remains authoritative. Errors are marked on recorded trees but the driver does not override a sampler that declines recording.

## 1. What one statement execution contains

### Plain `Statement`

```text
mssql.driver.statement.execute                  CLIENT   UNSET  43 ms
└─ mssql.driver.statement.attempt               INTERNAL UNSET  43 ms
   ├─ mssql.driver.statement.request_build      INTERNAL UNSET   2 ms
   └─ mssql.driver.statement.server_call        INTERNAL UNSET  41 ms  [direct_sql]
      └─ mssql.driver.statement.first_response  INTERNAL UNSET  39 ms
```

### First `PreparedStatement` execution with the default prepare policy

The first execution normally uses `sp_executesql` and avoids creating a reusable server handle.

```text
mssql.driver.statement.execute                  CLIENT   UNSET  48 ms
└─ mssql.driver.statement.attempt               INTERNAL UNSET  48 ms
   ├─ mssql.driver.statement.request_build      INTERNAL UNSET   4 ms
   └─ mssql.driver.statement.server_call        INTERNAL UNSET  44 ms  [sp_executesql]
      └─ mssql.driver.statement.first_response  INTERNAL UNSET  42 ms
```

### Reused `PreparedStatement` with default `sp_prepexec`

```text
mssql.driver.statement.execute                  CLIENT   UNSET  51 ms
└─ mssql.driver.statement.attempt               INTERNAL UNSET  51 ms
   ├─ mssql.driver.statement.request_build      INTERNAL UNSET   5 ms
   └─ mssql.driver.statement.server_call        INTERNAL UNSET  46 ms  [sp_prepexec]
      └─ mssql.driver.statement.first_response  INTERNAL UNSET  44 ms
```

`sp_prepexec` remains one server-call phase. The client cannot truthfully split its duration into preparation and execution.

### `PreparedStatement` with `prepareMethod=prepare`

```text
mssql.driver.statement.execute                  CLIENT   UNSET  58 ms
└─ mssql.driver.statement.attempt               INTERNAL UNSET  58 ms
   ├─ mssql.driver.statement.request_build      INTERNAL UNSET   5 ms
   ├─ mssql.driver.statement.server_call        INTERNAL UNSET  17 ms  [sp_prepare]
   │  └─ mssql.driver.statement.first_response  INTERNAL UNSET  16 ms
   └─ mssql.driver.statement.server_call        INTERNAL UNSET  36 ms  [sp_execute]
      └─ mssql.driver.statement.first_response  INTERNAL UNSET  34 ms
```

The existing code currently wraps `sp_prepare` with `STATEMENT_PREPARE` and the later call with `STATEMENT_EXECUTE`. The lifecycle adapter should make their order and parenting explicit while preserving both legacy close-time callbacks.

### Native fields and boundaries

| Native field | Value / rule |
|---|---|
| Root name | `mssql.driver.statement.execute`, or `mssql.driver.statement.execute_batch` for JDBC batch APIs. The name never contains SQL, database, procedure, table, or user text. |
| Kind | Root `CLIENT`; all driver implementation phases `INTERNAL`. |
| Parent | Current application context captured at `execute*()` entry. If absent, the statement execution starts a new trace. |
| Start | At the recognized JDBC execution invocation, before previous-response cleanup that can fail this invocation and before the first attempt. The central `executeStatement()` path and batch-insert bulk-copy bypass both require coverage. |
| End | Immediately before `executeQuery`, `executeUpdate`, `execute`, or batch execution returns or throws. For `executeQuery`, the returned `ResultSet` may still fetch rows later. |
| Status | `ERROR` only when that invocation throws a terminal failure. Successful execution is `UNSET`, including a successful execution after an internal retry. |
| Result-set boundary | Includes response processing needed to return the first JDBC result/update count. It does **not** include application iteration over the returned `ResultSet`. |

One root describes one JDBC execution API invocation, not the lifetime of the reusable `Statement` object and not the lifetime of a server prepared handle.

## 2. Blending existing performance activities into lifecycle phases

### Canonical mapping

| Existing activity | Lifecycle span | Relationship and compatibility rule |
|---|---|---|
| `STATEMENT_REQUEST_BUILD` | `mssql.driver.statement.request_build` | One child per execution attempt or server request build. Preserve its current boundary and restart it after retry. It excludes configured retry backoff. |
| `STATEMENT_FIRST_SERVER_RESPONSE` | `mssql.driver.statement.first_response` | Child of the owning `server_call`, never a peer that is added to server-call duration. Preserve the current `startResponse()` boundary. |
| `STATEMENT_PREPARE` | `mssql.driver.statement.server_call` with `mssql.statement.protocol_operation=sp_prepare` | Separate call only when `doPrep()` sends `sp_prepare`. Preserve the legacy `STATEMENT_PREPARE` callback. |
| `STATEMENT_PREPEXEC` | `mssql.driver.statement.server_call` with `mssql.statement.protocol_operation=sp_prepexec` | Combined server operation. Do not fabricate separate prepare and execute children. Preserve the legacy `STATEMENT_PREPEXEC` callback. |
| `STATEMENT_EXECUTE` | `mssql.driver.statement.server_call` with the actual protocol operation | Used for `direct_sql`, `sp_executesql`, `sp_execute`, cursor operations, or batch calls. Preserve the legacy `STATEMENT_EXECUTE` callback. |

### Why the spans are nested

The current execution scopes enclose `startFirstPacketToFirstResponseTracking()` and response/result processing. Therefore:

```text
server_call duration
└─ first_response duration  (already included)
```

Dashboards must not calculate total statement time by summing root, attempt, server-call, and first-response durations. Parent durations include child durations. The phase tree separates where time was observed; it is not an additive accounting ledger.

### Required compatibility behavior

1. Existing `publish(PerformanceActivity, connectionId, statementId, duration, exception)` calls remain unchanged in activity, units, SQL/thread-local behavior, and close-time ordering.
2. Statement lifecycle START/END boundaries use the existing `publish(PerformanceLogEvent)` overload, extended to carry `statementId` and a statement phase.
3. Lifecycle activities may be new enum values appended after existing values; do not repurpose or reorder existing enum constants.
4. The lifecycle adapter consumes immutable, approved snapshots. It never receives or exports raw SQL, parameter values, or a raw exception reference asynchronously.
5. A legacy scope and its lifecycle representation measure the same underlying boundary. They are two delivery views, not two independently timed operations.

The current `PerformanceLogEvent` contract is connection-specific: its phase comes from `PerformanceActivity.connectionPhase()`, and statement activities are intentionally excluded from that overload. Implementation must generalize the event model rather than pretending the existing statement close-only callbacks already carry a lifecycle tree.

### Proposed lifecycle-only activities

Append lifecycle-only enum values without changing existing ordinals:

| Proposed activity | Stable phase |
|---|---|
| `STATEMENT_INVOCATION` | `statement.execute` |
| `STATEMENT_ATTEMPT` | `statement.attempt` |
| `STATEMENT_SERVER_CALL` | `statement.server_call` |
| `STATEMENT_RESULT_PROCESS` | `statement.result_process` |
| `STATEMENT_ENCRYPTION_METADATA` | `statement.encryption_metadata` |

The existing `STATEMENT_REQUEST_BUILD` and `STATEMENT_FIRST_SERVER_RESPONSE` activities can become lifecycle-capable themselves because their current boundaries are already useful. The three existing enclosing activities map to `STATEMENT_SERVER_CALL` snapshots while still publishing their original legacy identities.

`STATEMENT_RESULT_PROCESS` is optional for MVP. Add it only around work after response acquisition that is required to identify and return the first JDBC result. Do not stretch it over application-owned row iteration.

## 3. Execution paths and exact protocol operation

### Plain statement

| Driver path | Protocol operation |
|---|---|
| Non-cursored `Statement` | `direct_sql` (`PKT_QUERY`) |
| Server-cursored `Statement` | `cursor_open` (`sp_cursoropen`) |
| Plain statement batch | `direct_sql_batch` |

### Prepared statement

| Condition | Protocol operation |
|---|---|
| First default execution, preparation deferred | `sp_executesql` |
| Preparation through default combined path | `sp_prepexec` |
| Reused prepared handle | `sp_execute` |
| `prepareMethod=prepare` preparation call | `sp_prepare` followed by a distinct `sp_execute` server call |
| `prepareMethod=none` | `direct_sql` |
| `scopeTempTablesToConnection` with detected temp-table operation | `direct_sql` |
| Prepared server cursor | `cursor_prepexec` or `cursor_execute`, according to the actual RPC |
| Prepared batch | `prepared_batch`; individual internal RPCs remain children/calls of the one batch invocation |
| Batch-insert bulk-copy optimization | `bulk_copy`; do not mislabel as ordinary `sp_execute` |

The protocol operation is selected from observed driver control flow, never inferred later from duration or SQL text. Do not export prepared-handle IDs.

## 4. Metadata and full attribute schema

Use instrumentation scope `com.microsoft.sqlserver.jdbc` with the actual driver version. The application owns resource identity. Unknown conditional values are omitted rather than represented as `null`, `N/A`, or guessed strings.

### Root attributes

| Attribute | Type | Requirement and meaning |
|---|---|---|
| `mssql.telemetry.schema.version` | String | Required; initially `1.0`. Version independently from the driver. |
| `db.system.name` | String | Required; `microsoft.sql_server`. Pin the semantic-convention version during implementation. |
| `mssql.connection.guid` | String | Required when available; physical-connection GUID retained from connection creation. Place once on the statement execution root for server/client correlation, not on every child/event. |
| `mssql.driver.user_agent.original` | String | Conditional, privacy-approved seven-field driver user-agent; root only. |
| `mssql.statement.type` | String | Required: `statement`, `prepared_statement`, or `callable_statement`. |
| `mssql.statement.api` | String | Required: `execute_query`, `execute_update`, `execute_large_update`, `execute`, `execute_batch`, `execute_large_batch`, or `internal`. |
| `mssql.statement.operation` | String | Conditional safe classification: `select`, `insert`, `update`, `delete`, `merge`, `call`, `ddl`, `other`, or `unknown`. Derived by the driver's existing syntax classification without exporting text. |
| `db.query.text` | String | Conditional root-only masked SQL. The adapter obtains raw SQL only through callback-scoped `getCurrentUserSql()`, removes comments, replaces string, Unicode-string, numeric, exponent, and hexadecimal literals with `?`, normalizes whitespace, and caps output at 4,096 characters. Omit on malformed lexical constructs, input over 16,384 characters, or masking uncertainty. Prepared-statement `?` markers remain markers. |
| `mssql.statement.outcome` | String | Required at end: `success`, `failure`, `timeout`, or `canceled`. |
| `mssql.statement.attempt_count` | Int64 | Required at end; total statement attempts begun, including internal configurable retries. |
| `mssql.statement.retry_count` | Int64 | Required at end; additional attempts actually begun. |
| `mssql.statement.batch_size` | Int64 | Batch roots only; number of submitted batch entries. Zero is valid. |
| `mssql.statement.parameter_count` | Int64 | Prepared/callable roots when known; count only, never values or names. |
| `mssql.statement.query_timeout` | Double | Conditional; effective configured seconds. Zero means no query timeout under JDBC semantics. |
| `mssql.statement.response_buffering` | String | Conditional: `adaptive` or `full`. Required when interpreting `first_response`. |
| `mssql.statement.result_kind` | String | Conditional at end: `result_set`, `update_count`, `multiple`, or `none`. It does not expose row contents. |
| `mssql.statement.update_count` | Int64 | Optional terminal update count only when already known at API return. Do not force response draining to compute it. |
| `mssql.error.category` | String | Terminal failures only; evidence-based category defined below. |
| `error.type` | String | Terminal failures only; stable server number, resource key, or observed exception class, never a message. |
| `mssql.statement.failure_phase` | String | Terminal failures only: `request_build`, `encryption_metadata`, `server_call`, `first_response`, `result_process`, `retry_backoff`, or `unknown`. |

### Attempt and phase attributes

| Attribute | Type | Placement / meaning |
|---|---|---|
| `mssql.statement.attempt` | Int64 | Attempt and descendants; one-based. |
| `mssql.statement.attempt_reason` | String | Attempt: `initial`, `configurable_retry`, `invalid_handle_retry`, or `enclave_retry`. |
| `mssql.statement.attempt_outcome` | String | Attempt at end: `success`, `failure`, `timeout`, or `canceled`. |
| `mssql.statement.protocol` | String | Server call: `sql_batch`, `rpc`, or `bulk_copy`. |
| `mssql.statement.protocol_operation` | String | Server call; one of the observed operations listed in section 3. |
| `mssql.statement.prepared_handle_source` | String | Prepared server call when known: `new`, `statement_cache`, or `statement_local`. No numeric handle. |
| `mssql.statement.encryption_metadata_cached` | Boolean | Encryption-metadata phase; whether approved parameter-encryption metadata was reused. |
| `mssql.statement.encrypted_parameter_count` | Int64 | Encryption-metadata/server call when safely known; count only. |
| `mssql.statement.batch.sent_count` | Int64 | Batch root at end; entries whose requests were sent. |
| `mssql.statement.batch.completed_count` | Int64 | Batch root at end; entries for which a result/update status was processed. |
| `mssql.statement.batch.failed_count` | Int64 | Batch root at end; entries marked failed. Do not add one error event per item by default. |

Do not emit statement object IDs, connection process IDs, prepared-handle IDs, cursor IDs, parameter indexes/types/lengths, column names, table names, procedure names, result-column counts, or row values in the MVP. They add volume or disclosure without being necessary to explain execution latency and failure.

## 5. Phases and implementation boundaries

| Span | Parent | Exact boundary |
|---|---|---|
| `mssql.driver.statement.execute` | Current application context | One public/internal execution invocation through return or terminal throw. |
| `mssql.driver.statement.execute_batch` | Current application context | One batch execution invocation, not one root per item. |
| `mssql.driver.statement.attempt` | Execution root | One initial or retry execution pass. Configured backoff is inside the root but outside attempts. |
| `mssql.driver.statement.request_build` | Attempt | Existing request-build scope: SQL rewriting/classification, parameter definition/value serialization, enclave package writing, and TDS request construction until the request is ready to send. |
| `mssql.driver.statement.encryption_metadata` | Attempt or request build | Only when the driver performs parameter-encryption metadata discovery or enclave initialization. A server metadata query may have a nested server call. Do not emit key paths, values, provider details, or ciphertext. |
| `mssql.driver.statement.server_call` | Attempt | One actual direct-query, RPC, cursor, or bulk-copy call from send through required response processing for that call. `sp_prepare` and `sp_execute` are separate calls; `sp_prepexec` is one call. |
| `mssql.driver.statement.first_response` | Server call | Existing `startResponse()` scope exactly. Its interpretation depends on response buffering. |
| `mssql.driver.statement.result_process` | Server call | Optional bounded parsing required to identify/return the first result or update count after response acquisition. Excludes later application iteration. |

### Important exclusions

- Parameter setters happen before `execute*()` and are application calls; do not make one span/event per setter.
- `addBatch()` only snapshots work for later execution; do not emit execution telemetry there.
- `ResultSet.next()`, getters, cursor fetch/move/close, and stream consumption require a separate result-set proposal because their lifetime can extend well beyond `executeQuery()`.
- Prepared-handle unprepare/close is cache/resource maintenance, not part of the earlier execution span.
- `commit`, `rollback`, savepoints, and auto-commit transitions are transaction operations, not statement phases.
- Idle connection recovery is a connection operation. If it occurs while starting a statement, correlate it with native links or explicit operation parenting; do not pretend the statement itself performed login phases.

## 6. Retries, batches, cancellation, and partial success

### Configurable statement retry

A configured retry produces another `statement.attempt` under the same execution root:

```text
mssql.driver.statement.execute                  CLIENT   UNSET  2.2 s
├─ mssql.driver.statement.attempt               INTERNAL ERROR  80 ms
│  ├─ request_build
│  └─ server_call                               INTERNAL ERROR
│     └─ mssql.driver.error
├─ mssql.driver.statement.retry_decision        [event]
├─ retry backoff                                [no child span]
└─ mssql.driver.statement.attempt               INTERNAL UNSET  95 ms
   ├─ request_build
   └─ server_call                               INTERNAL UNSET
```

The successful second attempt leaves the root status `UNSET` and outcome `success`. The first attempt remains `ERROR`. Backoff is visible as the gap between attempts and as `mssql.retry.delay`; it is not request-build or server latency.

Invalid cached-handle and enclave retries use the same shape with distinct bounded reasons. Recursive implementation must still produce one root and monotonically numbered attempts.

### Batch behavior

One JDBC batch invocation has one root. A plain statement batch sends concatenated SQL; a prepared batch can group several parameter sets in a request and process several results. Do not create a child span for every batch entry: large batches would create unbounded telemetry.

A `BatchUpdateException` can represent partial completion. Therefore:

- root outcome is `failure` unless the JDBC invocation returns normally;
- `sent_count`, `completed_count`, and `failed_count` describe observed progress;
- one compact terminal error event represents the exception returned by the invocation;
- partial update-count arrays and SQL text are not exported;
- severe connection failure may leave later entries neither sent nor completed.

### Cancellation and timeout

| Condition | Outcome | Required evidence |
|---|---|---|
| Query timeout task terminates the command | `timeout` | Actual driver timeout code/task evidence, not SQLState text alone. |
| `Statement.cancel()` interruption confirmed | `canceled` | Command cancellation/interruption evidence. |
| Server returns a lock/deadlock/error before timeout | `failure` | Preserve actual server error even if a timeout was configured. |
| Socket read expires | `timeout` | Actual `SocketTimeoutException`/driver timeout evidence; phase normally `first_response`. |

Timeout and cancellation can leave connection cleanup work after the user-visible failure. End the statement root at the JDBC invocation boundary; do not extend it indefinitely to measure unrelated later cleanup.

## 7. Events and failures

Attach `mssql.driver.error` once to the lowest recorded phase that owns the observed terminal or retryable failure. Enclosing spans carry status/category/type summaries but no duplicate error event.

### Statement error categories

Error categorization is a first-class part of this proposal, not a generic `database_error` fallback. The connection proposal audited 123 grouped connection conditions. A corresponding source audit of the direct statement execution surface found **80 distinct JDBC resource keys** referenced by `SQLServerStatement`, `SQLServerPreparedStatement`, `SQLServerCallableStatement`, `Parameter`, `DTV`, and `TDSParser`, before adding arbitrary SQL Server error numbers, Java/JVM/OS failures, I/O failures, recovery failures, and external key-provider failures.

The 80-key count is an inventory boundary, not a claim that every key occurs inside an `execute*()` span. Some fail during construction, setters, `addBatch()`, callable output retrieval, or later `ResultSet` consumption. The mapping below explicitly says when an error is outside the execution-root lifetime. Do not stretch a statement execution span to absorb adjacent APIs merely to increase coverage.

#### Canonical taxonomy

Reuse connection categories where the same evidence means the same thing. Add statement-specific categories where collapsing everything into `database_error` would destroy the diagnostic value of the inventory.

| Category | Meaning |
|---|---|
| `query_syntax_semantics` | SQL Server or driver SQL parsing establishes invalid syntax, an invalid object/column reference, or unsupported SQL escape syntax. Do not parse localized message text to distinguish these. |
| `constraint_violation` | Confirmed SQL Server constraint/data-integrity rejection, such as duplicate key, nullability, check, or referential-integrity failure. Requires an actual server number/catalog mapping. |
| `authorization` | SQL Server explicitly denies statement/object permission or execution authorization. Authentication and connection policy remain connection categories. |
| `concurrency_conflict` | Confirmed deadlock victim, lock timeout, snapshot/update conflict, or equivalent server concurrency rejection. It is not automatically safe to replay a transaction. |
| `server_availability` | SQL Server/database/service is explicitly unavailable, paused, warming, failing over, or unable to process the request. |
| `resource_throttling` | SQL Server explicitly reports a service/resource/operation limit or busy condition. Client memory/handle exhaustion remains `client_resource_exhaustion`. |
| `database_error` | Confirmed SQL Server error that cannot be safely assigned to a narrower approved category. The server number in `error.type` preserves specificity. |
| `timeout` | Confirmed query, socket-read, or cancellation-timeout expiration. |
| `canceled` | Confirmed explicit cancellation/interruption. |
| `network_connectivity` | Transport closes/resets during request or response. |
| `connection_lifecycle` | Statement or connection is closed/invalid for execution. |
| `connection_recovery` | Confirmed session-recovery failure affecting execution. |
| `configuration` | Invalid statement option, cursor mode, response mode, generated-key mode, retry interval, or unsupported API combination detected locally. |
| `parameter_validation` | Missing, out-of-range, incompatible, unsupported, or unencodable prepared/callable parameter input. |
| `result_contract` | The selected JDBC API expects a result set/update count/output state that execution did not produce. |
| `batch_operation` | Batch-only structural or partial-completion failure where no narrower underlying category is established. A batch wrapper never replaces a known underlying cause. |
| `encryption_security` | Always Encrypted/enclave metadata, normalization, key, provider, or encryption operation fails with positive evidence. Authentication/network subcauses remain their specific category when established. |
| `data_conversion` | Client-side value conversion, range, encoding, stream-length, TVP, or SQL type conversion fails. |
| `protocol_error` | Invalid TDS token/response or protocol invariant. |
| `client_resource_exhaustion` | Confirmed client memory/thread/socket/handle exhaustion. |
| `internal_error` | Confirmed driver internal invariant failure. |
| `unknown` | Available evidence cannot establish a safer category. |

The statement and connection registries deliberately share `timeout`, `canceled`, `network_connectivity`, `server_availability`, `configuration`, `protocol_error`, `connection_lifecycle`, `connection_recovery`, `client_resource_exhaustion`, `internal_error`, and `unknown`. They deliberately differ where statement execution has additional evidence. `error.type` supplies stable detail, such as `sqlserver.1205`, `sqlserver.2627`, `jdbc:R_noResultset`, or an observed exception class.

#### Classification precedence

Apply the first rule supported by **source evidence**, not by localized message matching:

1. Confirmed cancellation or timeout mechanism.
2. Confirmed SQL Server `ERROR` token: classify by an approved server-number registry; otherwise `database_error`.
3. Literal JDBC resource key captured where the driver creates the error.
4. Driver internal code plus preserved exception/cause type for I/O, protocol, recovery, or runtime failures.
5. `unknown` when wrappers discarded the evidence or the condition is ambiguous.

The outer exception class does not win over stronger inner evidence. A `BatchUpdateException` preserves the mapped underlying item failure; an `SQLServerException` wrapping a socket reset remains `network_connectivity`; a server number is trusted only when it came from a parsed `SQLServerError`.

### Statement/prepared-statement error mapping

The table groups related resource keys only when they have the same telemetry meaning. The names are literal keys from the driver, not values reconstructed from translated message text. Source links point to the execution site; exact English templates are defined in [SQLServerResource](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResource.java).

| Error / evidence | Phase | Category | Meaning / qualification | Source |
|---|---|---|---|---|
| `R_statementIsClosed` | Invocation validation | `connection_lifecycle` | Statement handle was already closed. Capture only when the failing public execution API is covered by the root; the same key from unrelated setters/getters is outside that root. | [Statement guards](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1340-L1360), [prepared guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L550-L570) |
| `R_connectionIsClosed` | Invocation / server call | `connection_lifecycle` | Physical/logical connection is closed. It does not explain why it closed. | [Connection guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L410-L435) |
| `R_unsupportedCursor`, `R_unsupportedConcurrency`, `R_unsupportedCursorAndConcurrency` | Construction | `configuration` | Unsupported result-set type/concurrency selection. Normally outside execution root because construction fails first. | [Statement construction](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L710-L815) |
| `R_unsupportedStmtColEncSetting` | Construction | `configuration` | Null statement column-encryption setting; outside execution root. | [Statement construction](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L735-L750) |
| `R_invalidLength`, `R_invalidRowcount`, `R_invalidMaxRows` | Setter/configuration | `configuration` | Invalid max-field/max-row setting. Usually fails before `execute*()` and must not be attached later. | [Statement settings](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1365-L1460) |
| `R_invalidQueryTimeOutValue`, `R_invalidCancelQueryTimeout` | Setter/configuration | `configuration` | Invalid configured timeout value, not an expired timeout. Usually outside execution root. | [Query timeout](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1470-L1525) |
| `R_invalidFetchDirection`, `R_invalidFetchSize`, `R_invalidresponseBuffering` | Setter/configuration | `configuration` | Invalid fetch/buffering option. Do not label as server or protocol failure. | [Fetch settings](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2135-L2190), [buffering](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2895-L2920) |
| `R_invalidAutoGeneratedKeys`, `R_invalidColumnArrayLength` | Invocation validation | `configuration` | Invalid generated-key mode or malformed generated-key column array; no server call is required. | [Generated-key overloads](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2550-L2735) |
| `R_limitOffsetNotSupported`, `R_limitEscapeSyntaxError` | Request build | `query_syntax_semantics` | Driver JDBC escape rewrite rejects unsupported/invalid LIMIT syntax. This is client parsing, not a SQL Server error. | [Limit escape rewrite](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L3210-L3270) |
| `R_InvalidRetryInterval` | Retry decision | `configuration` | Configured retry wait exceeds query timeout. This is not the original server failure and not a timeout that already elapsed. | [Statement retry loop](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L265-L325) |
| `R_NullValue` for prepared SQL | Construction | `configuration` | Prepared SQL input is null. Outside execution root because preparation cannot be constructed. The same generic key elsewhere requires site-specific classification. | [Prepared construction](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L315-L340) |
| `R_cannotTakeArgumentsPreparedOrCallable` | Invocation validation | `configuration` | Application used a SQL-taking `Statement` overload on a prepared/callable statement. | [Prepared API guards](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L4145-L4210) |
| `R_invalidParameterLength`, `R_unsupportedTypeForDefineParamType`, `R_defineParameterTypeTypeMismatch`, `R_parameterTypeValueLengthExceedsHint` | Parameter configuration / request build | `parameter_validation` | Invalid length hint, unsupported declared type, setter/type-family mismatch, or value exceeding the approved hint. Never export values or lengths. | [Prepared type hints](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L220-L260), [parameter validation](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L130-L165), [length check](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L700-L735) |
| `R_valueNotSetForParameter` | Request build | `parameter_validation` | Required parameter has no value. The parameter number in the localized message is not exported. | [Prepared execution guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L575-L600) |
| `R_indexOutOfRange`, `R_invalidOutputParameter` | Parameter configuration / callable retrieval | `parameter_validation` | Parameter index is invalid. Callable retrieval after execution is outside the execution root unless a separate callable-result operation is instrumented. | [Prepared index guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L1560-L1580), [callable index guard](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerCallableStatement.java#L95-L125) |
| `R_outputParameterNotRegisteredForOutput`, `R_parameterNotDefinedForProcedure` | Callable result access | `parameter_validation` | OUT parameter was not registered or named parameter was not defined. Normally after execution; do not retroactively fail the execution span. | [Callable OUT validation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerCallableStatement.java#L430-L465), [named parameter lookup](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerCallableStatement.java#L1625-L1700) |
| `R_invalidOutputParameter`, `R_statementMustBeExecuted` | Callable/result access | `result_contract` | Output/result access is invalid or attempted before execution. Outside a completed execution root. | [Callable output access](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerCallableStatement.java#L430-L465), [statement result access](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2745-L2770) |
| `R_TVPnotWorkWithSetObjectResultSet`, `R_TVPInvalidValue`, `R_TVPEmptyMetadata` | Parameter configuration / request build | `parameter_validation` | Unsupported TVP API/value or missing TVP fields. Do not inspect or serialize TVP contents. | [TVP setter](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L1980-L2010), [parameter TVP checks](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L415-L445) |
| `R_errorReadingStream`, `R_mismatchedStreamLength` | Request build | `data_conversion` | Application stream read failed or produced a different length. Preserve an observed I/O cause type when safe; never export stream data or raw message. | [Parameter stream](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L1440-L1520), [DTV stream length](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L2280-L2310) |
| `R_valueOutOfRange`, `R_valueOutOfRangeSQLType`, `R_zoneOffsetError` | Request build / result process | `data_conversion` | Client-side value/range/temporal conversion fails. Phase distinguishes input serialization from first-result processing. | [DTV conversions](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L1080-L1130) |
| `R_unsupportedConversionFromTo`, `R_unsupportedConversionTo`, `R_errorConvertingValue`, `R_unsupportedEncoding` | Request build / result process | `data_conversion` | Unsupported or failed client-side type/encoding conversion. Do not expose source values or column names. | [DTV conversion](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L1935-L1965), [conversion failures](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L3560-L3605) |
| `R_invalidProbbytes`, `R_invalidDataTypeSupportForSQLVariant` | Result process | `protocol_error` | Invalid probability bytes or unsupported/invalid SQL_VARIANT type received from the protocol. If raised during later `ResultSet.next()`, it belongs to future result-set telemetry instead. | [DTV response decoding](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L4060-L4295) |
| `R_noResultset` | Result process | `result_contract` | `executeQuery()` produced no result set. This is a JDBC method/result mismatch, not necessarily invalid SQL. | [Statement result check](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1125-L1140), [prepared result check](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L855-L875) |
| `R_resultsetGeneratedForUpdate` | Result process | `result_contract` | `executeUpdate()`/batch produced a result set. Keep an underlying earlier server error if one exists. | [Statement result check](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1135-L1150), [prepared result check](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L860-L875) |
| `R_updateCountOutofRange` | Result process | `result_contract` | Result update count cannot be represented by the selected non-large JDBC API. The server execution may already have succeeded. | [Statement update result](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L915-L940), [prepared update result](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L650-L675) |
| `R_outParamsNotPermittedinBatch` | Invocation validation | `batch_operation` | Callable OUT/INOUT parameters make this JDBC batch invalid. No item should be sent. | [Prepared batch validation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L2600-L2630) |
| `R_selectNotPermittedinBatch` | Request build | `batch_operation` | Prepared batch contains a SELECT where the implementation requires update counts. | [Prepared batch](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L3430-L3460) |
| `R_colNotMatchTable`, `R_BulkTypeNotSupported`, `R_BulkTypeNotSupportedDW` | Bulk-copy batch optimization | `batch_operation` | Bulk-copy optimization metadata/type is incompatible. If the implementation intentionally falls back, emit no failed execution; only terminal surfaced failures count. | [Bulk batch metadata](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L2635-L2670), [bulk type checks](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L2980-L3050) |
| `R_invalidSQL`, `R_endOfQueryDetected`, `R_onlyFullParamAllowed` | Bulk-copy batch parsing | `batch_operation` | Driver cannot parse the INSERT into the restricted bulk-copy optimization form. A successful fallback is not an execution failure. Raw SQL embedded by these templates is prohibited from telemetry. | [Bulk batch parser](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L3040-L3405) |
| `BatchUpdateException` with known item cause | Result process | Underlying mapped category | Wrapper describes batch API/partial counts; never replace a known `sqlserver:*`, timeout, cancel, conversion, or transport cause with `batch_operation`. | [Plain batch](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2250-L2320), [prepared batch](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L2570-L2790) |
| `R_UnexpectedDescribeParamFormat`, `R_InvalidEncryptionKeyOrdinal`, `R_MissingParamEncryptionMetadata` | Encryption metadata | `protocol_error` or `internal_error` | Invalid/missing `sp_describe_parameter_encryption` result. Use `protocol_error` for malformed server metadata; `internal_error` only for a proven driver invariant. Templates can contain SQL/procedure/ordinal data and must not be exported raw. | [Parameter encryption metadata](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L1200-L1290) |
| `R_UnableRetrieveParameterMetadata`, `R_metaDataErrorForParameter` | Encryption metadata | Underlying category or `encryption_security` | Broad wrapper. Preserve a known server, network, timeout, provider, or protocol cause; otherwise use `encryption_security`. | [Encryption metadata](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L1270-L1290), [parameter metadata](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L2510-L2540) |
| `R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumn`, `R_ForceEncryptionTrue_HonorAEFalse` | Request build | `encryption_security` | Force-encryption policy conflicts with server metadata or disabled encryption. Omit statement/procedure and parameter identifiers from telemetry. | [Prepared metadata policy](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L1240-L1280), [parameter policy](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L380-L420) |
| `R_InvalidDataForAE`, `R_UnsupportedDataTypeAE`, `R_unsupportedConversionAE`, `R_StreamingDataTypeAE` | Request build | `encryption_security` | Input cannot be converted/encrypted under Always Encrypted restrictions. Do not export types if the final approved schema excludes them. | [AE parameter validation](../src/main/java/com/microsoft/sqlserver/jdbc/Parameter.java#L375-L420), [AE DTV conversion](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L1390-L1500) |
| `R_UnsupportedNormalizationVersionAE`, `R_NormalizationErrorAE` | Result process / request build | `encryption_security` | Invalid normalization version or normalization/decryption failure. Preserve malformed-protocol evidence separately when established. | [AE normalization](../src/main/java/com/microsoft/sqlserver/jdbc/DTV.java#L3530-L3770) |
| `R_AE_NotSupportedByServer` | Request build / server call | `configuration` | Column encryption was requested against a server that does not support it. This is not evidence of a TLS/security attack. | [AE server capability](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L7810-L7840) |
| Statement key-store provider map/name/value errors and `R_UnrecognizedStatementKeyStoreProviderName` | Configuration / encryption metadata | `encryption_security` | Invalid provider registration or lookup. Provider names and key paths are not exported. | [Statement provider registration](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2945-L3055) |
| `R_invalidTDS`, `R_unexpectedToken` | First response / result process | `protocol_error` | TDS token stream is invalid or unexpected. Capture at parser site before a generic wrapper obscures it. | [Connection token parsing](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5095-L5125), [TDS parser](../src/main/java/com/microsoft/sqlserver/jdbc/TDSParser.java) |
| `R_noServerResponse`, `R_truncatedServerResponse` during an established statement | First response | `network_connectivity` | EOF before or during a response. Not automatically a timeout or server crash. The connection is closed. | [Packet reader](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java#L7140-L7230) |
| `SocketException`, connection reset/abort/broken pipe with retained cause | Request send / first response | `network_connectivity` | Runtime/OS-dependent transport failure. Do not infer firewall, service outage, or retry safety from message text. | [TDS I/O](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java) |
| `SocketTimeoutException` / driver `SOCKET_TIMEOUT` code during statement response | First response | `timeout` | Socket-read expiration. Distinct from query-timeout cancellation and from connection login timeout. | [TDS I/O](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java), [driver codes](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java) |
| `R_queryTimedOut` / driver `QUERY_TIMEOUT` code | Server call / first response | `timeout` | Statement query deadline expired. Root outcome is timeout even when cancellation is used internally to enforce it. | [Exception construction](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java#L185-L210), [statement translation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L320-L350) |
| `R_queryCanceled` / SQLState `HY008` with confirmed explicit cancel | Owning phase | `canceled` | Explicit `Statement.cancel()`/command interruption. Do not classify every `HY008` as user cancellation when timeout enforcement produced it. | [Statement cancellation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L1525-L1545) |
| `R_crClientUnrecoverable`, `R_crServerSessionStateNotRecoverable`, `R_crClientNoRecoveryAckFromLogin` surfaced during execution | Server call | `connection_recovery` | Recovery failed or was vetoed. Preserve a more specific nested network/TLS/login failure when available. Never imply the failed statement was replayed. | [Recovery guards](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L5200-L5240), [recovery acknowledgement](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L8235-L8265) |
| Invalid cached prepared handle recognized by the driver's dedicated retry path | Server call | `connection_lifecycle` | Stale handle is retried internally. Retained first attempt is error; successful retry leaves root successful. Do not export the handle or classify all server prepare errors as stale handles. | [Prepared retry](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L780-L850) |
| Enclave invalid-session error `33195` in the dedicated retry path | Server call | `encryption_security` | Invalidates enclave session cache and may retry once. `retry_scheduled` is a decision, not a claim that the original operation was idempotent outside this driver path. | [Prepared enclave retry](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L825-L850) |
| JVM heap/direct-buffer allocation failure | Any active phase | `client_resource_exhaustion` | Require explicit allocation failure. It does not prove a driver leak, and telemetry export may itself fail. | [Statement/request allocation paths](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java), [I/O buffers](../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java) |
| Unrecognized driver/runtime/provider failure | Any | `unknown` | Preserve native IDs, phase, observed exception class, SQLState/vendor number when approved; never invent a resource key or parse localized text. | [Exception handling](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerException.java) |

#### SQL Server error-number registry

SQL Server can return thousands of statement errors, so the driver cannot maintain a row for every possible message. The registry maps stable **numbers and source context**, while unknown numbers safely retain `error.type=sqlserver.<number>` and category `database_error`. The initial registry should include at least the following high-value families and be validated against the supported SQL Server/Azure SQL error catalog before implementation:

| Confirmed server evidence | Category | Qualification |
|---|---|---|
| Syntax/semantic numbers such as 102, 156, 207, 208, 2812 | `query_syntax_semantics` | Examples cover syntax, invalid column/object, and missing stored procedure. Use only exact catalog-confirmed mappings; never infer from text. |
| Integrity numbers such as 2601, 2627, 515, 547 | `constraint_violation` | Duplicate key, nullability, and constraint conflicts. Does not identify the sensitive key/value. |
| Permission numbers such as 229 and 230 | `authorization` | Explicit object/statement permission denial. Do not expose principal/object names from the message. |
| 1205, 1222, and catalog-confirmed update-conflict numbers | `concurrency_conflict` | Deadlock victim, lock timeout, or update conflict. `retryable` is a separate policy decision; transaction replay may be unsafe. |
| 40197, 40143, 40166, 40540, 40020, 40613, 4221, 42108, 42109 | `server_availability` | Service/database/replica/pool availability family already inventoried by the driver. Exact semantics remain number-specific. |
| 40501, 10928, 10929, 49918, 49919, 49920 | `resource_throttling` | Explicit busy/resource/operation-limit family. Honor server retry guidance separately; do not put incident IDs in telemetry. |
| 10053, 10054, 64 from an actual parsed server error | `network_connectivity` | Transport family. A Java socket exception with a similar OS number is not automatically a parsed SQL Server error. |
| 4060 | `database_error` unless clarified | Cannot-open-database text can represent name/access/availability; transient-list membership is not causal proof. |
| 233 | `unknown` unless clarified | Driver comments list transport, version, load, and resource possibilities; do not force one category. |
| Any other parsed SQL Server error | `database_error` | Keep `sqlserver.<number>`, SQLState, state, and severity as approved structured evidence. |

The driver's `SQLServerError.TransientError` list is a **retry input**, not a category oracle. Membership can inform a retry decision, but does not itself prove availability, idempotence, or safe transaction replay. Configurable statement retry can also name arbitrary server numbers and query constraints; those user rules must never become category definitions or exported SQL text.

#### Evidence capture and stable codes

- `mssql.error.code=sqlserver:<number>` only for an actual parsed `SQLServerError`.
- `mssql.error.code=jdbc:<resource-key>` only when the literal resource key is captured at the construction site; never reverse-map localized messages.
- `error.type=sqlserver.<number>` for confirmed server errors; otherwise use the observed exception class or a bounded driver identifier.
- SQLState, JDBC vendor code, internal driver code, server state, and server severity are separate fields. Do not treat one as another.
- Preserve the original failing phase before an outer `BatchUpdateException`, `SQLTimeoutException`, `SQLServerException`, or recovery wrapper obscures it.
- Capture causes with a bounded, non-lazy source projection. Do not traverse `SQLException.getNextException()` or stream/result objects on the telemetry callback path.

#### Coverage outside the execution root

The audited keys also include failures from APIs adjacent to execution. Their category mapping is still useful, but their telemetry owner differs:

| Adjacent API | Examples | Correct owner |
|---|---|---|
| Statement/prepared construction | Unsupported cursor/concurrency, null prepared SQL | No execution span; optional future configuration diagnostic. |
| Statement setters | Timeout, fetch, max-row, response-buffering validation | No execution span; fail synchronously on the setter. |
| Parameter setters / `addBatch()` | Index/type/TVP/value validation | No execution span if failure occurs before invocation; future API diagnostic only. |
| Callable OUT getters | Output not registered, parameter undefined, statement not executed | Separate callable-result access, not the ended execution root. |
| Later `ResultSet.next()` and getters | Closed result set, conversion, invalid column, stream/decryption failures | Future result-set/fetch proposal. |
| Prepared-handle close/unprepare | Cleanup RPC or cache maintenance failure | Separate maintenance operation if ever instrumented; never append to an ended execution. |

### Compact error event

| Attribute | Type | Requirement |
|---|---|---|
| `mssql.error.category` | String | Required. |
| `mssql.error.phase` | String | Required; original lifecycle phase. |
| `mssql.error.source` | String | Required: `driver`, `sql_server`, `jvm`, `os`, `key_provider`, `callback`, or `unknown`. |
| `mssql.error.code` | String | Optional namespaced actual code, for example `sqlserver:1205` or `jdbc:R_noResultset`. |
| `mssql.error.retry_decision` | String | Optional actual decision: `retry_scheduled`, `not_retryable`, `limit_reached`, `timeout`, or `canceled`. |
| `exception.type` | String | Optional observed class at the source. |
| `mssql.error.message` | String | Optional approved normalized diagnostic, maximum 1,024 UTF-8 bytes. Raw SQL Server/JDBC exception text is not safe by default. |
| `mssql.error.sql_state` | String | Optional actual nonempty SQLState. |
| `mssql.error.server_state` | Int64 | Optional parsed SQL Server state. |
| `mssql.error.server_severity` | Int64 | Optional parsed SQL Server severity. |
| `mssql.error.driver_code` | Int64 | Optional actual internal driver code. |

### Decision events

| Event | Placement | Allowed attributes |
|---|---|---|
| `mssql.driver.statement.retry_decision` | Execution root | Originating `mssql.statement.attempt`, next `mssql.retry.attempt`, bounded `mssql.retry.reason`, `mssql.retry.delay` in seconds, and `error.type`. |
| `mssql.driver.timeout` | Failing phase | `mssql.timeout.phase`, `mssql.timeout.kind` (`query`, `socket_read`, `cancel_ack`, or `unknown`), configured `mssql.timeout.value` in seconds when known, and attempt index. |
| `mssql.driver.statement.partial_batch` | Batch root | `sent_count`, `completed_count`, and `failed_count`; emit only when partial completion is observed. |

No event contains SQL text, parameter values, update-count arrays, database objects, endpoint identity, transaction contents, or retry-query configuration text.

## 8. Privacy and cardinality

### Masked statement text contract

The optional adapter may export **masked** SQL as `db.query.text` on the statement execution root. Raw SQL is never placed in `PerformanceLogEvent`, a queue, an asynchronous worker, logs, events, child spans, or metrics.

1. `PerformanceLog` exposes `getCurrentUserSql()` only during the synchronous callback invocation, including statement lifecycle publication.
2. The adapter calls it only on statement-root START and immediately applies bounded lexical masking on the JDBC thread.
3. Line and block comments are removed because they can contain arbitrary customer text.
4. SQL string/Unicode-string, numeric, decimal, exponent, and hexadecimal literals become `?`; escaped quote pairs are consumed without copying their contents.
5. Quoted and bracketed identifiers are retained to preserve query shape. They may still be customer-sensitive object names; deployments needing stronger minimization must suppress this attribute or apply a stricter collector policy.
6. Malformed comments/quotes, oversized input/output, or uncertain lexical state fail closed by omitting the attribute.
7. Masking is data minimization, not a guarantee of de-identification. Never put credentials or regulated data in identifiers or comments, and never use this attribute as an authorization/audit record.

### Never export

- Raw SQL text, literal values, SQL comments, or unbounded query text. Only the masked `db.query.text` contract above is permitted.
- Parameter names, indexes, JDBC types, lengths, values, encrypted values, TVP contents, streams, or batch value arrays.
- Database/user/schema names, connection strings, host names, addresses, URLs, key paths, enclave data, certificates, tokens, or prepared/cursor handles.
- Raw exception messages, `SQLException` chains, stack traces, arbitrary callback metadata, application baggage, or JUL log text.
- Raw `getCurrentUserSql()` output outside the synchronous sanitizer. It must never be retained or passed to asynchronous processing.

### Safe low-cardinality dimensions

Statement type, JDBC API, bounded operation class, protocol kind, protocol operation, response-buffering mode, attempt reason, outcome, and error category are suitable only from the enumerations in this proposal. Unknown future values must not be copied from arbitrary user text.

## 9. Export, sampling, and metrics

Use the same nonblocking architecture as connection telemetry:

```text
JDBC thread: capture boundary → synchronously mask callback SQL → try enqueue safe snapshot → return
                                                    │
                                           bounded event queue
                                                    │
worker: validate, assemble lifecycle tree, apply sampling/privacy
                                                    │
worker: construct OTel spans/events → application-owned SDK
```

- No SDK/exporter call, semantic SQL parsing, cause-chain walk, queue wait, force flush, token acquisition, or network I/O on the JDBC execution path. The bounded lexical masker is the deliberate exception and must be benchmarked.
- The callback snapshot contains approved scalar metadata and source-captured classification only. Remove exception references before asynchronous admission.
- Respect the application sampler. Unlike the failed-connection diagnostic MVP, statement spans are high volume and cannot promise complete failure retention after a non-recording head-sampling decision.
- Provisional limits per invocation: 32 spans and 64 events. Reserve the root, terminal attempt, failing ancestor chain, and terminal error event. Set root truncation/drop counts if limits are exceeded.
- Preserve existing performance callback metrics. Do not create a second synchronous metric path in the driver.

A future OTel metric bridge may map existing activities to histograms, but it must avoid double counting nested durations:

| Candidate instrument | Source | Suggested low-cardinality attributes |
|---|---|---|
| `mssql.driver.statement.execution.duration` | Execution root | `statement.type`, `statement.api`, `statement.outcome` |
| `mssql.driver.statement.request_build.duration` | Existing request-build activity | `statement.type` |
| `mssql.driver.statement.server_call.duration` | Existing prepare/prepexec/execute activities | `protocol_operation`, `statement.outcome` |
| `mssql.driver.statement.first_response.duration` | Existing first-response activity | `response_buffering` |

Do not sum these histograms to derive end-to-end latency. Do not use connection GUID, statement ID, error code, SQLState, SQL operation text, batch size, or prepared-handle source as metric labels.

## 10. Worked examples

### Prepared execution succeeds through `sp_prepexec`

```json
{
  "spans": [
    {
      "span_id": "4444444444444401",
      "name": "mssql.driver.statement.execute",
      "kind": "CLIENT",
      "status": "UNSET",
      "attributes": {
        "mssql.telemetry.schema.version": "1.0",
        "db.system.name": "microsoft.sql_server",
        "mssql.connection.guid": "44444444-4444-4444-8444-444444444444",
        "mssql.statement.type": "prepared_statement",
        "mssql.statement.api": "execute_query",
        "mssql.statement.operation": "select",
        "mssql.statement.outcome": "success",
        "mssql.statement.attempt_count": 1,
        "mssql.statement.retry_count": 0,
        "mssql.statement.parameter_count": 2,
        "mssql.statement.query_timeout": 30.0,
        "mssql.statement.response_buffering": "adaptive",
        "mssql.statement.result_kind": "result_set"
      }
    },
    {
      "span_id": "4444444444444402",
      "parent_span_id": "4444444444444401",
      "name": "mssql.driver.statement.attempt",
      "kind": "INTERNAL",
      "status": "UNSET",
      "attributes": {
        "mssql.statement.attempt": 1,
        "mssql.statement.attempt_reason": "initial",
        "mssql.statement.attempt_outcome": "success"
      }
    },
    {
      "span_id": "4444444444444403",
      "parent_span_id": "4444444444444402",
      "name": "mssql.driver.statement.request_build",
      "kind": "INTERNAL",
      "status": "UNSET",
      "attributes": { "mssql.statement.attempt": 1 }
    },
    {
      "span_id": "4444444444444404",
      "parent_span_id": "4444444444444402",
      "name": "mssql.driver.statement.server_call",
      "kind": "INTERNAL",
      "status": "UNSET",
      "attributes": {
        "mssql.statement.attempt": 1,
        "mssql.statement.protocol": "rpc",
        "mssql.statement.protocol_operation": "sp_prepexec",
        "mssql.statement.prepared_handle_source": "new"
      }
    },
    {
      "span_id": "4444444444444405",
      "parent_span_id": "4444444444444404",
      "name": "mssql.driver.statement.first_response",
      "kind": "INTERNAL",
      "status": "UNSET",
      "attributes": {
        "mssql.statement.attempt": 1,
        "mssql.statement.response_buffering": "adaptive"
      }
    }
  ]
}
```

The envelope is synthetic decoded JSON, not literal OTLP. Literal and parameter values are absent. A production root may additionally carry bounded `db.query.text`, for example `SELECT status FROM orders WHERE customer_id = ? AND created_at >= ?`. `server_call` corresponds to the existing `STATEMENT_PREPEXEC` timing; `first_response` is nested and already included in that duration.

### Direct statement times out waiting for response

```text
mssql.driver.statement.execute                  CLIENT   ERROR  30.0 s
└─ mssql.driver.statement.attempt               INTERNAL ERROR 30.0 s
   ├─ mssql.driver.statement.request_build      INTERNAL UNSET  1 ms
   └─ mssql.driver.statement.server_call        INTERNAL ERROR 30.0 s
      └─ mssql.driver.statement.first_response  INTERNAL ERROR 30.0 s
         ├─ mssql.driver.timeout [event]
         └─ mssql.driver.error   [event]
```

Root attributes include outcome `timeout`, failure phase `first_response`, category `timeout`, and the stable observed error type. The root, attempt, and server call have no duplicate error events.

### Prepared execution retries an invalid cached handle

The first attempt fails during `sp_execute`; the driver invalidates/replaces the handle and retries. A second successful attempt keeps the overall root successful. The retry event states `invalid_handle`; it does not expose the handle value and does not imply arbitrary statement replay is safe outside this exact driver-controlled path.

## 11. Implementation plan

### Driver lifecycle state

Add a bounded `StatementPerformanceState`, parallel in design to `ConnectionPerformanceState`, keyed by one execution invocation rather than statement-object lifetime. It owns:

- root and attempt scope IDs;
- captured caller `SpanContext` through the adapter boundary, not core OTel dependencies;
- immutable approved root metadata;
- attempt numbering and reason;
- observed protocol operation;
- bounded decision events;
- source-captured error classification;
- truncation/drop counts.

### Integration points

| Source location | Proposed change |
|---|---|
| `SQLServerStatement.executeStatement()` | Open one invocation root before previous-response cleanup, create an attempt for each configurable-retry loop pass, close the root on final return/throw, and record backoff as a decision event/gap. Public batch paths that can bypass this method, especially prepared batch-insert bulk copy, require an equivalent outer root. |
| `startCreationToFirstPacketTracking()` / `endCreationToFirstPacketTracking()` | Publish request-build START/END lifecycle boundaries while retaining the existing legacy timing callback. |
| `doExecuteStatement()` and cursor/batch variants | Set observed protocol operation and place server-call lifecycle around the existing `STATEMENT_EXECUTE` scope. |
| `SQLServerPreparedStatement.doExecutePreparedStatement()` | Record `sp_executesql`, `sp_prepexec`, `sp_execute`, or direct execution from actual `doPrepExec()` control flow; represent invalid-handle/enclave retries as attempts. |
| `SQLServerPreparedStatement.doPrep()` | Publish an `sp_prepare` server call and nest the corresponding first-response phase; preserve `STATEMENT_PREPARE`. |
| `startFirstPacketToFirstResponseTracking()` / end helper | Publish nested first-response lifecycle boundaries with effective buffering mode. |
| Prepared and plain batch implementations | One bounded batch root with aggregate progress, not per-item spans. |
| Failure source sites | Capture actual server number/resource key/driver code and phase before wrappers lose evidence. Never reconstruct classifications from localized text. |
| `PerformanceLogEvent` | Generalize connection-only wording; add `statementId` and stable statement phase without exposing SQL. |
| `PerformanceLog.Scope` | Allow lifecycle snapshots for statement scopes while keeping legacy callback delivery unchanged. |
| Optional OTel callback | Call `getCurrentUserSql()` synchronously at statement-root START, mask literals/comments with bounded fail-closed logic, and enqueue only the masked `db.query.text`. |

### Phased delivery

1. Root invocation, attempt, request-build, server-call, and first-response lifecycle.
2. Accurate protocol-operation tagging across direct, prepared, cursor, and batch paths.
3. Structured statement error classification and retry/timeout events.
4. Optional encryption-metadata and result-process phases after boundary validation.
5. Separate future proposals for result-set fetch, transactions, bulk-copy internals, and callable OUT-parameter retrieval.

## 12. Acceptance checklist

- Preserve every existing statement `PerformanceActivity`, callback signature, duration unit, and legacy SQL/thread-local behavior.
- Maintain a machine-testable registry for all 80 directly audited statement/prepared/callable resource keys, with explicit phase, category, source, and execution-root ownership. Adding a new execution-path resource key requires either a mapping or an intentional `unknown` review entry.
- Test every canonical statement category with source-created evidence; verify literal `jdbc:<resource-key>` capture and reject reverse lookup from localized text.
- Test the initial SQL Server number registry, unknown server-number fallback, server state/severity/SQLState separation, and the rule that transient-list or configurable-retry membership does not determine category or replay safety.
- Test outer wrappers (`BatchUpdateException`, `SQLTimeoutException`, recovery wrappers) without losing the underlying category, phase, server number/resource key, or partial-batch progress.
- Assert that construction, setter, parameter-binding-before-execute, callable-output-access, later result-set, and unprepare failures are not attached retroactively to an execution root.
- Assert lifecycle nesting: root → attempt → request build/server call → first response. No overlapping server call and first response may be represented as additive peers.
- Verify default prepared lifecycle: first `sp_executesql`, later `sp_prepexec`, then cached `sp_execute`; verify `prepareMethod=prepare`, `none`, temp-table direct mode, cursor, plain batch, prepared batch, and bulk-copy optimization.
- Verify `sp_prepexec` remains one indivisible server call and `sp_prepare` + `sp_execute` remain two calls.
- Verify one root across configurable retry, invalid-handle retry, and enclave retry, with separate attempts and backoff excluded from request/server phases.
- Verify `executeQuery()` ends before later `ResultSet.next()` work and does not claim full row-consumption latency.
- Verify batch partial completion without per-item span explosion or update-count-array export.
- Verify timeout versus explicit cancel versus server database error, with one error event at the originating phase and no duplicate root event.
- Reject raw SQL, literals, comments, parameter data, endpoints, raw exception messages, prepared/cursor handles, and unmasked `getCurrentUserSql()` from exported lifecycle snapshots. Verify masked `db.query.text` preserves shape but none of the fixture literal/comment values.
- Exercise escaped/Unicode strings, decimals, exponents, hex literals, nested block comments, line comments, quoted/bracketed identifiers, malformed quotes/comments, and input/output limits; malformed or oversized input must omit the attribute.
- Validate nonblocking admission, bounded queues/trees, callback failure isolation, application sampler authority, and no exporter work on JDBC threads.
- Measure enabled/disabled overhead for plain, prepared cached, prepexec, batch, adaptive-buffered, and full-buffered executions before enabling by default.
