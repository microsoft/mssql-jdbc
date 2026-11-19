# User-owned OpenTelemetry endpoint

## Related design specifications and implementation status

The following three documents are the **authoritative design specifications**.
Read this current automatic-route implementation contract alongside them:

| Design document | Coverage |
| --- | --- |
| [Connection error categories and span contract](mssql-jdbc-connection-error-category-mapping.md) | Source-grounded connection error taxonomy, connection lifecycle spans, error evidence, and proposed extensions. |
| [Performance activity metrics proposal](mssql-jdbc-performance-activity-metrics-proposal.md) | Metric instruments, bounded dimensions, activity identities, retry accounting, histogram boundaries, SDK ownership, and acceptance criteria. |
| [Statement spans proposal](mssql-jdbc-statement-spans-proposal.md) | Invocation/attempt/phase spans, statement attributes, sampling, sanitized SQL policy, and diagnostic events. |

The automatic route now shares the four-instrument metric projection, honors an
unsampled parent, exports approved root-only masked SQL, and replays allowlisted
diagnostics. This describes the current in-progress source, not a released-version
guarantee or a claim that every proposal acceptance criterion has passed. The older
programmatic adapter remains a separate configuration path.

### Reconciled gaps and remaining design differences

| Area | Design specification | Current automatic route / remaining gap |
| --- | --- | --- |
| Metric instruments | Metrics proposal §3.1 requires count, duration, error count, and retry count. | Implemented through shared `PerformanceActivityMetrics`; retries count additional attempts actually begun, on roots only. |
| Metric dimensions and identities | Metrics proposal §3.2 and §4 specify stable dotted activity names and bounded dimensions. | Implemented: `mssql.performance.activity`, operation kind/outcome, statement type/protocol, connection authentication method, retry reason, and schema version, with instrument-specific applicability below. |
| Histogram boundaries | Metrics proposal §5.3 specifies 17 explicit boundaries from 0.1 ms through 30,000 ms. | Implemented as explicit histogram boundary advice in seconds, not SDK default buckets. |
| Error accounting | Metrics proposal §3.1 and §5.5 define terminal `failure`, `timeout`, and `canceled` outcomes. | The shared projection resolves bounded terminal outcomes and increments error count for every non-success outcome; count/duration retain the outcome dimension. Exhaustive path validation remains separate. |
| SDK/configuration ownership | Metrics proposal §3, §6, and §7 describe an application-supplied SDK, opt-in metric configuration, and application-controlled resources. | Intentional automatic-route difference: `otelEndpoint` owns transport selection and enables metrics in a private SDK with fixed `service.name=mssql-jdbc` and a 60-second reader. Scope is now `com.microsoft.sqlserver.jdbc` with driver version. Application/global exporter settings do not configure this route. |
| Statement sampling | Statement proposal keeps the application sampler authoritative. | Parent sampling is honored with `parentBased(alwaysOn())` and whole-tree suppression for a valid unsampled parent; metrics remain eligible. The private SDK does not invoke an application's custom sampler: absent a valid parent, it samples eligible new roots. |
| Statement SQL metadata | Statement proposal permits conditional root-only masked `db.query.text`. | Implemented through the bounded lexical sanitizer using callback-scoped SQL at root END. The proposal's implementation sequence mentions START; the current callback availability/ownership boundary is END. Raw SQL is never queued. |
| Diagnostic events | Connection/statement designs describe structured retry, timeout, and authentication diagnostics in addition to originating errors. | Implemented root diagnostic replay with independent name/value allowlists, timestamps within the root, and a 128-event replay bound; origin errors remain on their originating spans. Only actually captured approved events are exported. |
| Connection trace population and extensions | Connection design describes a failed-open diagnostic MVP and proposed additional lifecycle work, including logical pool acquisition/reset. | Levels and thresholds select automatic-route trees, including successful roots at level 2. Logical pool checkout/reset and session-recovery reconnect trees are not implemented by this route. |
| Dashboard/acceptance coverage | Metrics proposal §8 and §11 include outcome distribution, maximum duration, and broader cardinality/outcome validation. | Live/Grafana dashboards query outcome counts, root retries, per-activity means and percentiles. Exact maximum remains unavailable through classic Prometheus histograms. Source alignment and demo checks do not establish exhaustive acceptance, high-cardinality stress, or deployment parity. |

Source evidence: [automatic SDK setup and span replay](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java),
[shared metric projection](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/PerformanceActivityMetrics.java),
[core END metadata enrichment](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceLog.java),
and [live dashboard projection](otel-poc/internal/fdh-ui/src/live.ts).
The sections below describe the current automatic route without changing the specifications.

## Scope and activation

This document describes the current automatic, per-physical-connection user telemetry route, not a released-version availability guarantee. It sends OTLP/HTTP protobuf metrics and optional connection/statement span trees to an application-selected collector. It does not send OpenTelemetry log records.

- Put the matching core driver, optional `com.microsoft.sqlserver:mssql-jdbc-otel` JAR, and its runtime OpenTelemetry dependencies on the application's runtime classpath. The optional module currently declares version `13.6.0`, OpenTelemetry `1.41.0`, and Java 8 source/target; confirm packaging and release availability before publishing installation instructions.
- Set `otelEndpoint` on the connection URL, connection `Properties`, or concrete `SQLServerDataSource`. The driver discovers `UserOtelTelemetryProvider` through `ServiceLoader`; the optional JAR supplies `UserEndpointTelemetryProvider`. No manual performance-callback registration or SDK construction is required.
- The core driver has no OpenTelemetry SDK dependency. With no user options, it bypasses user-option allocation and provider discovery. A callback or level alone does not enable export.
- Explicit malformed user options fail connection setup with the sanitized message `Invalid user telemetry configuration.`, even without an endpoint. Missing/broken optional providers and subsequent instrumentation/transport failures are fail-open for SQL operations: successful SQL setup does not prove telemetry is running.

Sources: [core validation and discovery](../src/main/java/com/microsoft/sqlserver/jdbc/UserOtelConfiguration.java), [provider contract](../src/main/java/com/microsoft/sqlserver/jdbc/UserOtelTelemetryProvider.java), [SPI registration](../mssql-jdbc-otel/src/main/resources/META-INF/services/com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider), [optional module dependencies](../mssql-jdbc-otel/pom.xml).

## Exact connection options

| Option | Default | Accepted value and effect |
| --- | --- | --- |
| `otelEndpoint` | Unset: automatic export disabled | Absolute HTTP or HTTPS collector URI with a host; no URI user-info/credentials, query, or fragment. Port `0` and ports above `65535` are rejected. |
| `otelAccessTokenCallback` | Unset: no Authorization header | Optional fully qualified class name implementing `SQLServerAccessTokenCallback`, with a public no-argument constructor. This is a class-name string, not an instance or token. It applies only to telemetry. |
| `otelLogLevel` | Numeric `0` | Integer `0`, `1`, or `2`; see below. The descriptive name INFO means **numeric `0`**, not the literal string `INFO`. Named logging levels are not accepted. This does not change JUL logging. |
| `otelConnectionSpanDurationThreshold` | Absent: no duration filter | Finite nonnegative decimal **seconds**, supplied as a string, applied to the connection root. Fractional and scientific-notation values are supported. |
| `otelStatementSpanDurationThreshold` | Absent: no duration filter | Same format, applied to the statement invocation root. |

Thresholds must not exceed `9223372036.854775807` seconds (signed-long maximum nanoseconds). Decimal precision is retained through comparison, including sub-nanosecond fractions; it is not truncated to whole seconds or rounded to a nanosecond first. Negative, NaN, infinity, and out-of-range values are rejected.

The concrete `SQLServerDataSource` provides the following getter/setter pairs:

| Setter | Getter |
| --- | --- |
| `setOtelEndpoint(String)` | `getOtelEndpoint()` |
| `setOtelAccessTokenCallback(String)` | `getOtelAccessTokenCallback()` |
| `setOtelLogLevel(int)` | `getOtelLogLevel()` |
| `setOtelConnectionSpanDurationThreshold(String)` | `getOtelConnectionSpanDurationThreshold()` |
| `setOtelStatementSpanDurationThreshold(String)` | `getOtelStatementSpanDurationThreshold()` |

String getters return `null` when unset; the level getter returns `0`. Setters store values; validation occurs when connecting. Passing `null` to a string setter leaves its current value unchanged; it does not clear an existing setting.

These are the five automatic-route options. Do not substitute `otelAccessTokenCallbackClass` or assume that programmatic/demo properties such as `otelHeaders`, `otelBearerToken`, `otelTokenScope`, `otelTokenAuthority`, `otelJdbcMetricsEnabled`, or insecure-endpoint flags configure this route. Only the five allowlisted options reach the SPI; SQL credentials do not.

Sources: [driver option declarations](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDriver.java), [DataSource API](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerDataSource.java), [validation tests](../src/test/java/com/microsoft/sqlserver/jdbc/UserOtelPropertiesTest.java).

### Endpoint paths and transport security

The adapter normalizes a base URI to `<base>/v1/traces` and `<base>/v1/metrics`. A terminal `/v1/traces` or `/v1/metrics` is replaced as appropriate; trailing slashes are removed and escaped path components are preserved. For example, `https://collector.example/ingest/v1/metrics` selects `https://collector.example/ingest/v1/traces` and `https://collector.example/ingest/v1/metrics`. This is not a gRPC endpoint or a pair of independently configurable signal URLs.

**Current implementation caveat:** the automatic route accepts HTTP, including `http://127.0.0.1:4318`, without an extra opt-in flag. HTTP acceptance is **not restricted to loopback**; valid remote HTTP hosts are accepted too. Use HTTPS for nonlocal deployments and for bearer-token confidentiality. The automatic route does not enforce that recommendation. Do not describe this feature as HTTPS-only or as requiring the older adapter's local/development HTTP flags. The older programmatic configuration retains its separate, stricter HTTP policy.

Sources: [user endpoint translation and path/security rules](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OtlpConfiguration.java), [legacy configuration tests](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/OtlpConfigurationTest.java).

## Levels, root selection, and thresholds

| `otelLogLevel` | Aggregate metrics | Span trees eligible for export |
| --- | --- | --- |
| `0` (default; INFO description) | Yes | None, including failures. |
| `1` | Yes | Connection or statement trees whose **root has a captured failure**. |
| `2` | Yes | Successful and failed connection/statement trees. |

Eligibility still requires a complete admitted tree and a root duration **strictly greater than** its configured threshold. Thresholds also suppress failed roots. Absence disables duration filtering, including for a zero-duration root; explicit `0` excludes a zero-duration root. A root lasting `11` ns passes `0.0000000105` seconds but fails `0.000000011` seconds.

These are whole-tree decisions, not per-activity slow-span filters. Once a root qualifies, its captured descendants are retained regardless of their individual durations. A failed child or failed retry attempt does not by itself qualify a successful root at level `1`; failure-looking outcome metadata is not a substitute for the captured root failure flag. No partial tree is intentionally exported to satisfy a filter.

Span selection also honors the captured parent's sampled flag. The private tracer provider uses `Sampler.parentBased(Sampler.alwaysOn())`; a valid unsampled application parent suppresses the entire connection or statement tree, even if its level/threshold qualifies. With no valid parent, eligible roots are sampled. This honors parent context, not arbitrary application SDK sampler configuration. Only the parent's `SpanContext` is captured at root start; the application context, baggage, and thread-local state are not retained or installed by the export worker.

Metrics are **unsampled and independent of span level and duration thresholds**. This does not make them lossless: bounded ingestion, unavailable transports, failed exports, and shutdown can still lose observations or delivery.

Sources: [route filtering and tree admission](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java), [level, decimal-boundary, root-failure, and unsampled-parent tests](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java).

## Metrics actually emitted

The automatic route uses shared `PerformanceActivityMetrics` for each ingested lifecycle END with a supported activity and nonnegative duration, including roots, attempts, and phases. START and unsupported legacy-only wrappers are not counted. Legacy callback overloads are no-ops on this route, avoiding duplicate observations.

| Instrument | Kind | Unit | Observation |
| --- | --- | --- | --- |
| `db.client.operation.count` | Long counter | `{operation}` | Add `1` per supported END, with terminal outcome. |
| `db.client.operation.error.count` | Long counter | `{error}` | Add `1` for terminal `failure`, `timeout`, or `canceled`; no outcome dimension. |
| `db.client.operation.duration` | Double histogram | `s` | Nonnegative END duration in nanoseconds divided by `1,000,000,000`, with terminal outcome. |
| `db.client.operation.retry.count` | Long counter | `{retry}` | Add `attempt_count - 1` when greater than zero, only on connection-open or statement-invocation root END; no outcome dimension. |

Bounded metric point dimensions match the metrics proposal:

| Dimension | Applicability / values |
| --- | --- |
| `mssql.performance.activity` | All instruments; stable dotted identities in metrics proposal §4, such as `connection.open`, `connection.dns`, `statement.execute`, `statement.first_response`, and `statement.server_call.prepare` / `.prepexec` / `.execute`. |
| `mssql.operation.kind` | All instruments; `connection` or `statement`, derived from activity identity rather than an identifier. |
| `mssql.operation.outcome` | Count/duration only; `success`, `failure`, `timeout`, `canceled`. |
| `mssql.statement.type` | Statement activities; `statement`, `prepared_statement`, `callable_statement`, `not_applicable`. |
| `mssql.statement.protocol_operation` | Statement server-call activities only; approved protocol enum or `not_applicable`. |
| `mssql.connection.auth_method` | Connection activities; approved authentication enum or `not_recorded`. |
| `mssql.retry.reason` | Retry counter only; `retry`, `redirect`, `failover`, `configurable_retry`, `invalid_handle_retry`, `enclave_retry`, or `not_applicable`. |
| `mssql.telemetry.schema.version` | All instruments; `1.0`. |

Outcome resolution uses root/attempt outcome metadata first, then timeout/canceled error category, then captured failure/category evidence, otherwise success. Core END enrichment supplies approved root authentication/statement type to descendants and a retry reason only when additional attempts actually begun have one approved common reason. Mixed/unknown reasons use `not_applicable`; a scheduled retry alone does not count. A retry or error series can be absent until first recorded; absence is not zero.

The 17 explicit histogram boundaries, in **seconds**, are `0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30`. These are SDK boundary advice, with no adapter-side bucket aggregation.

The explicit resource is `service.name=mssql-jdbc`; meter and tracer scope is `com.microsoft.sqlserver.jdbc`, versioned from the core package implementation version (fallback: driver major/minor). Metrics are aggregated by the private SDK and exported every **60 seconds**, or on explicit flush. No SQL, endpoint, identifier, exception, credential, or error category is a metric dimension.

Select `mssql.performance.activity=connection.open` or `statement.execute` for root counts. Never sum nested durations; compute each activity's mean from its own histogram sum/count and percentiles from its own buckets. Classic Prometheus buckets/sums do not retain an exact maximum, so the live dashboard leaves max unavailable. Region is shown only when emitted; it is not inferred.

**Metric label migration:** dashboards and alerts using the old `mssql.activity` / `mssql.activity.type` point dimensions and raw enum names must migrate to the new schema. Prometheus translates the new keys to `mssql_performance_activity`, `mssql_operation_kind`, `mssql_operation_outcome`, etc.; old and new queries are not interchangeable. The span-only `mssql.activity` attribute is not a metric label contract.

Sources: [shared instruments, registries, outcome and retry rules](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/PerformanceActivityMetrics.java), [automatic scope and SDK ownership](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java).

## Connection and statement spans actually supported

Eligible lifecycle trees are replayed with original start/end timestamps and parent relationships. Root spans are `CLIENT`; descendant spans are `INTERNAL`.

| Tree | Root span | Descendants when the corresponding instrumented path runs |
| --- | --- | --- |
| Physical connection open | `mssql.driver.connection.open` | `mssql.driver.connection.` plus `configuration`, `attempt`, `instance_discovery`, `dns`, `socket_connect`, `prelogin`, `tls`, `login`, `token_acquisition`, `redirect`, or `initialize`. |
| Statement invocation | `mssql.driver.statement.execute` | `mssql.driver.statement.` plus `attempt`, `request_build`, `server_call`, or `first_response`. |

Statement support is present, not merely planned: `SQLServerStatement.executeStatement()` creates invocation and attempt scopes, and existing statement/prepared-statement timing scopes become lifecycle children. `STATEMENT_PREPARE`, `STATEMENT_PREPEXEC`, and `STATEMENT_EXECUTE` map to `server_call`; they are not three distinct span-name suffixes. Statement metadata is allowlisted by root/attempt/phase and can describe statement type/API, operation, outcome, protocol, buffering, retries, counts, and timeouts when supplied by the core.

Coverage follows those existing scopes, not every public JDBC method: this is not a span for each result-set read, connection close, logical pool checkout, or all background recovery work. Statement invocation timing is not total result-set consumption time. In full response-buffering mode, `first_response` can include buffering the whole response rather than just first-byte latency. Connection-open lifecycle instrumentation explicitly excludes session-recovery reconnect scopes.

Failed spans receive `ERROR` status and allowlisted error category/type. Detailed `mssql.driver.error` events are attached only where origin error evidence exists, not repeated on every failed ancestor. Root lifecycle diagnostics are replayed through the connection/statement name and value allowlists, using only valid timestamps within the root and at most the last 128 entries. Approved authentication, retry/retry-decision, redirect, and timeout events appear only when captured by the corresponding instrumented path; unknown events/values are not passed through.

Only the statement invocation root may carry `db.query.text`. At root END (when level is nonzero), the callback synchronously masks `getCurrentUserSql()` with the bounded fail-closed lexical sanitizer: remove comments, replace string/Unicode-string/numeric/exponent/hexadecimal literals with `?`, normalize whitespace, preserve parameter markers, and omit malformed or uncertain input. Input over 16,384 characters is rejected; output is bounded at 4,096. Only masked output is queued, never raw SQL. Externally supplied query enrichment is discarded rather than trusted; children, events, and metrics never receive SQL. Identifiers/query shape may remain in approved masked text, so masking is not anonymization of schema names.

Raw SQL, parameter values, server addresses, tokens, options, raw exceptions, exception messages, and stack traces are not exported by this route. Allowlisted connection GUIDs and safe configuration/statement metadata can appear on **spans**; this is not a claim that spans contain no identifiers. The live dashboard displays masked text only from the emitted statement root after its own fail-closed lexical display check; missing/rejected text stays unavailable.

Sources: [activity-to-phase mapping](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceActivity.java), [statement invocation instrumentation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java), [prepared-statement instrumentation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java), [span and diagnostic replay](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java), [SQL sanitizer](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/SqlStatementSanitizer.java), [connection allowlist](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/ConnectionAttributePolicy.java), [statement allowlist](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/StatementAttributePolicy.java), [dashboard display policy](otel-poc/internal/fdh-ui/src/live.ts).

## Optional callback authentication

`otelAccessTokenCallback` is independent of SQL login's `accessTokenCallback` / `accessTokenCallbackClass`. Without it, telemetry is sent without an Authorization header; the collector must allow that or provide its own network-side access control.

The interface's actual Java signature is:

```java
SqlAuthenticationToken getAccessToken(String spn, String stsurl);
```

For **this telemetry route**, the arguments are:

1. `spn`: the configured endpoint after core trimming/URI normalization, before the adapter rewrites signal-path suffixes. It is an endpoint hint, not an automatically discovered OAuth scope; the same value is used for metrics and traces.
2. `stsurl`: the empty string `""`. No tenant or authority is discovered.

The application must map the endpoint to its own issuer/audience/scope and acquire the token. Neither the core nor this adapter automatically acquires Microsoft credentials, discovers Microsoft ingestion endpoints, or reuses SQL login credentials. The automatic route does not expose separate scope/authority/header/static-token options.

The public callback class and its public no-argument constructor are resolved lazily using the application context class loader captured when the connection route was created. Construction and invocation run on export workers, never the JDBC execution thread. Do not rely on application request thread-locals or baggage. Headers for both signals share one synchronized configuration, so a blocked callback can stall authentication for both signals of that transport; callback implementations must bound their own work.

For each outgoing export message's header request, the automatic configuration calls `getAccessToken()` again, even if a previously returned token has not expired. It reuses the callback **instance**, not the previous bearer token. A valid result must have a nonempty safe bearer token and a nonnull future expiration; the `long` token constructor takes epoch **milliseconds**, not a lifetime or epoch seconds. A case-insensitive existing `Bearer ` prefix is accepted; the transport writes one Authorization header.

On construction/invocation failure, a null/expired token, or an unsafe token, authentication fails closed: the message is not sent anonymously and no previously successful unexpired token or static fallback is reused. Failures impose a **1-second backoff shared by traces and metrics for that transport**. Attempts during the backoff fail without another callback invocation. Later export attempts may recover after the backoff; this is not a promise to replay failed messages. OTLP exporter retries are disabled. The older programmatic `OtlpConfiguration` path retains its separate early-refresh credential cache.

Sources: [callback interface](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerAccessTokenCallback.java), [token constructors](../src/main/java/com/microsoft/sqlserver/jdbc/SqlAuthenticationToken.java), [automatic authentication and legacy cache distinction](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OtlpConfiguration.java), [per-message, failure-after-success, backoff, and isolation tests](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java).

## Configuration examples

No SQL credentials or telemetry tokens are embedded in these examples. Supply SQL authentication independently using the application's existing secure configuration.

### Connection string

Metrics only (numeric `0` is also the default):

```text
jdbc:sqlserver://db.example:1433;encrypt=true;otelEndpoint=https://collector.example/ingest;otelLogLevel=0;
```

Aggregate metrics plus failed-root trees, with optional application callback and independent root thresholds:

```text
jdbc:sqlserver://db.example:1433;encrypt=true;otelEndpoint=https://collector.example/ingest;otelAccessTokenCallback=example.TelemetryTokenCallback;otelLogLevel=1;otelConnectionSpanDurationThreshold=0.25;otelStatementSpanDurationThreshold=0.0105;
```

### DataSource

```java
SQLServerDataSource ds = new SQLServerDataSource();
ds.setServerName("db.example");
ds.setPortNumber(1433);
ds.setEncrypt("true");
ds.setOtelEndpoint("https://collector.example/ingest");
ds.setOtelAccessTokenCallback("example.TelemetryTokenCallback"); // Optional.
ds.setOtelLogLevel(2);
ds.setOtelConnectionSpanDurationThreshold("0.25");
ds.setOtelStatementSpanDurationThreshold("0.0105");
// Configure database authentication through the application's secure configuration.
```

### Application callback template

`ApplicationTelemetryTokens.acquireForEndpoint` below is an **application-supplied integration point**, not a driver API. Implement it using the application's approved token provider, an explicit trusted-endpoint-to-audience mapping, and bounded timeouts. It must return a real `SqlAuthenticationToken` with its actual expiration; never log the token or put it in a URL.

```java
package example;

import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;

public final class TelemetryTokenCallback implements SQLServerAccessTokenCallback {
    public TelemetryTokenCallback() {}

    @Override
    public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
        // For this route authority is ""; endpoint is not itself an OAuth scope.
        // This helper belongs to the application, not the JDBC driver.
        return ApplicationTelemetryTokens.acquireForEndpoint(endpoint);
    }
}
```

## Asynchronous delivery, bounds, and ownership

The connection route is installed before the physical-open scope begins. JDBC callbacks snapshot immutable lifecycle events without retaining exception objects, capture only root parent span identity, synchronously mask statement SQL at root END when enabled, and make nonblocking offers. SDK creation, aggregation, tree filtering/replay, credential construction, and network export happen asynchronously. This is **best-effort fire-and-forget telemetry**, not an audit or durable delivery channel; it still has snapshot/masking/queue overhead on the producer thread.

Current implementation bounds are fixed, not connection options:

| Resource | Bound/behavior |
| --- | --- |
| Shared ingestion queue | `8192` queued tasks; admission also drops immediately on lock contention. |
| Shared transport registry | `32` endpoint/callback/class-loader keys; a new key is declined at capacity without evicting an active transport. |
| Pending span trees | `256` across the registry. |
| One pending tree | Up to `512` START entries and `512` END entries (at most `512` spans); malformed/incomplete/oversized trees are dropped whole. |
| Pending-tree expiry | Older than `1` minute at cleanup. |
| Per-transport span export | Single daemon worker; `64` queued tasks and `2048` span permits across queued/in-progress trees. Whole-tree admission, not individual-span dropping. |
| Transport timeouts | `3` seconds for HTTP connect/export and the tree export wait; these do not forcibly time-limit arbitrary application callback code. |
| Idle transport expiry | More than `5` minutes since its last accepted event, checked during cleanup; expiry closes the owned pipeline. |

Cleanup is opportunistic (idle ingestion polling and new-pipeline admission), not a precise scheduled eviction deadline. Ingestion loss invalidates pending/older queued span trees, including trees belonging to other routes in the shared registry, to avoid replaying incomplete traces. END events that do reach ingestion can still update metrics even when their trees are rejected; events dropped before ingestion cannot contribute metrics. Backpressure, collector/authentication errors, and process exit can lose telemetry.

Transports/private SDKs/credential state are shared only for the same endpoint string, callback class name, and captured class-loader identity. Different endpoints, callbacks (including no callback), or loaders get separate configurations. Different per-connection levels and thresholds can share a transport without sharing their filters or tree identities. Conversely, routes with the same key intentionally share aggregate metric series and callback state; this is not per-connection credential-instance isolation. Equivalent-looking endpoint spellings are not guaranteed to collapse to one registry key.

Source: [registry, pipeline, and whole-tree exporter implementation](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java).

### Flush and shutdown APIs

The following are **public static methods on the optional adapter's concrete class**, not methods on the core SPI or on `SQLServerConnection`:

- `UserEndpointTelemetryProvider.forceFlush(Duration timeout)`: drains queued events and requests SDK flushes within the supplied nonnegative wait budget; returns a boolean. It may block and belongs at an application lifecycle boundary, not on a request's critical path. Completion is **not a collector-delivery acknowledgement** and does not report every earlier export failure. A successful flush can coexist with authentication failure/no HTTP request.
- `UserEndpointTelemetryProvider.shutdown()`: idempotently stops admission to the current registry, discards pending work/trees, and initiates asynchronous cleanup of its owned SDKs and credentials. It returns `void`, has no completion-wait overload, and is not a flush. Existing connection callbacks become no-ops; new connections can create a fresh registry. It affects all routes sharing that adapter registry.

Closing a SQL connection does **not** close its shared exporter. At application shutdown, stop producers, optionally call `forceFlush(Duration.ofSeconds(5))`, then call `shutdown()`. Daemon workers do not keep the process alive. Shutdown does not wait on a blocked callback's authentication monitor; do not promise forced cancellation or prompt completion of arbitrary user callback code. These APIs do not shut down the application's SDK or global callback.

Source: [public lifecycle methods](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java).

## Compatibility and verification boundaries

- No installation/replacement of `GlobalOpenTelemetry`, no manual global performance-callback registration, and no application JUL-level changes are performed by the automatic route. Private SDKs have their own resource, meter, tracer, and exporters.
- The application performance callback and user route receive lifecycle boundaries independently; failure in one route does not suppress the other. The automatic callback ignores legacy overloads. Existing legacy publication selects the current global callback at scope close and retains scope-time duration units; adding this route must not be presented as changing those semantics.
- The optional route's no-raw-SQL/no-exception-message export policy does not sanitize an application's own callback or preexisting JUL logging. Their behavior remains independent.
- The feature has focused test coverage, not proof of no regressions across every JRE, database, authentication method, pool, and deployment. This documentation change does not add or rerun that certification.

Reviewed evidence:

| Source/test | What it establishes |
| --- | --- |
| [UserOtelPropertiesTest](../src/test/java/com/microsoft/sqlserver/jdbc/UserOtelPropertiesTest.java) | Exact driver option names, sanitized early validation, DataSource defaults/string round trips. |
| [UserEndpointTelemetryTest](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java) | SPI loading, a real core configuration failure alongside an application callback, wire-level OTLP metrics/spans, synthetic connection/statement trees, levels/thresholds, unsampled parents, per-message auth, failure backoff/no reuse, endpoint/callback-loader isolation, bounded queues and whole-tree loss. Most tree cases are fixtures, not a live SQL Server workload. |
| [OtlpConfigurationTest](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/OtlpConfigurationTest.java) | Legacy endpoint security, safe headers, lazy cached-token refresh, and fail-closed behavior; do not confuse those legacy options/cache rules with automatic routing. |
| [PerformanceLogLifecycleTest](../src/test/java/com/microsoft/sqlserver/jdbc/PerformanceLogLifecycleTest.java) | Lifecycle identity, exception-free snapshots, route failure isolation, callback registration-change behavior, and preservation of legacy callback selection/units. |
| [PerformanceActivityMetricsTest](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/PerformanceActivityMetricsTest.java) and [PerformanceLogEndMetadataTest](../src/test/java/com/microsoft/sqlserver/jdbc/PerformanceLogEndMetadataTest.java) | Focused metric projection and core END-enrichment checks; inspect current tests/results rather than treating this documentation as a test-pass report. |

### Publication checklist

Before merging/publishing, confirm optional-JAR/SPI packaging and release coordinates; state numeric `0` rather than literal `INFO`; retain the broad HTTP acceptance and private-SDK ownership caveats; migrate metric labels in dashboards/alerts; verify masked SQL and diagnostic allowlists against real payloads. Do not claim durable delivery, flush acknowledgement, an exact Prometheus maximum, or completion of all proposal acceptance criteria. Live SQL statement-path and multi-profile validation remain separate release checks, not conclusions from this summary.