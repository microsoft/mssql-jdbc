# OpenTelemetry metrics from JDBC performance activities

## Proposal summary

This document proposes OpenTelemetry metrics derived from the same lifecycle END boundaries already published for connection and statement span assembly. Unlike the failure-only diagnostic spans, these metrics cover **all completed connection and statement activities**, including successes, failures, timeouts, cancellations, and successful operations after an internal retry.

The design keeps the existing `PerformanceLogCallback` contract and failure-span path unchanged. When `publish(PerformanceLogEvent)` receives a valid END event, the optional adapter records one counter observation and one duration observation using bounded attributes. The application-supplied OpenTelemetry SDK performs normal counter and histogram aggregation and periodic export. There is no second driver instrumentation path, driver-owned window, aggregate snapshot SPI, or replay of individual observations.

This follows the earlier `users/machavan/otelexperiment` proof of concept—counters, duration histograms, periodic SDK export, and bounded dimensions—while reusing the lifecycle events now required for span construction. It removes IDs, SQL text, trace context, and other unbounded values from metric attributes.

> **Decision:** spans answer “what happened to this failed operation?” Metrics answer “how is the whole driver workload behaving?” They are complementary signals and have different admission rules.

## 1. Goals and non-goals

### Goals

1. Measure every connection and statement activity already published through the lifecycle span callback, not only failures.
2. Derive activity counts, outcomes, durations, and retries only from existing lifecycle END events.
3. Delegate aggregation, temporality, interval rotation, and export to the application-supplied OpenTelemetry SDK.
4. Keep metric cardinality bounded and independent of connection count, statement count, SQL diversity, and customer data.
5. Export standard OpenTelemetry cumulative or delta metric data through the existing optional adapter.
6. Preserve existing callback behavior, logging behavior, span behavior, and disabled-path cost.
7. Support useful fleet dashboards: throughput, success rate, error rate, percentiles, connection lifecycle latency, and statement pipeline latency.

### Non-goals

- Metrics do not carry connection IDs, statement IDs, SQL text, query hashes, table names, procedure names, exception messages, stack traces, usernames, database names, server names, or trace IDs.
- Metrics do not replace diagnostic spans or their structured failure classification.
- Parent and child activity durations are not summed. Nested durations overlap by design.
- The driver does not implement a Prometheus endpoint.
- The core driver does not acquire Azure credentials or implement OTLP transport.
- The first version does not aggregate server-side CPU, reads, rows, or execution plans.

## 2. Signal model

The metric stream is independent from failure-tree retention:

| Signal | Population | Detail | Retention path |
|---|---|---|---|
| Failure spans | Terminal failures selected by the bounded failure adapter | One execution tree, approved root attributes, sanitized events, masked SQL when safe | Trace pipeline |
| Performance metrics | Every completed measured activity | Interval aggregates only | Metric pipeline |

A successful connection contributes to connection activity metrics even though it emits no failure span. A successful statement contributes to statement metrics even though it emits no statement span. A failed operation contributes to both metrics and, if admitted, the diagnostic trace pipeline.

## 3. Instruments

Use instrumentation scope `com.microsoft.sqlserver.jdbc` with the actual driver version.

### 3.1 Core instruments

| Instrument | Type | Unit | Meaning |
|---|---|---|---|
| `db.client.operation.count` | Monotonic sum | `{operation}` | Completed activities in the interval. |
| `db.client.operation.duration` | Histogram | `s` | Duration distribution for completed activities. |
| `db.client.operation.error.count` | Monotonic sum | `{error}` | Activities whose terminal outcome is `failure`, `timeout`, or `canceled`. |
| `db.client.operation.retry.count` | Monotonic sum | `{retry}` | Additional connection or statement attempts actually begun. Root activities only. |

The initial implementation uses these four instruments. In-flight gauges require START/END state and are intentionally deferred; metrics are otherwise stateless in the adapter.

### 3.2 Metric attributes

Only the following bounded dimensions are allowed:

| Attribute | Values | Applies to |
|---|---|---|
| `mssql.performance.activity` | Stable enum identity listed below | All instruments |
| `mssql.operation.kind` | `connection`, `statement` | All instruments |
| `mssql.operation.outcome` | `success`, `failure`, `timeout`, `canceled` | Count and duration |
| `mssql.statement.type` | `statement`, `prepared_statement`, `callable_statement`, `not_applicable` | Statement activities |
| `mssql.statement.protocol_operation` | Approved protocol enum, or `not_applicable` | Statement server-call activities only |
| `mssql.connection.auth_method` | Existing bounded authentication-method enum, or `not_recorded` | Connection activities only |
| `mssql.retry.reason` | Approved retry-reason enum, or `not_applicable` | Retry count only |
| `mssql.telemetry.schema.version` | Initially `1.0` | All instruments |

`service.name`, deployment environment, cloud region, and host identity belong to OpenTelemetry resource attributes supplied by the application or collector. They are not copied into each driver series.

### 3.3 Explicitly forbidden metric attributes

The aggregator must reject rather than truncate these dimensions: connection GUID, connection ID, statement ID, SQL text, masked SQL, SQL hash, database name, catalog, schema, table, procedure, server address, user name, application name, exception class, SQL state, SQL Server error number, resource key, correlation ID, trace ID, span ID, and arbitrary callback attributes.

Error category and error type remain trace/event detail. If fleet-level error-category metrics are needed later, they require a separate cardinality review and a closed category registry.

## 4. Activity catalog

Every activity that already publishes START/END lifecycle boundaries has a metric series. Broad legacy-only callback wrappers are not duplicated into the lifecycle path solely for metrics.

### 4.1 Connection activities

| `PerformanceActivity` | Metric activity value | Population and boundary |
|---|---|---|
| `CONNECTION` | `connection.open` | Every physical open invocation, including terminal failures and successful opens. Root throughput and end-to-end connection latency. |
| `PRELOGIN` | `connection.prelogin_legacy` | Existing broad prelogin wrapper. Retained for callback compatibility and historical comparison. |
| `CONNECTION_CONFIGURATION` | `connection.configuration` | Validated connection configuration phase. |
| `CONNECTION_ATTEMPT` | `connection.attempt` | Every endpoint attempt, including retries. |
| `INSTANCE_DISCOVERY` | `connection.instance_discovery` | SQL Browser instance discovery when used. |
| `DNS` | `connection.dns` | Every actual host-resolution operation. |
| `SOCKET_CONNECT` | `connection.socket_connect` | Every socket connection candidate measured by the lifecycle. |
| `TLS` | `connection.tls` | Every TLS negotiation. |
| `LOGIN_EXCHANGE` | `connection.login` | Narrow TDS login exchange for the selected endpoint. |
| `TOKEN_REQUEST` | `connection.token_request` | Actual token request, excluding surrounding callback work. |
| `CONNECTION_INITIALIZE` | `connection.initialize` | Required post-login connection initialization. |
| `CONNECTION_REDIRECT` | `connection.redirect` | Each server routing transition. |

`LOGIN` and `TOKEN_ACQUISITION` remain legacy callback-only wrappers and therefore do not produce these metrics. Their accurately bounded lifecycle replacements are `LOGIN_EXCHANGE` and `TOKEN_REQUEST`. This avoids a second publication path and prevents differently bounded activities from being merged into one histogram.

### 4.2 Statement activities

| `PerformanceActivity` | Metric activity value | Population and boundary |
|---|---|---|
| `STATEMENT_INVOCATION` | `statement.execute` | Every JDBC execute invocation, including successes and terminal failures. Root throughput and end-to-end invocation latency. |
| `STATEMENT_ATTEMPT` | `statement.attempt` | Initial and internally retried attempts. |
| `STATEMENT_REQUEST_BUILD` | `statement.request_build` | Request construction per attempt. Retry backoff is excluded. |
| `STATEMENT_FIRST_SERVER_RESPONSE` | `statement.first_response` | Existing `startResponse()` boundary. With full buffering it can include complete response buffering. |
| `STATEMENT_PREPARE` | `statement.server_call.prepare` | `sp_prepare` calls when `prepareMethod=prepare`. |
| `STATEMENT_PREPEXEC` | `statement.server_call.prepexec` | Combined `sp_prepexec`; never split into fabricated prepare/execute metrics. |
| `STATEMENT_EXECUTE` | `statement.server_call.execute` | Direct SQL, `sp_executesql`, `sp_execute`, cursor, or batch server call using the existing boundary. |

For the three server-call activities, `mssql.statement.protocol_operation` may distinguish the approved values `direct_sql`, `direct_sql_batch`, `sp_executesql`, `sp_prepexec`, `sp_prepare`, `sp_execute`, `prepared_batch`, `cursor_open`, `cursor_prepexec`, `cursor_execute`, and `bulk_copy`.

## 5. Recording and aggregation design

### 5.1 Lifecycle reuse

The optional adapter observes the lifecycle stream already used for span assembly:

```text
driver activity closes
        │
        └─ PerformanceLogEvent END
                    ├─ record count/duration/error/retry into OTel API instruments
                    └─ continue existing failure-only span admission and assembly
                                      │
                                      └─ application OTel SDK aggregates and exports
```

Metric recording happens before failure-only span filtering, so successful activities and spans rejected by trace admission still contribute metrics. Recording never performs network I/O; the SDK metric reader/exporter owns periodic asynchronous collection and transport.

### 5.2 SDK aggregation

Each END records into OpenTelemetry API counter and histogram instruments. The SDK owns sums, bucket counts, temporality, concurrency, collection intervals, and exporter handoff. Instruments are lazily initialized once when metrics are enabled. SDK failures are isolated inside the callback and never suppress span admission or affect the SQL operation.

### 5.3 Histogram boundaries

Use duration boundaries suitable for both local driver work and network/server work:

`0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000` milliseconds.

The callback converts the existing nanosecond duration to seconds and records it once. Explicit bucket boundary advice is supplied when constructing the histogram; the SDK performs bucket classification without adapter-side observation replay.

### 5.4 Cardinality bounds

The adapter accepts only fixed activity identities and closed attribute registries. No customer-controlled value, identifier, SQL-derived value, exception text, or callback-provided arbitrary attribute is copied. Therefore series cardinality is bounded by the finite cross-product of instrument, activity, operation kind, outcome, statement type, protocol operation, and authentication method. SDK cardinality limits and exporter queue policy remain application concerns.

Expected normal cardinality is far below the bound. For example, root statement metrics with four outcomes and three statement types require at most 12 series before optional protocol dimensions.

### 5.5 Outcome rules

- `success`: scope closes without a terminal exception, including success after retry.
- `timeout`: classified timeout reaches the activity as its terminal outcome.
- `canceled`: application or driver cancellation reaches the activity as its terminal outcome.
- `failure`: any other terminal exception.
- Child timing-only activities that do not own a terminal error use the outcome visible at their own close boundary; they are not retroactively relabeled after export.

Root retry counters use attempts actually begun: `max(attempt_count - 1, 0)`. A retry decision that is never attempted does not increment retry count.

## 6. API and compatibility boundary

The existing `PerformanceLogCallback` overloads remain unchanged. No metric SPI is added. The optional adapter consumes `PerformanceLogEvent.Type.END` from the existing lifecycle overload and records directly into OpenTelemetry API instruments. Applications using legacy callbacks continue receiving individual events exactly as today.

Recommended ownership:

| Component | Responsibility |
|---|---|
| Core driver | Existing activity boundaries, duration, outcome classification, retries, and immutable lifecycle END event |
| Optional OTel adapter | Bounded metric dimension projection and OpenTelemetry API instrument recording |
| OpenTelemetry SDK | Aggregation, histogram buckets, temporality, collection interval, queueing, export, flush, and shutdown |
| Application/collector | Resource identity, credentials, transport policy, routing, retention |

The core driver must remain free of OpenTelemetry API, SDK, exporter, Azure Identity, and HTTP dependencies.

## 7. Configuration

Metrics are opt-in in the first release.

| Setting | Default | Rule |
|---|---|---|
| `OTEL_JDBC_METRICS_ENABLED` | `false` | Enables aggregate collection when the optional adapter is present. |
| `OTEL_METRICS_EXPORTER` | Host-defined | Standard OpenTelemetry SDK exporter selection; the driver does not invent credentials. |

Connection-string secrets and per-connection endpoint selection must not become metric dimensions. Prefer one application-level OpenTelemetry pipeline. If per-connection export targets remain a requirement, target count must be bounded separately and target identity must not appear in metric attributes.

## 8. Dashboard design

Place a **Performance metrics** section below the failed-statement diagnostics on the existing customer page. It should visibly state that metrics include successful and failed operations.

### KPI cards

1. Connection opens per minute (`connection.open` count rate)
2. Statement executions per minute (`statement.execute` count rate)
3. Connection success rate
4. Statement p95 duration

### Charts

- Operation throughput over time, split into connection opens and statement executions.
- Connection lifecycle p95 by activity: configuration, DNS, socket connect, prelogin, TLS, login, initialize.
- Statement pipeline p95 by activity: request build, prepare, prepexec, execute, first response.
- Outcome distribution: success, failure, timeout, canceled.

### Aggregate table

Columns: activity, operation kind, count, success rate, error count, p50, p95, p99, and maximum duration. The table must never contain SQL or identifiers.

Dashboard math must not add nested phase durations. Percentiles are calculated independently from each activity histogram. Throughput uses only root activities unless the panel explicitly says “attempts” or “phase calls.”

## 9. Export semantics

The selected OpenTelemetry SDK and metric reader own cumulative or delta temporality. The adapter does not maintain a second cumulative state or window sequence. A telemetry pipeline flush includes the meter provider within its bounded flush timeout. JDBC connection close does not flush global metrics.

## 10. Security and privacy

- No SQL or masked SQL enters metric state.
- No raw exception enters metric state.
- No credential, token, connection string, endpoint, or authorization header enters metric state.
- No customer-controlled string is accepted as a metric dimension.
- Aggregate snapshots may reveal workload volume and latency and therefore use the same authenticated transport and access controls as other production telemetry.
- Exporter failures are not logged with payloads, headers, exception messages, or endpoint query strings.

## 11. Validation and acceptance criteria

1. Successful connection and statement controls produce metrics but no failure spans.
2. Failed controls produce metrics and the existing failure trees.
3. For each root activity, interval count equals successes plus failures plus timeouts plus cancellations.
4. Histogram count equals operation count for every activity series.
5. p50/p95/p99 computed from exported buckets stay within their enclosing bucket boundaries for deterministic workloads.
6. No metric attribute key or value contains SQL, IDs, server/customer names, exception text, or trace context.
7. Series count remains unchanged during one million unique SQL statements and one million unique connection IDs.
8. Export slowdown is isolated by the configured SDK metric reader/exporter and never performs transport on the JDBC callback thread.
9. Disabled mode creates no metric instrument or OpenTelemetry metric observation.
10. Existing callback, failure-span, Kusto/Delta trace parity, and customer diagnostic dashboard checks remain unchanged.
11. The metrics dashboard renders nonzero successful-operation throughput and distinguishes root throughput from phase-call counts.
12. Validation covers boundary advice, outcome accounting, finite dimensions, successful-operation recording, failure-span independence, and telemetry flush.

## 12. Implementation sequence

1. Add bounded metric projection and instruments to the optional adapter.
2. Record once for each valid lifecycle END before failure-only span filtering.
3. Add an owned meter provider and periodic OTLP metric exporter to the POC transport.
4. Retain trace privacy gates and add exact metric evidence for successful and failed controls.
5. Add Kusto and Delta metric parity checks if both stores support the selected OTLP metric schema.
6. Wire the customer dashboard to aggregate metric queries; retain deterministic mock data for design review.

## 13. Open questions

- Whether the first production implementation should expose only root activities by default or all phase activities by default.
- Whether authentication method is sufficiently useful to justify its series multiplier.
- Whether protocol operation should be emitted only for statement server-call metrics or omitted from the first release.
- Whether metrics should be enabled automatically when a host `MeterProvider` is present or remain explicitly opt-in.
