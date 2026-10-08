# Pre-aggregated metrics for JDBC performance activities

## Proposal summary

This document proposes interval-based OpenTelemetry metrics for every performance activity already measured by the Microsoft JDBC Driver for SQL Server. Unlike the failure-only diagnostic spans, these metrics cover **all completed connections and all completed statement executions**, including successes, failures, timeouts, cancellations, and successful operations after an internal retry.

The design keeps the existing `PerformanceLogCallback` contract and the failure-span proof of concept unchanged. It adds a bounded in-process aggregator between performance activity completion and the optional OpenTelemetry exporter. The driver records into fixed-cardinality counters and histograms on the application thread, rotates the active window at a configurable interval, and exports immutable aggregate snapshots asynchronously.

This follows the useful properties of the earlier `users/machavan/otelexperiment` proof of concept—counters, duration histograms, periodic metric export, and bounded dimensions—but changes the hot path from one OpenTelemetry SDK call per activity to one driver-owned aggregation update per activity. It also removes IDs, SQL text, trace context, and other unbounded values from metric attributes.

> **Decision:** spans answer “what happened to this failed operation?” Metrics answer “how is the whole driver workload behaving?” They are complementary signals and have different admission rules.

## 1. Goals and non-goals

### Goals

1. Measure every existing connection and statement `PerformanceActivity`, not only failures.
2. Pre-aggregate activity counts, outcomes, durations, and retries inside the driver for a bounded interval.
3. Keep the operation hot path allocation-free after a metric series is initialized.
4. Keep metric cardinality bounded and independent of connection count, statement count, SQL diversity, and customer data.
5. Export standard OpenTelemetry cumulative or delta metric data through the optional adapter.
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
| `db.client.operation.inflight` | Observable up/down sum | `{operation}` | Optional current root operations in progress: connection opens and statement invocations only. |
| `mssql.jdbc.metrics.dropped_series` | Monotonic sum | `{series}` | New series rejected after a cardinality bound is reached. No rejected attribute values are exported. |
| `mssql.jdbc.metrics.dropped_windows` | Monotonic sum | `{window}` | Completed aggregate windows dropped because the async handoff was full. |

The initial dashboard needs the first four instruments. `inflight` and self-observability counters are recommended for production hardening.

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

Every enum value already defined by the driver has an activity series. Existing enum identities remain stable.

### 4.1 Connection activities

| `PerformanceActivity` | Metric activity value | Population and boundary |
|---|---|---|
| `CONNECTION` | `connection.open` | Every physical open invocation, including terminal failures and successful opens. Root throughput and end-to-end connection latency. |
| `PRELOGIN` | `connection.prelogin_legacy` | Existing broad prelogin wrapper. Retained for callback compatibility and historical comparison. |
| `LOGIN` | `connection.login_legacy` | Existing broad login/authentication wrapper. Retained for compatibility. |
| `TOKEN_ACQUISITION` | `connection.token_acquisition_legacy` | Existing broad federated token wrapper when invoked. |
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

The legacy and lifecycle values are intentionally distinct. Dashboards should prefer the narrow lifecycle values and use legacy values only for compatibility panels. This prevents two differently bounded activities from being merged into one histogram.

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

## 5. Pre-aggregation design

### 5.1 Window lifecycle

The optional metrics bridge owns two windows:

```text
application threads                  one daemon exporter
        │                                    │
        ├─ record count/duration ──> ACTIVE  │
        │                           window    │
        │                                    │
interval deadline / explicit flush           │
        └─ atomic rotate ──────────> CLOSED ─┼─> OTel aggregate snapshot
                                    window    │
                                             └─> OTLP exporter configured by host
```

Default aggregation interval: **60 seconds**. Supported range: 10–300 seconds. The interval is captured when the bridge starts and does not change until it is restarted.

Rotation swaps the active series map with a fresh bounded map. The closed map is immutable after rotation and is offered to a single-slot or otherwise tightly bounded async handoff. Application threads never perform network I/O and never wait for export.

### 5.2 Per-series aggregate

Each series key is the instrument identity plus the approved bounded attribute tuple. Each interval aggregate contains:

- `count`
- `errorCount`
- `sumDurationNanos`
- `minDurationNanos`
- `maxDurationNanos`
- fixed duration bucket counts
- retry count where applicable

Use `LongAdder` or striped counters for count/sum/buckets and atomic min/max updates. Once initialized, recording a duration performs bounded counter increments only.

### 5.3 Histogram boundaries

Use duration boundaries suitable for both local driver work and network/server work:

`0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000` milliseconds.

Internally record nanoseconds and classify using integer nanosecond boundaries. Export seconds to align with OpenTelemetry semantic conventions. The aggregate snapshot carries bucket counts, sum, count, min, and max; the adapter maps it to explicit-bucket histogram data without replaying individual observations.

### 5.4 Cardinality bounds

The bridge enforces:

- Maximum 512 active series per window by default.
- No dynamic string values outside closed registries.
- No series creation after the bound; increment `dropped_series` instead.
- Maximum one pending closed window by default. If export is behind, drop the older unexported window and increment `dropped_windows`; never block JDBC work.
- Empty windows are not exported.

Expected normal cardinality is far below the bound. For example, root statement metrics with four outcomes and three statement types require at most 12 series before optional protocol dimensions.

### 5.5 Outcome rules

- `success`: scope closes without a terminal exception, including success after retry.
- `timeout`: classified timeout reaches the activity as its terminal outcome.
- `canceled`: application or driver cancellation reaches the activity as its terminal outcome.
- `failure`: any other terminal exception.
- Child timing-only activities that do not own a terminal error use the outcome visible at their own close boundary; they are not retroactively relabeled after export.

Root retry counters use attempts actually begun: `max(attempt_count - 1, 0)`. A retry decision that is never attempted does not increment retry count.

## 6. API and compatibility boundary

The existing legacy `PerformanceLogCallback.publish(...)` overloads remain unchanged. The new aggregate path should use a separate internal SPI so an adapter does not have to reconstruct histograms from individual callback events:

```java
interface PerformanceMetricSink {
    void publish(PerformanceMetricWindow window);
}
```

`PerformanceMetricWindow` is immutable, contains no raw exceptions or SQL, and exposes only approved aggregate series. The optional OpenTelemetry module installs the sink and maps aggregate snapshots to SDK metric data/export. Applications that register a legacy callback continue receiving individual events exactly as today.

Recommended ownership:

| Component | Responsibility |
|---|---|
| Core driver | Activity boundaries, outcome classification, bounded series key, fixed-bucket aggregation, interval rotation, immutable snapshot |
| Optional OTel adapter | Meter provider integration, temporality mapping, async export, shutdown/flush, exporter health |
| Application/collector | Resource identity, credentials, transport policy, routing, retention |

The core driver must remain free of OpenTelemetry API, SDK, exporter, Azure Identity, and HTTP dependencies.

## 7. Configuration

Metrics are opt-in in the first release.

| Setting | Default | Rule |
|---|---|---|
| `OTEL_JDBC_METRICS_ENABLED` | `false` | Enables aggregate collection when the optional adapter is present. |
| `OTEL_JDBC_METRICS_INTERVAL_SECONDS` | `60` | Integer 10–300. |
| `OTEL_JDBC_METRICS_MAX_SERIES` | `512` | Integer 64–4096; restart required. |
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

Delta temporality is preferred because each snapshot represents one closed interval. If the selected SDK/exporter requires cumulative temporality, the adapter maintains cumulative aggregate state off the JDBC hot path and resets it only when the meter provider is recreated.

Each exported datapoint uses the window start and end timestamps. Export delay does not change the measurement interval. Retries of the same closed window must retain a stable window sequence number so the adapter can avoid duplicate cumulative application.

A flush at shutdown rotates the current non-empty window and waits only within the adapter's bounded flush timeout. JDBC connection close does not flush global metrics.

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
7. Series count remains bounded during one million unique SQL statements and one million unique connection IDs.
8. Export slowdown never blocks application threads and increments `dropped_windows` when the handoff is saturated.
9. Disabled mode creates no aggregation thread, window, series, or OpenTelemetry object.
10. Existing callback, failure-span, Kusto/Delta trace parity, and customer diagnostic dashboard checks remain unchanged.
11. The metrics dashboard renders nonzero successful-operation throughput and distinguishes root throughput from phase-call counts.
12. Tests cover interval rotation, boundary buckets, outcome accounting, cardinality rejection, dropped windows, shutdown flush, and temporality conversion.

## 12. Implementation sequence

1. Add immutable metric key, fixed-bucket aggregate, active window, and closed window types in core.
2. Record existing legacy activity completions into the aggregator without changing callback publication.
3. Record lifecycle-only roots and attempts, ensuring one completion per scope.
4. Add bounded interval rotation and async snapshot handoff.
5. Add the internal aggregate sink SPI.
6. Map snapshots to OpenTelemetry metrics in the optional adapter.
7. Add a trace-and-metrics collector pipeline to the POC; retain trace-only privacy gates.
8. Add exact aggregate evidence verification for successful and failed controls.
9. Add Kusto and Delta metric parity checks if both stores support the selected OTLP metric schema.
10. Wire the customer dashboard to aggregate metric queries; retain deterministic mock data for design review.

## 13. Open questions

- Whether the first production implementation should expose only root activities by default or all phase activities by default.
- Whether authentication method is sufficiently useful to justify its series multiplier.
- Whether protocol operation should be emitted only for statement server-call metrics or omitted from the first release.
- Whether a native aggregate metric SPI should be public or remain internal until its compatibility contract is proven.
- Whether metrics should be enabled automatically when a host `MeterProvider` is present or remain explicitly opt-in.
