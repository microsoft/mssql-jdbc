# JDBC OpenTelemetry demo Kusto table reference

## Purpose and scope

This document is agent context for diagnosing the path from a Java application through the Microsoft JDBC Driver to SQL Server using the telemetry exported by this demo.

It intentionally documents **only Kusto tables populated by this demo**:

- JDBC client failure traces for connections, `Statement`, and `PreparedStatement` executions.
- JDBC client operation metrics for successful and failed lifecycle activities.
- OpenTelemetry resource and instrumentation-scope attributes associated with those signals.

It does not describe unrelated tables that may exist in a shared Kusto database.

## Important server-side boundary

The demo's SQL Server container does **not** export server-side telemetry to Kusto. The Compose stack contains no SQL Server Database Watcher, SQL Agent telemetry collector, Extended Events exporter, DMV poller, or SQL Server log exporter. SQL Server errors such as 208, 2627, and 18456 are observed by the JDBC driver and exported as **client trace attributes/events**; that does not make them independent server-side telemetry.

Therefore:

| Signal owner | Tables exported by this demo |
|---|---|
| JDBC client traces | `spans`, `span_attrs`, `span_events`, `span_event_attrs`, `span_links`, `span_link_attrs` |
| JDBC client metrics | `univariate_metrics`, `number_data_points`, `number_dp_attrs`, `histogram_data_points`, `histogram_dp_attrs` |
| Shared OTLP identity | `resource_attrs`, `scope_attrs` |
| SQL Server-side telemetry | **None** |

Do not query `database_watcher_*`, Arc SQL telemetry, Azure SQL backend telemetry, SQL DMVs, or SQL error-log tables as if this demo populated them. A future server collector must document its own tables and correlation keys before an agent uses them.

## Deployment identity

The cloud demo takes its target from runtime configuration:

- Cluster: `KUSTO_CLUSTER_URI`
- Database: `KUSTO_DATABASE`
- Application discriminator: generated `CLOUD_SERVICE_NAME`, also exported as the Kusto `application` column and the `service.name` resource attribute
- Instrumentation scope: `com.microsoft.sqlserver.jdbc`

Always filter by `application` before joining or aggregating. Each cloud run uses a unique application value so stale runs do not satisfy verification.

## OTAP relationship and join rules

The exporter normalizes OTLP into parent and child tables. Its surrogate IDs are scoped to one export batch.

Use this batch key for all surrogate-ID joins:

```kusto
application, export_time_unix_nano
```

Relationship map:

| Child table | Child key | Parent table | Parent key |
|---|---|---|---|
| `resource_attrs` | `parent_id` | `spans` or `univariate_metrics` | `resource_id` |
| `scope_attrs` | `parent_id` | `spans` or `univariate_metrics` | `scope_id` |
| `span_attrs` | `parent_id` | `spans` | `id` |
| `span_events` | `parent_id` | `spans` | `id` |
| `span_event_attrs` | `parent_id` | `span_events` | `id` |
| `span_links` | `parent_id` | `spans` | `id` |
| `span_link_attrs` | `parent_id` | `span_links` | `id` |
| `number_data_points` | `parent_id` | `univariate_metrics` | `id` |
| `number_dp_attrs` | `parent_id` | `number_data_points` | `id` |
| `histogram_data_points` | `parent_id` | `univariate_metrics` | `id` |
| `histogram_dp_attrs` | `parent_id` | `histogram_data_points` | `id` |

Never join only on `id` or `parent_id`. Correct example:

```kusto
spans
| where application == App
| join kind=leftouter (
    span_attrs
    | where application == App
) on $left.application == $right.application,
     $left.export_time_unix_nano == $right.export_time_unix_nano,
     $left.id == $right.parent_id
```

`trace_id` and `span_id` are real OpenTelemetry identities rather than OTAP surrogate IDs. Use `(application, trace_id, span_id)` to reconstruct a trace across export batches.

## Attribute value encoding

The five attribute tables use the same typed-value layout:

- `resource_attrs`
- `scope_attrs`
- `span_attrs`
- `span_event_attrs`
- `span_link_attrs`
- `number_dp_attrs`
- `histogram_dp_attrs`

Decode the value selected by `type`:

| `type` | Value column | Meaning |
|---:|---|---|
| 1 | `str` | String |
| 2 | `int` | Signed integer |
| 3 | `double` | Floating-point number |
| 4 | `bool` | Boolean |
| 5 | `bytes` | Byte value encoded as a string by the Kusto mapping |
| other | `ser` | Serialized fallback value |

Reusable KQL expression:

```kusto
extend attribute_value = case(
    type == 1, str,
    type == 2, tostring(['int']),
    type == 3, tostring(['double']),
    type == 4, tolower(tostring(['bool'])),
    type == 5, bytes,
    ser)
```

## Trace tables

### `spans`

One row per exported JDBC failure span. Successful operations contribute metrics but do not create diagnostic spans.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP surrogate row ID. Join child tables to this ID only within the same application/export batch. |
| `resource_id` | string | OTAP surrogate ID for the OpenTelemetry resource. |
| `resource_schema_url` | string | Resource schema URL, if supplied. |
| `resource_dropped_attributes_count` | long | Resource attributes dropped before export. |
| `scope_id` | string | OTAP surrogate ID for the instrumentation scope. |
| `scope_name` | string | Instrumentation scope; expected `com.microsoft.sqlserver.jdbc`. |
| `scope_version` | string | Instrumentation scope version when supplied. |
| `scope_dropped_attributes_count` | long | Scope attributes dropped before export. |
| `schema_url` | string | Scope schema URL, if supplied. |
| `start_time_unix_nano` | datetime | Span start time. The name is inherited from OTLP even though Kusto stores a `datetime`. |
| `duration_time_unix_nano` | long | Span duration in **microseconds** under this demo's `otap-microseconds-v1` cloud schema. Divide by 1,000 for milliseconds. |
| `trace_id` | string | OpenTelemetry trace ID. All spans in one failed connection or statement tree share it. |
| `span_id` | string | OpenTelemetry span ID. |
| `trace_state` | string | W3C trace-state value, normally empty in this demo. |
| `parent_span_id` | string | Parent OpenTelemetry span ID; empty for a root. |
| `name` | string | Span name, such as `mssql.driver.connection.open` or `mssql.driver.statement.execute`. |
| `kind` | int | OpenTelemetry span-kind enum. Roots are client spans; phases are internal spans. |
| `dropped_attributes_count` | long | Span attributes dropped before export. |
| `dropped_events_count` | long | Span events dropped before export. |
| `dropped_links_count` | long | Span links dropped before export. |
| `status_code` | int | OpenTelemetry status enum. Failed spans use ERROR; diagnostic roots in this demo are failures. |
| `status_status_message` | string | Status free text. The adapter intentionally does not export error messages here. |
| `export_time_unix_nano` | datetime | Export-batch timestamp and part of the OTAP join key. |
| `application` | string | Run/service discriminator. Filter on this first. |

Expected root names:

| Root | Meaning |
|---|---|
| `mssql.driver.connection.open` | Failed physical connection-open lifecycle |
| `mssql.driver.statement.execute` | Failed JDBC `Statement` or `PreparedStatement` invocation |

Expected child names include connection configuration, attempt, DNS, socket connect, prelogin, TLS, login, token acquisition, redirect, initialization, and statement attempt, request build, server call, and first response.

### `span_attrs`

One row per span attribute.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent `spans.id` within the same export batch. |
| `key` | string | Attribute name. |
| `type` | int | Typed-value discriminator described above. |
| `str` | string | String value when `type == 1`. |
| `int` | long | Integer value when `type == 2`. |
| `double` | real | Floating-point value when `type == 3`. |
| `bool` | bool | Boolean value when `type == 4`. |
| `bytes` | string | Byte value when `type == 5`. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

Important client diagnostic keys include:

- Connection: `mssql.connection.guid`, `mssql.connection.outcome`, `mssql.connection.failure_phase`, attempt/retry/redirect counts, bounded settings, and `mssql.authentication.method`.
- Statement: `mssql.statement.type`, API, operation, outcome, failure phase, attempt/retry counts, protocol operation, and root-only masked `db.query.text`.
- Error summary: `mssql.error.category` and `error.type`.
- Common: `db.system.name` and `mssql.telemetry.schema.version`.

`db.query.text` is masked and bounded. Metrics never contain it.

### `span_events`

One row per sanitized event attached to a span.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP event surrogate ID. |
| `parent_id` | string | Parent `spans.id` within the same export batch. |
| `time_unix_nano` | datetime | Event timestamp. |
| `name` | string | Event name. Expected values are `mssql.driver.error`, connection/statement retry decisions, and timeout events. |
| `dropped_attributes_count` | long | Event attributes dropped before export. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

### `span_event_attrs`

One row per sanitized event attribute.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent `span_events.id` within the same export batch. |
| `key` | string | Event attribute name, such as error code, error source, timeout phase, or retry decision. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

The adapter exports structured codes and categories, not exception messages or stack traces.

### `span_links`

Schema-supported OpenTelemetry links. Current JDBC demo trees normally produce no rows.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP link surrogate ID. |
| `parent_id` | string | Parent `spans.id` within the same export batch. |
| `trace_id` | string | Linked trace ID. |
| `span_id` | string | Linked span ID. |
| `trace_state` | string | Linked W3C trace-state value. |
| `dropped_attributes_count` | long | Link attributes dropped before export. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

### `span_link_attrs`

Schema-supported link attributes. Current JDBC demo normally produces no rows.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent `span_links.id` within the same export batch. |
| `key` | string | Link attribute name. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

## Metric tables

The metric tables are created by the OTAP Kusto exporter when the first metric batch reaches a database. An older dedicated database can therefore contain only trace tables until a metrics-enabled cloud run completes.

This demo emits four metric instruments:

| Instrument | Kusto shape | Meaning |
|---|---|---|
| `db.client.operation.count` | Sum in `number_data_points` | Completed lifecycle activities |
| `db.client.operation.error.count` | Sum in `number_data_points` | Completed activities with failure, timeout, or cancellation |
| `db.client.operation.retry.count` | Sum in `number_data_points` | Additional root attempts actually begun |
| `db.client.operation.duration` | Histogram in `histogram_data_points` | Duration in seconds for completed lifecycle activities |

### `univariate_metrics`

One row per metric descriptor in an export batch.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP metric surrogate ID. Data points join to this ID within the export batch. |
| `resource_id` | string | OTAP resource surrogate ID. |
| `resource_schema_url` | string | Resource schema URL, if supplied. |
| `resource_dropped_attributes_count` | long | Resource attributes dropped before export. |
| `scope_id` | string | OTAP instrumentation-scope surrogate ID. |
| `scope_name` | string | Expected `com.microsoft.sqlserver.jdbc`. |
| `scope_version` | string | Scope version when supplied. |
| `scope_dropped_attributes_count` | long | Scope attributes dropped before export. |
| `schema_url` | string | Metric schema URL, if supplied. |
| `metric_type` | int | OTAP metric enum: gauge, sum, histogram, exponential histogram, or summary. This demo uses sums and histograms. |
| `name` | string | Metric instrument name. |
| `description` | string | Instrument description from the adapter. |
| `unit` | string | `{operation}`, `{error}`, `{retry}`, or `s`. |
| `aggregation_temporality` | int | OpenTelemetry temporality enum selected by the SDK/exporter. Do not assume delta or cumulative without decoding this field. |
| `is_monotonic` | bool | True for monotonic counters; false/not applicable for histograms. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

### `number_data_points`

Counter data points for operation, error, and retry counts.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP number-point surrogate ID. |
| `parent_id` | string | Parent `univariate_metrics.id` within the same export batch. |
| `start_time_unix_nano` | datetime | Start of the aggregation interval. |
| `time_unix_nano` | datetime | End/observation time. Use this for time filtering. |
| `int_value` | long | Integer point value when represented as an integer. |
| `double_value` | real | Floating-point point value when represented as a double. |
| `flags` | long | OpenTelemetry data-point flags. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

Use `coalesce(todouble(int_value), double_value)` carefully because zero and null have different meanings. A safer projection is `iff(isnull(double_value), toreal(int_value), double_value)`.

### `number_dp_attrs`

Bounded dimensions for a number data point.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent `number_data_points.id` within the same export batch. |
| `key` | string | Metric dimension name. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

Expected keys are `mssql.performance.activity`, `mssql.operation.kind`, `mssql.operation.outcome`, `mssql.telemetry.schema.version`, and either bounded statement dimensions or bounded authentication method. No SQL, endpoint, database, server, connection ID, statement ID, trace ID, or exception text is allowed.

### `histogram_data_points`

Explicit-bucket duration data points for `db.client.operation.duration`.

| Column | Kusto type | Description |
|---|---|---|
| `id` | string | OTAP histogram-point surrogate ID. |
| `parent_id` | string | Parent `univariate_metrics.id` within the same export batch. |
| `start_time_unix_nano` | datetime | Start of the aggregation interval. |
| `time_unix_nano` | datetime | End/observation time. |
| `count` | decimal | Number of recorded activity completions represented by the point. |
| `sum` | real | Sum of durations in the metric unit, seconds. |
| `bucket_counts` | dynamic | Counts for each explicit bucket, including the overflow bucket. |
| `explicit_bounds` | dynamic | Upper bounds in seconds. The demo advises 0.0001 through 30 seconds. |
| `flags` | long | OpenTelemetry data-point flags. |
| `min` | real | Minimum observed duration in seconds when emitted by the SDK. |
| `max` | real | Maximum observed duration in seconds when emitted by the SDK. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

Percentiles must be estimated from `bucket_counts` and `explicit_bounds`; do not calculate p95 from `sum / count`.

### `histogram_dp_attrs`

Bounded dimensions for a histogram data point.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent `histogram_data_points.id` within the same export batch. |
| `key` | string | Metric dimension name. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

The dimension contract is the same as `number_dp_attrs`.

## Shared identity tables

### `resource_attrs`

Resource attributes shared by trace or metric records.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent resource surrogate ID (`resource_id`) within the same export batch. |
| `key` | string | Resource attribute name, notably `service.name`. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator derived from service identity. |

### `scope_attrs`

Instrumentation-scope attributes. The scope name itself is stored on `spans` and `univariate_metrics`; this table contains only extra scope attributes.

| Column | Kusto type | Description |
|---|---|---|
| `parent_id` | string | Parent scope surrogate ID (`scope_id`) within the same export batch. |
| `key` | string | Scope attribute name. |
| `type` | int | Typed-value discriminator. |
| `str` | string | String value. |
| `int` | long | Integer value. |
| `double` | real | Floating-point value. |
| `bool` | bool | Boolean value. |
| `bytes` | string | Byte value. |
| `ser` | string | Serialized fallback value. |
| `export_time_unix_nano` | datetime | Export-batch timestamp. |
| `application` | string | Run/service discriminator. |

## Agent diagnostic workflow

1. Identify the unique `application` value for the run and bound every query by time.
2. Check client metrics first to determine whether the symptom is fleet-wide or isolated to failed traces.
3. Use root metrics only for throughput: `connection.open` and `statement.execute`.
4. Use per-phase histogram metrics to localize latency. Do not add nested phase durations.
5. Query failed roots in `spans`, then pivot `span_attrs` to obtain outcome, failure phase, category, type, attempts, and masked SQL.
6. Reconstruct the complete failure tree with `(application, trace_id)` and parent `span_id` relationships.
7. Join `span_events` and `span_event_attrs` for structured origin errors, timeout evidence, and retry decisions.
8. If the evidence points beyond the client boundary, explicitly report that this demo has no server-side Kusto telemetry. Do not infer server CPU, waits, blocking, query plans, or engine health from JDBC client timing alone.

## Starter queries

### Inventory tables populated for one run

```kusto
let App = '<application>';
union withsource=TableName isfuzzy=true
    spans, span_attrs, span_events, span_event_attrs, span_links, span_link_attrs,
    resource_attrs, scope_attrs, univariate_metrics, number_data_points,
    number_dp_attrs, histogram_data_points, histogram_dp_attrs
| where application == App
| summarize Rows=count(), First=min(export_time_unix_nano), Last=max(export_time_unix_nano) by TableName
| order by TableName asc
```

### Decode root failure attributes

```kusto
let App = '<application>';
let Attrs = span_attrs
| where application == App
| extend value = case(type == 1, str, type == 2, tostring(['int']),
                      type == 3, tostring(['double']), type == 4, tolower(tostring(['bool'])),
                      type == 5, bytes, ser)
| summarize attributes=make_bag(pack(key, value)) by application, parent_id, export_time_unix_nano;
spans
| where application == App
| where name in ('mssql.driver.connection.open', 'mssql.driver.statement.execute')
| join kind=leftouter Attrs on application, export_time_unix_nano, $left.id == $right.parent_id
| project start_time_unix_nano, trace_id, name, duration_ms=toreal(duration_time_unix_nano)/1000.0,
          status_code, attributes
| order by start_time_unix_nano desc
```

### Metric descriptor and point inventory

```kusto
let App = '<application>';
univariate_metrics
| where application == App
| summarize Series=dcount(id), First=min(export_time_unix_nano), Last=max(export_time_unix_nano)
    by name, metric_type, unit, aggregation_temporality, is_monotonic
| order by name asc
```

### Join counter points to metric names

```kusto
let App = '<application>';
number_data_points
| where application == App
| project application, export_time_unix_nano, point_id=id, metric_id=parent_id,
          time_unix_nano, value=iff(isnull(double_value), toreal(int_value), double_value)
| join kind=inner (
    univariate_metrics
    | where application == App
    | project application, export_time_unix_nano, metric_id=id, metric_name=name, unit
) on application, export_time_unix_nano, metric_id
| order by time_unix_nano desc
```

## Maintenance rule

Update this reference when the optional adapter adds an instrument, attribute dimension, OTLP signal, or server-side collector. Validate schemas against the target Kusto database after exporter changes. Never add a table merely because it exists in another database; include it only when this demo actually routes records to it.
