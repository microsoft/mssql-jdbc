# Complete local JDBC OpenTelemetry demo

This is the executable companion to the [feature reference](../../user-otel-endpoint.md).
It runs **local SQL Server → JDBC automatic OTel provider → Collector →
Prometheus/Jaeger → live diagnostics dashboard**, with Grafana as an additional viewer.
No cloud account, Azure service, existing database, application callback, or Java
agent is needed. It uses packaged driver JARs built from this checkout.

The authoritative designs are the [connection error/span contract](../../mssql-jdbc-connection-error-category-mapping.md),
[performance activity metrics proposal](../../mssql-jdbc-performance-activity-metrics-proposal.md),
and [statement spans proposal](../../mssql-jdbc-statement-spans-proposal.md).
The [feature reference](../../user-otel-endpoint.md) describes the current
in-progress implementation and reconciles remaining differences: automatic
`otelEndpoint` selection owns a private SDK/transport, not the proposals'
application-owned SDK configuration. A successful demo does not certify every
proposal acceptance criterion.

## Let an agent run it

Open this branch in VS Code with GitHub Copilot Agent mode. Invoke
**`/user-otel-demo`**, or ask:

> Run the complete local JDBC user-endpoint OpenTelemetry demo using
> docs/otel-poc/user-endpoint-demo/README.md. Check prerequisites first and ask
> before accepting the SQL Server license. Show the live diagnostics dashboard
> and verify real connection, statement, and prepared-statement telemetry.

The [repository prompt](../../../.github/prompts/user-otel-demo.prompt.md) guides
the agent through prerequisites, license approval, execution, evidence, and cleanup.
An agent without VS Code prompt support can follow this README directly.

## Prerequisites and safety

- Native **x64** Windows/Linux or Intel macOS, with a **local Linux x64 Docker
  engine** and Docker Compose v2. SQL Server ARM/Apple Silicon emulation is not
  supported by this workflow. Remote Docker engines are rejected.
- **PowerShell 7+**, **JDK 21** (`java`, `javac`, `jar`) and **Maven** on PATH.
  The runner honors an existing `JAVA_HOME`; it does not install tools or change
  global configuration. Host Node/npm is **not required** for the Docker dashboard.
- Public network access for Maven Central, npm, and container images; allow several
  GiB of downloads/disk and at least **6 GiB free memory** for SQL plus monitoring
  and builds. First run can take several minutes.
- Free loopback ports: **14333, 4318, 8889, 13133, 9090, 16686, 3000, 3002**.
  The runner stops on conflicts; it never terminates another process/container.
  A different project name does not change these ports.
- Explicit acceptance of the [SQL Server container license](https://go.microsoft.com/fwlink/?linkid=857698)
  before supplying `-AcceptSqlServerLicense`. SQL runs Developer edition for testing.

Images are pinned to a historical compatible demo baseline, **not asserted current,
secure, or production-approved**: SQL Server 2022 CU20, Collector 0.111.0,
Prometheus 2.55.0, Jaeger 1.62.0, Grafana 11.2.2, and the frontend Dockerfile's
Node/Nginx versions. Review security status before running; dependency installation
has reported advisories. Do not expose this unauthenticated stack publicly.

The runner generates a random SQL password in process memory and passes it through
the environment, never a file or command-line password argument. Docker admins can
still inspect container environment variables. Do not use shell transcription,
debug tracing, environment dumps, or resolved `docker compose config` output.
Only the dedicated local SQL container is used. Workload queries are constant
reads, a two-second `WAITFOR`, and deliberate syntax errors; no database objects
are created. Local self-signed SQL TLS uses demo-only `trustServerCertificate=true`.

## Run manually

From the repository root, using PowerShell 7 on any supported OS:

```powershell
pwsh -NoProfile -File ./docs/otel-poc/user-endpoint-demo/run.ps1 -Action Start -AcceptSqlServerLicense
```

`Start` performs these steps and fails on missing evidence:

1. Check prerequisites, project ownership, and host port availability.
2. Build/install core with Maven `jre21`, package the optional OTel JAR, verify its
   provider descriptor, resolve the classpath, and compile the checked-in workload.
   Integration tests are intentionally skipped; this is a demo build, not certification.
3. Build the React/Fluent dashboard in Docker and launch all six Compose services.
4. Wait for SQL, telemetry backends, and dashboard proxy readiness.
5. Generate configuration and DNS failures, then five local SQL iterations. Each
   iteration includes a successful physical open, fast and slow statements, an
   ordinary statement syntax failure, and prepared-statement success/failure.
6. Verify real driver metric families/root counters, successful and failed roots
   for both statement types, SQL error `102` origin events, connection error origins,
   and a measured successful statement root longer than one second.

The deterministic workload uses `otelLogLevel=2`, no thresholds, no authentication
callback, and a loopback OTLP endpoint. It flushes and allows multiple scrapes;
flush completion alone is not delivery acknowledgement.

| Viewer | URL |
| --- | --- |
| Screenshot-style live dashboard | <http://127.0.0.1:3002> |
| Grafana | <http://127.0.0.1:3000/d/jdbc-user-endpoint> |
| Jaeger | <http://127.0.0.1:16686> |
| Prometheus | <http://127.0.0.1:9090> |
| Driver metric exposition | <http://127.0.0.1:8889/metrics> |

The live dashboard refreshes every 30 seconds; select a failure row to open
**Trace / Attributes / Events**. Counts cover only returned Jaeger traces (last
24 hours, maximum 1000), not all failures. Prometheus stores two hours in this
ephemeral stack, so a 24-hour query only includes retained data. Metric counter
increases need two scrapes and omit the first observed value; histogram percentiles
and five-minute throughput rates are estimates. Missing observations remain `—`.
Masked SQL is displayed only from emitted statement-root `db.query.text` after a
fail-closed lexical check; absent/rejected text stays unavailable. Raw SQL is never
queued or displayed. Masking preserves query shape and may retain identifiers, so
review screenshots before sharing. Exact maximum duration remains unavailable:
classic Prometheus histogram buckets/sums do not retain it. Region appears only
when emitted, never inferred or fabricated.

## Current signal contract

The checked-in [runner](run.ps1), [workload harness](UserEndpointDemo.java),
[Compose stack](compose.yaml), [Collector configuration](collector.yaml),
[Prometheus configuration](prometheus.yaml), and [Grafana assets](grafana/dashboards/jdbc-user-endpoint.json)
are the executable sources of truth; do not materialize a second set of payloads.
The dashboard image owns its [Nginx configuration](../internal/fdh-ui/nginx.conf),
which resolves `jaeger` and `prometheus` through Compose service DNS. Host Vite
development proxies remain loopback-only.

- Four instruments: `db.client.operation.count`, `.duration`, `.error.count`, and
  `.retry.count`. The provider shares the bounded projection in
  [PerformanceActivityMetrics](../../../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/PerformanceActivityMetrics.java).
  Counts/durations cover supported lifecycle END events, including successes;
  root retries count additional attempts actually begun, not retry decisions.
- Metric identities use `mssql.performance.activity` with dotted values such as
  `connection.open`, `statement.execute`, and `statement.first_response`, plus
  `mssql.operation.kind` and schema version `1.0`. Count/duration include outcome;
  error/retry counters omit outcome. Bounded statement type, server-call protocol,
  connection authentication method, and retry reason apply as specified in the
  feature reference. Missing error/retry series are not zero; the baseline does
  not force retries.
- **Dashboard/alert migration:** old `mssql_activity` / `mssql_activity_type`
  labels and raw enum values must be replaced with `mssql_performance_activity` /
  `mssql_operation_kind` and dotted activity values. Outcome translates to
  `mssql_operation_outcome`. The [Grafana queries](grafana/dashboards/jdbc-user-endpoint.json)
  and [live adapter](../internal/fdh-ui/src/live.ts) use the new schema, outcome
  distributions, root retry breakdowns, and per-activity means/percentiles. Never
  sum nested phase durations or treat `statement.first_response` as root latency;
  with full buffering it can include the complete response.
- Duration is recorded in seconds with 17 explicit boundary advice values from
  `0.0001` through `30` seconds. Meter/tracer scope is `com.microsoft.sqlserver.jdbc`
  with core package implementation version (fallback: driver major/minor).
  The private resource is `service.name=mssql-jdbc`, with a 60-second metric reader.
- Trace levels/strict root-duration thresholds still apply. The private
  `parentBased(alwaysOn())` sampler also suppresses a whole tree under a valid
  unsampled parent; eligible metric observations remain. No valid parent means
  eligible roots are sampled, not that an application's custom sampler is called.
- Approved root-only masked SQL is captured synchronously at statement END;
  the bounded lexical sanitizer removes comments and replaces literals with `?`.
  Root diagnostics replay only approved names/values within root timestamps, at
  most 128 entries; error-origin events remain on their originating spans. Only
  diagnostics actually captured by the executed path are expected.

## Exploratory levels and thresholds

These are **additional checks**, not claims made by a successful baseline run.
The runner and checked-in harness deliberately fix level `2` with absent thresholds;
they do not read `DEMO_LEVEL`, `DEMO_CONNECTION_THRESHOLD`, or
`DEMO_STATEMENT_THRESHOLD`, and have no `validate` mode. Do not set those obsolete
variables and assume an experiment ran. For deterministic level, strict-boundary,
invalid-option and callback checks, use the existing
[provider tests](../../../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java)
and [property tests](../../../src/test/java/com/microsoft/sqlserver/jdbc/UserOtelPropertiesTest.java).
Any separately approved live variant should reuse the checked-in assets and adapt
only the harness's `telemetry()` properties; it is not the baseline verified by
the runner. Never replace automatic provider selection with a manual callback.

| Options under test | Expected new trace trees; metrics remain eligible in every row |
| --- | --- |
| `otelLogLevel=0`, absent thresholds | None, including forced failures. |
| `otelLogLevel=1`, absent thresholds | Failed roots only; a successful root with a failed retried child does not qualify. |
| `otelLogLevel=2`, absent thresholds | Successful and failed roots on supported paths. |
| Level `2`, connection threshold `9223372036.854775807`, statement threshold `1` | No practical connection trees; only statement roots measured above one second. Fast successes and failures are filtered alike. |
| Level `2`, both thresholds `9223372036.854775807` | No practical trees for this short workload, even its failures. |

Threshold property names are `otelConnectionSpanDurationThreshold` and
`otelStatementSpanDurationThreshold`, in decimal **seconds**. Selection is strict
`duration > threshold`, combined with the level. Absent differs from explicit zero:
zero excludes a zero-duration root. Negative, nonfinite and out-of-range values
must be rejected with sanitized errors before creating the telemetry route; no
telemetry is expected for an invalid telemetry option. Use synthetic provider
tests for exact equality rather than comparing wall-clock SQL sleep durations.

Record a bounded time window/trace IDs for each experiment so old traces cannot
masquerade as new evidence. If a reset is needed, use the runner's owned
`Stop -RemoveVolumes` then `Start` lifecycle with the same project/state arguments,
never direct teardown of another stack. A reset/start also regenerates baseline
data: account for it in the experiment window. Inspect actual root durations;
`WAITFOR`, delayed result consumption, and a slow phase alone do not prove a slow
execute root. Keep full buffering for the baseline, and report missing qualifying
roots as unverified. A valid unsampled application parent suppresses the entire
eligible tree, not metrics; this harness installs no application parent and does
not promise connection and statement roots share a trace.

### Evidence, privacy, and authentication boundaries

- Inspect root `connection.open` and `statement.execute` observations, applicable
  children, origin error events and safe masked SQL. Never sum nested activities
  as user-operation totals. Counts reset in a new JVM; missing series are not zero.
  Use the checked-in Grafana queries rather than maintaining copied PromQL here.
- Review payloads locally before sharing: no raw SQL/literals, credentials,
  connection strings, server addresses, exception messages or stack traces are
  expected from this route. Masked SQL can retain identifiers/query shape. A
  harmless nonsecret literal sentinel can test masking; never print real secrets.
  This does not certify other callbacks, JUL, proxies or SDK diagnostics.
- The Collector receiver is unauthenticated. Do not send real tokens to plaintext
  OTLP or infer token validation from successful ingestion. Provider wire tests
  check callback/header behavior with recording receivers; issuer, audience and
  signature validation need a separately approved authenticated receiver.
- SQL success, backend health and a flush boolean are not delivery acknowledgements.
  Bounded asynchronous queues, incomplete trees, outages and process exit can lose
  telemetry. Collector accepted/refused/export-failed counters describe only that
  hop, not driver-side drops. Do not infer losslessness from absent refused series.
- Report infrastructure, packaged SPI/classpath, configuration/DNS failures,
  SQL success, statement/prepared success/failure, metrics, levels/thresholds and
  privacy separately. Include unrun checks and safe timestamps/trace IDs; fixture
  checks are not live ingestion evidence.

## Repeat, verify, and clean up

Use the same checkout and optional `-Project`/`-StateDirectory` for every action:

```powershell
pwsh -NoProfile -File ./docs/otel-poc/user-endpoint-demo/run.ps1 -Action Workload -Iterations 5
pwsh -NoProfile -File ./docs/otel-poc/user-endpoint-demo/run.ps1 -Action Verify
pwsh -NoProfile -File ./docs/otel-poc/user-endpoint-demo/run.ps1 -Action Stop -RemoveVolumes
```

State and compiled/classpath files live in the ignored checkout build-output
directory under `target/user-endpoint-demo/<Project>`. The manifest contains no
secrets and protects against adopting/removing another project. Keep it until
cleanup finishes. `Stop` removes only owned Compose containers/network; the switch
also removes owned anonymous data volumes. Shared images, Maven caches, build
outputs, and state remain. No Docker prune is used. Restart with `Start` only after
`Stop`; it rebuilds and generates a new password. Telemetry is disposable.

If a command fails, the owned stack remains available for diagnosis. Resolve the
reported prerequisite/port blocker; do not take over a conflicting stack. For
workload/evidence failures, inspect the owned services locally without publishing
secrets, then rerun `Workload` or use scoped `Stop`. DNS behavior can differ by
resolver; a DNS verification timeout means that phase is **not verified**.

## Frontend checks (optional)

With Node 22+ installed, from the frontend directory:

```powershell
Set-Location ./docs/otel-poc/internal/fdh-ui
npm ci
npm test
npm run build
npm run test:browser
```

The optional browser test requires Node with built-in `WebSocket` support, an
existing Chromium/Edge installation (`BROWSER_PATH` on non-default systems), and
the running demo. It exercises both failure drawers, all tabs, search, refresh,
metrics, and browser runtime errors. A skipped browser test is not a pass. It
saves a screenshot to the OS temporary directory, not the checkout.

Deterministic adapter/browser fixtures exercise masked SQL display rather than
assuming all SQL is unavailable; they must still reject raw/child/resource SQL
and leave exact maxima and unreported regions unavailable. Fixture checks are not
evidence of live SQL ingestion or exhaustive privacy/acceptance coverage.
