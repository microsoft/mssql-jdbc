# mssql-jdbc OpenTelemetry POC Runbook

A complete, **Docker-only** local stack and runnable workload for the OTel POC on
this branch: driver metrics to Prometheus/Grafana and traces to Jaeger.

> You need **nothing but Docker + bash** on the host. JDK, Maven, and SQL Server
> all run in containers — there is no host install of Java, Maven, or a database.
> See [contrib/README.md](../../contrib/README.md) to get a fresh machine with Docker.

```bash
cd docs/otel-poc
./.scripts/dev.sh
```

That one command builds the driver, brings the whole stack up, proves it green,
and then leaves a load generator running so the dashboards stay live. When it
finishes you will see a **GREEN** banner and these URLs:

- Grafana: http://localhost:3000  (Dashboards -> mssql-jdbc -> *mssql-jdbc - driver metrics*)
- Prometheus: http://localhost:9090
- Jaeger UI: http://localhost:16686  (service: `mssql-jdbc-poc-loadgen`)
- Collector Prometheus endpoint: http://localhost:8889/metrics

Tear it down with `./.scripts/dev.sh down` (or `clean` to also drop the SQL
Server volume and the build output).

## What `dev.sh` does

`dev.sh` is the only thing you run. Internally it:

1. **Builds** the builder/runner image (`docs/otel-poc/Dockerfile`) — JDK 11 +
   Maven with a warm dependency cache baked at image-build time.
2. **Brings up** the stack from `docker-compose.yml`: a containerized **SQL
   Server**, the **OTel Collector**, **Prometheus**, **Grafana**, and **Jaeger**.
3. **Compiles** the driver and test classes (`-Pjre11 test-compile`).
4. **Proves green** (see below).
5. **Starts a long-running load generator** (`loadgen` service) so Grafana and
   Jaeger keep receiving fresh data until you tear down.

### Definition of GREEN (asserted automatically)

| # | Gate | How it is checked |
|---|------|-------------------|
| 1 | Smoke test passes | `OtelPocSmokeTest` (JUnit) runs in a container against the SQL Server. |
| 2 | Metrics flow | Prometheus exposes at least one `db_client_*` series from the load generator. |
| 3 | Traces flow | Jaeger lists service `mssql-jdbc-poc-loadgen` with at least one trace. |
| 4 | Health probes | HTTP 200 from collector (`:13133`), Prometheus, Grafana, and Jaeger, plus a healthy SQL Server container. |

If any gate fails, `dev.sh` prints the relevant container logs and exits non-zero.

## Commands

```bash
./.scripts/dev.sh            # = up: build, start, prove green, leave loadgen running
./.scripts/dev.sh status     # container status + live health summary
./.scripts/dev.sh logs       # tail all logs
./.scripts/dev.sh logs loadgen   # tail just the load generator
./.scripts/dev.sh down       # stop + remove the stack (keeps the SQL volume)
./.scripts/dev.sh clean      # stop + remove the stack, the SQL volume, and target/
```

### Useful overrides (environment variables)

| Variable | Default | Purpose |
|----------|---------|---------|
| `MSSQL_SA_PASSWORD` | `Otel_Poc_Str0ng!Pass` | SA password for the containerized SQL Server. |
| `SQL_SERVER_IMAGE` | `mcr.microsoft.com/mssql/server:2022-latest` | SQL Server image (auto-switches to `azure-sql-edge` on arm64). |
| `BURST_SECONDS` | `60` | Length of the timed load burst used for the green gate. |

Example: `MSSQL_SA_PASSWORD='My$tr0ngPwd!' BURST_SECONDS=120 ./.scripts/dev.sh`

## Files in this folder

- `Dockerfile` — builder/runner image (JDK 11 + Maven + warm cache + curl/jq).
- `docker-compose.yml` — SQL Server, collector, Prometheus, Grafana, Jaeger, and the `app`/`loadgen` runners.
- `otel-collector-config.yaml` — collector pipelines + `health_check` extension.
- `prometheus.yml` — scrape config for the collector's Prometheus endpoint.
- `grafana/provisioning/...` and `grafana/dashboards/mssql-jdbc.json` — auto-provisioned datasources + dashboard.
- `.scripts/dev.sh` — the host entrypoint.

The driver/test sources exercised by the POC:

- Load generator: `src/test/java/com/microsoft/sqlserver/jdbc/otel/OtelPocLoadGen.java`
- Smoke test: `src/test/java/com/microsoft/sqlserver/jdbc/otel/OtelPocSmokeTest.java`

## Using your own OTel endpoint (optional)

If you already have an OTLP/HTTP backend (Azure Monitor, Grafana Cloud,
Honeycomb, Datadog, a remote collector, etc.) you can point the driver straight
at it instead of the local collector. Run the load generator in the `app`
container with your endpoint:

```bash
cd docs/otel-poc
docker compose run --rm app mvn -B -Pjre11 \
  org.codehaus.mojo:exec-maven-plugin:3.1.0:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen \
  -Dexec.args="forever" \
  -DotelEndpoint=https://otlp.example.com/v1/metrics \
  -DotelExportInterval=10
```

For backends that need an API key, add `otelHeaders` to the JDBC connection
string via `MSSQL_SA_PASSWORD`-style env injection, or bake it into
`mssql_jdbc_test_connection_properties`:

```
...;otelEndpoint=https://otlp.example.com/v1/metrics;otelHeaders=x-api-key=<YOUR_KEY>;otelServiceName=my-team-app;
```

Common endpoint formats:

| Backend                | `otelEndpoint` value                                      | Header needed |
|------------------------|-----------------------------------------------------------|---------------|
| Grafana Cloud          | `https://<instance>.grafana.net/otlp/v1/metrics`          | `Authorization=Basic <base64(user:token)>` |
| Honeycomb              | `https://api.honeycomb.io/v1/metrics`                     | `x-honeycomb-team=<API_KEY>` |
| Azure Monitor (via collector) | `http://<collector-host>:4318/v1/metrics`          | none (collector handles auth) |
| Datadog                | `https://api.datadoghq.com/api/v0.2/series`               | `DD-API-KEY=<KEY>` |
| Local collector        | `http://otel-collector:4318/v1/metrics`                   | none |

> The driver ships **metrics and traces on the same base endpoint**. It derives
> the traces URL by replacing `/v1/metrics` with `/v1/traces`. Make sure your
> backend or collector accepts both paths, or use a collector as a fan-out proxy.

## OTel connection-string properties

The load generator sets these automatically; you can override them via `-D`
system properties (e.g. `-DotelEndpoint=...`) or set them directly in
`mssql_jdbc_test_connection_properties`.

| Property              | Example value                              | Description                                                      |
|-----------------------|--------------------------------------------|------------------------------------------------------------------|
| `otelEndpoint`        | `http://otel-collector:4318/v1/metrics`    | Required to activate OTel export. Point at the collector or any OTLP/HTTP backend. |
| `otelServiceName`     | `mssql-jdbc-poc-loadgen`                   | `service.name` resource attribute on OTel spans/metrics          |
| `otelExportInterval`  | `5`                                        | Metric export interval in seconds (default 60)                   |
| `otelHeaders`         | `Authorization=Bearer eyJ...,X-Tenant=demo`| Comma-separated `key=value` pairs sent as HTTP headers           |

## What you should see

**Grafana** — http://localhost:3000 -> Dashboards -> mssql-jdbc ->
*mssql-jdbc - driver metrics*. Panels populate within ~10 s of the load burst.

**Jaeger** — http://localhost:16686, service `mssql-jdbc-poc-loadgen`. Example
operations:

- `db.client.statement.execute`
- `db.client.statement.prepexec`
- `db.client.connection`

**Collector logs** (metric + trace export) — `./.scripts/dev.sh logs otel-collector`.

## Notes

- The driver uses the connection property `otelEndpoint` to turn OTel export on.
- If `GlobalOpenTelemetry` is already set by the host app, the driver reuses it
  (this is exactly how `OtelPocSmokeTest` asserts metrics in-process).
- Existing `PerformanceLogCallback` duration behavior remains backward-compatible.
- Builds run as root inside the container, so `target/` written into the repo is
  root-owned; `./.scripts/dev.sh clean` removes it for you.
