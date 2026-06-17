# mssql-jdbc OpenTelemetry POC Runbook

Follow [README](../../contrib/README.md) to setup a fresh WSL machine with Docker.

This folder contains a complete local stack and runnable workload for the OTel
POC in this branch: metrics to Prometheus/Grafana and traces to Jaeger.

## Branch Scope

Changes in this branch wire OTel export from the driver and provide:

- OTel callback + bootstrap in the driver.
- Load generator and smoke test in test sources.
- Docker Compose stack for collector, Prometheus, Grafana, and Jaeger.
- Provisioned Grafana datasources/dashboard.

## Files You Need

- Stack entrypoint: docs/otel-poc/docker-compose.yml
- Collector config: docs/otel-poc/otel-collector-config.yaml
- Prometheus config: docs/otel-poc/prometheus.yml
- Grafana datasources: docs/otel-poc/grafana/provisioning/datasources/datasource.yml
- Grafana dashboard provider: docs/otel-poc/grafana/provisioning/dashboards/provider.yml
- Grafana dashboard JSON: docs/otel-poc/grafana/dashboards/mssql-jdbc.json
- Load generator: src/test/java/com/microsoft/sqlserver/jdbc/otel/OtelPocLoadGen.java
- Smoke test: src/test/java/com/microsoft/sqlserver/jdbc/otel/OtelPocSmokeTest.java

## Get The Code

```powershell
git clone https://github.com/microsoft/mssql-jdbc.git
cd mssql-jdbc
git checkout users/machavan/otelexperiment
```

Verify you are on the right branch:

```powershell
git branch --show-current
# should print: users/machavan/otelexperiment
```

## Prerequisites

| Requirement | Minimum version | Notes |
|-------------|----------------|-------|
| JDK | 11 (build target) — JDK 26 recommended | Any JDK ≥ 11 works for the `jre11` Maven profile used in all commands below. JDK 26 is required to build the `jre26` profile or the full multi-profile build. |
| Maven | 3.9+ | The project uses `mvn` from `PATH`. If Maven is not on `PATH`, prefix every `mvn` command with the full path, e.g. `C:\tools\apache-maven-3.9.9\bin\mvn`. |
| Docker Desktop | Latest | Required only for `docker compose up`. If you prefer a native stack (Windows binaries for otelcol, Prometheus, Grafana, Jaeger) see the "Native Windows stack" section below. |
| SQL Server | 2016+ / Azure SQL / LocalDB | Any instance reachable from the machine running the load generator. |

### Install JDK (if needed)

Download from https://adoptium.net (Temurin) or https://www.microsoft.com/openjdk.

Verify: `java -version`

### Install Maven (if needed)

Download from https://maven.apache.org/download.cgi, extract, and add `bin/` to `PATH`.

Verify: `mvn -version`

## Using Your Own OTel Endpoint

If you already have an OTLP/HTTP backend (Azure Monitor, Grafana Cloud, Honeycomb,
Datadog, a remote collector, etc.) you can skip `docker compose up` entirely and
point the driver straight at it.

**Step 1 — Set your connection string as normal:**

```powershell
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://<HOST>:1433;user=<USER>;password=<PASSWORD>;trustServerCertificate=true;"
```

**Step 2 — Pass your endpoint and any auth headers via `-D` system properties:**

```powershell
mvn -B -Pjre11 exec:java -Dexec.classpathScope=test `
  -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen `
  -Dexec.args="forever" `
  -DotelEndpoint=https://otlp.example.com/v1/metrics `
  -DotelExportInterval=10
```

For backends that require an API key or auth header, append it directly in the
connection string (the `otelHeaders` property is the cleanest way):

```powershell
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://<HOST>:1433;...;otelEndpoint=https://otlp.example.com/v1/metrics;otelHeaders=x-api-key=<YOUR_KEY>;otelServiceName=my-team-app;"
```

Common endpoint formats:

| Backend                | `otelEndpoint` value                                      | Header needed |
|------------------------|-----------------------------------------------------------|---------------|
| Grafana Cloud          | `https://<instance>.grafana.net/otlp/v1/metrics`          | `Authorization=Basic <base64(user:token)>` |
| Honeycomb              | `https://api.honeycomb.io/v1/metrics`                     | `x-honeycomb-team=<API_KEY>` |
| Azure Monitor (via collector) | `http://<collector-host>:4318/v1/metrics`          | none (collector handles auth) |
| Datadog                | `https://api.datadoghq.com/api/v0.2/series`               | `DD-API-KEY=<KEY>` |
| Local collector        | `http://localhost:4318/v1/metrics`                        | none |

> The driver ships **metrics and traces on the same base endpoint**. It derives
> the traces URL by replacing `/v1/metrics` with `/v1/traces`. Make sure your
> backend or collector accepts both paths, or use a collector as a fan-out proxy.

## Bring Up The Local Observability Stack (Docker)

From repository root:

```powershell
cd docs\otel-poc
docker compose up -d
```

Verify containers:

```powershell
docker compose ps
```

## URLs

- Grafana: http://localhost:3000
- Prometheus: http://localhost:9090
- Jaeger UI: http://localhost:16686
- OTLP HTTP ingest (driver -> collector): http://localhost:4318/v1/metrics and http://localhost:4318/v1/traces
- Prometheus scrape endpoint (collector): http://localhost:8889/metrics

## Build Driver And Test Classes

From repository root:

```powershell
mvn -B -Pjre11 -DskipTests test-compile
```

## Run The Load Generator (Post-build)

### 1. Set your SQL Server connection string

Replace `<HOST>`, `<USER>`, and `<PASSWORD>` with your values.
`trustServerCertificate=true` is needed for local/self-signed SQL Server instances.

```powershell
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://<HOST>:1433;user=<USER>;password=<PASSWORD>;trustServerCertificate=true;"
```

Example (LocalDB):
```powershell
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://localhost\SQLEXPRESS:1433;user=sa;password=YourPassword;trustServerCertificate=true;"
```

### 2. OTel connection-string properties (all optional)

The load generator appends these automatically; you can override them via JVM system properties:

| JVM system property   | Connection-string property | Default                                  | Description                                 |
|-----------------------|----------------------------|------------------------------------------|---------------------------------------------|
| `otelEndpoint`        | `otelEndpoint`             | `http://localhost:4318/v1/metrics`       | OTLP/HTTP endpoint the driver POSTs to      |
| `otelExportInterval`  | `otelExportInterval`       | `5` (seconds)                            | How often metrics/traces are flushed        |
| `loadgen.sleepMs`     | —                          | `200` (ms)                               | Sleep between load iterations               |

You can also set them directly in the connection string passed to `mssql_jdbc_test_connection_properties`:

```
...;otelEndpoint=http://localhost:4318/v1/metrics;otelServiceName=my-app;otelExportInterval=10;otelHeaders=Authorization=Bearer <token>;
```

All available `otel*` connection-string properties:

| Property              | Example value                              | Description                                                      |
|-----------------------|--------------------------------------------|------------------------------------------------------------------|
| `otelEndpoint`        | `http://localhost:4318/v1/metrics`         | Required to activate OTel export. Point at the collector or any OTLP/HTTP backend. |
| `otelServiceName`     | `mssql-jdbc-poc-loadgen`                   | `service.name` resource attribute in OTel spans/metrics          |
| `otelExportInterval`  | `5`                                        | Metric export interval in seconds (default 60)                   |
| `otelHeaders`         | `Authorization=Bearer eyJ…,X-Tenant=demo` | Comma-separated `key=value` pairs sent as HTTP headers           |

### 3. Run commands

Run forever (Ctrl+C to stop):

```powershell
mvn -B -Pjre11 exec:java -Dexec.classpathScope=test -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen -Dexec.args="forever"
```

Run for 120 seconds:

```powershell
mvn -B -Pjre11 exec:java -Dexec.classpathScope=test -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen -Dexec.args="120 s"
```

Override OTel properties at the command line:

```powershell
mvn -B -Pjre11 exec:java -Dexec.classpathScope=test -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen -Dexec.args="forever" -DotelEndpoint=http://localhost:4318/v1/metrics -DotelExportInterval=5 -Dloadgen.sleepMs=200
```

## Run The Smoke Test

```powershell
mvn -B -Pjre11 -Dtest=OtelPocSmokeTest -DfailIfNoTests=false test
```

## See Metrics In Grafana

- Open http://localhost:3000
- Dashboards -> mssql-jdbc -> mssql-jdbc - driver metrics

## See Traces In Jaeger

- Open http://localhost:16686
- Service: mssql-jdbc-poc-loadgen
- Operation examples:
  - db.client.statement.execute
  - db.client.statement.prepexec
  - db.client.connection

You can also query Jaeger API directly:

```powershell
Invoke-RestMethod -Uri "http://localhost:16686/api/services"
Invoke-RestMethod -Uri "http://localhost:16686/api/traces?service=mssql-jdbc-poc-loadgen&limit=5&lookback=1h"
```

## See Collector Logs (Metrics + Trace Export)

```powershell
cd docs\otel-poc
docker compose logs -f otel-collector
```

## See Jaeger Logs

```powershell
cd docs\otel-poc
docker compose logs -f jaeger
```

## Stop Everything

```powershell
cd docs\otel-poc
docker compose down
```

## Notes

- The driver uses connection property otelEndpoint to turn OTel export on.
- If GlobalOpenTelemetry is already set by the host app, the driver reuses it.
- Existing PerformanceLogCallback duration behavior remains backward-compatible.
