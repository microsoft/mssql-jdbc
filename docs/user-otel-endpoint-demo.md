# Agent-run local user-endpoint OpenTelemetry demo

> **For the complete, runnable local demo**, use the [checked-in demo runner and
> guide](otel-poc/user-endpoint-demo/README.md), or invoke `/user-otel-demo` in
> Copilot Agent mode. It includes local SQL Server and the screenshot-style live
> dashboard. The manual materialization workflow below is an alternative; do not
> run both stacks on the same host ports.

## Contract and safety gate

This is a **runbook, not an executed demo**. No installation, image pull, build, SQL connection, or runtime verification is implied by this document. A future agent should execute it only when asked to run the demo. Create the generated files below **outside the checkout**, in one disposable temporary directory. Do not scaffold a new Maven project, edit driver sources, register a performance callback, or install a Java agent.

Use only a user-approved **test SQL Server/database and test credentials**. This demo makes real physical connections, executes constant read-only SQL, deliberately submits invalid SQL, and can hold a connection during a two-second `WAITFOR`. It creates no database objects. Do not run against production or invent database credentials. If test SQL access is unavailable, run only the explicitly SQL-free configuration-failure case and report successful connection/statement coverage as **not verified**.

The observability stack is public OpenTelemetry Collector Contrib, Prometheus, Jaeger, and Grafana. No Microsoft telemetry ingestion, endpoint discovery, cloud resources, identity service, or subscription is required. The JDBC driver is the component under test. Maven and Docker still need network access for public dependencies/images; dependencies on the broad test classpath are not evidence of cloud credential use.

Before execution, confirm Docker Engine/Desktop with Linux containers and Compose v2, JDK **21** (`java` and `javac`), Maven, sufficient disk/RAM (allow roughly 2–4 GiB for this small stack), and unused host ports listed below. Bash examples target Linux/macOS; PowerShell examples target PowerShell 7 on Windows. Do not silently install missing prerequisites. Builds populate repository build outputs and the local Maven cache; all *demo-generated* sources/configuration/classpath files stay in the temporary directory.

Images are deliberately pinned to a mutually compatible historical demo baseline, **not asserted current, vulnerability-free, or production-approved**:

| Component | Public image |
| --- | --- |
| Collector | `otel/opentelemetry-collector-contrib:0.111.0` |
| Jaeger | `jaegertracing/all-in-one:1.62.0` |
| Prometheus | `prom/prometheus:v2.55.0` |
| Grafana OSS | `grafana/grafana:11.2.2` |

Review their security status before use; obtain approval for replacements and recheck configuration/metric translation. Tags are version-pinned, not immutable digests; record resolved image digests if exact provenance matters. Never expose this unauthenticated historical stack beyond the host. Pulls, storage, CPU, and SQL usage have cost even though no paid monitoring backend is configured.

## 1. Ground truth to preserve

The authoritative designs are the [connection error/span contract](mssql-jdbc-connection-error-category-mapping.md),
[performance activity metrics proposal](mssql-jdbc-performance-activity-metrics-proposal.md),
and [statement spans proposal](mssql-jdbc-statement-spans-proposal.md). The
[feature reference](user-otel-endpoint.md) reconciles current implementation with
those designs, including intentional automatic-endpoint/private-SDK configuration
differences. This runbook is not exhaustive proposal acceptance evidence.

Read these current sources again if the branch changes:

| Source | Relevant contract |
| --- | --- |
| [Core configuration](../src/main/java/com/microsoft/sqlserver/jdbc/UserOtelConfiguration.java) | Five allowlisted options, early sanitized validation, `ServiceLoader`, fail-open optional provider. |
| [Automatic provider](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryProvider.java) | Actual metrics, dimensions, seconds, root filtering, private SDKs, bounded queues, lifecycle APIs. |
| [Shared metric projection](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/PerformanceActivityMetrics.java) and [core END enrichment](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceLog.java) | Four instruments, bounded schema, terminal outcomes, inherited type/authentication, and attempts-actually-begun retries. |
| [Transport configuration](../mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OtlpConfiguration.java) | Base endpoint becomes `/v1/traces` and `/v1/metrics`; optional per-message token callback. |
| [Root POM](../pom.xml) and [optional POM](../mssql-jdbc-otel/pom.xml) | Core and optional artifact version `13.6.0`; core `jre21` profile; standalone optional module uses OpenTelemetry `1.41.0`, Java source/target 8, core dependency **provided**. |
| [SPI descriptor](../mssql-jdbc-otel/src/main/resources/META-INF/services/com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider) | Registers `com.microsoft.sqlserver.jdbc.otel.UserEndpointTelemetryProvider`. Must survive packaging. |
| [Provider tests](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java) | SPI discovery, real core configuration failure, fixture trees, wire assertions, captured-loader isolation, thresholds and fail-closed callback tests. Not full live SQL certification. |
| [Property tests](../src/test/java/com/microsoft/sqlserver/jdbc/UserOtelPropertiesTest.java) | Exact names and sanitized validation. |
| [Statement implementation](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java) and [activities](../src/main/java/com/microsoft/sqlserver/jdbc/PerformanceActivity.java) | Execute invocation/attempt scopes; timing is not total result-set consumption. |
| [Feature reference](user-otel-endpoint.md) | Detailed privacy, bounds, authentication and ownership caveats. |

Use `UserEndpointDemo` to exercise the automatic user-supplied endpoint route. Do not register a programmatic adapter or manual callback in place of that route.

### Metrics and Prometheus translation

Every ingested supported lifecycle **END** with nonnegative duration (root, attempt, or phase) records an operation and duration; terminal failure/timeout/cancellation also records an error. START and unsupported legacy-only wrappers are not counted. Levels, thresholds, and parent trace sampling do not filter metrics, but ingestion/export losses still apply.

| OTLP instrument | Unit / kind | Prometheus exposition with this configuration |
| --- | --- | --- |
| `db.client.operation.count` | `{operation}`, monotonic counter | `db_client_operation_count_total` |
| `db.client.operation.error.count` | `{error}`, monotonic counter | `db_client_operation_error_count_total` |
| `db.client.operation.duration` | `s`, double histogram | `db_client_operation_duration_seconds_bucket`, `_sum`, `_count` |
| `db.client.operation.retry.count` | `{retry}`, monotonic counter | `db_client_operation_retry_count_total` |

The provider divides nanoseconds by `1,000,000,000`: **not milliseconds**. Collector 0.111.0 normalizes dots, adds the `seconds` unit suffix, ignores brace annotation units, and adds `_total` to monotonic sums with `add_metric_suffixes: true`. See the pinned [translator](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.111.0/pkg/translator/prometheus/normalize_name.go) and [exporter contract](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.111.0/exporter/prometheusexporter/README.md). Verify actual exposition before accepting dashboards; do not silently change queries to hypothetical `_ms` families.

All instruments use `mssql.performance.activity` (stable dotted names), `mssql.operation.kind` (`connection`/`statement`), and `mssql.telemetry.schema.version=1.0`. Prometheus translates these to **`mssql_performance_activity`**, **`mssql_operation_kind`**, and **`mssql_telemetry_schema_version`**. Count/duration also carry `mssql_operation_outcome` (`success`, `failure`, `timeout`, `canceled`); error/retry counters deliberately omit outcome. Statement activities carry `mssql_statement_type`; server-call activities alone add `mssql_statement_protocol_operation`. Connection activities carry `mssql_connection_auth_method`. Retry counts alone add `mssql_retry_reason`. Values come from closed registries, not SQL or IDs.

**Migration warning:** old dashboards/alerts using `mssql_activity`, `mssql_activity_type`, or raw enum values must migrate; those are not aliases for the new metric schema. The span attribute `mssql.activity` is separate. Scope/resource/scrape machinery can add labels such as `otel_scope_name`, `otel_scope_version`, `job`, or `instance`. Scope is `com.microsoft.sqlserver.jdbc` with the core package implementation version (fallback: driver major/minor). Resource service name is fixed to `mssql-jdbc`; resource-to-point conversion is disabled below.

Select root activity `connection.open` or `statement.execute` when counting user operations. Summing roots, attempts, and children double-counts nested work. A phase histogram is not root latency. Explicit boundary advice in seconds is `0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30`. Compute each activity's mean from its own sum/count; never add nested durations. Classic Prometheus histograms do not preserve exact maxima.

Retry counts use `max(attempt_count - 1, 0)` on root END only, with an approved common reason from additional attempts actually begun (otherwise `not_applicable`). A retry decision alone does not count. Missing error/retry series do not mean zero; the baseline does not guarantee a retry occurs.

| Numeric level | Metrics | Eligible complete trace trees |
| --- | --- | --- |
| `0` (default) | Yes | None, even on failure. |
| `1` | Yes | Roots with a captured failure. Successful roots with a failed retried child do not qualify. |
| `2` | Yes | Successful and failed roots. |

Optional `otelConnectionSpanDurationThreshold` and `otelStatementSpanDurationThreshold` are decimal **seconds**, per root kind, combined with level selection using **strict `duration > threshold`**, including failed roots. Leave them **absent** in the baseline. Absent is not an implicit zero: explicit `0` excludes a zero-duration root. Maximum accepted value is `9223372036.854775807`; negative/nonfinite/out-of-range values fail validation.

Eligible trace trees also honor a valid unsampled application parent: `parentBased(alwaysOn())` suppresses the entire tree while retaining eligible metrics. Without a valid parent, eligible roots are sampled. The automatic route does not use the application's SDK/exporter configuration or call its custom sampler.

## 2. Allocate one disposable directory

Keep the shell variables for subsequent steps. Run **one** shell variant. Do not run with shell tracing/transcription that captures credentials. If the checkout location differs, change only `REPO`/`$Repo`.

**Bash**

```bash
set -euo pipefail
REPO=/c/work/jdbc/repos/mssql-jdbc # Replace with the actual Linux/macOS checkout path.
DEMO=$(mktemp -d "${TMPDIR:-/tmp}/jdbc-user-otel-demo.XXXXXXXX")
export REPO DEMO
mkdir -p "$DEMO/grafana/provisioning/datasources" \
  "$DEMO/grafana/provisioning/dashboards" "$DEMO/grafana/dashboards" "$DEMO/classes"
printf 'Disposable directory: %s\n' "$DEMO"
java -version
javac -version
mvn -version
docker compose version
```

**PowerShell 7**

```powershell
$ErrorActionPreference = 'Stop'
$Repo = 'C:\work\jdbc\repos\mssql-jdbc'
$Demo = Join-Path ([IO.Path]::GetTempPath()) ('jdbc-user-otel-demo.' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $Demo | Out-Null
'grafana/provisioning/datasources','grafana/provisioning/dashboards','grafana/dashboards','classes' |
    ForEach-Object { New-Item -ItemType Directory -Path (Join-Path $Demo $_) -Force | Out-Null }
function Check-Native { if ($LASTEXITCODE -ne 0) { throw 'Previous command failed; stop here.' } }
Write-Host "Disposable directory: $Demo"
java -version; Check-Native
javac -version; Check-Native
mvn -version; Check-Native
docker compose version; Check-Native
```

### File-materialization instructions for the executing agent

Under that directory, write the following **five inline infrastructure payloads**, copy the checked-in Grafana dashboard as directed below, and write the Java payload in section 5 (seven files total). Use UTF-8 without BOM and LF; do not include the path comment in file contents. The same payloads work in both shells. Use file-edit tools, or these shell patterns with the full corresponding payload pasted in place of `PASTE_PAYLOAD` (a marker, not actual file content):

```bash
# Example pattern; repeat with the exact relative path and full payload below.
cat > "$DEMO/compose.yaml" <<'PAYLOAD'
PASTE_PAYLOAD
PAYLOAD
# Bind-mounted configs must be readable by the containers' non-root users.
chmod -R a+rX "$DEMO"
```

```powershell
# Single-quoted here-strings prevent expansion of Java/YAML/JSON content.
$Payload = @'
PASTE_PAYLOAD
'@
[IO.File]::WriteAllText((Join-Path $Demo 'compose.yaml'), ($Payload -replace "`r`n", "`n") + "`n", [Text.UTF8Encoding]::new($false))
```

## 3. Exact infrastructure payloads

### Compose payload

Host publishers all bind `127.0.0.1`. Inside containers, listeners must bind `0.0.0.0` to receive traffic over the Docker network. Do not confuse those two scopes. No database or credentials belong in Compose. Container data is ephemeral (no named data volumes).

```yaml
# Relative path: compose.yaml
services:
  collector:
    image: otel/opentelemetry-collector-contrib:0.111.0
    command: ["--config=/etc/otelcol-contrib/config.yaml"]
    volumes:
      - ./collector.yaml:/etc/otelcol-contrib/config.yaml:ro
    ports:
      - "127.0.0.1:4318:4318"
      - "127.0.0.1:8889:8889"
      - "127.0.0.1:13133:13133"
    depends_on: [jaeger]
  prometheus:
    image: prom/prometheus:v2.55.0
    command: ["--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.retention.time=2h"]
    volumes:
      - ./prometheus.yaml:/etc/prometheus/prometheus.yml:ro
    ports: ["127.0.0.1:9090:9090"]
    depends_on: [collector]
  jaeger:
    image: jaegertracing/all-in-one:1.62.0
    environment:
      COLLECTOR_OTLP_ENABLED: "true"
      SPAN_STORAGE_TYPE: memory
      MEMORY_MAX_TRACES: "10000"
    ports: ["127.0.0.1:16686:16686"]
  grafana:
    image: grafana/grafana:11.2.2
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_AUTH_DISABLE_LOGIN_FORM: "true"
      GF_ANALYTICS_REPORTING_ENABLED: "false"
      GF_ANALYTICS_CHECK_FOR_UPDATES: "false"
      GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES: "false"
      GF_NEWS_NEWS_FEED_ENABLED: "false"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["127.0.0.1:3000:3000"]
    depends_on: [prometheus, jaeger]
```

### Collector payload

Application → **OTLP HTTP/protobuf** on host `4318` for both signals. Collector → Jaeger uses **OTLP gRPC** on internal `jaeger:4317` with local plaintext (`tls.insecure`). Jaeger gRPC is not published to the host. Prometheus scrapes the Collector's **Prometheus exporter** at internal `collector:8889`; it does not scrape the JDBC process or OTLP receiver. Internal Collector self-metrics on `8888` are a separate target.

```yaml
# Relative path: collector.yaml
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 256
    spike_limit_mib: 64
  batch:
    timeout: 1s
    send_batch_size: 256
exporters:
  prometheus:
    endpoint: 0.0.0.0:8889
    add_metric_suffixes: true
    metric_expiration: 30m
    resource_to_telemetry_conversion:
      enabled: false
  otlp/jaeger:
    endpoint: jaeger:4317
    tls:
      insecure: true
service:
  extensions: [health_check]
  telemetry:
    metrics:
      address: 0.0.0.0:8888
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheus]
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/jaeger]
```

No debug exporter or request/header logger is enabled. HTTP is intentionally unauthenticated, local demo only. A GET to `4318/` is **not** a meaningful receiver health check; OTLP uses POST to signal paths.

### Prometheus payload

```yaml
# Relative path: prometheus.yaml
global:
  scrape_interval: 5s
  evaluation_interval: 5s
scrape_configs:
  - job_name: jdbc-user-endpoint
    static_configs:
      - targets: ["collector:8889"]
  - job_name: collector-internal
    static_configs:
      - targets: ["collector:8888"]
```

### Grafana data sources payload

These are **server-side Docker-network URLs**, not host loopback URLs. Stable UIDs are used by the dashboard. No plugin download is needed.

```yaml
# Relative path: grafana/provisioning/datasources/datasources.yaml
apiVersion: 1
datasources:
  - name: Demo Prometheus
    uid: demo-prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
    jsonData:
      timeInterval: 5s
  - name: Demo Jaeger
    uid: demo-jaeger
    type: jaeger
    access: proxy
    url: http://jaeger:16686
    editable: false
```

### Grafana dashboard provider payload

```yaml
# Relative path: grafana/provisioning/dashboards/provider.yaml
apiVersion: 1
providers:
  - name: JDBC user endpoint demo
    orgId: 1
    folder: JDBC Demo
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards
```

### Grafana dashboard JSON payload

Use the [checked-in Grafana dashboard](otel-poc/user-endpoint-demo/grafana/dashboards/jdbc-user-endpoint.json)
as the single source of query configuration instead of duplicating JSON here.
Copy it unchanged into the disposable directory:

```bash
cp "$REPO/docs/otel-poc/user-endpoint-demo/grafana/dashboards/jdbc-user-endpoint.json" \
  "$DEMO/grafana/dashboards/jdbc-user-endpoint.json"
```

```powershell
Copy-Item -LiteralPath (Join-Path $Repo 'docs/otel-poc/user-endpoint-demo/grafana/dashboards/jdbc-user-endpoint.json') `
    -Destination (Join-Path $Demo 'grafana/dashboards/jdbc-user-endpoint.json')
```

Its panels use the current dotted activity schema for root totals, outcome-based
success ratios, outcome distributions, root retries, per-activity means, and
histogram percentiles. Totals make a short run visible; rate/p95/increase panels
need multiple scrapes. Missing error/retry series stay missing, not zero.
Percentiles are bucket estimates; **exact maximum is unavailable** in this classic
Prometheus path. There is no fake metrics-to-trace correlation: inspect the Jaeger
data source in Explore or follow the dashboard link.

## 4. Build matching artifacts and an explicit runtime classpath

Run from the checkout using JDK 21. Root and optional module are **not a reactor**: install core first, then package the standalone module. These are execution instructions, not a request to execute during documentation authoring. `maven.test.skip` deliberately avoids integration tests; it is not test certification. Use the organization's approved Maven settings if needed, without printing their credentials. Never work around repository failure by replacing a matching local driver with an unrelated published version.

`dependency:build-classpath` normally omits **provided** dependencies at runtime scope. Use **`-DincludeScope=test`** here so the installed core JAR is included. This also includes JUnit, SDK-testing, and demo-only dependencies; acceptable for this disposable harness, not a production dependency recommendation. Do not run any existing test demo main class.

**Bash**

```bash
cd "$REPO"
mvn -B -Pjre21 -Dmaven.test.skip=true -Dmaven.javadoc.skip=true install
mvn -B -f mssql-jdbc-otel/pom.xml -Dmaven.test.skip=true package
mvn -B -f mssql-jdbc-otel/pom.xml \
  org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath \
  -DincludeScope=test "-Dmdep.outputFile=$DEMO/deps.cp"
CP="$REPO/mssql-jdbc-otel/target/mssql-jdbc-otel-13.6.0.jar:$(cat "$DEMO/deps.cp")"
export CP
jar tf "$REPO/mssql-jdbc-otel/target/mssql-jdbc-otel-13.6.0.jar" | grep 'META-INF/services/com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider'
```

**PowerShell**

```powershell
Set-Location $Repo
mvn -B -Pjre21 '-Dmaven.test.skip=true' '-Dmaven.javadoc.skip=true' install; Check-Native
mvn -B -f mssql-jdbc-otel/pom.xml '-Dmaven.test.skip=true' package; Check-Native
mvn -B -f mssql-jdbc-otel/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.6.1:build-classpath '-DincludeScope=test' "-Dmdep.outputFile=$Demo/deps.cp"; Check-Native
$Cp = (Join-Path $Repo 'mssql-jdbc-otel/target/mssql-jdbc-otel-13.6.0.jar') + [IO.Path]::PathSeparator + [IO.File]::ReadAllText((Join-Path $Demo 'deps.cp')).Trim()
jar tf (Join-Path $Repo 'mssql-jdbc-otel/target/mssql-jdbc-otel-13.6.0.jar') | Select-String 'META-INF/services/com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider'
Check-Native
```

The runtime core is the freshly installed local Maven repository JAR at coordinate `com.microsoft.sqlserver:mssql-jdbc:13.6.0` (normally `~/.m2/repository/com/microsoft/sqlserver/mssql-jdbc/13.6.0/mssql-jdbc-13.6.0.jar`), **not** an older `target/classes`, `bin/main`, or the unrelated core subdirectory. Root `jre21` produces the build JAR named `mssql-jdbc-13.6.0.jre21.jar`, but Maven installs it under the unsuffixed coordinate filename. Do not add a second core JAR or a second version of OpenTelemetry. The harness below checks the actual class origin and SPI provider before doing SQL.

## 5. Exact standalone Java harness

Save as relative path `UserEndpointDemo.java` in the disposable directory. This uses `DriverManager` plus connection `Properties` only: no manual callback, SDK construction, or `GlobalOpenTelemetry`. The direct provider import is only for application-lifecycle flush/shutdown and classpath verification. Exception messages/stack traces, connection URLs, options, SQL results, passwords and tokens are never printed. Library logging policies remain separate; do not enable verbose JDBC/HTTP logging.

Required environment: `JDBC_DEMO_URL` (test URL without credentials or `otel*` properties), `JDBC_DEMO_USER`, `JDBC_DEMO_PASSWORD` for SQL-auth workload. Preserve normal certificate validation: arrange a trusted test certificate rather than silently adding `trustServerCertificate=true`. This example does not provision SQL Server or implement other authentication flows. SQL-free modes need none of those three variables.

Optional environment: `DEMO_LEVEL` (default `2`), `DEMO_ENDPOINT` (default local HTTP), `DEMO_MODE` (`workload`, `config`, `dns`, `validate`), `DEMO_ITERATIONS` (default `20`), `DEMO_CONNECTION_THRESHOLD`, `DEMO_STATEMENT_THRESHOLD` (absent by default). Remove threshold variables rather than setting empty strings. The harness requires explicit approval before sending telemetry anywhere except the supplied local endpoint.

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.ServiceLoader;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider;
import com.microsoft.sqlserver.jdbc.otel.UserEndpointTelemetryProvider;

public final class UserEndpointDemo {
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required demo environment variable: " + name);
        }
        return value;
    }

    private static void option(Properties p, String variable, String property) {
        String value = System.getenv(variable);
        if (value != null) {
            p.setProperty(property, value);
        }
    }

    private static Properties telemetry() {
        Properties p = new Properties();
        String endpoint = env("DEMO_ENDPOINT", "http://127.0.0.1:4318");
        if (!"http://127.0.0.1:4318".equals(endpoint)
                && !"yes".equals(env("DEMO_APPROVE_REMOTE_ENDPOINT", "no"))) {
            throw new IllegalArgumentException("Nondefault telemetry endpoint requires explicit approval");
        }
        p.setProperty("otelEndpoint", endpoint);
        p.setProperty("otelLogLevel", env("DEMO_LEVEL", "2"));
        option(p, "DEMO_CONNECTION_THRESHOLD", "otelConnectionSpanDurationThreshold");
        option(p, "DEMO_STATEMENT_THRESHOLD", "otelStatementSpanDurationThreshold");
        return p;
    }

    private static void verifyClasspath() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        System.out.println("Core origin: " + SQLServerDriver.class.getProtectionDomain().getCodeSource().getLocation());
        System.out.println("Adapter origin: " + UserEndpointTelemetryProvider.class.getProtectionDomain()
                .getCodeSource().getLocation());
        boolean found = false;
        for (UserOtelTelemetryProvider provider : ServiceLoader.load(UserOtelTelemetryProvider.class)) {
            found |= provider.getClass() == UserEndpointTelemetryProvider.class;
        }
        if (!found) {
            throw new IllegalStateException("Automatic provider missing from runtime SPI");
        }
        System.out.println("Automatic provider SPI present");
    }

    private static void expectedConnectionFailure(Properties p, boolean dns) throws Exception {
        p.setProperty("loginTimeout", "3");
        p.setProperty("connectRetryCount", "0");
        // No real SQL credentials are sent to the reserved .invalid hostname.
        p.setProperty("user", "demo-unused");
        p.setProperty("password", "demo-unused");
        if (!dns) {
            p.setProperty("portNumber", "invalid");
        }
        String url = dns ? "jdbc:sqlserver://jdbc-user-endpoint-demo.invalid:1433"
                         : "jdbc:sqlserver://";
        try (Connection unexpected = DriverManager.getConnection(url, p)) {
            throw new IllegalStateException("Expected connection failure did not occur");
        } catch (SQLException expected) {
            System.out.println(dns ? "Expected DNS connection failure observed"
                                   : "Expected port configuration failure observed");
        }
    }

    private static void workload(Properties p) throws Exception {
        String url = required("JDBC_DEMO_URL");
        if (!url.startsWith("jdbc:sqlserver://")) {
            throw new IllegalArgumentException("A SQL Server JDBC test URL is required");
        }
        p.setProperty("user", required("JDBC_DEMO_USER"));
        p.setProperty("password", required("JDBC_DEMO_PASSWORD"));
        p.setProperty("loginTimeout", "10");
        p.setProperty("connectRetryCount", "0");
        // Force delayed response/error processing into executeQuery, not a later fetch.
        p.setProperty("responseBuffering", "full");
        int iterations = Integer.parseInt(env("DEMO_ITERATIONS", "20"));
        if (iterations < 1 || iterations > 100) {
            throw new IllegalArgumentException("Demo iterations must be between 1 and 100");
        }
        for (int i = 0; i < iterations; i++) {
            try (Connection c = DriverManager.getConnection(url, p); Statement s = c.createStatement()) {
                s.setQueryTimeout(15);
                try (ResultSet rs = s.executeQuery("SELECT 1 AS demo_value")) {
                    while (rs.next()) {
                        if (rs.getInt(1) != 1) {
                            throw new IllegalStateException("Unexpected test result");
                        }
                    }
                }
                try (ResultSet rs = s.executeQuery("WAITFOR DELAY '00:00:02'; SELECT 1 AS demo_value")) {
                    while (rs.next()) {
                        rs.getInt(1);
                    }
                }
                boolean failed = false;
                try {
                  s.executeQuery("SELECT 1 + ;");
                } catch (SQLException expected) {
                  if (expected.getErrorCode() != 102) {
                    throw new IllegalStateException("Expected SQL Server syntax error 102 was not observed");
                  }
                    failed = true;
                    // Report numeric evidence only; never the server's exception message.
                    System.out.println("Expected syntax error code: " + expected.getErrorCode());
                }
                if (!failed) {
                    throw new IllegalStateException("Expected syntax failure did not occur");
                }
            }
            System.out.println("Completed successful connection, fast/slow SQL, and failing SQL iteration " + (i + 1));
            // Explicit flush also exports metrics before their normal 60-second interval.
            System.out.println("Flush completed (not delivery ACK): "
                    + UserEndpointTelemetryProvider.forceFlush(Duration.ofSeconds(10)));
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) {
        int exit = 0;
        try {
            verifyClasspath();
            Properties p = telemetry();
            switch (env("DEMO_MODE", "workload")) {
                case "workload":
                    workload(p);
                    // A distinct fresh route proves connection errors as well as statement errors.
                    expectedConnectionFailure(telemetry(), false);
                    break;
                case "config":
                    expectedConnectionFailure(p, false);
                    break;
                case "dns":
                    expectedConnectionFailure(p, true);
                    break;
                case "validate":
                    p.setProperty("otelStatementSpanDurationThreshold", "-0.1");
                    try (Connection unexpected = DriverManager.getConnection("jdbc:sqlserver://", p)) {
                        throw new IllegalStateException("Invalid threshold was accepted");
                    } catch (SQLException expected) {
                        if (!"Invalid user telemetry configuration.".equals(expected.getMessage())) {
                            throw new IllegalStateException("Unexpected validation result");
                        }
                        System.out.println("Sanitized negative-threshold rejection observed");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unknown demo mode");
            }
        } catch (Exception | LinkageError | java.util.ServiceConfigurationError failure) {
            System.err.println("Demo failed; diagnostic class only: " + failure.getClass().getName());
            exit = 1;
        } finally {
            try {
                System.out.println("Final flush completed (not delivery ACK): "
                        + UserEndpointTelemetryProvider.forceFlush(Duration.ofSeconds(15)));
                // Allow collector batching and several Prometheus scrapes before returning.
                Thread.sleep(15000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                exit = 1;
            } finally {
                UserEndpointTelemetryProvider.shutdown();
            }
        }
        System.exit(exit);
    }
}
```

`shutdown()` is **parameterless** and asynchronous; there is no `shutdown(Duration)` overload. Stop all JDBC producers, flush with a bound, allow external observation, then shut down. Shutdown itself discards pending trees and is not a flush. Workers are daemons; simply exiting can lose data. Normal metric export interval is **60 seconds**. This harness explicitly flushes, so it need not wait for that interval. Without this lifecycle helper, keep the application alive beyond at least one full interval plus export/scrape time (for example 75–90 seconds), but waiting alone is still not delivery proof. No public API here waits for shutdown completion.

## 6. Start infrastructure, compile, and run

### Start and distinguish health from evidence

**Bash**

```bash
chmod -R a+rX "$DEMO"
cd "$DEMO"
docker compose -p jdbc-user-otel-demo config --quiet
docker compose -p jdbc-user-otel-demo pull
docker compose -p jdbc-user-otel-demo up -d
docker compose -p jdbc-user-otel-demo ps
curl --fail http://127.0.0.1:13133/
curl --fail http://127.0.0.1:9090/-/ready
curl --fail http://127.0.0.1:3000/api/health
curl --fail http://127.0.0.1:16686/api/services
javac -cp "$CP" -d "$DEMO/classes" "$DEMO/UserEndpointDemo.java"
```

**PowerShell**

```powershell
Set-Location $Demo
docker compose -p jdbc-user-otel-demo config --quiet; Check-Native
docker compose -p jdbc-user-otel-demo pull; Check-Native
docker compose -p jdbc-user-otel-demo up -d; Check-Native
docker compose -p jdbc-user-otel-demo ps; Check-Native
Invoke-WebRequest http://127.0.0.1:13133/ | Select-Object StatusCode
Invoke-WebRequest http://127.0.0.1:9090/-/ready | Select-Object StatusCode
Invoke-RestMethod http://127.0.0.1:3000/api/health
Invoke-RestMethod http://127.0.0.1:16686/api/services
javac -cp $Cp -d (Join-Path $Demo 'classes') (Join-Path $Demo 'UserEndpointDemo.java'); Check-Native
```

Startup is asynchronous; `depends_on` is ordering, not readiness. Retry health requests after 5 seconds for up to 60 seconds; if still failing, inspect `docker compose -p jdbc-user-otel-demo logs --tail=100` locally and stop rather than proceeding. Health, `up=1`, and a present SPI descriptor are **not proof** that driver observations arrived.

Use a unique Compose project name if the fixed demo name is already in use; change it in **all** commands and teardown. Never take over an existing user's stack. Host port conflicts still require coordination.

### Test environment and baseline run

Obtain the real URL/user from approved test configuration. Do not print or commit values, store them in generated files, or use command-line password arguments. Do not dump the process environment. The examples below prompt without echoing the password; environment variables are still visible to privileged local processes. Run in an isolated test session. Clear secrets after the run. The Java URL must omit credentials and telemetry options to avoid precedence ambiguity.

**Bash**

```bash
read -r -p 'Approved test JDBC URL (no credentials or otel options): ' JDBC_DEMO_URL
read -r -p 'Test SQL user: ' JDBC_DEMO_USER
read -r -s -p 'Test SQL password: ' JDBC_DEMO_PASSWORD; printf '\n'
export JDBC_DEMO_URL JDBC_DEMO_USER JDBC_DEMO_PASSWORD
export DEMO_LEVEL=2 DEMO_MODE=workload DEMO_ITERATIONS=20
unset DEMO_CONNECTION_THRESHOLD DEMO_STATEMENT_THRESHOLD
java -cp "$DEMO/classes:$CP" UserEndpointDemo
```

**PowerShell**

```powershell
$env:JDBC_DEMO_URL = Read-Host 'Approved test JDBC URL (no credentials or otel options)'
$env:JDBC_DEMO_USER = Read-Host 'Test SQL user'
$Secret = Read-Host 'Test SQL password' -AsSecureString
$Credential = [PSCredential]::new('unused', $Secret)
$env:JDBC_DEMO_PASSWORD = $Credential.GetNetworkCredential().Password
$env:DEMO_LEVEL = '2'; $env:DEMO_MODE = 'workload'; $env:DEMO_ITERATIONS = '20'
Remove-Item Env:DEMO_CONNECTION_THRESHOLD,Env:DEMO_STATEMENT_THRESHOLD -ErrorAction SilentlyContinue
java -cp ((Join-Path $Demo 'classes') + [IO.Path]::PathSeparator + $Cp) UserEndpointDemo; Check-Native
Remove-Variable Secret,Credential
```

Expect successful iterations and real server syntax errors (normally SQL Server error `102`), followed by an expected configuration failure. Do not count a failed initial login as a successful workload. The controlled syntax error must surface from `executeQuery` to mark the invocation root failed; full buffering is set for this reason. Consuming a large result slowly after execute returns does not necessarily make the invocation span slow.

For **SQL-free connection-error smoke only**, set `DEMO_MODE=config` and run the same Java command without SQL environment variables. `DEMO_MODE=dns` additionally uses the reserved `.invalid` domain, fixed dummy credentials, three-second login budget, and no retry; DNS/proxy behavior can vary and resolver calls may outlast that budget. Confirm a DNS phase/category in traces rather than assuming any connection failure proves DNS instrumentation. The deterministic invalid-port case is the required connection-error check.

## 7. Acceptance: inspect real driver data

Open:

| URL | Purpose |
| --- | --- |
| <http://127.0.0.1:3000/d/jdbc-user-endpoint> | Provisioned dashboard under JDBC Demo; anonymous read-only access. |
| <http://127.0.0.1:3000/explore> | Choose Demo Jaeger and search service `mssql-jdbc`. |
| <http://127.0.0.1:9090/targets> | Both scrape targets should be UP. |
| <http://127.0.0.1:8889/metrics> | Raw translated **driver** metrics. |
| <http://127.0.0.1:16686> | Jaeger trace search and span trees. |

### Exposition before dashboards

**Bash**

```bash
curl --fail --silent http://127.0.0.1:8889/metrics | grep -E '^(# (HELP|TYPE) db_client_operation|db_client_operation_)'
curl --fail --silent --get http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum by (mssql_performance_activity,mssql_operation_kind,mssql_operation_outcome) (db_client_operation_count_total{job="jdbc-user-endpoint"})'
curl --fail --silent --get http://127.0.0.1:16686/api/traces \
  --data-urlencode 'service=mssql-jdbc' --data-urlencode 'limit=20' \
  --data-urlencode 'operation=mssql.driver.statement.execute'
```

**PowerShell**

```powershell
(Invoke-WebRequest http://127.0.0.1:8889/metrics).Content -split "`n" |
    Where-Object { $_ -match '^(# (HELP|TYPE) db_client_operation|db_client_operation_)' }
$Query = 'sum by (mssql_performance_activity,mssql_operation_kind,mssql_operation_outcome) (db_client_operation_count_total{job="jdbc-user-endpoint"})'
$Result = Invoke-RestMethod ('http://127.0.0.1:9090/api/v1/query?query=' + [uri]::EscapeDataString($Query))
$Result.data.result | ConvertTo-Json -Depth 8
$Traces = Invoke-RestMethod 'http://127.0.0.1:16686/api/traces?service=mssql-jdbc&limit=20&operation=mssql.driver.statement.execute'
$Traces.data | Select-Object traceID
```

Required evidence (record timestamps, counts, trace IDs, and safe screenshots, not secrets):

1. Actual exposition contains operation/error counters and `_seconds_bucket/_sum/_count`, current `mssql_performance_activity`/`mssql_operation_kind`/schema labels, outcome on count/duration, and nonzero root observations. Verify applicable statement type/protocol and connection authentication labels plus the 17 finite histogram boundaries. Retry exposition is conditional on a begun additional attempt; the baseline need not produce it. Merely seeing Collector self-metrics is insufficient.
2. PromQL below returns connection and statement counts, plus failed roots. Twenty successful iterations normally yield at least twenty workload connection roots and sixty statement invocation roots (fast, slow, invalid SQL), plus one failed connection root. Do not demand exact global totals: initialization paths, prior runs, resets, losses, or changed instrumentation can affect observations. Investigate unexpectedly missing workload roots.
3. At level `2` without thresholds, Jaeger service `mssql-jdbc` contains successful `mssql.driver.connection.open`, successful `mssql.driver.statement.execute`, and failed roots of both kinds. Inspect children (for example `configuration`, `attempt`, `dns`, `socket_connect`, `request_build`, `server_call`, `first_response`) **only when the corresponding code path ran**. Not every connection has every phase. This harness installs no application parent, so connection and statement roots are not promised to share one trace.
4. Failed roots have `ERROR` status; `mssql.driver.error` events appear at error origins when evidence exists, not on every failed ancestor. Verify the syntax failure at the statement root, not just an error-looking child tag. A fast parser failure can have a very short duration.
5. Slow SQL has a measured invocation root longer than one second in this controlled full-buffered workload. Verify the actual root duration rather than inferring it from the SQL or a phase. This supports the threshold check below; it does not prove all JDBC API/result-reading timings.
6. Where captured, statement roots contain approved masked `db.query.text` (for example `SELECT ? + ;`), not fixture literals; child spans/events/metrics contain no SQL. Inspect allowlisted root diagnostics when the relevant path runs, without requiring every diagnostic family from this baseline. Missing regions stay unavailable; do not infer them from hostnames.

Useful exact PromQL (enter in Prometheus or Grafana Explore):

```promql
sum(db_client_operation_count_total{job="jdbc-user-endpoint",mssql_performance_activity="connection.open",mssql_operation_kind="connection"})
```

```promql
sum(db_client_operation_count_total{job="jdbc-user-endpoint",mssql_performance_activity="statement.execute",mssql_operation_kind="statement"})
```

```promql
sum by (mssql_performance_activity) (db_client_operation_error_count_total{job="jdbc-user-endpoint",mssql_performance_activity=~"connection[.]open|statement[.]execute"})
```

```promql
sum(rate(db_client_operation_duration_seconds_sum{job="jdbc-user-endpoint",mssql_performance_activity="statement.execute"}[5m]))
/
sum(rate(db_client_operation_duration_seconds_count{job="jdbc-user-endpoint",mssql_performance_activity="statement.execute"}[5m]))
```

The last query is mean invocation seconds over the window; rate/quantile queries need at least two scrape samples and nonzero changes. Short/frozen runs may produce empty or NaN results. Root totals remain useful. Metrics retained by the Collector expire after 30 minutes without updates; Prometheus keeps already-scraped history for two hours while its container survives. A new JVM resets cumulative SDK counters; do not treat a later process's latest total as a cross-process lifetime count.

## 8. Isolated levels, thresholds, and validation experiments

**Reset between experiments** to avoid mistaking earlier traces/counters for new ones. Stop Java first. From the disposable directory, use the following in either shell (check exit codes in PowerShell):

```text
docker compose -p jdbc-user-otel-demo down --volumes --remove-orphans
docker compose -p jdbc-user-otel-demo up -d
```

There are no persistent data mounts: this clears Jaeger memory, Prometheus container data and Grafana data; provisioning recreates the dashboard. Wait for readiness again. Alternatively use tightly bounded trace time windows, but do not claim absence while searching all prior runs.

For each row, set environment with `export DEMO_LEVEL=0` / `$env:DEMO_LEVEL='0'` syntax, then rerun the same Java command from section 6. Restore/remove thresholds explicitly after every experiment.

| Fresh-stack experiment | Variables | Required outcome |
| --- | --- | --- |
| Metrics only | Level `0`, mode `workload`, no thresholds | Metrics for successes/errors, **no new driver traces** even for forced failures. |
| Error roots only | Level `1`, mode `workload`, no thresholds | Failed syntax statement and invalid-port connection trees. No successful connection or successful statement trees. |
| All roots | Level `2`, mode `workload`, no thresholds | Both successful and failed roots, as above. |
| Slow statement filter | Level `2`, mode `workload`, connection threshold `9223372036.854775807`, statement threshold `1` | No connection trees; measured >1-second WAITFOR invocation trees eligible; fast SELECT and fast syntax failure normally suppressed. Metrics still include fast/slow/errors. |
| Suppress all practical roots | Level `2`, mode `workload`, both thresholds `9223372036.854775807` | Metrics still present; no driver trees for this short workload, including its failures. |
| Negative configuration | Mode `validate`, otherwise default options | Sanitized rejection before a telemetry route/physical connection; **no telemetry is expected for this invalid telemetry option**. |

Example **threshold experiment** setup:

```bash
export DEMO_LEVEL=2 DEMO_MODE=workload
export DEMO_CONNECTION_THRESHOLD=9223372036.854775807 DEMO_STATEMENT_THRESHOLD=1
java -cp "$DEMO/classes:$CP" UserEndpointDemo
unset DEMO_CONNECTION_THRESHOLD DEMO_STATEMENT_THRESHOLD
```

```powershell
$env:DEMO_LEVEL='2'; $env:DEMO_MODE='workload'
$env:DEMO_CONNECTION_THRESHOLD='9223372036.854775807'; $env:DEMO_STATEMENT_THRESHOLD='1'
java -cp ((Join-Path $Demo 'classes') + [IO.Path]::PathSeparator + $Cp) UserEndpointDemo; Check-Native
Remove-Item Env:DEMO_CONNECTION_THRESHOLD,Env:DEMO_STATEMENT_THRESHOLD -ErrorAction SilentlyContinue
```

Do not try to validate strict equality with wall-clock SQL sleep: scheduling/network time varies. The pinned source tests demonstrate synthetic 9/10/11 ns boundaries, including 11 ns vs 10.5 ns, exact equality rejection, and absent vs zero behavior. Live acceptance checks the observed root against the configured threshold. Do not advertise a guarantee that every WAITFOR produces a statement root or that every short statement is below one second: inspect buffering, execute-path coverage and actual spans. If no qualifying statement root is observed, report the gap rather than manufacturing a manual callback event.

### Optional callback-authentication test boundary

The baseline intentionally does **not** set `otelAccessTokenCallback`. This Collector has **no receiver authentication**, so successful ingestion with a mock token would only show transport acceptance, **not token validation**. Do not use a real token against this plaintext demo. Do not claim authentication security from dashboard data.

For callback behavior, the existing [wire tests](../mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/UserEndpointTelemetryTest.java) cover public no-arg callback construction, captured application class loader, calls per outgoing HTTP message, endpoint hint/empty authority, expiry, failure backoff and no stale/anonymous fallback. Most use a mock HTTP receiver, not an authenticated Collector. A separate optional experiment may use a dummy-token callback and a local recording receiver that asserts exactly one Authorization header without logging its value; do not add it to this baseline or interpret it as issuer/audience/signature validation. Real auth acceptance requires a separately approved authenticated receiver configuration and negative tests; that is outside this unauthenticated stack.

## 9. Privacy, loss, troubleshooting, and reporting

### Privacy acceptance

Inspect safe trace details and raw metric labels locally before sharing artifacts. The provider discards supplied query enrichment and only exports its own synchronously sanitized callback SQL as statement-root `db.query.text`. The bounded lexical sanitizer removes comments and masks literals; malformed/uncertain or oversized input is omitted. Raw SQL is never queued. Expect **no raw SQL, parameter values, passwords, bearer tokens, connection string, server address, raw exception message, or stack trace** in its payload. Masked SQL may retain identifiers/query shape, so review it before sharing. Safe allowlisted metadata (including connection GUIDs, statement type/API, outcomes, error category/type, SQL error codes) can appear on **spans**. Metrics may have bounded statement type/protocol but never GUID/SQL/error-category dimensions. An error category/class name is not an exception message.

The live viewer displays masked SQL only from emitted statement-root `db.query.text`, after a fail-closed lexical display check; it does not invent SQL from aliases, resources, or children. Exact maximum remains unavailable because classic histogram buckets/sums do not retain it; region appears only when emitted. Root diagnostics are replayed through name/value allowlists (up to 128, within the root timestamps), separately from origin error events.

This is a source contract plus a manual payload inspection, not proof of absence across all code paths. Other application callbacks, JDBC JUL settings, proxies and SDK diagnostics have their own logging behavior; do not claim this route sanitizes them. Do not attach full trace JSON or collector logs to public issues without review. For a stronger check, use a harmless synthetic sentinel in a nonsecret test SQL literal and confirm its absence from locally inspected exported spans; never search by printing a real password/token.

### Best effort and drop evidence

JDBC callbacks enqueue nonblocking immutable snapshots. SDK creation, filtering, callback tokens and export run asynchronously on private workers, without replacing the application's global SDK or legacy callback. SQL success and `forceFlush=true` are **not delivery acknowledgements**. Metrics are independent of trace filtering, not lossless. The ingestion queue is bounded at 8192; transport keys 32; pending trees 256; tree size 512 spans; span export admission 2048 spans/64 queued tasks. Admission contention, queue overflow, one-minute pending-tree expiry, collector outages, authentication failure, three-second transport timeouts and exit can lose telemetry. Incomplete trees are dropped whole; prior successful sends are not rolled back. Provider exporter retries are disabled. Collector retry/queue behavior is a different hop.

Collector self-metrics such as `otelcol_receiver_accepted_spans`, `otelcol_receiver_refused_spans`, `otelcol_receiver_accepted_metric_points`, `otelcol_receiver_refused_metric_points`, and `otelcol_exporter_send_failed_spans` can help when present (check actual `collector-internal` exposition/names before querying; zero series may be absent). They observe **Collector-side** activity only. The driver exposes no dedicated dropped-event metric here. Zero refused spans at the Collector cannot prove zero driver-side loss. Do not attempt overload as a baseline acceptance criterion.

| Symptom | Check/action |
| --- | --- |
| SQL works, no telemetry | Fail-open provider path: check actual core/adapter class origins, exact SPI descriptor, matching installed `13.6.0`, all OpenTelemetry `1.41.0` dependencies, application context loader, endpoint and signal paths. Do not add a manual callback to hide the failure. |
| `NoSuchMethodError`/`ClassNotFoundException` | Stale core JAR, missing provided driver, duplicate OTel versions, or wrong JDK. Reinstall root first and regenerate the explicit classpath; do not use IDE launch defaults. |
| SPI check passes, no metrics | Provider transport construction can still fail asynchronously. Check HTTP `4318` reachability, container logs locally, receiver metrics pipeline, explicit flush and Prometheus scrape. Without flush allow >60 seconds. |
| Prometheus target UP, empty dashboard | UP proves scraping only. Inspect `8889/metrics`, family suffixes/labels, root activity filters, time range and scrape samples. `collector-internal` is not JDBC metrics. |
| Metrics, no traces | Level `0`, strict thresholds including failed roots, valid unsampled parent, missing complete tree, unsupported path, admission loss, or wrong Jaeger hop. Verify level `2` with absent thresholds and inspect parent sampling, then `otlp/jaeger` → `jaeger:4317`, not port `16686` or HTTP `4318`. |
| Jaeger UI health but no `mssql-jdbc` service | Actual traces have not arrived, storage was reset, or lookback is wrong. Health alone is not evidence. |
| Slow SQL lacks slow invocation | Verify `responseBuffering=full`, execute vs later fetch time, measured root duration and supported statement path. Do not equate delayed result consumption with invocation timing. |
| TLS/login errors instead of successful SQL | Fix approved test SQL URL/auth/certificate trust. Do not silently disable encryption or certificate validation. Report only failure coverage until real success is shown. |
| Port/config volume errors | Host ports must be free; all seven generated files must be real readable files, not accidentally created directories. On SELinux systems approve a bind-label adjustment locally rather than disabling host security. |

Report these categories separately: **infrastructure healthy**, **SPI/classpath verified**, **configuration-error route verified**, **successful SQL connection verified**, **successful/failed statement roots verified**, **metric exposition/PromQL verified**, **levels/thresholds verified**, **privacy inspected**. Include unrun/failed checks, actual version/digest information and safe timestamps/trace IDs. Do not report the guide itself, a flush boolean, or fixture tests as a completed live demo.

## 10. Teardown

Stop Java before teardown. From the recorded disposable directory only:

**Bash**

```bash
cd "$DEMO"
docker compose -p jdbc-user-otel-demo down --volumes --remove-orphans
unset JDBC_DEMO_URL JDBC_DEMO_USER JDBC_DEMO_PASSWORD
unset DEMO_LEVEL DEMO_MODE DEMO_ITERATIONS DEMO_ENDPOINT DEMO_APPROVE_REMOTE_ENDPOINT
unset DEMO_CONNECTION_THRESHOLD DEMO_STATEMENT_THRESHOLD CP
# Inspect the printed path before approving deletion of this exact temporary directory.
printf 'Remove only this demo directory after review: %s\n' "$DEMO"
cd "$REPO"
case "$DEMO" in */jdbc-user-otel-demo.*) rm -rf -- "$DEMO" ;; *) echo 'Refusing unexpected path' >&2; exit 1 ;; esac
unset DEMO
```

**PowerShell**

```powershell
Set-Location $Demo
docker compose -p jdbc-user-otel-demo down --volumes --remove-orphans; Check-Native
'JDBC_DEMO_URL','JDBC_DEMO_USER','JDBC_DEMO_PASSWORD','DEMO_LEVEL','DEMO_MODE','DEMO_ITERATIONS',
'DEMO_ENDPOINT','DEMO_APPROVE_REMOTE_ENDPOINT','DEMO_CONNECTION_THRESHOLD','DEMO_STATEMENT_THRESHOLD' |
    ForEach-Object { Remove-Item "Env:$_" -ErrorAction SilentlyContinue }
Remove-Variable Secret,Credential,Cp -ErrorAction SilentlyContinue
Write-Host "Remove only this demo directory after review: $Demo"
Set-Location $Repo
if ((Split-Path $Demo -Leaf) -notlike 'jdbc-user-otel-demo.*' -or
    (Split-Path $Demo -Parent).TrimEnd('\','/') -ne ([IO.Path]::GetTempPath()).TrimEnd('\','/')) {
    throw 'Refusing unexpected path'
}
Remove-Item -LiteralPath $Demo -Recurse -Force
Remove-Variable Demo
```

Do not use global Docker prune, delete the checkout, remove other stacks, or clear shared Maven caches. Images and Maven artifacts remain cached; deleting this directory does not reclaim those caches. No SQL objects were created to drop. If an experiment added anything beyond this guide, report and separately approve its cleanup.