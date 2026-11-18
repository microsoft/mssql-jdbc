# Complete local JDBC OpenTelemetry demo

This is the executable companion to the [feature reference](../../user-otel-endpoint.md).
It runs **local SQL Server → JDBC automatic OTel provider → Collector →
Prometheus/Jaeger → live diagnostics dashboard**, with Grafana as an additional viewer.
No cloud account, Azure service, existing database, application callback, or Java
agent is needed. It uses packaged driver JARs built from this checkout.

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
SQL text, exact maxima, and unreported regions are not fabricated.

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

The older [manual runbook](../../user-otel-endpoint-demo.md) remains useful for
signal-contract details and exploratory level/threshold scenarios. Prefer this
checked-in runner for reproducible full-demo startup; do not mix the two stacks.