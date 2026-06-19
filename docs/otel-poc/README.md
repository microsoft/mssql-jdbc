# mssql-jdbc OpenTelemetry POC — internal pipeline + Aspire Dashboard

Docker-only stack: the driver ships OTLP to `otelcol-arcdata` (our collector
image), which fans every signal to **Azure Delta Lake** (Managed Identity +
Event Hub + Delta Bulk Loader) **and** a local **.NET Aspire Dashboard** for
metrics, traces, and logs.

```
driver ─OTLP/HTTP:4318─▶ otelcol-arcdata ─deltalake─▶ Azure Delta Lake
                              └─otlp:18889─▶ Aspire Dashboard (http://localhost:18888)
driver ─TDS:1433───────▶ sqlserver (in-network only)
```

## Prerequisites

Docker + bash, plus a host Azure session (the `token-server` vends Managed
Identity tokens from your `~/.azure`):

- `az login` to the tenant owning the target storage/Event Hub (defaults:
  `mdrrahmansandbox`, tenant `72f9…`).
- **AcrPull** on `arcdataanalyticsacr.azurecr.io` — `dev.sh` runs `az acr login`.
- **RBAC** on the storage account / Event Hub only if you want data to land in
  Delta Lake; the local green gates never inspect Azure.

JDK, Maven, and SQL Server all run in containers.

## Quickstart

```bash
cd docs/otel-poc
./.scripts/dev.sh
```

Each `up` starts fresh (`down --volumes` + clears `target/`), brings the stack
up, proves it green, and leaves a load generator running. It ends with a
**GREEN** banner and the Aspire URL:

- **Aspire Dashboard** — http://localhost:18888 (service `mssql-jdbc-poc-loadgen`).
  On a remote dev container, forward port **18888**.
- Collector OTLP — `localhost:4318` (HTTP) / `4317` (gRPC).

## Commands

```bash
./.scripts/dev.sh            # up: fresh start, build, prove green, leave loadgen running
./.scripts/dev.sh status     # container status + health + Aspire URL
./.scripts/dev.sh logs [svc] # tail all logs, or one service (loadgen, otelcol-arcdata, …)
./.scripts/dev.sh down       # stop + remove the stack
./.scripts/dev.sh clean      # down + drop the SQL volume + target/
```

## GREEN (asserted automatically; local only — Azure is best-effort, never inspected)

| #   | Gate                        | Check                                                                         |
| --- | --------------------------- | ----------------------------------------------------------------------------- |
| 1   | Smoke test                  | `OtelPocSmokeTest` passes against SQL Server.                                 |
| 2   | Telemetry reaches collector | arcdata `debug` exporter logs driver **metrics** + **traces**.                |
| 3   | Aspire receiving            | Dashboard answers on `:18888`; `otlp/aspire` export not failing.              |
| 4   | Health                      | sqlserver + token-server healthy; arcdata, delta-bulk-loader, aspire running. |

## Overrides (env)

| Variable                                                     | Default                                             | Purpose                       |
| ------------------------------------------------------------ | --------------------------------------------------- | ----------------------------- |
| `MSSQL_SA_PASSWORD`                                          | `Otel_Poc_Str0ng!Pass`                              | SA password.                  |
| `BURST_SECONDS`                                              | `60`                                                | Green-gate load-burst length. |
| `OTELCOL_ARCDATA_IMAGE` / `DELTA_BULK_LOADER_IMAGE`          | pinned                                              | Override the private images.  |
| `STORAGE_ACCOUNT` / `STORAGE_CONTAINER` / `ROOT_PATH`        | `mdrrahmansandbox` / `onelake` / `arcdata-otel-poc` | Delta Lake target.            |
| `EVENT_HUB_NAMESPACE` / `EVENT_HUB_NAME` / `AZURE_TENANT_ID` | sandbox defaults                                    | Event Hub + tenant.           |

## Layout

- `docker-compose.yml`, `Dockerfile`, `settings.xml`, `.scripts/dev.sh`
- `internal/` (vendored): `token-server/`,
  `otelcol-arcdata/deltalake-e2e.yaml` (`otlp` → `deltalake` + `otlp/aspire` + `debug`),
  `delta-bulk-loader/appsettings.json`
- Driver sources: `src/test/java/com/microsoft/sqlserver/jdbc/otel/{OtelPocLoadGen,OtelPocSmokeTest}.java`

## OTel export auth

Attach an `Authorization` header to OTLP/HTTP exports via connection-string properties:

| Property                       | Default | Effect                                                                                                                                                    |
| ------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `otelUseSqlAccessToken`        | `false` | Reuse the SQL AAD access token as OTLP `Authorization: Bearer …`.                                                                                         |
| `otelAccessTokenCallbackClass` | —       | FQCN of a `SQLServerAccessTokenCallback`; the driver calls it to mint the OTLP bearer JWT and re-mints it near expiry — **independent of the SQL login**. |
| `otelBearerToken`              | —       | Explicit static bearer token sent as `Authorization: Bearer …`.                                                                                           |
| `otelHeaders`                  | —       | Comma-separated `key=value` headers (e.g. vendor API keys).                                                                                               |

Precedence: `otelUseSqlAccessToken` (when a SQL AAD token was acquired) → `otelAccessTokenCallbackClass`
→ `otelBearerToken` → none, plus anything in `otelHeaders`. The `Authorization` header is resolved on
**every OTLP export** via a `Supplier`, so token renewals are picked up without rebuilding the pipeline:
the reused SQL AAD token refreshes on each fedAuth (re)connect, and the callback JWT is re-minted shortly
before its expiry. A callback failure is non-fatal — export continues with the last good bearer (or none).

The POC ships a demo callback,
`com.microsoft.sqlserver.jdbc.otel.AzureCliAccessTokenCallback`, that mints the JWT with
`AzureCliCredential` (the `az` CLI + mounted `~/.azure`). The `loadgen` service uses it
(`otelUseSqlAccessToken=false` + `otelAccessTokenCallbackClass=…AzureCliAccessTokenCallback`), so SQL
stays on SA auth while OTLP export carries a real AAD JWT. Prove it in isolation:

```bash
docker compose run --rm --no-deps app mvn -B -Pjre11 -DskipTests test-compile \
  org.codehaus.mojo:exec-maven-plugin:3.1.0:java -Dexec.classpathScope=test \
  -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelTokenHeaderCheck
```

(Override the requested scope with `-DotelAccessTokenScope=…`; default `https://database.windows.net/.default`.)

## Notes

- `otelcol-arcdata` has **no Prometheus exporter** — the Aspire Dashboard (over
  OTLP) is the local viz, replacing the old Prometheus/Grafana/Jaeger stack.
- The collector serves Aspire even if Azure isn't fully provisioned; Delta Lake
  writes just log errors until RBAC is in place, so the POC stays green locally.
- The driver activates OTel via the `otelEndpoint` connection property; the load
  generator sets `otelEndpoint` / `otelServiceName` / `otelExportInterval` for you.
- `target/` is written root-owned inside the container; `clean` removes it.
