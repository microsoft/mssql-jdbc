# Connection-error OpenTelemetry demo

Source-built Java 17 demo of **failure-only JDBC connection spans**. No statement
load generator, metric producer, Java agent, SQL provisioning or automatic Azure
login is included. Successful physical opens must emit no spans.

The default stack is public/local and needs no SQL Server or Azure account:

```text
ConnectionErrorDemo -- OTLP/HTTP --> otelcol-contrib -- OTLP --> Aspire
                                           |
                                           +--> OTLP JSON evidence volume
```

Only Aspire's UI is published, at http://localhost:18888, bound to loopback.
OTLP receivers are private Docker-network endpoints; none publish gRPC or HTTP
to the host. Aspire is anonymous for this isolated demo: do not expose or forward
it to an untrusted network. Image versions are demo compatibility pins, not a
statement that these old pins are appropriate for production.

## Prerequisites and source integration

- Docker Engine/Desktop with Linux containers and Docker Compose v2.24+ (v5 also
  works), and Bash (Git Bash or WSL on Windows). Run commands below from this
  directory. No host JDK, Maven, SQL Server, Node or Azure CLI is needed locally.
- Internet access to public container registries and Maven Central at build time.
- The current core connection-event implementation, optional OTLP bootstrap, and
  `com.microsoft.sqlserver.jdbc.otel.ConnectionErrorDemo` must be present in the
  optional module's test source tree. The Java main takes **no arguments**.
  This infrastructure does not implement or overwrite those separately owned
  Java sources. The Docker build fails explicitly if the demo class is missing.
- For `login` / `success` only: a reachable SQL Server and an intentionally supplied
  `JDBC_CONNECTION_STRING`. Inside Docker, `localhost` means the app container;
  use a reachable DNS name, or Docker Desktop's host address when appropriate.
  Use a sandbox login, least privilege, TLS and a bounded JDBC `loginTimeout`.

The [Dockerfile](Dockerfile) builds the **root standalone jar** with `-Pjre17`
and `install`, skipping tests; then separately test-compiles the optional module
and copies its test-scope dependencies (including the provided driver). The
runtime executes `java -cp target/test-classes:target/classes:target/dependency/*`
with the main above. It does not run Maven, mount the repository or use an agent.
The [public Maven settings](settings.xml) override the root feed without auth.
The [build-context allowlist](Dockerfile.dockerignore) excludes Git history,
scratch, host outputs, certificates, credentials and the unrelated untracked
core Main producer. No host Maven or Azure session is copied into the image.

## Local quickstart

```bash
cd docs/otel-poc
bash .scripts/dev.sh config
bash .scripts/dev.sh up
```

`up` builds the source image, starts the local collector/dashboard, waits for HTTP
ingestion, runs `config,dns` once, and checks real captured OTLP. It neither wipes
existing volumes nor leaves a load generator running. Expected connection
failures are the workload, not failed infrastructure. The demo must close/flush
its bootstrap before exit; flush completion alone is **not** delivery evidence.

Open **Traces** in Aspire and select the unique `mssql-jdbc-connection-gate-*`
service printed by the gate. Inspect failed open trees, configuration failure
before network activity, and DNS failure before SQL login. No JDBC connection
string is required to export either early failure.

```bash
bash .scripts/dev.sh gate        # another isolated evidence-checked run
bash .scripts/dev.sh run         # run selected scenarios, without evidence assertions
bash .scripts/dev.sh status
bash .scripts/dev.sh logs otelcol
bash .scripts/dev.sh down        # remove this project's containers, preserve evidence
bash .scripts/dev.sh clean       # EXPLICIT removal of this project's evidence volume
```

Build separately with `bash .scripts/dev.sh build`. The script defaults to help,
never authenticates, downloads certificates, resets Git, removes host build
outputs or prunes Docker globally. Use the same `DEMO_STACK` when stopping an
internal/Delta stack; stop one mode before switching to another.

## Scenario and environment contract

Use process environment variables for secrets; never paste tokens, passwords or
credential-bearing URLs into command arguments. The optional [.env.example](.env.example)
contains non-secret defaults/placeholders; a populated local environment file is
ignored by Git. Compose receives credentials only at run time, not as image
build arguments. Docker administrators can inspect container environments: this
is a developer sandbox, not a secret-storage mechanism. Never publish unredacted
Compose configuration, environment dumps, container inspections or token responses.

| Variable | Behavior |
| --- | --- |
| `DEMO_SCENARIOS` | `config,dns` by default; choose comma-separated `config,dns,login,success`, without duplicates. |
| `DEMO_REPEAT` / `DEMO_PAUSE_SECONDS` | Default `1` / `1`; keep runs small. The evidence gate supports 1–1000 repeats and bounded capture files. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Compose fixes `http://otelcol:4318`; the bootstrap appends/normalizes `/v1/traces`. For host runs, explicitly supply an approved HTTPS receiver. |
| `OTEL_SERVICE_NAME` | Default `mssql-jdbc-connection-demo`; `gate` overrides it with a unique name to reject stale evidence. |
| `OTEL_AUTH_MODE` | `none` locally; `azure_cli`, `managed_identity`, `default`, or `static` explicitly elsewhere. No automatic fallback to SQL credentials. |
| `OTEL_ACCESS_TOKEN_SCOPE` | **Required for every non-`none` mode.** Supply the exact audience-approved credential scope; do not infer it from a resource ID. |
| `OTEL_TOKEN_AUTHORITY` | Optional explicit callback authority; no actual tenant is baked into this demo. |
| `OTEL_BEARER_TOKEN` | Environment-only static token. Short-lived developer diagnostic, not a refreshable production credential. |
| `OTEL_ARM_RESOURCE_ID` | Exact MISE CheckAccess target; sent as an HTTP header, not inferred or copied into span attributes. |
| `OTEL_ALLOW_INSECURE_DEVELOPMENT_ENDPOINT` | Compose sets `true` **only** for the internal single-label Docker HTTP endpoint. Never enable for production transport. |
| `JDBC_CONNECTION_STRING` | Explicit SQL connection for `login`/`success`; never logged. Not used for default early failures or to derive telemetry credentials. |
| `DEMO_LOGIN_USER` / `DEMO_LOGIN_PASSWORD` | Intentional invalid SQL login overrides for the `login` scenario; provide them through the environment. Do not use a real user's account for deliberate failures. |
| `DEMO_DISCOVERY_CONNECTION_STRING` | Optional separate bootstrap SQL connection for opt-in discovery from `msdb.dbo.SQLServerAzureArcProperties`, with a bounded timeout. Not needed for default early failures. |

For a controlled login failure, supply the three JDBC/login variables via your
environment manager, with a deliberately nonexistent `DEMO_LOGIN_USER`, then:

```bash
DEMO_SCENARIOS=config,dns,login bash .scripts/dev.sh gate
```

The resulting login failure must be classified as `authentication` at `login`.
A DNS, TCP or TLS failure is not an acceptable substitute and fails the gate.
For a successful-open negative control, supply a valid `JDBC_CONNECTION_STRING`:

```bash
DEMO_SCENARIOS=config,dns,success bash .scripts/dev.sh gate
```

The successful open must add **zero** roots/spans. Keep a positive failure control:
a success-only capture could mean a dead exporter, so `gate` rejects success-only
selection. `run` permits inspecting a standalone success scenario without claiming
proof. The Java demo must itself fail if an intended success fails or an intended
login failure unexpectedly succeeds. None of these scenarios runs a statement
workload. Optional discovery performs a bootstrap metadata query only.

Explicit Compose routing is the default. Discovery is a separate Java opt-in:
use only an operator-controlled discovery database and require the Java bootstrap
to validate the returned endpoint before attaching credentials. No SQL initialization
script, resource defaults, table seeding or broad telemetry destination trust is
imported from the fork.

## What the automated gate proves

[The verifier](.scripts/verify-traces.mjs) reads collector file-exporter OTLP JSON,
selects the unique run's service, deduplicates retry deliveries, and checks:

- Exactly the requested failed open roots per repeat: configuration, DNS, and
  optional login; correct failure phase/category and ERROR status.
- Only the driver's connection span names/scope, with descendants attached to
  retained failed roots; no unexpected roots, statement spans, metrics or logs
  in the evidence. Credential/header and exception free-text attribute keys are
  rejected without printing their values.
- Five consecutive matching snapshots after the app finishes, allowing exporter
  batching/file flush. Evidence files are bounded (10 MB, two backups); large
  repeated runs that roll the current capture must be reduced rather than treated
  as a pass. A timeout is a failed gate, not a successful empty run.
- In internal mode, unauthenticated HTTP ingestion must be rejected before the
  credentialed Java run. The internal collector forwards accepted traces to a
  private evidence sidecar running the same public capture pipeline.

There are no metric/log pipelines and collector self-metrics are disabled. The
Java bootstrap must also keep adapter metrics disabled and install no metric or
statement producers. A trace-only capture cannot prove that some unrelated process
never attempted a rejected metric request; source configuration/tests remain part
of that check. Unit-test the verifier with `node --test .scripts/verify-traces.test.mjs`
when Node 22+ is available; no service or database is needed.

**Not claimed:** Aspire receipt merely because its UI is healthy; Azure delivery
merely because the local gate passes; full authorization correctness from a single
401/403. Inspect matching trace IDs in Aspire. For internal validation also test
expired/wrong-audience credentials and a valid identity denied on a different
resource in an approved sandbox; verify no matching traces pass. For Delta, verify
actual committed trace records and matching trace IDs in the destination, plus
Event Hub consumption/checkpoints. An accepted local export is not persistence.

## Opt-in internal MISE pipeline

```text
Java -- authenticated OTLP/HTTP --> private otelcol -- accepted OTLP --> evidence --> Aspire
                                      |
                                      +--> MISE ValidateRequest / CheckAccess
```

Use [the internal Compose overlay](docker-compose.internal.yml). Required values
are listed in [.env.example](.env.example); no tenant, subscription, application,
resource, certificate or private registry credential is supplied. Before running:

1. Obtain approved immutable `MISE_IMAGE` and `OTELCOL_ARCDATA_IMAGE` references
   and registry pull access through your normal process, outside this script.
2. Supply the MISE tenant, onboarded application, **exact audience**, region,
   owner-approved required first-party-subscription allowlist value, and an
   existing approved PFX path. Do not widen the allowlist to bypass CheckAccess.
   Supply the PFX password through `MISE_CERT_PASSWORD` if applicable.
3. Verify that the selected MISE image applies .NET environment overrides to
   [its base configuration](internal/mise/mise.appsettings.json). Identity fields
   are intentionally blank until overridden. `ShowPII=false` is set both there
   and in Compose. Startup/readiness alone does not prove overrides were applied.
4. Set `OTEL_AUTH_MODE` in the **process environment**, with exact scope and ARM
   target; supply an independently acquired static token or real available identity.
   The default Java image intentionally has no Azure CLI and mounts no host session.

```bash
DEMO_STACK=internal bash .scripts/dev.sh config
DEMO_STACK=internal bash .scripts/dev.sh up
DEMO_STACK=internal bash .scripts/dev.sh down
```

Callback scope and validator audience are distinct explicit inputs. The fork used
the SQL audience `https://database.windows.net` with scope
`https://database.windows.net/.default`; that is a **POC compatibility example only**,
not a recommendation to accept SQL tokens at an arbitrary production telemetry
endpoint. Have the endpoint owner approve both values and the intended resource
authorization semantics. No resource/scope is guessed or inherited from JDBC login.

There is no gRPC ingestion bypass. Authorization is consumed by authentication
only; the fork's header-to-payload metadata processor is removed. The producer
does not invent `X-Forwarded-For`, `Original-Uri` or `Original-Method` headers.
If your private MISE/collector build needs forwarded request metadata, provide an
owner-reviewed trusted ingress which overwrites untrusted values and binds them
to the real `/v1/traces` request. This demo deliberately fails rather than faking
an ingress or disabling authorization. Private image/schema compatibility,
certificate onboarding and that ingress requirement are external validation gates.

### Azure CLI mode on a host (optional)

`azure_cli` needs an already authorized Azure CLI on the machine running Java;
authentication is a separate explicit operator action, never performed by this
demo. This image does not support that mode. With JDK 17/Maven on your host and
an approved HTTPS telemetry endpoint, first provide all env variables above, then
from the repository root in Bash:

```bash
mvn -s docs/otel-poc/settings.xml -Pjre17 -DskipTests -Dmaven.javadoc.skip=true install
mvn -s docs/otel-poc/settings.xml -f mssql-jdbc-otel/pom.xml -DskipTests test-compile \
  org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
  -DincludeScope=test -DoutputDirectory=target/dependency
# Linux/macOS/WSL classpath separator:
java -cp 'mssql-jdbc-otel/target/test-classes:mssql-jdbc-otel/target/classes:mssql-jdbc-otel/target/dependency/*' \
  com.microsoft.sqlserver.jdbc.otel.ConnectionErrorDemo
# On native Windows Java (including Git Bash), use semicolons instead:
java -cp 'mssql-jdbc-otel/target/test-classes;mssql-jdbc-otel/target/classes;mssql-jdbc-otel/target/dependency/*' \
  com.microsoft.sqlserver.jdbc.otel.ConnectionErrorDemo
```

Run only the Java command appropriate to your OS. The Docker receivers are not
published; a host run must use a separately approved endpoint. `managed_identity`
needs a real reachable platform identity source; `default` requires an explicitly
configured SDK credential chain. Compose passes relevant Azure credential/identity
environment variables but never manufactures an identity or mounts token caches.

## Optional Delta Lake / Event Hub / bulk loader

[The Delta overlay](docker-compose.delta.yml), layered after the internal overlay,
preserves the fork's `deltalake/otap` exporter and .NET MirrorMaker bulk-loader
configuration, but **only for connection traces**:

```text
private otelcol -- parquet --> Storage
       |                         ^
       +--> Event Hub --> Delta Bulk Loader (commits Delta log)
       +--> local evidence --> Aspire
```

Supply all Delta placeholders in [.env.example](.env.example), including a pinned
bulk-loader image, exact storage/container/prefix, Event Hub FQDN/name/tenant,
dedicated consumer group and checkpoint storage. Provision resources and grant
least-privilege storage data access, Event Hub sender (collector) and receiver
(loader), and checkpoint access separately. The script performs no provisioning,
role assignment or Azure requests itself; starting this opt-in stack causes its
containers to contact the destinations you configure.

Both downstream containers use `ManagedIdentityCredential` through explicitly
supplied `IDENTITY_ENDPOINT` and required secret `IDENTITY_HEADER`. They must be
real platform-provided, reachable identity plumbing for your approved environment.
The identity's resource access must be limited to these demo targets. No default
shared secret, host CLI token broker, guessed token audience or IMDS override is
provided. Ordinary Docker Desktop does **not** acquire Managed Identity by setting
these variables: missing platform identity is a blocker, not a local green result.

The fork's host-CLI token broker is deliberately **not imported**. Such a broker
is privileged delegation of a developer's CLI session, not real Managed Identity.
If separately approved tooling is ever used, it needs an exact resource allowlist,
a required per-session identity secret, internal-only exposure, bounded/cache-aware
token requests and no token/request/error logging. Never expose arbitrary resource
minting, mount the host CLI session into the app, or rely on a shared default secret.

Verify the private loader honors the .NET environment overrides in
[its configuration](internal/delta-bulk-loader/appsettings.json), and start it before
the trace workload (its initial Event Hub offset is `Latest`). Its own telemetry
exporters are disabled; no unauthenticated gRPC port is opened for it.

```bash
DEMO_STACK=delta bash .scripts/dev.sh config
DEMO_STACK=delta bash .scripts/dev.sh up
DEMO_STACK=delta bash .scripts/dev.sh down
```

Wait for Delta flush/commit (at least the configured 60-second buffer window), then
independently inspect committed trace rows. The automatic gate checks **local trace
receipt only**. Private component support, loader readiness, Entra/RBAC, storage
writes and durable Delta commits require your environment; they are not certified
by static Compose validation or the Aspire dashboard.

## Provenance and deliberate differences

Adapted using `git show` from the MIT-licensed
[fork demo at commit 9d46cd4a6c6f4ed9bb6e50fe774fe08acc675144](https://github.com/mdrakiburrahman/mssql-jdbc/tree/9d46cd4a6c6f4ed9bb6e50fe774fe08acc675144/docs/otel-poc).
The repository's MIT license applies. The transferred ideas are the source-built
Maven container, public mirror, Compose lifecycle, Aspire, MISE CheckAccess, and
optional OTAP Delta/Event Hub/bulk-loader topology/configuration.

- Retained the fork's Aspire `8.0` compatibility pin. Its private collector used
  tag `protobus-benchmark-20260623233640`, MISE used `20260622.1`, and the bulk
  loader used an unpinned `latest`. Consult the pinned upstream Compose for registry
  paths, then choose approved immutable references; no private image default or
  automatic registry authentication is embedded here.
- Replaced the fork's core producer with the separately owned optional module's
  no-argument `ConnectionErrorDemo`; Java 17 multistage image instead of a live
  source mount, credential cache and JDK 11 runner.
- Default public local mode instead of mandatory Azure infrastructure. Added real
  trace-evidence gates; removed metrics/statement workload, metric gates and
  assertions of Azure success based on health or absence of export errors.
- Removed SQL/Arc table seeding, default database/identity passwords, resource IDs,
  tenant defaults, certificate downloads, host-CLI broker and fake forwarded headers.
- **Excluded Portainer and its credential file entirely**, including its Docker
  socket mount. No credential file was read or transferred.
- Removed authorization payload copying, `ShowPII=true`, unauthenticated gRPC,
  automatic volume deletion and host build-output cleanup.

For this infrastructure change, static tests/configuration validation do not build
the image or validate private binaries. Missing Java demo/bootstrap integration,
public artifact/image availability, approved private images/certificates and real
identity infrastructure remain explicit build/runtime gates. No services are
started, credentials acquired or Azure resources deployed by authoring these files.