#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# dev.sh — Docker-only driver for the mssql-jdbc OpenTelemetry POC, wired to the
# team's INTERNAL telemetry pipeline (otelcol-arcdata -> Azure Delta Lake) with a
# .NET Aspire Dashboard for local viz.
#
# The host needs Docker + bash + an `az login` session (the token-server vends
# Managed Identity tokens from ~/.azure) with AcrPull on arcdataanalyticsacr and
# RBAC on the target storage account / event hub. JDK, Maven, and SQL Server all
# run in containers; every build step and probe runs inside the stack.
#
# Usage:
#   ./.scripts/dev.sh [up]      Build, bring up the stack, prove green, then leave
#                               a load generator running for live dashboards.
#   ./.scripts/dev.sh down      Stop and remove the stack (keeps the SQL volume).
#   ./.scripts/dev.sh clean     Stop, remove the stack + SQL volume + build output.
#   ./.scripts/dev.sh status    Show container status and a live health summary.
#   ./.scripts/dev.sh logs [svc] Tail logs (all services, or one, e.g. loadgen).
#
# Definition of GREEN (all asserted automatically by `up`, LOCAL only — the Azure
# Delta Lake side is best-effort and never inspected):
#   1. OtelPocSmokeTest (JUnit) passes.
#   2. otelcol-arcdata received the driver's metrics AND traces (debug exporter).
#   3. Aspire Dashboard is reachable and the otlp/aspire export is not failing.
#   4. Health: sqlserver + token-server healthy; arcdata, delta-bulk-loader,
#      aspire-dashboard running.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_DIR="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(cd "$POC_DIR/../.." && pwd)"
cd "$POC_DIR"

# --- knobs (override via env) ----------------------------------------------
BURST_SECONDS="${BURST_SECONDS:-60}"             # timed burst length for the green gate
OTEL_ENDPOINT="http://otelcol-arcdata:4318/v1/metrics"
LOADGEN_SERVICE="mssql-jdbc-poc-loadgen"         # otelServiceName the load generator uses
ASPIRE_URL="${ASPIRE_URL:-http://localhost:18888}"   # Aspire Dashboard UI (published to the host)
PORTAINER_URL="${PORTAINER_URL:-https://localhost:9443}" # Portainer Docker UI (published to the host)
PORTAINER_ADMIN_PASSWORD="$(cat "$POC_DIR/internal/portainer/admin-password" 2>/dev/null || true)" # Pre-seeded Portainer admin password
ACR_REGISTRY="${ACR_REGISTRY:-arcdataanalyticsacr}"  # ACR that hosts the private images
SQL_HEALTH_TIMEOUT="${SQL_HEALTH_TIMEOUT:-240}"  # seconds to wait for SQL Server
ASSERT_RETRIES="${ASSERT_RETRIES:-30}"           # metric/trace assertion attempts
ASSERT_INTERVAL="${ASSERT_INTERVAL:-5}"          # seconds between assertion attempts

# Image-based services (the rest are built locally).
IMAGE_SERVICES=(sqlserver otelcol-arcdata delta-bulk-loader aspire-dashboard portainer)

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_RED="\033[1;31m"; C_YEL="\033[1;33m"; C_OFF="\033[0m"
log()  { printf "${C_BLUE}==>${C_OFF} %s\n" "$*"; }
ok()   { printf "${C_GREEN}  OK${C_OFF} %s\n" "$*"; }
warn() { printf "${C_YEL}  ..${C_OFF} %s\n" "$*"; }
die()  { printf "${C_RED}FAIL${C_OFF} %s\n" "$*" >&2; exit 1; }

# Print the Aspire UI URL prominently and report whether it is reachable from the
# host. The port is published to the host (0.0.0.0:18888) by docker-compose, so it
# is locally accessible; in a remote dev container, forward port 18888 to your machine.
print_aspire_url() {
  local code="n/a"
  if command -v curl >/dev/null 2>&1; then
    code="$(curl -s -o /dev/null -w '%{http_code}' "$ASPIRE_URL" 2>/dev/null || echo 000)"
  fi
  case "$code" in
    200|301|302) printf "${C_GREEN}>>> Aspire Dashboard:${C_OFF} ${C_BLUE}%s${C_OFF} ${C_GREEN}[reachable on host: http %s]${C_OFF}\n" "$ASPIRE_URL" "$code" ;;
    *)           printf "${C_GREEN}>>> Aspire Dashboard:${C_OFF} ${C_BLUE}%s${C_OFF} ${C_YEL}[port published; open it in your browser]${C_OFF}\n" "$ASPIRE_URL" ;;
  esac
}

# Print the Portainer UI URL prominently and report whether it answers on the host.
# Portainer serves HTTPS on 9443 with a self-signed cert (curl -k) and prompts for a
# one-time admin account on first visit. In a remote dev container, forward port 9443.
print_portainer_url() {
  local code="n/a"
  if command -v curl >/dev/null 2>&1; then
    code="$(curl -sk -o /dev/null -w '%{http_code}' "$PORTAINER_URL" 2>/dev/null || echo 000)"
  fi
  case "$code" in
    200|301|302|307|308) printf "${C_GREEN}>>> Portainer:${C_OFF} ${C_BLUE}%s${C_OFF} ${C_GREEN}[reachable on host: http %s — self-signed TLS; log in with admin / the SA password]${C_OFF}\n" "$PORTAINER_URL" "$code" ;;
    *)                   printf "${C_GREEN}>>> Portainer:${C_OFF} ${C_BLUE}%s${C_OFF} ${C_YEL}[port published; open it in your browser (self-signed TLS)]${C_OFF}\n" "$PORTAINER_URL" ;;
  esac
}

# Detect arch: on arm64 the SQL Server 2022 image won't run — use Azure SQL Edge.
detect_sql_image() {
  if [[ -z "${SQL_SERVER_IMAGE:-}" ]]; then
    case "$(uname -m)" in
      aarch64|arm64)
        export SQL_SERVER_IMAGE="mcr.microsoft.com/azure-sql-edge:latest"
        warn "arm64 detected — using $SQL_SERVER_IMAGE for SQL Server" ;;
    esac
  fi
  if [[ -n "${SQL_SERVER_IMAGE:-}" ]]; then
    log "SQL Server image: $SQL_SERVER_IMAGE"
  else
    log "SQL Server image: default (mcr.microsoft.com/mssql/server:2022-latest)"
  fi
  return 0
}

# Run a bash snippet inside a throwaway app container (has curl + jq), on the
# stack network, without re-triggering dependency waits.
in_net() { docker compose run --rm --no-deps -T app bash -lc "$1"; }

mvn_run() { docker compose run --rm -T app mvn "$@"; }

# Log in to the ACR that hosts the private collector + bulk-loader images. If the
# login fails but both images already exist locally, continue with a warning.
ensure_acr() {
  log "Authenticating to ACR '$ACR_REGISTRY' for the private images..."
  if az acr login --name "$ACR_REGISTRY" >/dev/null 2>&1; then
    ok "ACR login succeeded"
    return 0
  fi
  warn "az acr login failed (is 'az login' done with AcrPull on $ACR_REGISTRY?)"
  local arc dbl
  arc="$(docker compose config --images 2>/dev/null | grep otelcol-arcdata || true)"
  dbl="$(docker compose config --images 2>/dev/null | grep dataplane-mirror-maker-service || true)"
  if [[ -n "$arc" ]] && docker image inspect "$arc" >/dev/null 2>&1 \
     && [[ -n "$dbl" ]] && docker image inspect "$dbl" >/dev/null 2>&1; then
    warn "private images already present locally — continuing"
    return 0
  fi
  die "cannot obtain the private images: run 'az login' on the host with AcrPull on $ACR_REGISTRY"
}

# ---------------------------------------------------------------------------
wait_sql_healthy() {
  log "Waiting for SQL Server to become healthy (timeout ${SQL_HEALTH_TIMEOUT}s)..."
  local deadline=$(( $(date +%s) + SQL_HEALTH_TIMEOUT )) status
  while true; do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
                mssql-jdbc-sqlserver 2>/dev/null || echo missing)"
    [[ "$status" == "healthy" ]] && { ok "sqlserver healthy"; return 0; }
    (( $(date +%s) >= deadline )) && {
      docker compose logs --tail=40 sqlserver || true
      die "sqlserver did not become healthy in ${SQL_HEALTH_TIMEOUT}s (status=$status)"
    }
    sleep 5
  done
}

wait_container_healthy() {  # <container-name> <pretty-name> [timeout]
  local cname="$1" pretty="$2" timeout="${3:-120}"
  log "Waiting for $pretty to become healthy (timeout ${timeout}s)..."
  local deadline=$(( $(date +%s) + timeout )) status
  while true; do
    status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
                "$cname" 2>/dev/null || echo missing)"
    [[ "$status" == "healthy" ]] && { ok "$pretty healthy"; return 0; }
    (( $(date +%s) >= deadline )) && {
      docker compose logs --tail=40 "${pretty}" 2>/dev/null || true
      die "$pretty did not become healthy in ${timeout}s (status=$status)"
    }
    sleep 3
  done
}

container_running() {  # <container-name>
  [[ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || echo false)" == "true" ]]
}

wait_arcdata_ready() {
  wait_container_healthy mssql-jdbc-otelcol-arcdata otelcol-arcdata 90
}

wait_aspire_ready() {
  log "Waiting for the Aspire Dashboard to answer on :18888..."
  in_net '
    deadline=$(( $(date +%s) + 120 ))
    while true; do
      code="$(curl -s -o /dev/null -w "%{http_code}" http://aspire-dashboard:18888/ || echo 000)"
      case "$code" in 200|302|301) echo "  OK   aspire-dashboard (http $code)"; exit 0;; esac
      [ "$(date +%s)" -ge "$deadline" ] && { echo "  FAIL aspire-dashboard last=$code"; exit 1; }
      sleep 3
    done
  ' || die "Aspire Dashboard never answered on :18888"
}

assert_arcdata_flow() {
  log "Asserting otelcol-arcdata received driver metrics + traces (gate 2)..."
  local i logs m t
  for (( i=1; i<=ASSERT_RETRIES; i++ )); do
    logs="$(docker compose logs --tail=1000 otelcol-arcdata 2>/dev/null || true)"
    m="$(printf '%s' "$logs" | grep -c '"otelcol.component.id": "debug".*"otelcol.signal": "metrics"' || true)"
    t="$(printf '%s' "$logs" | grep -c '"otelcol.component.id": "debug".*"otelcol.signal": "traces"' || true)"
    if [[ "${m:-0}" -gt 0 && "${t:-0}" -gt 0 ]]; then
      ok "arcdata saw $m metrics batch(es) and $t trace batch(es) from the driver"
      return 0
    fi
    warn "attempt $i/${ASSERT_RETRIES}: metrics=$m traces=$t — retrying..."
    sleep "$ASSERT_INTERVAL"
  done
  docker compose logs --tail=80 otelcol-arcdata || true
  die "otelcol-arcdata never logged both driver metrics and traces"
}

assert_aspire_receiving() {
  log "Asserting the Aspire export path is healthy (gate 3)..."
  local i fails
  for (( i=1; i<=ASSERT_RETRIES; i++ )); do
    # A successful otlp export is silent; only failures log. Require a clean
    # recent window so transient startup-race backoffs have rolled off.
    fails="$(docker compose logs --since 45s otelcol-arcdata 2>/dev/null \
              | grep 'otlp/aspire' | grep -c 'Exporting failed' || true)"
    if [[ "${fails:-0}" -eq 0 ]]; then
      ok "no otlp/aspire export failures in the last 45s — Aspire is receiving"
      return 0
    fi
    warn "attempt $i/${ASSERT_RETRIES}: $fails recent otlp/aspire failure(s) — retrying..."
    sleep "$ASSERT_INTERVAL"
  done
  docker compose logs --tail=40 otelcol-arcdata aspire-dashboard || true
  die "otelcol-arcdata kept failing to export to the Aspire Dashboard"
}

health_summary() {
  log "Health probe summary:"
  local name cname st
  for pair in "sqlserver:mssql-jdbc-sqlserver" "token-server:mssql-jdbc-token-server" \
              "otelcol-arcdata:mssql-jdbc-otelcol-arcdata" "delta-bulk-loader:mssql-jdbc-delta-bulk-loader" \
              "aspire-dashboard:mssql-jdbc-aspire-dashboard" "portainer:mssql-jdbc-portainer"; do
    name="${pair%%:*}"; cname="${pair##*:}"
    st="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{if .State.Running}}running{{else}}{{.State.Status}}{{end}}{{end}}' \
            "$cname" 2>/dev/null || echo missing)"
    printf "    %-18s %s\n" "$name" "$st"
  done
  in_net 'code="$(curl -s -o /dev/null -w "%{http_code}" http://aspire-dashboard:18888/ || echo 000)"; printf "    %-18s http %s\n" "aspire(:18888)" "$code"' || true
}

# ---------------------------------------------------------------------------
# Blow away every trace of a prior run so `up` always starts from a clean slate:
# all stack containers, the SQL Server data volume, and the build output.
nuke_state() {
  log "Fresh start: removing prior containers, the SQL volume, and build output..."
  docker compose --profile tools --profile loadgen down --remove-orphans --volumes >/dev/null 2>&1 || true
  # target/ is written root-owned from inside the container; drop it via a tiny
  # throwaway container so no app image build is needed yet.
  docker run --rm -v "$REPO_ROOT:/work" -w /work busybox rm -rf target >/dev/null 2>&1 || true
  ok "prior state cleared"
}

# ---------------------------------------------------------------------------
cmd_up() {
  nuke_state
  detect_sql_image
  ensure_acr

  log "Pulling image-based services..."
  docker compose pull "${IMAGE_SERVICES[@]}"
  ok "images pulled"

  log "Building the token-server + Java builder/runner image (first run downloads deps)..."
  docker compose build token-server app
  ok "images built"

  log "Bringing up the internal telemetry + SQL Server stack..."
  docker compose up -d sqlserver token-server otelcol-arcdata delta-bulk-loader aspire-dashboard portainer

  wait_sql_healthy
  wait_container_healthy mssql-jdbc-token-server token-server 90
  wait_arcdata_ready
  wait_aspire_ready

  log "Compiling driver + test classes (incremental against warm cache)..."
  mvn_run -B -Pjre11 -DskipTests test-compile
  ok "test-compile succeeded"

  log "Running OtelPocSmokeTest (gate 1)..."
  if mvn_run -B -Pjre11 -Dtest=OtelPocSmokeTest -DfailIfNoTests=false test; then
    ok "smoke test passed"
  else
    die "OtelPocSmokeTest failed"
  fi

  log "Running a ${BURST_SECONDS}s load burst to push metrics + traces through arcdata..."
  mvn_run -B -Pjre11 org.codehaus.mojo:exec-maven-plugin:3.1.0:java \
    -Dexec.classpathScope=test \
    -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen \
    -Dexec.args="${BURST_SECONDS} s" \
    -DotelEndpoint="${OTEL_ENDPOINT}" \
    -DotelExportInterval=5
  ok "load burst complete"

  assert_arcdata_flow
  assert_aspire_receiving
  health_summary

  log "Starting the long-running load generator for live dashboards..."
  docker compose up -d loadgen
  ok "loadgen running"

  printf "\n${C_GREEN}========================================================${C_OFF}\n"
  printf "${C_GREEN} GREEN - mssql-jdbc OTel POC is up and verified.${C_OFF}\n"
  printf "${C_GREEN}========================================================${C_OFF}\n"
  cat <<EOF

  Aspire Dashboard  ${ASPIRE_URL}   (metrics, traces, structured logs)
  Portainer         ${PORTAINER_URL}   (Docker UI: logs, stats, shells — login admin / ${PORTAINER_ADMIN_PASSWORD})
  Collector OTLP    localhost:4318 (HTTP) / 4317 (gRPC)
  Azure Delta Lake  best-effort sink via deltalake exporter + delta-bulk-loader

  Live load:  ./.scripts/dev.sh logs loadgen
  Tear down:  ./.scripts/dev.sh down   (or 'clean' to drop the SQL volume + target/)

EOF
  print_aspire_url
  print_portainer_url
}

cmd_down() {
  log "Stopping and removing the stack..."
  docker compose --profile tools --profile loadgen down --remove-orphans
  ok "stack down (SQL volume preserved; use 'clean' to remove it)"
}

cmd_clean() {
  log "Removing build output (target/) via a throwaway container..."
  docker compose run --rm --no-deps -T app rm -rf target 2>/dev/null || true
  log "Stopping and removing the stack + volumes..."
  docker compose --profile tools --profile loadgen down --remove-orphans --volumes
  ok "clean complete"
}

cmd_status() {
  docker compose ps
  echo
  detect_sql_image
  health_summary
  print_aspire_url
  print_portainer_url
}

cmd_logs() { docker compose logs -f "${1:-}"; }

main() {
  case "${1:-up}" in
    up)      cmd_up ;;
    down)    cmd_down ;;
    clean)   cmd_clean ;;
    status)  cmd_status ;;
    logs)    shift || true; cmd_logs "${1:-}" ;;
    *)       die "unknown command: ${1:-} (use: up | down | clean | status | logs)" ;;
  esac
}

main "$@"
