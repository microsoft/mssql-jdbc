#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# dev.sh — Docker-only driver for the mssql-jdbc OpenTelemetry POC.
#
# The host needs nothing but Docker + bash. JDK, Maven, and SQL Server all run
# in containers; every build step, probe, and assertion runs inside the stack,
# so no java/mvn/curl/jq is required on the host.
#
# Usage:
#   ./.scripts/dev.sh [up]     Build, bring up the stack, prove green, then leave
#                              a load generator running for live dashboards.
#   ./.scripts/dev.sh down      Stop and remove the stack (keeps the SQL volume).
#   ./.scripts/dev.sh clean     Stop, remove the stack + SQL volume + build output.
#   ./.scripts/dev.sh status    Show container status and a live health summary.
#   ./.scripts/dev.sh logs [svc] Tail logs (all services, or one, e.g. loadgen).
#
# Definition of GREEN (all asserted automatically by `up`):
#   1. OtelPocSmokeTest (JUnit) passes.
#   2. Prometheus has >=1 db_client_* series from the load generator.
#   3. Jaeger lists service "mssql-jdbc-poc-loadgen" with >=1 trace.
#   4. Health probes pass for sqlserver, otel-collector, prometheus, grafana, jaeger.
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POC_DIR="$(dirname "$SCRIPT_DIR")"
cd "$POC_DIR"

# --- knobs (override via env) ----------------------------------------------
BURST_SECONDS="${BURST_SECONDS:-60}"        # timed burst length for the green gate
OTEL_ENDPOINT="http://otel-collector:4318/v1/metrics"
LOADGEN_SERVICE="mssql-jdbc-poc-loadgen"    # otelServiceName the load generator uses
SQL_HEALTH_TIMEOUT="${SQL_HEALTH_TIMEOUT:-240}"  # seconds to wait for SQL Server
ASSERT_RETRIES="${ASSERT_RETRIES:-30}"      # metric/trace assertion attempts
ASSERT_INTERVAL="${ASSERT_INTERVAL:-5}"     # seconds between assertion attempts

C_BLUE="\033[1;34m"; C_GREEN="\033[1;32m"; C_RED="\033[1;31m"; C_YEL="\033[1;33m"; C_OFF="\033[0m"
log()  { printf "${C_BLUE}==>${C_OFF} %s\n" "$*"; }
ok()   { printf "${C_GREEN}  OK${C_OFF} %s\n" "$*"; }
warn() { printf "${C_YEL}  ..${C_OFF} %s\n" "$*"; }
die()  { printf "${C_RED}FAIL${C_OFF} %s\n" "$*" >&2; exit 1; }

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

# Probe the four HTTP services from inside the network until all return 200.
wait_http_healthy() {
  log "Probing collector / prometheus / grafana / jaeger health endpoints..."
  in_net '
    set -e
    probe() {
      local name="$1" url="$2" deadline=$(( $(date +%s) + 120 ))
      while true; do
        code="$(curl -s -o /dev/null -w "%{http_code}" "$url" || echo 000)"
        if [ "$code" = "200" ]; then echo "  OK   $name ($url)"; return 0; fi
        if [ "$(date +%s)" -ge "$deadline" ]; then echo "  FAIL $name ($url) last=$code"; return 1; fi
        sleep 3
      done
    }
    probe otel-collector http://otel-collector:13133/
    probe prometheus     http://prometheus:9090/-/healthy
    probe grafana        http://grafana:3000/api/health
    probe jaeger         http://jaeger:16686/
  ' || die "one or more service health probes failed"
  ok "all HTTP health probes returned 200"
}

assert_prometheus_metrics() {
  log "Asserting Prometheus has db_client_* series (gate 2)..."
  in_net "
    for i in \$(seq 1 ${ASSERT_RETRIES}); do
      n=\$(curl -s 'http://prometheus:9090/api/v1/label/__name__/values' \
            | jq -r '.data[]?' | grep -c '^db_client_' || true)
      if [ \"\${n:-0}\" -gt 0 ]; then
        echo \"  found \$n db_client_* metric name(s):\"
        curl -s 'http://prometheus:9090/api/v1/label/__name__/values' \
          | jq -r '.data[]?' | grep '^db_client_' | sed 's/^/    /'
        exit 0
      fi
      echo \"  attempt \$i/${ASSERT_RETRIES}: no db_client_* series yet, retrying...\"
      sleep ${ASSERT_INTERVAL}
    done
    exit 1
  " || { docker compose logs --tail=60 otel-collector prometheus || true; die "Prometheus never exposed db_client_* metrics"; }
  ok "Prometheus is serving driver metrics"
}

assert_jaeger_traces() {
  log "Asserting Jaeger has traces for ${LOADGEN_SERVICE} (gate 3)..."
  in_net "
    for i in \$(seq 1 ${ASSERT_RETRIES}); do
      if curl -s 'http://jaeger:16686/api/services' | jq -r '.data[]?' \
           | grep -Fxq '${LOADGEN_SERVICE}'; then
        cnt=\$(curl -s 'http://jaeger:16686/api/traces?service=${LOADGEN_SERVICE}&limit=5&lookback=1h' \
               | jq -r '.data | length' 2>/dev/null || echo 0)
        echo \"  service '${LOADGEN_SERVICE}' present; sampled \${cnt} trace(s)\"
        [ \"\${cnt:-0}\" -gt 0 ] && exit 0
      fi
      echo \"  attempt \$i/${ASSERT_RETRIES}: traces not visible yet, retrying...\"
      sleep ${ASSERT_INTERVAL}
    done
    exit 1
  " || { docker compose logs --tail=60 jaeger otel-collector || true; die "Jaeger never reported traces for ${LOADGEN_SERVICE}"; }
  ok "Jaeger is serving driver traces"
}

health_summary() {
  log "Health probe summary:"
  local sql
  sql="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' \
           mssql-jdbc-sqlserver 2>/dev/null || echo missing)"
  printf "    %-16s %s\n" "sqlserver" "$sql"
  in_net '
    line() { code="$(curl -s -o /dev/null -w "%{http_code}" "$2" || echo 000)"; printf "    %-16s http %s\n" "$1" "$code"; }
    line otel-collector http://otel-collector:13133/
    line prometheus     http://prometheus:9090/-/healthy
    line grafana        http://grafana:3000/api/health
    line jaeger         http://jaeger:16686/
  ' || true
}

# ---------------------------------------------------------------------------
cmd_up() {
  detect_sql_image
  log "Building the OTel POC builder/runner image (first run downloads deps)..."
  docker compose build app
  ok "image built"

  log "Bringing up the observability + SQL Server stack..."
  docker compose up -d sqlserver otel-collector prometheus grafana jaeger
  wait_sql_healthy
  wait_http_healthy

  log "Compiling driver + test classes (incremental against warm cache)..."
  mvn_run -B -Pjre11 -DskipTests test-compile
  ok "test-compile succeeded"

  log "Running OtelPocSmokeTest (gate 1)..."
  if mvn_run -B -Pjre11 -Dtest=OtelPocSmokeTest -DfailIfNoTests=false test; then
    ok "smoke test passed"
  else
    die "OtelPocSmokeTest failed"
  fi

  log "Running a ${BURST_SECONDS}s load burst to populate metrics + traces..."
  mvn_run -B -Pjre11 org.codehaus.mojo:exec-maven-plugin:3.1.0:java \
    -Dexec.classpathScope=test \
    -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen \
    -Dexec.args="${BURST_SECONDS} s" \
    -DotelEndpoint="${OTEL_ENDPOINT}" \
    -DotelExportInterval=5
  ok "load burst complete"

  assert_prometheus_metrics
  assert_jaeger_traces
  health_summary

  log "Starting the long-running load generator for live dashboards..."
  docker compose up -d loadgen
  ok "loadgen running"

  printf "\n${C_GREEN}========================================================${C_OFF}\n"
  printf "${C_GREEN} GREEN - mssql-jdbc OTel POC is up and verified.${C_OFF}\n"
  printf "${C_GREEN}========================================================${C_OFF}\n"
  cat <<EOF

  Grafana     http://localhost:3000   (Dashboards -> mssql-jdbc)
  Prometheus  http://localhost:9090
  Jaeger      http://localhost:16686  (service: ${LOADGEN_SERVICE})
  Collector   http://localhost:8889/metrics

  Live load:  ./.scripts/dev.sh logs loadgen
  Tear down:  ./.scripts/dev.sh down   (or 'clean' to drop the SQL volume + target/)

EOF
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
