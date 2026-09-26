#!/usr/bin/env bash
# Fork-derived demo lifecycle, without login, credential downloads or implicit cleanup.
set +x
set -euo pipefail

POC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$POC_DIR"
# Prevent Git Bash rewriting Docker/Linux paths on Windows.
export MSYS_NO_PATHCONV=1
compat=false
if [[ "${1:-}" == compat ]]; then
  compat=true
  shift
  export DEMO_STACK=delta DEMO_WITH_SQL=true
  # Aliases are process-environment only. Never source an operator file as shell code.
  export OTEL_ARM_RESOURCE_ID="${OTEL_ARM_RESOURCE_ID:-${ARM_RESOURCE_ID:-${AZURE_RESOURCE_ID:-}}}"
  export MISE_CERT_PATH="${MISE_CERT_PATH:-${MISE_CERT_FILE:-}}"
  export AZURE_HOST_PATH="${AZURE_HOST_PATH:-${HOME}/.azure}"
  export IDENTITY_ENDPOINT='http://token-server:8080/metadata/identity/oauth2/token'
  export OTEL_AUTH_MODE="${OTEL_AUTH_MODE:-azure_cli}"
  export DEMO_ENDPOINT_MODE="${DEMO_ENDPOINT_MODE:-explicit}"
fi
mode="${DEMO_STACK:-local}"
compose=(docker compose --project-name mssql-jdbc-connection-poc -f docker-compose.yml)
services=(aspire-dashboard otelcol)
case "$mode" in
  local) ;;
  internal|delta)
    compose+=(-f docker-compose.internal.yml)
    services+=(mise evidence)
    if [[ "$mode" == delta ]]; then
      compose+=(-f docker-compose.delta.yml)
      services+=(delta-bulk-loader)
    fi
    ;;
  *) echo 'DEMO_STACK must be local, internal or delta.' >&2; exit 2 ;;
esac
case "${DEMO_WITH_SQL:-false}" in
  true)
    compose+=(-f docker-compose.sql.yml)
    services+=(sqlserver)
    ;;
  false) ;;
  *) echo 'DEMO_WITH_SQL must be true or false.' >&2; exit 2 ;;
esac
if [[ "$compat" == true ]]; then
  compose+=(-f docker-compose.compat.yml)
  services+=(token-server)
  case "$DEMO_ENDPOINT_MODE" in
    explicit) ;;
    discovery) compose+=(-f docker-compose.discovery.yml) ;;
    *) echo 'DEMO_ENDPOINT_MODE must be explicit or discovery.' >&2; exit 2 ;;
  esac
fi

check_delta_group() {
  if [[ "$mode" == delta && ( -z "${EVENT_HUB_CONSUMER_GROUP:-}" || "${EVENT_HUB_CONSUMER_GROUP,,}" == '$default' ) ]]; then
    echo 'Delta requires a fresh dedicated EVENT_HUB_CONSUMER_GROUP in the process environment, never $Default.' >&2
    exit 2
  fi
}

prepare_sql() {
  if [[ "${DEMO_WITH_SQL:-false}" == true ]]; then
    # run --no-deps does not enforce app.depends_on; wait on the actual SQL healthcheck.
    "${compose[@]}" up -d --wait --wait-timeout 180 sqlserver
    if [[ "$compat" == true && "$DEMO_ENDPOINT_MODE" == discovery ]]; then
      # Repeat safely: seed refuses conflicting/multiple rows, never replaces operator metadata.
      "${compose[@]}" run --rm --no-deps -T sqlserver-init
    fi
  fi
}

check_auth() {
  check_delta_group
  if [[ "$mode" != local && "${OTEL_AUTH_MODE:-none}" == none ]]; then
    echo 'Internal runs require an explicitly selected OTEL_AUTH_MODE in the process environment.' >&2
    exit 2
  fi
  if [[ "$compat" == true ]]; then
    local identity_header="${IDENTITY_HEADER:-}"
    if [[ ! -d "$AZURE_HOST_PATH" || ${#identity_header} -lt 32 ]]; then
      echo 'Compat requires an existing authorized CLI cache and a runtime IDENTITY_HEADER of at least 32 characters.' >&2
      exit 2
    fi
    if [[ "${OTEL_AUTH_MODE:-}" != azure_cli ]]; then
      echo 'Compat requires OTEL_AUTH_MODE=azure_cli; other auth modes use the normal overlays.' >&2
      exit 2
    fi
  elif [[ "${OTEL_AUTH_MODE:-none}" == azure_cli ]]; then
    echo 'The source-only runtime has no Azure CLI or host CLI session. Use the documented host workflow or another explicit mode.' >&2
    exit 2
  fi
}

gate() {
  check_auth
  prepare_sql
  # Unique service identity prevents an earlier successful run satisfying this gate.
  export OTEL_SERVICE_NAME="mssql-jdbc-connection-gate-$(date +%s)-$$-${RANDOM}"
  "${compose[@]}" run --rm --no-deps -T verify --wait "$mode"
  "${compose[@]}" run --rm --no-deps -T app
  "${compose[@]}" run --rm --no-deps -T verify
  printf 'Inspect the same run in Aspire Traces: http://localhost:18888 (service %s)\n' "$OTEL_SERVICE_NAME"
}

case "${1:-help}" in
  config)
    # Quiet is intentional: resolved Compose output can contain secrets.
    check_delta_group
    "${compose[@]}" config --quiet
    ;;
  build)
    targets=(app)
    [[ "$compat" == false ]] || targets+=(token-server)
    "${compose[@]}" build "${targets[@]}"
    ;;
  up)
    check_auth
    "${compose[@]}" config --quiet
    targets=(app)
    [[ "$compat" == false ]] || targets+=(token-server)
    "${compose[@]}" build "${targets[@]}"
    "${compose[@]}" up -d "${services[@]}"
    gate
    ;;
  gate) gate ;;
  run)
    check_auth
    prepare_sql
    "${compose[@]}" run --rm --no-deps -T verify --wait "$mode"
    "${compose[@]}" run --rm --no-deps -T app
    ;;
  status) "${compose[@]}" ps ;;
  logs)
    shift
    "${compose[@]}" logs --tail=100 -f "$@"
    ;;
  down) "${compose[@]}" --profile tools down ;;
  clean)
    # Explicitly scoped to this Compose project; no host files or other volumes.
    "${compose[@]}" --profile tools down --volumes
    ;;
  help)
    echo 'Usage: bash .scripts/dev.sh config|build|up|gate|run|status|logs [service]|down|clean'
    echo 'DEMO_STACK=local (default), internal, or delta. up preserves existing volumes.'
    echo 'DEMO_WITH_SQL=true adds isolated local SQL in any mode; supply its password externally.'
    echo 'compat up|config|build|gate|run|status|logs|down|clean opts into SQL + MISE + Delta + CLI broker.'
    echo 'compat alone displays help. DEMO_ENDPOINT_MODE=discovery exercises seeded HTTPS discovery.'
    ;;
  *) echo 'Unknown command. Use help.' >&2; exit 2 ;;
esac