#!/usr/bin/env bash
# Dedicated connection-error cloud E2E lifecycle. Reads the operator file as data only.
set +x
set -euo pipefail

POC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$POC_DIR"
export MSYS_NO_PATHCONV=1 COMPOSE_DISABLE_ENV_FILE=1
export DEMO_UID="${DEMO_UID:-$(id -u)}" DEMO_GID="${DEMO_GID:-$(id -g)}"

env_file="${1:-}"
action="${2:-}"
if [[ -z "$env_file" || ! -f "$env_file" ]]; then
  echo 'Usage: cloud.sh ENV_FILE config|up|status|logs [service]|down|clean' >&2
  exit 2
fi
case "$action" in config|up|status|logs|down|clean) ;; *) echo 'Invalid cloud action.' >&2; exit 2 ;; esac

run_id="$(node --input-type=module -e "import {readFileSync} from 'node:fs'; import {parseEnv,configure} from './.scripts/cloud-e2e.mjs'; process.stdout.write(configure(parseEnv(readFileSync(process.argv[1],'utf8'))).run)" "$env_file")"
artifact_dir="$POC_DIR/.cloud/$run_id"
node .scripts/cloud-e2e.mjs generate "$env_file" "$artifact_dir" >/dev/null
config="$artifact_dir/config.json"
value() { node -e "const c=require(process.argv[1]); const v=c[process.argv[2]]; if(typeof v!=='string'||!v)process.exit(2); process.stdout.write(v)" "$config" "$1"; }

project="$(value project)"
subscription="$(value subscription)"
resource_group="$(value eventHubResourceGroup)"
namespace="$(value eventHubNamespace)"
hub="$(value eventHubName)"
consumer_group="$(value consumerGroup)"
storage_account="$(value storageAccount)"
storage_container="$(value storageContainer)"
root_path="$(value rootPath)"
checkpoint_account="$(value checkpointStorageAccount)"
checkpoint_container="$(value checkpointStorageContainer)"
config_count="$(node -e "const c=require(process.argv[1]); process.stdout.write(c.scenarioCounts.config)" "$config")"
dns_count="$(node -e "const c=require(process.argv[1]); process.stdout.write(c.scenarioCounts.dns)" "$config")"
login_count="$(node -e "const c=require(process.argv[1]); process.stdout.write(c.scenarioCounts.login)" "$config")"
success_count="$(node -e "const c=require(process.argv[1]); process.stdout.write(c.scenarioCounts.success)" "$config")"

compose=(docker compose --env-file "$env_file" --env-file "$artifact_dir/runtime.env" --project-name "$project" -f docker-compose.yml -f docker-compose.sql.yml -f docker-compose.cloud.yml)

preflight() {
  az account show --subscription "$subscription" --only-show-errors >/dev/null
  az eventhubs eventhub show --subscription "$subscription" --resource-group "$resource_group" --namespace-name "${namespace%%.*}" --name "$hub" --only-show-errors >/dev/null
  az storage fs exists --subscription "$subscription" --account-name "$storage_account" --name "$storage_container" --auth-mode login --only-show-errors --query exists -o tsv | grep -Fx true >/dev/null
  az storage fs exists --subscription "$subscription" --account-name "$checkpoint_account" --name "$checkpoint_container" --auth-mode login --only-show-errors --query exists -o tsv | grep -Fx true >/dev/null
}

create_group() {
  local metadata
  metadata="$(node -e "const c=require(process.argv[1]); process.stdout.write(JSON.stringify({owner:'mssql-jdbc-connection-cloud-e2e',run_id:c.run,root_path:c.rootPath}))" "$config")"
  az eventhubs eventhub consumer-group create --subscription "$subscription" --resource-group "$resource_group" --namespace-name "${namespace%%.*}" --eventhub-name "$hub" --name "$consumer_group" --user-metadata "$metadata" --only-show-errors >/dev/null
}

run_scenario() {
  local scenario="$1" count="$2"
  [[ "$count" == "0" ]] && return 0
  "${compose[@]}" run --rm --no-deps -T \
    -e "DEMO_SCENARIOS=$scenario" -e "DEMO_REPEAT=$count" -e DEMO_PAUSE_SECONDS=0 app
}

delete_group() {
  local metadata
  metadata="$(az eventhubs eventhub consumer-group show --subscription "$subscription" --resource-group "$resource_group" --namespace-name "${namespace%%.*}" --eventhub-name "$hub" --name "$consumer_group" --only-show-errors --query userMetadata -o tsv 2>/dev/null || true)"
  [[ -z "$metadata" ]] && return 0
  node -e "const m=JSON.parse(process.argv[1]); const c=require(process.argv[2]); if(m.owner!=='mssql-jdbc-connection-cloud-e2e'||m.run_id!==c.run||m.root_path!==c.rootPath)process.exit(2)" "$metadata" "$config"
  az eventhubs eventhub consumer-group delete --subscription "$subscription" --resource-group "$resource_group" --namespace-name "${namespace%%.*}" --eventhub-name "$hub" --name "$consumer_group" --only-show-errors
}

case "$action" in
  config)
    preflight
    "${compose[@]}" --profile tools config --quiet
    node .scripts/cloud-e2e.mjs config "$env_file"
    ;;
  up)
    preflight
    create_group
    "${compose[@]}" --profile tools config --quiet
    "${compose[@]}" --profile tools build app token-server grafana cloud-verifier
    "${compose[@]}" up -d --wait --wait-timeout 240 evidence token-server otelcol delta-bulk-loader sqlserver grafana-token-server grafana
    "${compose[@]}" run --rm --no-deps -T verify --wait local
    # Separate deterministic batches produce a realistic uneven distribution
    # while preserving one finite Java process and one root per failed open.
    run_scenario config "$config_count"
    run_scenario dns "$dns_count"
    run_scenario login "$login_count"
    run_scenario success "$success_count"
    "${compose[@]}" run --rm --no-deps -T verify
    "${compose[@]}" cp evidence:/evidence/traces.json "$artifact_dir/evidence.json"
    node .scripts/cloud-e2e.mjs expected "$env_file" "$artifact_dir/evidence.json" > "$artifact_dir/expected.json"
    "${compose[@]}" run --rm --no-deps -T cloud-verifier
    printf 'Grafana: http://127.0.0.1:3001/d/jdbc-connection-errors\n'
    ;;
  status) "${compose[@]}" ps ;;
  logs) shift 2; "${compose[@]}" logs --tail=100 -f "$@" ;;
  down) "${compose[@]}" --profile tools down; delete_group ;;
  clean)
    "${compose[@]}" --profile tools down --volumes
    delete_group
    # Delete only the exact owner/run prefix validated by cloud-e2e.mjs.
    az storage fs directory delete --subscription "$subscription" --account-name "$storage_account" --file-system "$storage_container" --name "$root_path" --auth-mode login --yes --only-show-errors >/dev/null 2>&1 || true
    ;;
esac