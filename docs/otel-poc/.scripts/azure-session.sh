#!/usr/bin/env bash
# Sourced by the two opt-in CLI entrypoints. No login or persistent credential copy.
set +x
set -euo pipefail

initialize_azure_session() {
  umask 077
  # Never fall back to the image layer, a named volume, or the host cache.
  if ! mountpoint -q /run/azure || [[ "$(stat -f -c %T /run/azure)" != tmpfs ]] \
      || [[ ! -r /host/.azure/azureProfile.json ]]; then
    echo 'An authorized readable CLI cache and private /run/azure tmpfs are required.' >&2
    exit 2
  fi
  # Avoid host-cache symlinks escaping the bounded session copy.
  if [[ -n "$(find /host/.azure -type l -print -quit 2>/dev/null)" ]]; then
    echo 'CLI cache symlinks are not supported.' >&2
    exit 2
  fi
  export HOME=/run/azure AZURE_CONFIG_DIR=/run/azure/session
  mkdir -p "$AZURE_CONFIG_DIR"
  if ! cp -R --no-preserve=mode,ownership /host/.azure/. "$AZURE_CONFIG_DIR/" 2>/dev/null; then
    echo 'Unable to initialize the ephemeral CLI session.' >&2
    exit 2
  fi
  export AZURE_CORE_COLLECT_TELEMETRY=false AZURE_CORE_ONLY_SHOW_ERRORS=true
}