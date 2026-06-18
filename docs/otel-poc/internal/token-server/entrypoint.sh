#!/bin/bash
set -euo pipefail

# az CLI rewrites a few status files inside AZURE_CONFIG_DIR (e.g. azureProfile.json
# command-runner cache, telemetry etc.). The host's ~/.azure is mounted read-only
# at /host/.azure, so we make a writable copy at $HOME/.azure for the duration
# of the container.
if [[ -d /host/.azure ]]; then
  mkdir -p "${HOME}/.azure"
  cp -a /host/.azure/. "${HOME}/.azure/" 2>/dev/null || true
fi
export AZURE_CONFIG_DIR="${HOME}/.azure"

exec /usr/local/bin/token_server.py
