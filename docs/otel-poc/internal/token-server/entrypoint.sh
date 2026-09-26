#!/usr/bin/env bash
set +x
set -euo pipefail
source /app/azure-session.sh
initialize_azure_session
exec python3 /app/token_server.py