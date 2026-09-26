#!/usr/bin/env bash
set +x
set -euo pipefail
source /app/azure-session.sh
initialize_azure_session
# SQL endpoint is fixed to this stack; the inherited source-built demo remains the producer.
exec bash /app/sql-app.sh