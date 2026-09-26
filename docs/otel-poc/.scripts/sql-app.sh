#!/usr/bin/env bash
# Isolated local SQL demo only. Never trace shell expansions or print credentials.
set +x
set -euo pipefail

if [[ -z "${MSSQL_SA_PASSWORD:-}" ]]; then
  echo 'The local SQL demo requires MSSQL_SA_PASSWORD.' >&2
  exit 2
fi
if [[ ",${DEMO_SCENARIOS:-config,dns}," == *,login,* && -z "${DEMO_LOGIN_PASSWORD:-}" ]]; then
  echo 'The login scenario requires DEMO_LOGIN_PASSWORD for the nonexistent demo login.' >&2
  exit 2
fi
export DEMO_LOGIN_USER="${DEMO_LOGIN_USER:-jdbc_demo_deliberately_nonexistent_login}"
# Escape closing braces so semicolons, braces and shell metacharacters remain data.
password="${MSSQL_SA_PASSWORD//\}/\}\}}"
export JDBC_CONNECTION_STRING="jdbc:sqlserver://sqlserver:1433;databaseName=master;user=sa;password={${password}};encrypt=true;trustServerCertificate=true;loginTimeout=5;connectRetryCount=0;"
if [[ "${DEMO_ENDPOINT_MODE:-explicit}" == discovery ]]; then
  # Force the actual Java preflight query, never a silently preferred explicit route.
  export DEMO_DISCOVERY_CONNECTION_STRING="$JDBC_CONNECTION_STRING"
  unset OTEL_EXPORTER_OTLP_ENDPOINT OTEL_ARM_RESOURCE_ID
fi
unset password MSSQL_SA_PASSWORD
exec java -cp 'target/test-classes:target/classes:target/dependency/*' \
  com.microsoft.sqlserver.jdbc.otel.ConnectionErrorDemo