#!/usr/bin/env bash
# Operator-supplied metadata in this stack's sandbox only. Never print SQL or values.
set +x
set -euo pipefail
fail() { echo 'Sandbox discovery seed rejected; check metadata, SQL readiness and existing rows.' >&2; exit 2; }
[[ -n "${MSSQL_SA_PASSWORD:-}" ]] || fail
for name in AZURE_RESOURCE_ID DEMO_LOCAL_OTEL_ENDPOINT AZURE_REGION OTEL_DISCOVERY_ALLOWED_HOST; do
  value="${!name:-}"
  [[ -n "$value" && "$value" != *$'\n'* && "$value" != *$'\r'* && "$value" != *$'\t'* ]] || fail
done
[[ ${#AZURE_RESOURCE_ID} -le 512 && ${#DEMO_LOCAL_OTEL_ENDPOINT} -le 512 && ${#AZURE_REGION} -le 128 ]] || fail
[[ "$AZURE_RESOURCE_ID" == /subscriptions/* && "$AZURE_REGION" =~ ^[a-zA-Z0-9-]+$ ]] || fail
# Reject credentials, fragments, queries and broad host patterns before writing anything.
[[ "$OTEL_DISCOVERY_ALLOWED_HOST" =~ ^[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?$ ]] || fail
[[ "$DEMO_LOCAL_OTEL_ENDPOINT" =~ ^https://([a-zA-Z0-9.-]+)(:[0-9]+)?(/v1/traces|/)?$ ]] || fail
[[ "${BASH_REMATCH[1],,}" == "${OTEL_DISCOVERY_ALLOWED_HOST,,}" ]] || fail

# SQL Unicode literals: double every single quote; -x below disables sqlcmd $(...) expansion.
arc="${AZURE_RESOURCE_ID//\'/\'\'}"
endpoint="${DEMO_LOCAL_OTEL_ENDPOINT//\'/\'\'}"
region="${AZURE_REGION//\'/\'\'}"
export SQLCMDPASSWORD="$MSSQL_SA_PASSWORD"
unset MSSQL_SA_PASSWORD
sqlcmd=''
for candidate in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  if [[ -x "$candidate" ]]; then sqlcmd="$candidate"; break; fi
done
[[ -n "$sqlcmd" ]] || fail
# Fixed host/database: no operator-selected production target. Certificate trust is sandbox-only.
# No GO tokens or arbitrary SQL from operators. Fail on multiple or conflicting rows rather
# than choosing an arbitrary TOP 1 result. Transaction holds the table lock until validation.
if ! "$sqlcmd" -C -S sqlserver -d msdb -U sa -l 5 -t 30 -b -x >/dev/null 2>&1 <<SQL
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN TRY
  BEGIN TRANSACTION;
  IF OBJECT_ID(N'dbo.SQLServerAzureArcProperties', N'U') IS NULL
    CREATE TABLE dbo.SQLServerAzureArcProperties (
      AzureResourceId NVARCHAR(512) NULL,
      DemoLocalOtelEndpoint NVARCHAR(512) NULL,
      AzureRegion NVARCHAR(128) NULL
    );
  DECLARE @count BIGINT;
  SELECT @count = COUNT_BIG(*) FROM dbo.SQLServerAzureArcProperties WITH (TABLOCKX, HOLDLOCK);
  IF @count = 0
    INSERT INTO dbo.SQLServerAzureArcProperties (AzureResourceId, DemoLocalOtelEndpoint, AzureRegion)
      VALUES (N'$arc', N'$endpoint', N'$region');
  ELSE IF @count <> 1 OR NOT EXISTS (
    SELECT 1 FROM dbo.SQLServerAzureArcProperties
    WHERE AzureResourceId COLLATE Latin1_General_100_BIN2 = N'$arc'
      AND DemoLocalOtelEndpoint COLLATE Latin1_General_100_BIN2 = N'$endpoint'
      AND AzureRegion COLLATE Latin1_General_100_BIN2 = N'$region')
    THROW 50001, 'Conflicting sandbox discovery metadata', 1;
  COMMIT;
END TRY
BEGIN CATCH
  IF @@TRANCOUNT > 0 ROLLBACK;
  THROW;
END CATCH;
SQL
then fail; fi
echo 'Sandbox discovery metadata ready.'