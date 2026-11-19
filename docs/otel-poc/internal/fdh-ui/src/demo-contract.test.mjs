import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const demo = new URL('../../../user-endpoint-demo/', import.meta.url);
const runner = fileURLToPath(new URL('run.ps1', demo));
const dashboard = JSON.parse(await readFile(new URL('grafana/dashboards/jdbc-user-endpoint.json', demo), 'utf8'));

test('Grafana uses dotted activity roots, outcome ratios and all four instruments', () => {
  const queries = dashboard.panels.flatMap(p => p.targets ?? []).map(t => t.expr);
  assert.ok(queries.every(q => !/mssql_activity\b|mssql_activity_type|STATEMENT_INVOCATION|CONNECTION/.test(q)));
  for (const family of ['count_total', 'error_count_total', 'retry_count_total', 'duration_seconds_bucket', 'duration_seconds_sum']) {
    assert.ok(queries.some(q => q.includes(`db_client_operation_${family}`)), family);
  }
  assert.ok(queries.some(q => q.includes('mssql_operation_outcome="success"')));
  assert.ok(queries.some(q => q.includes('mssql_operation_outcome)')));
  assert.ok(queries.every(q => !q.includes('or vector(0)')));
});

test('runner schema verifier is executable without Docker, checks dimensions/buckets and permits unexercised retries', () => {
  // Extract only this pure function; never execute the runner entry point, Docker, SQL or health checks.
  const script = `$ErrorActionPreference='Stop'
    $tokens=$null; $parseErrors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile('${runner.replaceAll("'", "''")}',[ref]$tokens,[ref]$parseErrors)
    if ($parseErrors.Count) { throw 'Runner syntax errors' }
    $f=$ast.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Assert-MetricSchema'},$true)
    if (!$f) { throw 'Missing schema verifier' }
    . ([scriptblock]::Create($f.Extent.Text))
    $labels='mssql_performance_activity="connection.open",mssql_operation_kind="connection",mssql_connection_auth_method="sql_password",mssql_telemetry_schema_version="1.0"'
    $lines=@(('db_client_operation_count_total{'+$labels+',mssql_operation_outcome="success"} 3'),
      ('db_client_operation_error_count_total{'+$labels+'} 1'),
      ('db_client_operation_duration_seconds_count{'+$labels+',mssql_operation_outcome="success"} 3'),
      ('db_client_operation_duration_seconds_sum{'+$labels+',mssql_operation_outcome="success"} 0.3'))
    foreach($le in @('0.0001','0.00025','0.0005','0.001','0.0025','0.005','0.01','0.025','0.05','0.1','0.25','0.5','1','2.5','5','10','30','+Inf')) {
      $lines += 'db_client_operation_duration_seconds_bucket{'+$labels+',mssql_operation_outcome="success",le="'+$le+'"} 3'
    }
    $nl=[Environment]::NewLine
    $good=$lines -join $nl
    Assert-MetricSchema $good
    Assert-MetricSchema ($good+$nl+'db_client_operation_retry_count_total{'+$labels+',mssql_retry_reason="redirect"} 0')
    $bad=@($good.Replace('mssql_operation_outcome="success"','mssql_operation_outcome="invented"'),
      $good.Replace('le="30"','le="31"'), $good.Replace('mssql_operation_kind="connection"','mssql_operation_kind="statement"'),
      $good.Replace('mssql_connection_auth_method="sql_password"','mssql_connection_auth_method="CUSTOMER"'),
      $good.Replace('mssql_telemetry_schema_version="1.0"','mssql_telemetry_schema_version="1.0",mssql_connection_guid="SECRET"'),
      ($good+$nl+'db_client_operation_retry_count_total{'+$labels+',mssql_operation_outcome="success",mssql_retry_reason="redirect"} 1'))
    foreach($value in $bad) {
      $rejected=$false; try { Assert-MetricSchema $value } catch { $rejected=$true }
      if (!$rejected) { throw 'Invalid schema accepted' }
    }
    'SCHEMA PASS'`;
  const result = spawnSync('pwsh', ['-NoProfile', '-NonInteractive', '-Command', script], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
  assert.match(result.stdout, /SCHEMA PASS/);
});