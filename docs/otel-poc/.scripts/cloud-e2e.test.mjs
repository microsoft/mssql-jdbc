import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { configure, parseEnv, collector, dashboard, expectedRows, compare, poll } from './cloud-e2e.mjs';

const run = 'a'.repeat(32);
const env = {
  CLOUD_RUN_ID: run, CLOUD_OWNER: 'alice', CLOUD_TARGET_ACK: 'dedicated-demo-targets',
  CLOUD_SCHEMA: 'otap-microseconds-v1', KUSTO_CLUSTER_URI: 'https://demo.canadacentral.kusto.windows.net',
  KUSTO_DATABASE: 'JdbcDemoAlice', STORAGE_ACCOUNT: 'demostore', STORAGE_CONTAINER: 'demo-alice',
  ROOT_PATH: `jdbc-demo-alice/${run}`, EVENT_HUB_NAMESPACE: 'demo.servicebus.windows.net',
  EVENT_HUB_NAME: 'jdbc-demo-alice', EVENT_HUB_CONSUMER_GROUP: `jdbc-${run}`,
  CHECKPOINT_STORAGE_ACCOUNT: 'demostore', CHECKPOINT_STORAGE_CONTAINER: 'checkpoints-alice',
  OTELCOL_ARCDATA_IMAGE: `example.invalid/collector@sha256:${run.repeat(2)}`,
  DELTA_BULK_LOADER_IMAGE: `example.invalid/loader@sha256:${run.repeat(2)}`,
  MISE_IMAGE: `example.invalid/mise@sha256:${run.repeat(2)}`,
  GRAFANA_IMAGE: `example.invalid/grafana@sha256:${run.repeat(2)}`,
  IDENTITY_HEADER: 'x'.repeat(48), GRAFANA_ADMIN_PASSWORD: 'x'.repeat(32),
  AZURE_TENANT_ID: '11111111-1111-1111-1111-111111111111',
  AZURE_HOST_PATH: '/existing/cache', MISE_CERT_PATH: '/existing/auth.pfx',
  MISE_TENANT_ID: '11111111-1111-1111-1111-111111111111', MISE_CLIENT_ID: '11111111-1111-1111-1111-111111111111',
  MISE_AUDIENCE: 'https://example.invalid', MISE_REGION: 'canadacentral', MISE_FIRST_PARTY_SUBSCRIPTION: '11111111-1111-1111-1111-111111111111',
  OTEL_ACCESS_TOKEN_SCOPE: 'https://example.invalid/.default', OTEL_ARM_RESOURCE_ID: '/subscriptions/demo/resourceGroups/demo',
  MSSQL_SA_PASSWORD: 'synthetic-only', DEMO_LOGIN_PASSWORD: 'different-synthetic-only'
};
const attr = (key, value) => ({ key, value: { stringValue: value } });
const options = { service: `jdbc-cloud-${run}`, scenarios: 'config,success', repeat: '1' };
function evidence() {
  const root = { traceId: '1'.repeat(32), spanId: '2'.repeat(16), name: 'mssql.driver.connection.open',
    startTimeUnixNano: '1720000000123456789', endTimeUnixNano: '1720000000124456999', kind: 3, status: { code: 2 },
    attributes: [attr('mssql.connection.outcome', 'failure'), attr('mssql.connection.failure_phase', 'configuration'), attr('mssql.error.category', 'configuration')] };
  const child = { ...root, spanId: '3'.repeat(16), parentSpanId: root.spanId, name: 'mssql.driver.connection.configuration', attributes: [] };
  return [child, root].map(s => ({ resourceSpans: [{ resource: { attributes: [attr('service.name', options.service)] },
    scopeSpans: [{ scope: { name: 'com.microsoft.sqlserver.jdbc' }, spans: [s] }] }] }));
}
test('strict env parser is data only, rejects ambiguous and interpolated values', () => {
  assert.deepEqual(parseEnv('A=one\nB="two words"\n# comment\n'), { A: 'one', B: 'two words' });
  for (const text of ['A=$(whoami)', 'A=${SECRET}', 'A=a\nA=b', 'export A=x', 'A="unterminated', 'A=`cmd`']) assert.throws(() => parseEnv(text));
});
test('requires explicit isolation, schema and digest-pinned approved images', () => {
  const c = configure(env);
  assert.equal(c.service, options.service);
  assert.equal(c.project, `jdbc-cloud-${run}`);
  for (const [key, value] of Object.entries({ ROOT_PATH: '../shared', EVENT_HUB_CONSUMER_GROUP: '$Default',
    CLOUD_OWNER: 'shared', CLOUD_TARGET_ACK: '', KUSTO_CLUSTER_URI: 'https://evil.invalid/?token=secret',
    OTELCOL_ARCDATA_IMAGE: 'image:latest', CLOUD_SCHEMA: 'nano', DEMO_LOGIN_PASSWORD: env.MSSQL_SA_PASSWORD })) {
    assert.throws(() => configure({ ...env, [key]: value }), key);
  }
  for (const prefix of ['', '/', 'protobus-demo', 'shared', `jdbc-demo-alice/${run}/../other`, `jdbc-demo-bob/${run}`]) {
    assert.throws(() => configure({ ...env, ROOT_PATH: prefix }));
  }
});
test('collector keeps MISE and traces only, routes identical accepted traffic to both stores and evidence', () => {
  const c = collector();
  assert.equal(c.receivers.otlp.protocols.http.auth.authenticator, 'misecontainerauth');
  assert.deepEqual(Object.keys(c.service.pipelines), ['traces']);
  assert.deepEqual(c.service.pipelines.traces.exporters, ['otlphttp/evidence', 'deltalake/otap', 'kustoexporter/otap']);
  assert.equal(c.exporters['kustoexporter/otap'].marshaler, 'otap_parquet');
  assert.equal(c.exporters['deltalake/otap'].drop_on_error, false);
});
test('local evidence requires positive controls and resolves parents across batches', () => {
  assert.equal(expectedRows(evidence(), options).length, 2);
  for (const batches of [[], [{ resourceMetrics: [] }, ...evidence()], [{ resourceLogs: [] }, ...evidence()]]) {
    assert.throws(() => expectedRows(batches, options));
  }
});
test('comparison checks complete IDs, status, attributes, times and rejects extra telemetry', () => {
  const expected = expectedRows(evidence(), options);
  assert.equal(expected[0].duration_us, '1000');
  assert.equal(expected[0].start_us, '1720000000123456');
  assert.equal(compare(expected, { rows: expected, unexpected: [] }), true);
  assert.equal(compare(expected, { rows: [], unexpected: [] }), false);
  assert.equal(compare(expected, { rows: expected.slice(1), unexpected: [] }), false);
  for (const key of ['span_id', 'trace_id', 'parent_span_id', 'status_code', 'attributes', 'start_us', 'duration_us']) {
    const changed = structuredClone(expected); changed[0][key] = 'wrong';
    assert.throws(() => compare(expected, { rows: changed, unexpected: [] }), key);
  }
  assert.throws(() => compare(expected, { rows: [...expected, { ...expected[0], span_id: 'f'.repeat(16) }], unexpected: [] }));
  for (const table of ['metrics', 'logs', 'span_events']) assert.throws(() => compare(expected, { rows: expected, unexpected: [table] }));
});
test('bounded polling retries incomplete evidence only; arbitrary errors fail immediately', async () => {
  let calls = 0;
  await assert.rejects(poll(async () => { calls++; return false; }, 20, 1));
  assert.ok(calls > 1 && calls < 100);
  calls = 0;
  await assert.rejects(poll(async () => { calls++; throw new Error('unsafe diagnostic'); }, 1000, 1));
  assert.equal(calls, 1);
});
test('Grafana contains connection and statement error drill-downs, never metrics queries', () => {
  const d = dashboard(expectedRows(evidence(), options), options.service);
  assert.equal(d.panels.length, 13);
  assert.deepEqual(d.templating.list.map(variable => variable.name), ['failure_phase', 'error_category', 'error_type', 'connection_guid', 'statement_trace_id']);
  assert.match(JSON.stringify(d), /failure_phase/);
  assert.match(JSON.stringify(d), /error_category/);
  assert.match(JSON.stringify(d), /duration_ms/);
  assert.match(JSON.stringify(d), /all root span attributes/);
  assert.match(JSON.stringify(d), /complete span tree/);
  assert.match(JSON.stringify(d), /sanitized errors and retry decisions/);
  assert.match(JSON.stringify(d), /Drill into this failed connection/);
  assert.match(JSON.stringify(d), /Drill into this failed statement/);
  assert.match(JSON.stringify(d), /masked_sql/);
  assert.doesNotMatch(JSON.stringify(d), /Prometheus|rate\(|success_count/);
  assert.ok(d.panels.every(p => p.targets[0].scenarioId === 'raw_frame'));
});
test('runtime exposes no OTLP host ingress and does not disable bulk loading validation', () => {
  const source = readFileSync(new URL('./cloud-e2e.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /DisableLoadingValidation|SkipLoadingValidation|\.drop tables|az login/);
  assert.match(source, /127\.0\.0\.1/);
});