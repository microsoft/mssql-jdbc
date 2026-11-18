import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test, afterEach } from 'node:test';
import ts from 'typescript';

// Transpile only this module: the type-only data import must not execute fixture data.
const source = await readFile(new URL('./live.ts', import.meta.url), 'utf8');
const { outputText } = ts.transpileModule(source, { compilerOptions: {
  target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext,
} });
const { loadTelemetry, emptyTelemetry } = await import(`data:text/javascript;base64,${Buffer.from(outputText).toString('base64')}`);
const originalFetch = globalThis.fetch;
const originalNow = Date.now;
afterEach(() => { globalThis.fetch = originalFetch; Date.now = originalNow; });

test('preserves prepared execution metrics as distinct bounded activities', async () => {
  mock({ counters: { STATEMENT_PREPARE: 4, STATEMENT_PREPEXEC: 8, TOKEN_REQUEST: 2, INSTANCE_DISCOVERY: 1 } });
  const snapshot = await loadTelemetry();
  assert.deepEqual(snapshot.performanceMetrics.map(metric => metric.activity), [
    'statement.server_call.prepare', 'statement.server_call.prepexec', 'connection.token_request', 'connection.instance_discovery',
  ]);
});

const tagged = values => Object.entries(values).map(([key, value]) => ({ key, value }));
const span = (spanID, operationName, startTime, parent, values = {}, logs = []) => ({
  spanID, traceID: 'abc123', operationName: `mssql.driver.${operationName}`,
  startTime, duration: 2500, processID: 'p1', tags: tagged(values), logs,
  references: parent ? [{ refType: 'CHILD_OF', traceID: 'abc123', spanID: parent }] : [],
});
const vector = values => ({ status: 'success', data: { resultType: 'vector',
  result: Object.entries(values).map(([mssql_activity, value]) => ({ metric: { mssql_activity }, value: [1, String(value)] })) } });
function mock({ traces = [], counters = {}, errors = {}, quantiles = {}, series = [], fail } = {}) {
  const requests = [];
  globalThis.fetch = async (input, options) => {
    const url = new URL(input, 'http://localhost');
    requests.push({ url, options });
    let payload;
    const query = url.searchParams.get('query') ?? '';
    if (fail?.(url)) return { ok: false, status: 503 };
    if (url.pathname === '/jaeger/api/traces') payload = { data: traces };
    else if (url.pathname.endsWith('query_range')) payload = { status: 'success', data: { resultType: 'matrix', result: series } };
    else if (query.startsWith('histogram_quantile')) payload = vector(quantiles);
    else payload = vector(query.includes('error_count') ? errors : counters);
    return { ok: true, json: async () => payload };
  };
  return requests;
}

test('maps actual GUID, units, CHILD_OF hierarchy, ordered siblings and safe error fields', async () => {
  const guid = 'c575b073-1fa5-481c-9c07-c5908ffaaeb3';
  const time = 1795000000000000;
  const root = span('r', 'connection.open', time, null, {
    error: true, 'span.kind': 'client', 'mssql.connection.guid': guid,
    'mssql.connection.failure_phase': 'dns', 'mssql.error.category': 'name_resolution',
    'mssql.connection.attempt_count': 0, 'db.query.text': 'SECRET SQL', password: 'SECRET',
  });
  const attempt = span('a', 'connection.attempt', time + 20, 'r');
  const dns = span('d', 'connection.dns', time + 30, 'a', {}, [{ timestamp: time + 50, fields: tagged({
    event: 'mssql.driver.error', 'mssql.error.phase': 'dns', 'mssql.error.source': 'jvm',
    'mssql.error.code': 'jdbc:R_test', 'mssql.error.retry_decision': 'not_retryable',
    'exception.message': 'SECRET MESSAGE',
  }) }]);
  const config = span('c', 'connection.configuration', time + 10, 'r');
  mock({ traces: [{ traceID: 'abc123', spans: [dns, attempt, root, config],
    processes: { p1: { tags: tagged({ 'cloud.region': 'eastus', token: 'SECRET' }) } } }] });
  const snapshot = await loadTelemetry();
  const failure = snapshot.failures[0];
  assert.equal(failure.id, guid);
  assert.equal(failure.duration, 2.5);
  assert.equal(failure.phase, 'DNS');
  assert.equal(failure.attempts, 0);
  assert.equal(failure.region, 'eastus');
  assert.equal(failure.source, 'jvm');
  assert.deepEqual(failure.spans.map(s => s.depth), [0, 1, 1, 2]);
  assert.equal(failure.spans[3].start, 0.03);
  assert.deepEqual(failure.events[0], { name: 'mssql.driver.error', phase: 'dns', source: 'jvm',
    code: 'jdbc:R_test', decision: 'not_retryable' });
  assert.ok(!JSON.stringify(snapshot).includes('SECRET'));
});

test('uses root outcomes, actual statement type, trace ID fallback and no fabricated metadata', async () => {
  const time = 1795000000000000;
  mock({ traces: [{ traceID: 'abc123', spans: [
    span('r', 'connection.open', time, null, { 'mssql.connection.outcome': 'failure' }),
    span('a', 'connection.attempt', time + 1, 'r'),
    span('s', 'statement.execute', time, null, { 'mssql.statement.outcome': 'failure',
      'mssql.statement.type': 'prepared_statement', 'mssql.statement.operation': 'insert' }),
    span('u', 'statement.execute', time, null, { error: true }),
    span('ok', 'connection.open', time, null, { 'mssql.connection.outcome': 'success' }),
    span('errchild', 'connection.login', time + 1, 'ok', { error: true }),
  ] }] });
  const snapshot = await loadTelemetry();
  assert.equal(snapshot.failures.length, 1);
  assert.equal(snapshot.failures[0].id, 'abc123');
  assert.equal(snapshot.failures[0].phase, 'Not emitted');
  assert.equal(snapshot.failures[0].region, 'Not emitted');
  assert.equal(snapshot.failures[0].attempts, 1);
  assert.equal(snapshot.statementFailures[0].statementType, 'prepared_statement');
  assert.equal(snapshot.statementFailures[1].statementType, 'Not emitted');
  assert.equal(snapshot.statementFailures[0].maskedSql, 'Not emitted');
  assert.equal(snapshot.statementFailures[0].connectionId, 'Not emitted');
});

test('queries bounded 24h increases and estimates; preserves missing values and aligned throughput gaps', async () => {
  Date.now = () => 1795000200000;
  const end = Math.floor(Date.now() / 1000 / 300) * 300;
  const start = end - 86400;
  const requests = mock({ counters: { CONNECTION: 20, STATEMENT_INVOCATION: 0, DNS: 8 }, errors: { CONNECTION: 2 },
    quantiles: { CONNECTION: 0.025, DNS: 'NaN' }, series: [
      { metric: { mssql_activity: 'CONNECTION' }, values: [[start, '2'], [start + 300, '0']] },
      { metric: { mssql_activity: 'STATEMENT_INVOCATION' }, values: [[start + 300, '10'], [start + 600, 'NaN']] },
    ] });
  const snapshot = await loadTelemetry();
  const connection = snapshot.performanceMetrics.find(m => m.activity === 'connection.open');
  assert.equal(connection.count, 20);
  assert.equal(connection.errors, 2);
  assert.equal(connection.successRate, 90);
  assert.equal(connection.p95, 25);
  assert.equal(connection.max, null);
  const statement = snapshot.performanceMetrics.find(m => m.activity === 'statement.execute');
  assert.equal(statement.errors, null);
  assert.equal(statement.successRate, null);
  assert.equal(snapshot.performanceMetrics.find(m => m.activity === 'connection.dns').p50, null);
  assert.equal(snapshot.throughputPoints.length, 289);
  assert.deepEqual(snapshot.throughputPoints[0], { label: new Date(start * 1000).toISOString(), connections: 2, statements: null });
  assert.equal(snapshot.throughputPoints[1].connections, 0);
  assert.equal(snapshot.throughputPoints[1].statements, 10);
  assert.equal(snapshot.throughputPoints[2].statements, null);
  assert.equal(requests.length, 7);
  assert.equal(requests[0].url.searchParams.get('limit'), '1000');
  assert.equal(requests[0].url.searchParams.get('start'), String(start * 1e6));
  assert.ok(requests.every(r => r.options.signal instanceof AbortSignal));
  assert.ok(requests.filter(r => r.url.pathname.endsWith('/query')).every(r => r.url.searchParams.get('query').includes('[24h]')));
  const range = requests.find(r => r.url.pathname.endsWith('query_range')).url;
  assert.equal(range.searchParams.get('step'), '300');
  assert.match(range.searchParams.get('query'), /rate\(.*\[5m\].*\* 60/);
  assert.ok(snapshot.warnings.some(w => w.includes('only returned Jaeger traces')));
});

test('partial HTTP failure retains other sources; all failures have nulls and no synthetic records', async () => {
  mock({ counters: { CONNECTION: 5 }, fail: url => url.pathname.includes('jaeger') || url.searchParams.get('query')?.includes('error_count') });
  let snapshot = await loadTelemetry();
  assert.equal(snapshot.performanceMetrics[0].count, 5);
  assert.equal(snapshot.performanceMetrics[0].errors, null);
  assert.equal(snapshot.warnings.filter(w => w.includes('unavailable')).length, 2);
  mock({ fail: () => true });
  snapshot = await loadTelemetry();
  assert.deepEqual(snapshot.failures, []);
  assert.deepEqual(snapshot.performanceMetrics, []);
  assert.ok(snapshot.throughputPoints.every(p => p.connections === null && p.statements === null));
  assert.equal(snapshot.warnings.filter(w => w.includes('unavailable')).length, 7);
  assert.equal(emptyTelemetry.loadedAt, '');
});

test('backend errors, malformed JSON and network failures are sanitized', async () => {
  let calls = 0;
  globalThis.fetch = async () => {
    calls++;
    if (calls === 1) throw new Error('SECRET network detail');
    if (calls === 2) return { ok: true, json: async () => { throw new Error('SECRET body'); } };
    return { ok: true, json: async () => ({ status: 'error', error: 'SECRET query' }) };
  };
  const snapshot = await loadTelemetry();
  assert.equal(snapshot.warnings.filter(w => w.includes('unavailable')).length, 7);
  assert.ok(!JSON.stringify(snapshot).includes('SECRET'));
});

test('hung requests time out and abort even when fetch ignores cancellation', async context => {
  context.mock.timers.enable({ apis: ['setTimeout'] });
  const signals = [];
  globalThis.fetch = async (_url, options) => {
    signals.push(options.signal);
    return new Promise(() => {});
  };
  const pending = loadTelemetry();
  context.mock.timers.tick(10000);
  const snapshot = await pending;
  assert.equal(signals.length, 7);
  assert.ok(signals.every(signal => signal.aborted));
  assert.equal(snapshot.warnings.filter(w => w.includes('unavailable')).length, 7);
  assert.deepEqual(snapshot.performanceMetrics, []);
});

test('backend partial-result warnings are retained without copying backend messages', async () => {
  globalThis.fetch = async input => ({ ok: true, json: async () => input.startsWith('/jaeger')
    ? { data: [], errors: [{ msg: 'SECRET backend' }] }
    : { ...vector({}), warnings: ['SECRET backend'] } });
  const snapshot = await loadTelemetry();
  assert.ok(snapshot.warnings.some(w => w.includes('partial results')));
  assert.ok(snapshot.warnings.some(w => w.includes('backend reported warnings')));
  assert.ok(!JSON.stringify(snapshot).includes('SECRET'));
});