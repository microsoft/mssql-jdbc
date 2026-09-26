import assert from 'node:assert/strict';
import test from 'node:test';
import { verify } from './verify-traces.mjs';

const attribute = (key, value) => ({ key, value: { stringValue: value } });
function root(phase, id) {
  return {
    name: 'mssql.driver.connection.open', traceId: id.repeat(32), spanId: id.repeat(16),
    status: { code: 2 },
    attributes: [attribute('mssql.connection.outcome', 'failure'),
      attribute('mssql.connection.failure_phase', phase),
      attribute('mssql.error.category', phase === 'dns' ? 'name_resolution' : 'configuration')]
  };
}
function batch(spans, service = 'test-run') {
  return { resourceSpans: [{ resource: { attributes: [attribute('service.name', service)] },
    scopeSpans: [{ scope: { name: 'com.microsoft.sqlserver.jdbc' }, spans }] }] };
}
const options = { service: 'test-run', scenarios: 'config,dns', repeat: '1' };

function child(parent, id, name = 'dns') {
  return { name: `mssql.driver.connection.${name}`, traceId: parent.traceId,
    spanId: id.repeat(16), parentSpanId: parent.spanId, attributes: [] };
}

test('accepts complete failed trees, zero/absent root parents, and distinct attempt outcomes', () => {
  const config = root('configuration', '1');
  config.parentSpanId = '0'.repeat(16);
  const dns = root('dns', '2');
  dns.status.code = 'STATUS_CODE_ERROR';
  const attempt = child(dns, '3', 'attempt');
  attempt.attributes.push(attribute('mssql.connection.attempt_outcome', 'failure'));
  const phase = child(attempt, '4');
  assert.deepEqual(verify([batch([phase, config, attempt, dns])],
    { ...options, scenarios: 'config,dns,success' }), { roots: 2, spans: 4 });
});

test('rejects DNS spans impersonating roots even with expected failure attributes', () => {
  for (const parent of [undefined, '0'.repeat(16), 'f'.repeat(16)]) {
    const fakeRoots = [root('configuration', '1'), root('dns', '2')];
    fakeRoots.forEach(span => { span.name = 'mssql.driver.connection.dns'; span.parentSpanId = parent; });
    assert.throws(() => verify([batch(fakeRoots)], options));
  }
});

test('rejects every extra open, including a successful nested open without outcome', () => {
  for (const outcome of [undefined, 'success', 'failure']) {
    const config = root('configuration', '1');
    const extra = child(config, '3', 'open');
    extra.status = { code: 1 };
    if (outcome) extra.attributes.push(attribute('mssql.connection.outcome', outcome));
    assert.throws(() => verify([batch([config, root('dns', '2'), extra])],
      { ...options, scenarios: 'config,dns,success' }));
  }
});

test('rejects missing root outcomes and outcomes attached to ordinary descendants', () => {
  const config = root('configuration', '1');
  config.attributes.shift();
  assert.throws(() => verify([batch([config, root('dns', '2')])], options));
  const dns = root('dns', '2');
  const phase = child(dns, '3');
  phase.attributes.push(attribute('mssql.connection.outcome', 'failure'));
  assert.throws(() => verify([batch([root('configuration', '1'), dns, phase])], options));
});

test('rejects unknown, external, self, cross-trace and nested root parents', () => {
  for (const parent of ['f'.repeat(16), '1'.repeat(16), '2'.repeat(16)]) {
    const config = root('configuration', '1');
    config.parentSpanId = parent;
    assert.throws(() => verify([batch([config, root('dns', '2')])], options));
  }
  const config = root('configuration', '1');
  const nested = root('dns', '2');
  nested.traceId = config.traceId;
  nested.parentSpanId = config.spanId;
  assert.throws(() => verify([batch([config, nested])], options));
});

test('rejects disconnected, cyclic, unknown-parent and cross-trace descendants', () => {
  for (const parent of [undefined, '', '0'.repeat(16), 'f'.repeat(16), '2'.repeat(16), '3'.repeat(16)]) {
    const config = root('configuration', '1');
    const phase = child(config, '3');
    phase.parentSpanId = parent;
    assert.throws(() => verify([batch([config, root('dns', '2'), phase])], options));
  }
  const config = root('configuration', '1');
  const first = child(config, '3');
  const second = child(first, '4');
  first.parentSpanId = second.spanId;
  assert.throws(() => verify([batch([config, root('dns', '2'), first, second])], options));
});

test('validates nonzero hexadecimal trace/span IDs and parent ID format on every span', () => {
  for (const field of ['traceId', 'spanId', 'parentSpanId']) {
    for (const invalid of ['bad', 'g'.repeat(field === 'traceId' ? 32 : 16), 123,
      '0'.repeat(field === 'traceId' ? 32 : 16)]) {
      const config = root('configuration', '1');
      const phase = child(config, '3');
      phase[field] = invalid;
      assert.throws(() => verify([batch([config, root('dns', '2'), phase])], options));
    }
  }
  const config = root('configuration', '1');
  config.traceId = 'invalid';
  assert.throws(() => verify([batch([config, root('dns', '2')])], options));
});

test('rejects conflicting duplicate deliveries rather than hiding a malformed span', () => {
  const config = root('configuration', '1');
  const conflict = { ...config, parentSpanId: 'f'.repeat(16) };
  const dns = root('dns', '2');
  assert.throws(() => verify([batch([conflict, config, dns])], options));
  assert.throws(() => verify([batch([config, conflict, dns])], options));
});

test('requires actual configuration and DNS failed roots, not health or unrelated spans', () => {
  assert.equal(verify([batch([root('configuration', '1'), root('dns', '2')])], options).roots, 2);
  assert.throws(() => verify([], options));
  assert.throws(() => verify([batch([root('configuration', '1'), root('dns', '2')], 'stale-run')], options));
  assert.throws(() => verify([batch([root('configuration', '1')])], options));
});
test('deduplicates OTLP retry deliveries', () => {
  const data = batch([root('configuration', '1'), root('dns', '2')]);
  assert.equal(verify([data, data], options).roots, 2);
});
test('success needs a positive control and must not add a successful root', () => {
  assert.throws(() => verify([], { ...options, scenarios: 'success' }));
  const success = root('initialize', '3');
  success.attributes[0] = attribute('mssql.connection.outcome', 'success');
  assert.throws(() => verify([batch([root('configuration', '1'), root('dns', '2'), success])],
    { ...options, scenarios: 'config,dns,success' }));
});
test('rejects metrics, statements and sensitive payload attributes', () => {
  assert.throws(() => verify([{ resourceMetrics: [{}] }], options));
  assert.throws(() => verify([{ resourceLogs: [{}] }], options));
  const statement = root('configuration', '3');
  statement.name = 'mssql.driver.statement.execute';
  assert.throws(() => verify([batch([root('configuration', '1'), root('dns', '2'), statement])], options));
  const sensitive = root('configuration', '1');
  sensitive.events = [{ attributes: [attribute('http.request.header.authorization', 'redacted-fixture')] }];
  assert.throws(() => verify([batch([sensitive, root('dns', '2')])], options));
});
test('rejects wrong category, missing ERROR, orphan phase and invalid scenario inputs', () => {
  const wrong = root('dns', '2');
  wrong.attributes[2] = attribute('mssql.error.category', 'configuration');
  assert.throws(() => verify([batch([root('configuration', '1'), wrong])], options));
  wrong.attributes[2] = attribute('mssql.error.category', 'name_resolution');
  wrong.status.code = 0;
  assert.throws(() => verify([batch([root('configuration', '1'), wrong])], options));
  const orphan = { name: 'mssql.driver.connection.dns', traceId: '3'.repeat(32), spanId: '3'.repeat(16) };
  assert.throws(() => verify([batch([root('configuration', '1'), root('dns', '2'), orphan])], options));
  assert.throws(() => verify([], { ...options, repeat: '0' }));
  assert.throws(() => verify([], { ...options, scenarios: 'config,config' }));
});