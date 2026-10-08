// Inspect collector file-exporter OTLP JSON, never print payloads or raw errors.
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { isDeepStrictEqual } from 'node:util';

const fail = () => { throw new Error('Trace evidence gate not satisfied'); };
const attributes = list => new Map((list ?? []).map(a => [a.key, a.value?.stringValue]));
const forbidden = /authorization|password|bearer|connection[._]string|db\.statement|exception\.(message|stacktrace)|http\.request\.header/i;
const validId = (id, length) => typeof id === 'string' && id.length === length
  && /^[0-9a-f]+$/i.test(id) && !/^0+$/.test(id);
const noParent = id => id === undefined || id === '' || id === '0'.repeat(16);
const spanKey = span => `${span.traceId}/${span.spanId}`;

function privateFields(value) {
  if (!value || typeof value !== 'object') return;
  if (typeof value.key === 'string' && forbidden.test(value.key)) fail();
  // No exception/status free text is expected in this sanitized driver adapter.
  if (value.status?.message) fail();
  for (const child of Object.values(value)) privateFields(child);
}

export function verify(batches, { service, scenarios, repeat, expectedCounts, expectedStatementRoots = '0' }) {
  const selected = scenarios.split(',');
  if (!service || !/^[1-9][0-9]*$/.test(String(repeat)) || Number(repeat) > 1000
      || new Set(selected).size !== selected.length
      || selected.some(s => !['config', 'dns', 'login', 'success', 'statements'].includes(s))
      || !/^(?:0|[1-9][0-9]{0,3})$/.test(String(expectedStatementRoots))) fail();
  const phases = { config: 'configuration', dns: 'dns', login: 'login' };
  const categories = { configuration: 'configuration', dns: 'name_resolution', login: 'authentication' };
  const configuredCounts = expectedCounts;
  const expected = configuredCounts
    ? new Map(configuredCounts.split(',').map(item => {
        const [phase, count] = item.split(':');
        if (!['configuration', 'dns', 'login'].includes(phase) || !/^(?:0|[1-9][0-9]{0,2}|1000)$/.test(count ?? '')) fail();
        return [phase, Number(count)];
      }))
    : new Map(selected.filter(s => Object.hasOwn(phases, s)).map(s => [phases[s], Number(repeat)]));
  for (const phase of ['configuration', 'dns', 'login']) if (!expected.has(phase)) expected.set(phase, 0);
  // Absence by itself could mean a dead exporter. Always require a positive control.
  if (![...expected.values()].some(count => count > 0) && Number(expectedStatementRoots) === 0) fail();
  const spans = new Map();
  for (const batch of batches) {
    if (batch.resourceMetrics || batch.resourceLogs) fail();
    for (const resource of batch.resourceSpans ?? []) {
      if (attributes(resource.resource?.attributes).get('service.name') !== service) continue;
      privateFields(resource);
      for (const scope of resource.scopeSpans ?? []) {
        if (scope.scope?.name !== 'com.microsoft.sqlserver.jdbc') fail();
        for (const input of scope.spans ?? []) {
          const span = { ...input };
            if ((span.events ?? []).some(event => !['mssql.driver.error', 'mssql.driver.connection.retry_decision',
              'mssql.driver.statement.retry_decision', 'mssql.driver.timeout'].includes(event.name))) fail();
            if (!/^mssql\.driver\.(?:connection\.(?:open|attempt|configuration|instance_discovery|dns|socket_connect|prelogin|tls|login|token_acquisition|redirect|initialize|unknown)|statement\.(?:execute|attempt|request_build|server_call|first_response))$/.test(span.name)
              || !validId(span.traceId, 32) || !validId(span.spanId, 16)
              || (!noParent(span.parentSpanId) && !validId(span.parentSpanId, 16))) fail();
          span.traceId = span.traceId.toLowerCase();
          span.spanId = span.spanId.toLowerCase();
          span.parentSpanId = noParent(span.parentSpanId) ? '' : span.parentSpanId.toLowerCase();
          const key = spanKey(span);
          // Retry copies must agree; otherwise a later copy could hide a malformed tree.
          if (spans.has(key) && !isDeepStrictEqual(spans.get(key), span)) fail();
          spans.set(key, span);
        }
      }
    }
  }
  const roots = new Map();
  const statementRoots = new Map();
  for (const span of spans.values()) {
    const attrs = attributes(span.attributes);
    if (span.name === 'mssql.driver.statement.execute') {
      const query = attrs.get('db.query.text');
      if (!noParent(span.parentSpanId) || attrs.get('mssql.statement.outcome') !== 'failure'
          || ![2, 'STATUS_CODE_ERROR'].includes(span.status?.code)
          || !['query_syntax_semantics', 'constraint_violation'].includes(attrs.get('mssql.error.category'))
          || !['statement', 'prepared_statement'].includes(attrs.get('mssql.statement.type'))
          || typeof query !== 'string' || query.length > 4096 || !query.includes('?')
          || /customer@example|555-0100|private-|pii:/i.test(query)) fail();
      statementRoots.set(spanKey(span), span);
      continue;
    }
    if (span.name !== 'mssql.driver.connection.open') {
      // Attempt outcomes use mssql.connection.attempt_outcome, never the root key.
      if (attrs.has('mssql.connection.outcome')) fail();
      continue;
    }
    // This executable creates no application parent. Unknown/external parents and
    // nested opens are not permitted, even if they carry plausible failure fields.
    if (!noParent(span.parentSpanId) || attrs.get('mssql.connection.outcome') !== 'failure'
        || ![2, 'STATUS_CODE_ERROR'].includes(span.status?.code)) fail();
    const phase = attrs.get('mssql.connection.failure_phase');
    if (!expected.has(phase) || attrs.get('mssql.error.category') !== categories[phase]) fail();
    roots.set(spanKey(span), span);
    expected.set(phase, expected.get(phase) - 1);
  }
  if ([...expected.values()].some(count => count !== 0)) fail();
  if (statementRoots.size !== Number(expectedStatementRoots)) fail();
  const expectedStatementKinds = new Map([
    ['statement', {
      category: 'query_syntax_semantics',
      errorType: 'sqlserver.208',
      errorCode: 'sqlserver:208',
      query: 'SELECT ? FROM dbo.__jdbc_otel_missing'
    }],
    ['prepared_statement', {
      category: 'constraint_violation',
      errorType: 'sqlserver.2627',
      errorCode: 'sqlserver:2627',
      query: 'INSERT INTO #jdbc_otel_stmt_demo (id, label) VALUES (?, ?)'
    }]
  ]);
  const statementKindCounts = new Map([...expectedStatementKinds.keys()].map(kind => [kind, 0]));
  for (const root of statementRoots.values()) {
    const rootAttrs = attributes(root.attributes);
    const kind = rootAttrs.get('mssql.statement.type');
    const contract = expectedStatementKinds.get(kind);
    if (!contract || rootAttrs.get('mssql.error.category') !== contract.category
        || rootAttrs.get('error.type') !== contract.errorType
        || rootAttrs.get('mssql.statement.failure_phase') !== 'server_call'
        || rootAttrs.get('db.query.text') !== contract.query) fail();
    statementKindCounts.set(kind, statementKindCounts.get(kind) + 1);

    const trace = [...spans.values()].filter(span => span.traceId === root.traceId);
    if (trace.length !== 5) fail();
    const byName = new Map(trace.map(span => [span.name, span]));
    const attempt = byName.get('mssql.driver.statement.attempt');
    const requestBuild = byName.get('mssql.driver.statement.request_build');
    const serverCall = byName.get('mssql.driver.statement.server_call');
    const firstResponse = byName.get('mssql.driver.statement.first_response');
    if (byName.size !== 5 || !attempt || !requestBuild || !serverCall || !firstResponse
        || attempt.parentSpanId !== root.spanId
        || requestBuild.parentSpanId !== attempt.spanId
        || serverCall.parentSpanId !== attempt.spanId
        || firstResponse.parentSpanId !== serverCall.spanId) fail();
    const errorEvents = trace.flatMap(span => span.events ?? [])
      .filter(event => event.name === 'mssql.driver.error');
    if (errorEvents.length !== 1
        || attributes(errorEvents[0].attributes).get('mssql.error.code') !== contract.errorCode) fail();
  }
  const statementIterations = Number(expectedStatementRoots) / expectedStatementKinds.size;
  if (!Number.isInteger(statementIterations)
      || [...statementKindCounts.values()].some(count => count !== statementIterations)) fail();
  const allRoots = new Map([...roots, ...statementRoots]);
  for (const span of spans.values()) {
    let current = span;
    const visited = new Set();
    while (!allRoots.has(spanKey(current))) {
      const key = spanKey(current);
      if (visited.has(key)) fail();
      visited.add(key);
      current = spans.get(`${current.traceId}/${current.parentSpanId}`);
      if (!current) fail();
    }
  }
  return { roots: roots.size, statementRoots: statementRoots.size, spans: spans.size };
}

async function main() {
  const argument = process.argv[2];
  if (argument === '--wait') {
    const internal = process.argv[3] !== 'local';
    for (let attempt = 0; attempt < 60; attempt++) {
      try {
        const response = await fetch('http://otelcol:4318/v1/traces', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}',
          signal: AbortSignal.timeout(2000)
        });
        // Internal ingress must reject an unauthenticated request, not just be alive.
        if (internal && response.ok) throw new Error('Unauthenticated ingress accepted');
        if (internal ? [401, 403].includes(response.status) : response.ok) {
          console.log(internal ? 'Unauthenticated HTTP ingestion rejected.' : 'Local trace receiver ready.');
          return;
        }
      } catch (error) {
        if (error.message === 'Unauthenticated ingress accepted') throw error;
      }
      await delay(1000);
    }
    throw new Error('Receiver readiness or authentication gate failed');
  }
  const options = {
    service: process.env.OTEL_SERVICE_NAME,
    scenarios: process.env.DEMO_SCENARIOS || 'config,dns',
    repeat: process.env.DEMO_REPEAT || '1',
    expectedCounts: process.env.DEMO_EXPECTED_FAILURE_COUNTS
    , expectedStatementRoots: process.env.DEMO_EXPECTED_STATEMENT_ROOTS || '0'
  };
  // Allow batching and file flush; require five consecutive matching snapshots.
  let stable = 0;
  let lastCount = -1;
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      const lines = readFileSync(argument, 'utf8').split('\n').filter(line => line.trim());
      const result = verify(lines.map(line => JSON.parse(line)), options);
      stable = result.spans === lastCount ? stable + 1 : 1;
      lastCount = result.spans;
      if (stable >= 5) {
        console.log(`OTLP evidence verified: ${result.roots} failed connection roots, ${result.statementRoots} failed statement roots, ${result.spans} spans; no extra roots.`);
        console.log('This does not certify Aspire delivery, MISE policy correctness or Azure persistence.');
        return;
      }
    } catch {
      stable = 0;
    }
    await delay(1000);
  }
  throw new Error('OTLP evidence gate failed; inspect sanitized traces and exporter status locally');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(() => {
    // Exceptions from JSON parsing/fetch may carry data; never echo them.
    console.error('Demo verification failed (missing, unexpected or unsafe trace evidence, or receiver/authentication unavailable).');
    process.exitCode = 1;
  });
}