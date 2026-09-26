// Inspect collector file-exporter OTLP JSON, never print payloads or raw errors.
import { readFileSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { isDeepStrictEqual } from 'node:util';

const fail = () => { throw new Error('Trace evidence gate not satisfied'); };
const attributes = list => new Map((list ?? []).map(a => [a.key, a.value?.stringValue]));
const forbidden = /authorization|password|bearer|connection[._]string|db\.(statement|query)|exception\.(message|stacktrace)|http\.request\.header/i;
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

export function verify(batches, { service, scenarios, repeat }) {
  const selected = scenarios.split(',');
  if (!service || !/^[1-9][0-9]*$/.test(String(repeat)) || Number(repeat) > 1000
      || new Set(selected).size !== selected.length
      || selected.some(s => !['config', 'dns', 'login', 'success'].includes(s))) fail();
  const phases = { config: 'configuration', dns: 'dns', login: 'login' };
  const categories = { configuration: 'configuration', dns: 'name_resolution', login: 'authentication' };
  const expected = new Map(selected.filter(s => s !== 'success').map(s => [phases[s], Number(repeat)]));
  // Absence by itself could mean a dead exporter. Always require a positive control.
  if (!expected.size) fail();
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
          if (!/^mssql\.driver\.connection\.(open|attempt|configuration|instance_discovery|dns|socket_connect|prelogin|tls|login|token_acquisition|redirect|initialize|unknown)$/.test(span.name)
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
  for (const span of spans.values()) {
    const attrs = attributes(span.attributes);
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
  for (const span of spans.values()) {
    let current = span;
    const visited = new Set();
    while (!roots.has(spanKey(current))) {
      const key = spanKey(current);
      if (visited.has(key)) fail();
      visited.add(key);
      current = spans.get(`${current.traceId}/${current.parentSpanId}`);
      if (!current) fail();
    }
  }
  return { roots: roots.size, spans: spans.size };
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
    repeat: process.env.DEMO_REPEAT || '1'
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
        console.log(`OTLP evidence verified: ${result.roots} failed roots, ${result.spans} connection spans; no extra roots.`);
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