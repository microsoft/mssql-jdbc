import type { ConnectionFailure, StatementFailure, PerformanceMetric, ThroughputPoint, Span, ErrorEvent, OutcomeDistribution, RetryMetric } from './data';

export type TelemetrySnapshot = {
  failures: ConnectionFailure[];
  statementFailures: StatementFailure[];
  performanceMetrics: PerformanceMetric[];
  throughputPoints: ThroughputPoint[];
  outcomeDistribution: OutcomeDistribution[];
  retries: RetryMetric[];
  loadedAt: string;
  warnings: string[];
};

export const emptyTelemetry: TelemetrySnapshot = {
  failures: [], statementFailures: [], performanceMetrics: [], throughputPoints: [], outcomeDistribution: [], retries: [], loadedAt: '', warnings: [],
};

type Row = Record<string, unknown>;
const absent = 'Not emitted';
const windowSeconds = 86400;
const step = 300;
const safeKeys = new Set([
  'db.system.name', 'error.type', 'mssql.activity', 'mssql.telemetry.schema.version',
  'mssql.connection.guid', 'mssql.connection.outcome', 'mssql.connection.failure_phase',
  'mssql.connection.attempt_count', 'mssql.connection.retry_count', 'mssql.connection.redirect_count',
  'mssql.connection.encrypt', 'mssql.connection.trust_server_certificate',
  'mssql.connection.login_timeout', 'mssql.connection.socket_timeout', 'mssql.authentication.method',
  'mssql.driver.version', 'mssql.driver.user_agent.original', 'cloud.region',
  'telemetry.sdk.language', 'telemetry.sdk.version', 'deployment.environment',
  'mssql.statement.type', 'mssql.statement.api', 'mssql.statement.operation',
  'mssql.statement.outcome', 'mssql.statement.failure_phase', 'mssql.statement.attempt_count',
  'mssql.statement.retry_count', 'mssql.statement.query_timeout', 'mssql.statement.result_kind',
  'mssql.statement.response_buffering', 'mssql.statement.parameter_count', 'mssql.statement.batch_size',
  'mssql.statement.update_count',
  'mssql.error.category', 'mssql.error.source', 'mssql.error.code', 'mssql.error.sql_state',
]);
const diagnosticNames = new Set(['mssql.driver.error', 'mssql.driver.authentication', 'mssql.driver.retry',
  'mssql.driver.connection.retry_decision', 'mssql.driver.statement.retry_decision', 'mssql.driver.redirect', 'mssql.driver.timeout']);
const diagnosticKeys = new Set(['mssql.error.phase', 'mssql.error.source', 'mssql.error.code',
  'mssql.error.category', 'mssql.error.sql_state', 'mssql.error.retry_decision', 'error.type', 'exception.type',
  'mssql.error.driver_code', 'mssql.error.server_state', 'mssql.error.server_severity',
  'mssql.connection.attempt', 'mssql.statement.attempt', 'mssql.retry.attempt', 'mssql.retry.delay',
  'mssql.timeout.phase', 'mssql.timeout.kind', 'mssql.timeout.value', 'mssql.authentication.method',
  'mssql.authentication.token_source', 'mssql.connection.redirect.index', 'mssql.connection.redirect.type',
  'mssql.connection.endpoint_role']);

// Trust only the provider's root field, not arbitrary SQL aliases/resource/child enrichment. This is a
// fail-closed display check, not a second sanitizer: literals/comments must already have been removed.
function maskedQuery(value: unknown): string {
  if (typeof value !== 'string' || !value.trim() || value.length > 4096 || /[\u0000-\u001f\u007f]/.test(value)) return absent;
  for (let i = 0; i < value.length; i++) {
    const c = value[i];
    if (c === '[' || c === '"') {
      const end = c === '[' ? ']' : '"';
      let closed = false;
      while (++i < value.length) {
        if (value[i] === end) {
          if (value[i + 1] === end) i++;
          else { closed = true; break; }
        }
      }
      if (!closed) return absent;
    } else if (c === "'" || value.startsWith('--', i) || value.startsWith('/*', i)
      || (/[0-9]/.test(c) && (i === 0 || !/[\p{L}\p{N}_$#]/u.test(value[i - 1])))) return absent;
  }
  return value;
}

function row(value: unknown): Row {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Row : {};
}
function rows(value: unknown): Row[] { return Array.isArray(value) ? value.map(row) : []; }
function tags(value: unknown): Row {
  return Object.fromEntries(rows(value).filter(t => typeof t.key === 'string').map(t => [t.key, t.value]));
}
function text(value: unknown): string {
  // Only short structured values are displayed; messages, SQL and arbitrary maps are never copied.
  return typeof value === 'string' && value.length > 0 && value.length <= 160
    && /^[A-Za-z0-9_.:/ -]+$/.test(value) ? value : absent;
}
function number(value: unknown): number | null {
  if ((typeof value !== 'number' && typeof value !== 'string') || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function attributes(values: Row): Record<string, string> {
  return Object.fromEntries(Object.entries(values).filter(([key, value]) => safeKeys.has(key)
    && (typeof value === 'number' ? Number.isFinite(value) : typeof value === 'boolean' || text(value) !== absent))
    .map(([key, value]) => [key, String(value)]));
}
function failed(span: Row): boolean {
  const values = tags(span.tags);
  return values.error === true || values.error === 'true' || values['otel.status_code'] === 'ERROR'
    || ['failure', 'timeout', 'canceled'].includes(String(values['mssql.connection.outcome']))
    || ['failure', 'timeout', 'canceled'].includes(String(values['mssql.statement.outcome']));
}
function spanName(value: unknown): string {
  return typeof value === 'string' && /^mssql\.driver\.[a-z_.]+$/.test(value) ? value : absent;
}

function tree(root: Row, all: Row[]): Array<{ span: Row; depth: number }> {
  const children = new Map<string, Row[]>();
  for (const span of all) {
    const parent = rows(span.references).find(ref => ref.refType === 'CHILD_OF'
      && (!ref.traceID || ref.traceID === root.traceID));
    if (parent) {
      const key = String(parent.spanID);
      children.set(key, [...(children.get(key) ?? []), span]);
    }
  }
  const result: Array<{ span: Row; depth: number }> = [];
  const seen = new Set<unknown>();
  const pending = [{ span: root, depth: 0 }];
  while (pending.length) {
    const next = pending.pop()!;
    if (seen.has(next.span.spanID)) continue;
    seen.add(next.span.spanID);
    result.push(next);
    const siblings = [...(children.get(String(next.span.spanID)) ?? [])]
      .sort((a, b) => Number(a.startTime) - Number(b.startTime));
    for (const span of siblings.reverse()) pending.push({ span, depth: next.depth + 1 });
  }
  return result;
}

function mapTraces(payload: Row, snapshot: TelemetrySnapshot) {
  for (const trace of rows(payload.data).slice(0, 1000)) {
    const all = rows(trace.spans);
    for (const root of all) {
      const connection = root.operationName === 'mssql.driver.connection.open';
      if ((!connection && !['mssql.driver.statement.execute', 'mssql.driver.statement.execute_batch'].includes(String(root.operationName))) || !failed(root)) continue;
      const start = number(root.startTime);
      const duration = number(root.duration);
      if (start === null || duration === null || !Number.isFinite(new Date(start / 1000).getTime())) continue;
      const resource = tags(row(row(trace.processes)[String(root.processID)]).tags);
      const values = { ...resource, ...tags(root.tags) };
      const descendants = tree(root, all);
      const spans: Span[] = descendants.map(({ span, depth }) => ({
        name: spanName(span.operationName), kind: tags(span.tags)['span.kind'] === 'client' ? 'CLIENT' : 'INTERNAL',
        status: failed(span) ? 'ERROR' : 'UNSET', start: (Number(span.startTime) - start) / 1000,
        duration: Number(span.duration) / 1000, depth,
      }));
      const logs = descendants.flatMap(({ span }) => rows(span.logs))
        .sort((a, b) => Number(a.timestamp) - Number(b.timestamp));
      const events: ErrorEvent[] = logs.flatMap(log => {
        const fields = tags(log.fields);
        const name = spanName(fields.event);
        if (!diagnosticNames.has(name)) return [];
        const details = Object.fromEntries(Object.entries(fields).filter(([key, value]) => diagnosticKeys.has(key)
          && (typeof value === 'number' ? Number.isFinite(value) && value >= 0 : text(value) !== absent))
          .map(([key, value]) => [key, String(value)]));
        return [{ name, phase: text(fields['mssql.error.phase'] ?? fields['mssql.timeout.phase']),
          source: text(fields['mssql.error.source']), code: text(fields['mssql.error.code']),
          decision: text(fields['mssql.error.retry_decision']), attributes: details }];
      });
      const guid = typeof values['mssql.connection.guid'] === 'string'
        && /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(values['mssql.connection.guid'])
        ? values['mssql.connection.guid'] : undefined;
      const traceId = text(root.traceID ?? trace.traceID);
      const common = { time: new Date(start / 1000).toISOString(), duration: duration / 1000,
        category: text(values['mssql.error.category']), errorType: text(values['error.type']),
        attributes: attributes(values), spans, events };
      if (connection) {
        const phase = text(values['mssql.connection.failure_phase']);
        snapshot.failures.push({ ...common, id: guid ?? traceId,
          phase: ({ configuration: 'Configuration', dns: 'DNS', login: 'Login' } as Record<string, string>)[phase] ?? phase,
          attempts: number(values['mssql.connection.attempt_count'])
            ?? descendants.filter(({ span }) => span.operationName === 'mssql.driver.connection.attempt').length,
          source: text(values['mssql.error.source'] ?? events.find(event => event.source !== absent)?.source),
          region: text(values['cloud.region']),
          driver: text(values['mssql.driver.version'] ?? values['mssql.driver.user_agent.original']),
        });
      } else {
        snapshot.statementFailures.push({ ...common, id: traceId, connectionId: guid ?? absent,
          statementType: text(values['mssql.statement.type']), operation: text(values['mssql.statement.operation']),
          maskedSql: maskedQuery(tags(root.tags)['db.query.text']),
        });
      }
    }
  }
  snapshot.failures.sort((a, b) => b.time.localeCompare(a.time));
  snapshot.statementFailures.sort((a, b) => b.time.localeCompare(a.time));
}

// Stable metric identities, not span phase names or legacy callback-only wrappers.
const activities = new Set(['connection.open', 'connection.configuration', 'connection.attempt',
  'connection.dns', 'connection.socket_connect', 'connection.instance_discovery', 'connection.redirect',
  'connection.token_request', 'connection.prelogin_legacy', 'connection.tls', 'connection.login',
  'connection.initialize', 'statement.execute', 'statement.attempt', 'statement.request_build',
  'statement.server_call.prepare', 'statement.server_call.prepexec', 'statement.server_call.execute', 'statement.first_response']);
const outcomes = ['success', 'failure', 'timeout', 'canceled'] as const;
const roots = new Set(['connection.open', 'statement.execute']);
const retryReasons = new Set(['retry', 'redirect', 'failover', 'configurable_retry', 'invalid_handle_retry', 'enclave_retry', 'not_applicable']);
const dimensions = 'mssql_performance_activity, mssql_operation_kind';
const rootSelector = 'mssql_performance_activity=~"connection[.]open|statement[.]execute"';

async function request(path: string, params: Record<string, string>, name: string, warnings: string[],
  prometheus = false): Promise<Row | null> {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => { controller.abort(); reject(new Error('timeout')); }, 10000);
    });
    const work = async () => {
      const response = await fetch(`${path}?${new URLSearchParams(params)}`, { signal: controller.signal,
        headers: { Accept: 'application/json' }, credentials: 'same-origin' });
      if (!response.ok) throw new Error('HTTP failure');
      const result = row(await response.json());
      if (prometheus ? result.status !== 'success' || !Array.isArray(row(result.data).result)
        : !Array.isArray(result.data)) throw new Error('API failure');
      if (rows(result.errors).length || (Array.isArray(result.errors) && result.errors.length)) {
        warnings.push(`${name}: backend reported partial results.`);
      }
      if (Array.isArray(result.warnings) && result.warnings.length) warnings.push(`${name}: backend reported warnings.`);
      return result;
    };
    return await Promise.race([work(), timeout]);
  } catch {
    warnings.push(`${name} unavailable (request failed, timed out, or returned an invalid response).`);
    return null;
  } finally { clearTimeout(timer); }
}

/** Load one bounded 24-hour snapshot. Missing observations stay missing, never synthetic. */
export async function loadTelemetry(): Promise<TelemetrySnapshot> {
  const end = Math.floor(Date.now() / 1000);
  const start = end - windowSeconds;
  const snapshot: TelemetrySnapshot = { failures: [], statementFailures: [], performanceMetrics: [],
    throughputPoints: [], outcomeDistribution: [], retries: [], loadedAt: '', warnings: [
      'Failure counts cover only returned Jaeger traces (last 24 hours, maximum 1000 traces), not all failures; sampling and retention may omit traces.',
      'Metrics use 24-hour counter increases (estimates requiring two samples; initial values before the first scrape cannot be counted); percentiles are histogram estimates. Exact maximum duration is not available: classic histogram buckets and sums do not preserve it. Throughput is root operations/minute from 5-minute rates; gaps are missing data. Success ratios use observed outcome counts, not missing error counters. Missing error/retry series do not imply zero.',
    ] };
  const instant = (query: string, name: string) => request('/prometheus/api/v1/query',
    { query, time: String(end) }, name, snapshot.warnings, true);
  const [traces, counts, errors, p50, p95, p99, sums, retries, throughput] = await Promise.all([
    request('/jaeger/api/traces', { service: 'mssql-jdbc', start: String(start * 1e6),
      end: String(end * 1e6), limit: '1000' }, 'Jaeger traces', snapshot.warnings),
    instant(`sum by (${dimensions}, mssql_operation_outcome) (increase(db_client_operation_count_total[24h]))`, 'Operation counts'),
    instant(`sum by (${dimensions}) (increase(db_client_operation_error_count_total[24h]))`, 'Error counts'),
    ...[0.5, 0.95, 0.99].map(q => instant(`histogram_quantile(${q}, sum by (le, ${dimensions}) (rate(db_client_operation_duration_seconds_bucket[24h])))`, `Percentile ${q}`)),
    instant(`sum by (${dimensions}) (increase(db_client_operation_duration_seconds_sum[24h]))`, 'Duration sums'),
    instant(`sum by (${dimensions}, mssql_retry_reason) (increase(db_client_operation_retry_count_total{${rootSelector}}[24h]))`, 'Retries'),
    request('/prometheus/api/v1/query_range', {
      query: `sum by (${dimensions}) (rate(db_client_operation_count_total{${rootSelector}}[5m])) * 60`,
      start: String(start), end: String(end), step: String(step),
    }, 'Throughput', snapshot.warnings, true),
  ]);
  if (traces) mapTraces(traces, snapshot);
  const activityOf = (series: Row) => {
    const labels = row(series.metric);
    const activity = String(labels.mssql_performance_activity);
    return activities.has(activity) && labels.mssql_operation_kind === (activity.startsWith('connection.') ? 'connection' : 'statement')
      ? activity : null;
  };
  const valueOf = (series: Row) => {
    const value = number(Array.isArray(series.value) ? series.value[1] : null);
    return value !== null && value >= 0 ? value : null;
  };
  const vector = (payload: Row | null) => new Map(rows(row(payload?.data).result)
    .filter(series => activityOf(series) !== null).map(series => [activityOf(series)!, valueOf(series)]));
  const outcomeCounts = new Map<string, Map<string, number | null>>();
  for (const series of rows(row(counts?.data).result)) {
    const activity = activityOf(series);
    const outcome = String(row(series.metric).mssql_operation_outcome);
    if (!activity || !outcomes.includes(outcome as typeof outcomes[number])) continue;
    const values = outcomeCounts.get(activity) ?? new Map<string, number | null>();
    values.set(outcome, valueOf(series));
    outcomeCounts.set(activity, values);
  }
  const totals = new Map([...outcomeCounts].map(([activity, values]) => [activity,
    [...values.values()].some(value => value === null) ? null : [...values.values()].reduce<number>((sum, value) => sum + value!, 0)]));
  const vectors = [totals, ...[errors, p50, p95, p99, sums].map(vector)];
  const observed = new Set(vectors.flatMap(values => [...values.keys()]));
  for (const activity of observed) {
    const [count, errorCount, median, high, tail, sum] = vectors.map(values => values.get(activity) ?? null);
    const success = outcomeCounts.get(activity)?.get('success') ?? null;
    snapshot.performanceMetrics.push({ activity, label: activity.split('.').slice(1).join('.'),
      kind: activity.startsWith('connection.') ? 'Connection' : 'Statement', count, errors: errorCount,
      successRate: count !== null && count > 0 && success !== null ? success / count * 100 : null,
      p50: median === null ? null : median * 1000, p95: high === null ? null : high * 1000,
      p99: tail === null ? null : tail * 1000, max: null,
      totalDurationMs: sum === null ? null : sum * 1000,
      meanDurationMs: sum !== null && count !== null && count > 0 ? sum / count * 1000 : null });
    if (roots.has(activity)) snapshot.outcomeDistribution.push({ activity,
      success, failure: outcomeCounts.get(activity)?.get('failure') ?? null,
      timeout: outcomeCounts.get(activity)?.get('timeout') ?? null,
      canceled: outcomeCounts.get(activity)?.get('canceled') ?? null });
  }
  for (const series of rows(row(retries?.data).result)) {
    const activity = activityOf(series);
    const reason = String(row(series.metric).mssql_retry_reason);
    if (activity && roots.has(activity) && retryReasons.has(reason)) {
      snapshot.retries.push({ activity, reason, count: valueOf(series) });
    }
  }
  const samples = new Map<string, Map<number, number | null>>();
  for (const series of rows(row(throughput?.data).result)) {
    const activity = String(row(series.metric).mssql_performance_activity);
    if (!roots.has(activity)) continue;
    const points = new Map<number, number | null>();
    if (Array.isArray(series.values)) for (const pair of series.values) {
      if (Array.isArray(pair)) points.set(Number(pair[0]), number(pair[1]));
    }
    samples.set(activity, points);
  }
  for (let timestamp = start; timestamp <= end; timestamp += step) {
    snapshot.throughputPoints.push({ label: new Date(timestamp * 1000).toISOString(),
      connections: samples.get('connection.open')?.get(timestamp) ?? null,
      statements: samples.get('statement.execute')?.get(timestamp) ?? null });
  }
  snapshot.loadedAt = new Date().toISOString();
  return snapshot;
}