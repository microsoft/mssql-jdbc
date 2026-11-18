import type { ConnectionFailure, StatementFailure, PerformanceMetric, ThroughputPoint, Span, ErrorEvent } from './data';

export type TelemetrySnapshot = {
  failures: ConnectionFailure[];
  statementFailures: StatementFailure[];
  performanceMetrics: PerformanceMetric[];
  throughputPoints: ThroughputPoint[];
  loadedAt: string;
  warnings: string[];
};

export const emptyTelemetry: TelemetrySnapshot = {
  failures: [], statementFailures: [], performanceMetrics: [], throughputPoints: [], loadedAt: '', warnings: [],
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
  'mssql.error.category', 'mssql.error.source', 'mssql.error.code', 'mssql.error.sql_state',
]);

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
      if ((!connection && root.operationName !== 'mssql.driver.statement.execute') || !failed(root)) continue;
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
        if (name === absent) return [];
        return [{ name, phase: text(fields['mssql.error.phase'] ?? fields['mssql.timeout.phase']),
          source: text(fields['mssql.error.source']), code: text(fields['mssql.error.code']),
          decision: text(fields['mssql.error.retry_decision']) }];
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
          maskedSql: absent,
        });
      }
    }
  }
  snapshot.failures.sort((a, b) => b.time.localeCompare(a.time));
  snapshot.statementFailures.sort((a, b) => b.time.localeCompare(a.time));
}

// Verified against PerformanceActivityMetrics and the live Prometheus activity label values.
const activities: Record<string, string> = {
  CONNECTION: 'connection.open', CONNECTION_CONFIGURATION: 'connection.configuration',
  CONNECTION_ATTEMPT: 'connection.attempt', DNS: 'connection.dns', SOCKET_CONNECT: 'connection.socket_connect',
  INSTANCE_DISCOVERY: 'connection.instance_discovery', CONNECTION_REDIRECT: 'connection.redirect',
  TOKEN_REQUEST: 'connection.token_request', TOKEN_ACQUISITION: 'connection.token_acquisition', LOGIN: 'connection.login_wrapper',
  PRELOGIN: 'connection.prelogin', TLS: 'connection.tls', LOGIN_EXCHANGE: 'connection.login',
  CONNECTION_INITIALIZE: 'connection.initialize', STATEMENT_INVOCATION: 'statement.execute',
  STATEMENT_ATTEMPT: 'statement.attempt', STATEMENT_REQUEST_BUILD: 'statement.request_build',
  STATEMENT_PREPARE: 'statement.server_call.prepare', STATEMENT_PREPEXEC: 'statement.server_call.prepexec',
  STATEMENT_EXECUTE: 'statement.server_call', STATEMENT_FIRST_SERVER_RESPONSE: 'statement.first_response',
};

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
    throughputPoints: [], loadedAt: '', warnings: [
      'Failure counts cover only returned Jaeger traces (last 24 hours, maximum 1000 traces), not all failures; sampling and retention may omit traces.',
      'Metrics use 24-hour counter increases (estimates requiring two samples; initial values before the first scrape cannot be counted); percentiles are histogram estimates. Max duration is not emitted. Throughput is operations/minute from 5-minute rates; gaps are missing data. Missing error series do not imply zero errors.',
    ] };
  const instant = (query: string, name: string) => request('/prometheus/api/v1/query',
    { query, time: String(end) }, name, snapshot.warnings, true);
  const [traces, counts, errors, p50, p95, p99, throughput] = await Promise.all([
    request('/jaeger/api/traces', { service: 'mssql-jdbc', start: String(start * 1e6),
      end: String(end * 1e6), limit: '1000' }, 'Jaeger traces', snapshot.warnings),
    instant('sum by (mssql_activity) (increase(db_client_operation_count_total[24h]))', 'Operation counts'),
    instant('sum by (mssql_activity) (increase(db_client_operation_error_count_total[24h]))', 'Error counts'),
    ...[0.5, 0.95, 0.99].map(q => instant(`histogram_quantile(${q}, sum by (le, mssql_activity) (rate(db_client_operation_duration_seconds_bucket[24h])))`, `Percentile ${q}`)),
    request('/prometheus/api/v1/query_range', {
      query: 'sum by (mssql_activity) (rate(db_client_operation_count_total{mssql_activity=~"CONNECTION|STATEMENT_INVOCATION"}[5m])) * 60',
      start: String(start), end: String(end), step: String(step),
    }, 'Throughput', snapshot.warnings, true),
  ]);
  if (traces) mapTraces(traces, snapshot);
  const vector = (payload: Row | null) => new Map(rows(row(payload?.data).result)
    .map(series => [String(row(series.metric).mssql_activity), number(Array.isArray(series.value) ? series.value[1] : null)]));
  const vectors = [counts, errors, p50, p95, p99].map(vector);
  const observed = new Set(vectors.flatMap(values => [...values.keys()]));
  for (const activity of observed) {
    const display = activities[activity];
    if (!display) continue;
    const [count, errorCount, median, high, tail] = vectors.map(values => values.get(activity) ?? null);
    snapshot.performanceMetrics.push({ activity: display, label: display.split('.').slice(1).join('.'),
      kind: display.startsWith('connection.') ? 'Connection' : 'Statement', count, errors: errorCount,
      successRate: count !== null && count > 0 && errorCount !== null ? (1 - errorCount / count) * 100 : null,
      p50: median === null ? null : median * 1000, p95: high === null ? null : high * 1000,
      p99: tail === null ? null : tail * 1000, max: null });
  }
  const samples = new Map<string, Map<number, number | null>>();
  for (const series of rows(row(throughput?.data).result)) {
    const activity = String(row(series.metric).mssql_activity);
    const points = new Map<number, number | null>();
    if (Array.isArray(series.values)) for (const pair of series.values) {
      if (Array.isArray(pair)) points.set(Number(pair[0]), number(pair[1]));
    }
    samples.set(activity, points);
  }
  for (let timestamp = start; timestamp <= end; timestamp += step) {
    snapshot.throughputPoints.push({ label: new Date(timestamp * 1000).toISOString(),
      connections: samples.get('CONNECTION')?.get(timestamp) ?? null,
      statements: samples.get('STATEMENT_INVOCATION')?.get(timestamp) ?? null });
  }
  snapshot.loadedAt = new Date().toISOString();
  return snapshot;
}