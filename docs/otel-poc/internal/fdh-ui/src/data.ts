export type FailurePhase = 'Configuration' | 'DNS' | 'Login';

export type Span = {
  name: string;
  kind: 'CLIENT' | 'INTERNAL';
  status: 'ERROR' | 'UNSET';
  start: number;
  duration: number;
  depth: number;
};

export type ErrorEvent = {
  name: string;
  phase: string;
  source: string;
  code: string;
  decision: string;
};

export type ConnectionFailure = {
  id: string;
  time: string;
  phase: FailurePhase;
  category: string;
  errorType: string;
  duration: number;
  attempts: number;
  source: string;
  region: string;
  driver: string;
  attributes: Record<string, string>;
  spans: Span[];
  events: ErrorEvent[];
};

export type StatementFailure = {
  id: string;
  time: string;
  statementType: 'Statement' | 'PreparedStatement';
  operation: string;
  maskedSql: string;
  category: string;
  errorType: string;
  duration: number;
  connectionId: string;
  attributes: Record<string, string>;
  spans: Span[];
  events: ErrorEvent[];
};

export type PerformanceMetric = {
  activity: string;
  label: string;
  kind: 'Connection' | 'Statement';
  count: number;
  successRate: number;
  errors: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
};

export type ThroughputPoint = {
  label: string;
  connections: number;
  statements: number;
};

const phaseCounts: Array<[FailurePhase, number]> = [
  ['DNS', 83],
  ['Configuration', 37],
  ['Login', 24],
];

function seeded(index: number, salt = 0) {
  const value = Math.sin(index * 9301 + salt * 49297) * 10000;
  return value - Math.floor(value);
}

function guid(index: number) {
  const hex = Array.from({ length: 32 }, (_, position) =>
    Math.floor(seeded(index, position) * 16).toString(16),
  ).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20)}`;
}

function spansFor(phase: FailurePhase, duration: number): Span[] {
  if (phase === 'Configuration') {
    return [
      { name: 'mssql.driver.connection.open', kind: 'CLIENT', status: 'ERROR', start: 0, duration, depth: 0 },
      { name: 'mssql.driver.connection.configuration', kind: 'INTERNAL', status: 'ERROR', start: 0.4, duration: duration - 0.8, depth: 1 },
    ];
  }
  if (phase === 'DNS') {
    return [
      { name: 'mssql.driver.connection.open', kind: 'CLIENT', status: 'ERROR', start: 0, duration, depth: 0 },
      { name: 'mssql.driver.connection.configuration', kind: 'INTERNAL', status: 'UNSET', start: 0.3, duration: 1.2, depth: 1 },
      { name: 'mssql.driver.connection.attempt', kind: 'INTERNAL', status: 'ERROR', start: 1.7, duration: duration - 2, depth: 1 },
      { name: 'mssql.driver.connection.dns', kind: 'INTERNAL', status: 'ERROR', start: 2.1, duration: duration - 2.8, depth: 2 },
    ];
  }
  return [
    { name: 'mssql.driver.connection.open', kind: 'CLIENT', status: 'ERROR', start: 0, duration, depth: 0 },
    { name: 'mssql.driver.connection.configuration', kind: 'INTERNAL', status: 'UNSET', start: 0.4, duration: 1.8, depth: 1 },
    { name: 'mssql.driver.connection.attempt', kind: 'INTERNAL', status: 'ERROR', start: 2.4, duration: duration - 2.8, depth: 1 },
    { name: 'mssql.driver.connection.dns', kind: 'INTERNAL', status: 'UNSET', start: 3, duration: 3.3, depth: 2 },
    { name: 'mssql.driver.connection.socket_connect', kind: 'INTERNAL', status: 'UNSET', start: 6.8, duration: 8.6, depth: 2 },
    { name: 'mssql.driver.connection.prelogin', kind: 'INTERNAL', status: 'UNSET', start: 15.9, duration: 6.8, depth: 2 },
    { name: 'mssql.driver.connection.tls', kind: 'INTERNAL', status: 'UNSET', start: 23.2, duration: 21.4, depth: 2 },
    { name: 'mssql.driver.connection.login', kind: 'INTERNAL', status: 'ERROR', start: 45.1, duration: Math.max(8, duration - 46), depth: 2 },
  ];
}

export const failures: ConnectionFailure[] = phaseCounts.flatMap(([phase, count], phaseIndex) =>
  Array.from({ length: count }, (_, position) => {
    const index = phaseIndex * 100 + position + 1;
    const duration = phase === 'Login'
      ? Math.round(78 + seeded(index, 3) * 145)
      : phase === 'DNS'
        ? Math.round(8 + seeded(index, 5) * 34)
        : Math.round(2 + seeded(index, 7) * 9);
    const category = phase === 'Login' ? 'Authentication' : phase === 'DNS' ? 'Name resolution' : 'Configuration';
    const errorType = phase === 'Login' ? 'sqlserver.18456' : phase === 'DNS' ? 'java.net.UnknownHostException' : 'jdbc.configuration';
    const id = guid(index);
    const secondsAgo = 90 + index * 37;
    const time = new Date(Date.now() - secondsAgo * 1000).toISOString();
    const source = phase === 'Login' ? 'SQL Server' : phase === 'DNS' ? 'JVM' : 'Driver';
    const events: ErrorEvent[] = phase === 'Login'
      ? [
          { name: 'mssql.driver.error', phase: 'login', source: 'sql_server', code: 'sqlserver:18456', decision: 'not_retryable' },
          { name: 'mssql.driver.connection.retry_decision', phase: 'login', source: 'driver', code: 'S0001', decision: 'limit_reached' },
        ]
      : [{ name: 'mssql.driver.error', phase: phase.toLowerCase(), source: source.toLowerCase(), code: errorType, decision: 'not_retryable' }];
    return {
      id,
      time,
      phase,
      category,
      errorType,
      duration,
      attempts: phase === 'Login' && seeded(index, 9) > 0.72 ? 2 : 1,
      source,
      region: ['East US', 'West Europe', 'Southeast Asia'][index % 3],
      driver: 'Microsoft JDBC 13.6.0',
      attributes: {
        'mssql.connection.guid': id,
        'mssql.connection.outcome': 'failure',
        'mssql.connection.failure_phase': phase.toLowerCase(),
        'mssql.error.category': category.toLowerCase().replace(' ', '_'),
        'error.type': errorType,
        'mssql.error.source': source.toLowerCase().replace(' ', '_'),
        'mssql.connection.attempt_count': phase === 'Login' && seeded(index, 9) > 0.72 ? '2' : '1',
        'mssql.connection.retry_count': phase === 'Login' && seeded(index, 9) > 0.72 ? '1' : '0',
        'mssql.connection.redirect_count': phase === 'Login' ? '1' : '0',
        'mssql.connection.login_timeout': '5',
        'mssql.connection.socket_timeout': '5',
        'mssql.connection.encrypt': phase === 'Login' ? 'true' : 'false',
        'mssql.connection.trust_server_certificate': 'false',
        'mssql.authentication.method': 'SqlPassword',
        'db.system.name': 'microsoft.sql_server',
        'server.address': phase === 'Login' ? 'sql-customer-prod.database.windows.net' : 'redacted',
        'server.port': '1433',
        'service.name': 'customer-orders-api',
        'deployment.environment': 'production',
        'cloud.region': ['eastus', 'westeurope', 'southeastasia'][index % 3],
        'telemetry.sdk.language': 'java',
        'mssql.telemetry.schema.version': '1.0',
      },
      spans: spansFor(phase, duration),
      events,
    };
  }),
).sort((a, b) => b.time.localeCompare(a.time));

export const statementFailures: StatementFailure[] = Array.from({ length: 42 }, (_, position): StatementFailure => {
  const prepared = position % 3 !== 0;
  const constraint = position % 5 < 3;
  const index = 500 + position;
  const duration = Math.round(12 + seeded(index, 12) * 95);
  const id = guid(index).replaceAll('-', '');
  const connectionId = guid(index + 1000);
  const category = constraint ? 'Constraint violation' : 'Query syntax & semantics';
  const errorType = constraint ? 'sqlserver.2627' : 'sqlserver.208';
  const maskedSql = constraint
    ? 'INSERT INTO #jdbc_otel_stmt_demo (id, label) VALUES (?, ?)'
    : 'SELECT ? FROM dbo.__jdbc_otel_missing';
  const statementType: StatementFailure['statementType'] = prepared ? 'PreparedStatement' : 'Statement';
  return {
    id,
    time: new Date(Date.now() - (240 + position * 89) * 1000).toISOString(),
    statementType,
    operation: constraint ? 'insert' : 'select',
    maskedSql,
    category,
    errorType,
    duration,
    connectionId,
    attributes: {
      'db.query.text': maskedSql,
      'db.system.name': 'microsoft.sql_server',
      'mssql.connection.guid': connectionId,
      'mssql.statement.type': prepared ? 'prepared_statement' : 'statement',
      'mssql.statement.api': constraint ? 'execute_update' : 'execute_query',
      'mssql.statement.operation': constraint ? 'insert' : 'select',
      'mssql.statement.outcome': 'failure',
      'mssql.statement.failure_phase': 'server_call',
      'mssql.statement.attempt_count': '1',
      'mssql.statement.retry_count': '0',
      'mssql.error.category': constraint ? 'constraint_violation' : 'query_syntax_semantics',
      'error.type': errorType,
      'mssql.telemetry.schema.version': '1.0',
    },
    spans: [
      { name: 'mssql.driver.statement.execute', kind: 'CLIENT', status: 'ERROR', start: 0, duration, depth: 0 },
      { name: 'mssql.driver.statement.attempt', kind: 'INTERNAL', status: 'ERROR', start: 0.2, duration: duration - 0.4, depth: 1 },
      { name: 'mssql.driver.statement.request_build', kind: 'INTERNAL', status: 'UNSET', start: 0.5, duration: Math.min(4, duration / 5), depth: 2 },
      { name: 'mssql.driver.statement.server_call', kind: 'INTERNAL', status: 'ERROR', start: 4.8, duration: Math.max(5, duration - 5.2), depth: 2 },
      { name: 'mssql.driver.statement.first_response', kind: 'INTERNAL', status: 'ERROR', start: 5.1, duration: Math.max(4, duration - 5.8), depth: 3 },
    ],
    events: [{
      name: 'mssql.driver.error',
      phase: 'server_call',
      source: 'sql_server',
      code: constraint ? 'sqlserver:2627' : 'sqlserver:208',
      decision: 'not_retryable',
    }],
  };
}).sort((a, b) => b.time.localeCompare(a.time));

export const performanceMetrics: PerformanceMetric[] = [
  { activity: 'connection.open', label: 'Open', kind: 'Connection', count: 8760, successRate: 98.36, errors: 144, p50: 31, p95: 94, p99: 181, max: 412 },
  { activity: 'connection.configuration', label: 'Configuration', kind: 'Connection', count: 8797, successRate: 99.58, errors: 37, p50: 0.7, p95: 1.8, p99: 3.2, max: 8.4 },
  { activity: 'connection.dns', label: 'DNS', kind: 'Connection', count: 8843, successRate: 99.06, errors: 83, p50: 3.4, p95: 12.8, p99: 29.6, max: 68 },
  { activity: 'connection.socket_connect', label: 'Socket', kind: 'Connection', count: 8760, successRate: 99.73, errors: 24, p50: 6.8, p95: 22.4, p99: 48.1, max: 133 },
  { activity: 'connection.prelogin', label: 'Prelogin', kind: 'Connection', count: 8736, successRate: 100, errors: 0, p50: 4.2, p95: 14.6, p99: 31.8, max: 91 },
  { activity: 'connection.tls', label: 'TLS', kind: 'Connection', count: 8736, successRate: 100, errors: 0, p50: 13.7, p95: 38.9, p99: 72.4, max: 188 },
  { activity: 'connection.login', label: 'Login', kind: 'Connection', count: 8736, successRate: 99.73, errors: 24, p50: 8.1, p95: 24.3, p99: 53.8, max: 146 },
  { activity: 'connection.initialize', label: 'Initialize', kind: 'Connection', count: 8712, successRate: 100, errors: 0, p50: 2.7, p95: 8.4, p99: 17.9, max: 47 },
  { activity: 'statement.execute', label: 'Execute', kind: 'Statement', count: 761640, successRate: 99.994, errors: 42, p50: 11.6, p95: 47.8, p99: 103.2, max: 1432 },
  { activity: 'statement.attempt', label: 'Attempt', kind: 'Statement', count: 763128, successRate: 99.994, errors: 42, p50: 11.2, p95: 46.9, p99: 101.8, max: 1427 },
  { activity: 'statement.request_build', label: 'Request build', kind: 'Statement', count: 763128, successRate: 100, errors: 0, p50: 0.4, p95: 1.9, p99: 4.8, max: 36 },
  { activity: 'statement.server_call.prepare', label: 'Prepare', kind: 'Statement', count: 24864, successRate: 99.99, errors: 2, p50: 4.8, p95: 16.2, p99: 37.4, max: 122 },
  { activity: 'statement.server_call.prepexec', label: 'Prepexec', kind: 'Statement', count: 138407, successRate: 99.99, errors: 13, p50: 12.4, p95: 51.7, p99: 111.3, max: 805 },
  { activity: 'statement.server_call.execute', label: 'Server call', kind: 'Statement', count: 599857, successRate: 99.995, errors: 27, p50: 9.7, p95: 43.6, p99: 96.5, max: 1401 },
  { activity: 'statement.first_response', label: 'First response', kind: 'Statement', count: 763128, successRate: 99.994, errors: 42, p50: 8.9, p95: 41.2, p99: 91.7, max: 1398 },
];

export const throughputPoints: ThroughputPoint[] = Array.from({ length: 32 }, (_, index) => ({
  label: `${String(Math.floor(index / 4)).padStart(2, '0')}:${String((index % 4) * 15).padStart(2, '0')}`,
  connections: Math.round(132 + seeded(index + 800, 21) * 31),
  statements: Math.round(11840 + seeded(index + 800, 22) * 1760),
}));