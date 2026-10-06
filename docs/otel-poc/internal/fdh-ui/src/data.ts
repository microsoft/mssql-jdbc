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