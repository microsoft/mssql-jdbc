import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

const root = fileURLToPath(new URL('../', import.meta.url));
const read = path => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const bash = process.platform === 'win32' ? 'C:/Program Files/Git/bin/bash.exe' : 'bash';
// Do not inherit credentials or read a populated .env. All values below are inert fixtures.
const env = {
  PATH: process.env.PATH, SystemRoot: process.env.SystemRoot, HOME: root,
  ProgramFiles: process.env.ProgramFiles, ProgramData: process.env.ProgramData,
  USERPROFILE: root, DOCKER_CONFIG: root,
  MSYS_NO_PATHCONV: '1', COMPOSE_DISABLE_ENV_FILE: '1',
  AZURE_HOST_PATH: root, MSSQL_SA_PASSWORD: 'synthetic-not-a-secret',
  OTEL_AUTH_MODE: 'azure_cli', OTEL_ACCESS_TOKEN_SCOPE: 'https://example.invalid/.default',
  OTEL_ARM_RESOURCE_ID: '/subscriptions/fixture/resourceGroups/demo/providers/Microsoft.Sql/servers/demo',
  IDENTITY_HEADER: 'synthetic-contract-fixture-not-a-secret',
  IDENTITY_ENDPOINT: 'http://token-server:8080/metadata/identity/oauth2/token',
  MISE_IMAGE: 'example.invalid/mise:test', OTELCOL_ARCDATA_IMAGE: 'example.invalid/collector:test',
  DELTA_BULK_LOADER_IMAGE: 'example.invalid/loader:test', MISE_TENANT_ID: 'fixture',
  MISE_CLIENT_ID: 'fixture', MISE_AUDIENCE: 'https://example.invalid', MISE_REGION: 'fixture',
  MISE_FIRST_PARTY_SUBSCRIPTION: 'fixture', MISE_CERT_PATH: `${root}Dockerfile`,
  STORAGE_ACCOUNT: 'fixture', STORAGE_CONTAINER: 'fixture', ROOT_PATH: 'fixture',
  EVENT_HUB_NAMESPACE: 'fixture.servicebus.windows.net', EVENT_HUB_NAME: 'fixture',
  EVENT_HUB_CONSUMER_GROUP: 'dedicated-fixture', AZURE_TENANT_ID: 'fixture',
  CHECKPOINT_STORAGE_ACCOUNT: 'fixture', CHECKPOINT_STORAGE_CONTAINER: 'fixture',
  DEMO_LOCAL_OTEL_ENDPOINT: 'https://collector.example.invalid',
  OTEL_DISCOVERY_ALLOWED_HOST: 'collector.example.invalid', AZURE_REGION: 'fixture'
};

function launch(args, overrides = {}) {
  // A shell function intercepts every Docker invocation. No daemon, build, login or service start.
  return spawnSync(bash, ['-c',
    'docker() { printf "docker"; printf " <%s>" "$@"; printf "\\n"; }; export -f docker; bash .scripts/dev.sh "$@"',
    'test', ...args], { cwd: root, env: { ...env, ...overrides }, encoding: 'utf8' });
}

test('no-argument launcher remains help; compat explicitly selects the full stack', () => {
  const help = launch([]);
  assert.equal(help.status, 0, help.stderr);
  assert.doesNotMatch(help.stdout, /docker </);
  const result = launch(['compat', 'config']);
  assert.equal(result.status, 0, result.stderr);
  for (const name of ['internal', 'delta', 'sql', 'compat']) {
    assert.match(result.stdout, new RegExp(`docker-compose\\.${name}\\.yml`));
  }
  assert.match(result.stdout, /<config> <--quiet>/);
  assert.doesNotMatch(result.stdout, /<up>|<login>|--volumes/);
});

test('compat discovery is explicit; it cannot silently use the fixed endpoint', () => {
  const result = launch(['compat', 'config'], { DEMO_ENDPOINT_MODE: 'discovery' });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /docker-compose\.discovery\.yml/);
  const bad = launch(['compat', 'config'], { DEMO_ENDPOINT_MODE: 'typo' });
  assert.equal(bad.status, 2);
});

test('credential initialization is tmpfs-only and fails closed', () => {
  const source = read('.scripts/azure-session.sh');
  assert.match(source, /mountpoint -q/);
  assert.match(source, /tmpfs/);
  assert.match(source, /umask 077/);
  assert.doesNotMatch(source, /az login|\|\| true/);
  const broker = read('internal/token-server/token_server.py');
  assert.match(broker, /compare_digest/);
  assert.match(broker, /timeout=20/);
  assert.match(broker, /ALLOWED_RESOURCES/);
  assert.match(broker, /Cache-Control/);
  assert.doesNotMatch(broker, /print\(.*(?:token|stderr|stdout)|_log\(.*exc/);
});

test('sandbox seed cannot target another SQL host or wipe metadata', () => {
  const source = read('internal/sqlserver-init/seed.sh');
  assert.match(source, /-S sqlserver -d msdb/);
  assert.match(source, /-x/); // Disable sqlcmd variable expansion of operator values.
  assert.doesNotMatch(source, /\bDELETE\b|\bTRUNCATE\b|\bDROP\b|\beval\b/);
  assert.match(source, /COUNT_BIG/);
  assert.match(source, /ROLLBACK/);
  assert.match(source, /DEMO_LOCAL_OTEL_ENDPOINT/);
});

test('optional CLI target still runs the standalone driver plus optional module', () => {
  const source = read('Dockerfile');
  assert.match(source, /AS runtime/);
  assert.match(source, /FROM runtime AS azure-cli/);
  assert.match(source, /FROM runtime AS default/);
  assert.match(source, /ConnectionErrorDemo/);
  assert.doesNotMatch(source, /OtelPocLoadGen|az login|COPY.*\.azure/);
});

test('compose compatibility contract resolves without credentials or a Docker daemon', () => {
  const files = ['docker-compose.yml', 'docker-compose.internal.yml', 'docker-compose.delta.yml',
    'docker-compose.sql.yml', 'docker-compose.compat.yml'];
  const resolve = extra => {
    const result = spawnSync('docker', ['compose', '--profile', 'tools', '--env-file', '.env.example',
      ...[...files, ...extra].flatMap(file => ['-f', file]), 'config', '--format', 'json'],
    { cwd: root, env, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr || 'Compose config must validate without daemon access.');
    return JSON.parse(result.stdout);
  };
  const config = resolve([]);
  const { app, 'token-server': broker, otelcol, 'delta-bulk-loader': loader } = config.services;
  assert.equal(app.build.target, 'azure-cli');
  assert.equal(app.image, 'mssql-jdbc-connection-poc:azure-cli');
  assert.equal(broker.ports, undefined);
  assert.equal(broker.read_only, true);
  for (const service of [app, broker]) {
    const session = service.volumes.find(v => v.target === '/host/.azure');
    assert.equal(session.read_only, true);
    assert.equal(session.bind.create_host_path, false);
    assert.ok(service.tmpfs.some(v => v.includes('/run/azure')));
  }
  assert.equal(otelcol.environment.IDENTITY_ENDPOINT, env.IDENTITY_ENDPOINT);
  assert.equal(loader.environment.IDENTITY_HEADER, env.IDENTITY_HEADER);
  assert.ok(otelcol.networks.ingress.aliases.includes('otelcol-arcdata'));
  assert.equal(config.networks.identity.internal, true);
  assert.equal(loader.environment.EventHubReplicationConfiguration__ReplicationConfiguration__0__SourceEventHubConfiguration__IngestionConfiguration__EventHubDefaultOffset, 'Earliest');
  assert.equal(app.environment.OTEL_METRICS_EXPORTER, 'none');
  const discovery = resolve(['docker-compose.discovery.yml']);
  assert.equal(discovery.services.app.environment.OTEL_EXPORTER_OTLP_ENDPOINT, '');
  assert.equal(discovery.services.app.environment.OTEL_ARM_RESOURCE_ID, '');
  assert.equal(discovery.services.app.environment.DEMO_ENDPOINT_MODE, 'discovery');
  assert.equal(discovery.services['sqlserver-init'].networks.sql !== undefined, true);
  assert.equal(discovery.services['sqlserver-init'].ports, undefined);
});