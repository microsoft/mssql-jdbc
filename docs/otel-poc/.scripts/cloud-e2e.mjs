import { chmodSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const REQUIRED = [
  'CLOUD_RUN_ID', 'CLOUD_OWNER', 'CLOUD_TARGET_ACK', 'CLOUD_SCHEMA',
  'KUSTO_CLUSTER_URI', 'KUSTO_DATABASE', 'STORAGE_ACCOUNT', 'STORAGE_CONTAINER',
  'ROOT_PATH', 'EVENT_HUB_NAMESPACE', 'EVENT_HUB_NAME', 'EVENT_HUB_CONSUMER_GROUP',
  'CHECKPOINT_STORAGE_ACCOUNT', 'CHECKPOINT_STORAGE_CONTAINER',
  'OTELCOL_ARCDATA_IMAGE', 'DELTA_BULK_LOADER_IMAGE', 'GRAFANA_IMAGE',
  'IDENTITY_HEADER', 'GRAFANA_ADMIN_PASSWORD', 'AZURE_TENANT_ID', 'AZURE_HOST_PATH',
  'MSSQL_SA_PASSWORD', 'DEMO_LOGIN_PASSWORD'
];
const NAME = /^[a-z][a-z0-9-]{0,62}$/;
const RUN = /^[a-f0-9]{32}$/;
const GUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const DIGEST = /^[a-z0-9.-]+(?::[0-9]+)?\/[a-z0-9._/-]+@sha256:[a-f0-9]{64}$/i;
const forbiddenAttribute = /authorization|password|bearer|connection[._]string|db\.(statement|query)|exception\.(message|stacktrace)|http\.request\.header/i;
const validId = (value, length) => typeof value === 'string' && value.length === length
  && /^[0-9a-f]+$/i.test(value) && !/^0+$/.test(value);
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
const fail = message => { throw new Error(message); };

export function parseEnv(text) {
  if (typeof text !== 'string') fail('Environment input must be text');
  const result = {};
  for (const raw of text.replace(/^\uFEFF/, '').split(/\r?\n/)) {
    const trimmed = raw.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    if (/^export\s/.test(trimmed) || /\$\(|\$\{|`/.test(trimmed)) fail('Shell syntax is forbidden');
    const match = /^([A-Z][A-Z0-9_]*)=(.*)$/.exec(trimmed);
    if (!match || own(result, match[1])) fail('Invalid or duplicate environment key');
    let value = match[2];
    if (value.startsWith('"')) {
      if (value.length < 2 || !value.endsWith('"')) fail('Unterminated quoted value');
      value = value.slice(1, -1);
      if (value.includes('"')) fail('Embedded quotes are forbidden');
    } else if (value.startsWith("'") || value.endsWith("'")) {
      if (value.length < 2 || !value.endsWith("'")) fail('Unterminated quoted value');
      value = value.slice(1, -1);
      if (value.includes("'")) fail('Embedded quotes are forbidden');
    } else if (/\s+#/.test(value)) {
      value = value.replace(/\s+#.*$/, '');
    }
    if (/\r|\n|\0/.test(value)) fail('Invalid environment value');
    result[match[1]] = value;
  }
  return result;
}

function required(env, key) {
  const value = env[key];
  if (typeof value !== 'string' || !value) fail(`${key} is required`);
  return value;
}

function safeOwner(value) {
  return NAME.test(value) && !['shared', 'default', 'prod', 'production'].includes(value);
}

function safeUrl(value, suffix) {
  let url;
  try { url = new URL(value); } catch { fail('Invalid URL'); }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash
      || url.pathname !== '/' || !url.hostname.endsWith(suffix)) fail('Unapproved URL');
  return url.origin;
}

export function configure(input) {
  const env = { ...input };
  for (const key of REQUIRED) required(env, key);
  const run = env.CLOUD_RUN_ID;
  const owner = env.CLOUD_OWNER;
  if (!RUN.test(run) || !safeOwner(owner)) fail('Invalid cloud ownership identity');
  if (env.CLOUD_TARGET_ACK !== 'dedicated-demo-targets') fail('Dedicated target acknowledgement required');
  if (env.CLOUD_SCHEMA !== 'otap-microseconds-v1') fail('Unsupported cloud schema');
  if (!GUID.test(env.AZURE_TENANT_ID)) {
    fail('Invalid Azure identity configuration');
  }
  const cluster = safeUrl(env.KUSTO_CLUSTER_URI, '.kusto.windows.net');
  const expectedRoot = `jdbc-demo-${owner}/${run}`;
  if (env.ROOT_PATH !== expectedRoot || env.ROOT_PATH.includes('..') || env.ROOT_PATH.startsWith('/')) {
    fail('ROOT_PATH is not dedicated to this owner and run');
  }
  if (env.KUSTO_DATABASE.toLowerCase().includes('apptelemetry') || !/^[A-Za-z][A-Za-z0-9_]{0,62}$/.test(env.KUSTO_DATABASE)) {
    fail('Kusto database must be a dedicated connection-error database');
  }
  for (const key of ['STORAGE_ACCOUNT', 'STORAGE_CONTAINER', 'CHECKPOINT_STORAGE_ACCOUNT', 'CHECKPOINT_STORAGE_CONTAINER']) {
    if (!NAME.test(env[key]) || env[key].includes('shared')) fail(`${key} is not isolated`);
  }
  if (!/^[a-z0-9.-]+\.servicebus\.windows\.net$/i.test(env.EVENT_HUB_NAMESPACE)
      || !NAME.test(env.EVENT_HUB_NAME) || env.EVENT_HUB_NAME.includes('shared')
      || env.EVENT_HUB_CONSUMER_GROUP !== `jdbc-${run}`) fail('Event Hub target is not run-isolated');
  for (const key of ['OTELCOL_ARCDATA_IMAGE', 'DELTA_BULK_LOADER_IMAGE', 'GRAFANA_IMAGE']) {
    if (!DIGEST.test(env[key])) fail(`${key} must be digest pinned`);
  }
  if (env.IDENTITY_HEADER.length < 32 || env.IDENTITY_HEADER.length > 256
      || env.GRAFANA_ADMIN_PASSWORD.length < 24 || env.MSSQL_SA_PASSWORD === env.DEMO_LOGIN_PASSWORD) {
    fail('Invalid or reused runtime secret');
  }
  const scenarios = env.DEMO_SCENARIOS || 'config,dns,login,success';
  const selected = scenarios.split(',');
  const repeat = env.DEMO_REPEAT || '1';
  const pause = env.DEMO_PAUSE_SECONDS || '0';
  const counts = Object.fromEntries(['config', 'dns', 'login', 'success'].map(scenario => {
    const key = `DEMO_${scenario.toUpperCase()}_COUNT`;
    return [scenario, env[key] ?? (selected.includes(scenario) ? repeat : '0')];
  }));
  if (new Set(selected).size !== selected.length
      || selected.some(item => !['config', 'dns', 'login', 'success'].includes(item))
      || !selected.includes('success') || !selected.some(item => item !== 'success')
      || !/^[1-9][0-9]{0,2}$/.test(repeat) || Number(repeat) > 1000
      || !/^(?:0|[1-9]|[1-5][0-9]|60)$/.test(pause)
      || Object.values(counts).some(count => !/^(?:0|[1-9][0-9]{0,2}|1000)$/.test(count))
      || Number(counts.success) < 1
      || !['config', 'dns', 'login'].some(scenario => Number(counts[scenario]) > 0)) {
    fail('Invalid cloud workload volume');
  }
  const service = `jdbc-cloud-${run}`;
  return Object.freeze({
    env: Object.freeze(env), run, owner, service, project: `jdbc-cloud-${run}`,
    cluster, database: env.KUSTO_DATABASE, rootPath: expectedRoot,
    consumerGroup: env.EVENT_HUB_CONSUMER_GROUP, counts: Object.freeze(counts)
  });
}

export function collector() {
  return {
    extensions: {
      misecontainerauth: {
        first_party_app_ids: [], first_party_tenant_ids: [],
        first_party_subscriptions: ['${env:MISE_FIRST_PARTY_SUBSCRIPTION}'],
        authz_check_access: { enabled: true, arm_resource_id_header: 'x-ms-arm-resource-id', decision_cache_max_size: 10000, decision_cache_ttl_sec: 0 },
        http: { mise_service_endpoint: 'http://mise:5000/ValidateRequest' }
      }
    },
    receivers: { otlp: { protocols: { http: { endpoint: '0.0.0.0:4318', include_metadata: true, auth: { authenticator: 'misecontainerauth' } } } } },
    processors: { memory_limiter: { check_interval: '1s', limit_mib: 512, spike_limit_mib: 128 }, batch: { timeout: '1s', send_batch_size: 128 } },
    exporters: {
      'otlphttp/evidence': { endpoint: 'http://evidence:4318' },
      'deltalake/otap': {
        storage_url: 'https://${env:STORAGE_ACCOUNT}.dfs.core.windows.net', container: '${env:STORAGE_CONTAINER}', root_path: '${env:ROOT_PATH}',
        auth: { authentication_method: 'SystemManagedIdentity' }, drop_on_error: false,
        delta_bulk_loader: { storage_account_csharp_named_auth_type: 'ManagedIdentityCredential', event_hub: { enabled: true, namespace: '${env:EVENT_HUB_NAMESPACE}', name: '${env:EVENT_HUB_NAME}', tenant_id: '${env:AZURE_TENANT_ID}', publish_timeout: '30s', auth: { authentication_method: 'SystemManagedIdentity' } } },
        parquet: { compression: 'zstd', row_group_target_rows: 1024, page_buffer_size: 65536, data_page_size_bytes: 262144 },
        buffer: { max_buffer_rows: 1024, max_buffer_bytes: 1048576, flush_timeout: '1s', flush_check_interval: '250ms', flush_timeout_max: '30s', max_memory_usage_in_mega_bytes: 0 },
        partition: { default: { time: 'event_year_date_hour', data_columns: ['application'] } }, sending_queue: { enabled: false }, timeout: '30s', marshaler: 'otap_parquet'
      },
      'kustoexporter/otap': {
        marshaler: 'otap_parquet', connection: { cluster_uri: '${env:KUSTO_CLUSTER_URI}', auth: { authentication_method: 'SystemManagedIdentity' } }, db_name: '${env:KUSTO_DATABASE}', ingestion_type: 'Queued', retry_on_ingestion_error: true,
        sending_queue: { enabled: false }, retry_on_failure: { enabled: false }, timeout: '30s', drop_on_error: false,
        parquet: { compression: 'zstd', row_group_target_rows: 1024, page_buffer_size: 65536, data_page_size_bytes: 262144 },
        buffer: { max_buffer_rows: 1024, max_buffer_bytes: 1048576, flush_timeout: '1s', flush_check_interval: '250ms', flush_timeout_max: '30s', max_memory_usage_in_mega_bytes: 0 },
        table_filter: { opt_out: ['^multivariate_metrics$'] }
      }
    },
    service: { extensions: ['misecontainerauth'], telemetry: { metrics: { level: 'none' }, logs: { level: 'warn' } }, pipelines: { traces: { receivers: ['otlp'], processors: ['memory_limiter', 'batch'], exporters: ['otlphttp/evidence', 'deltalake/otap', 'kustoexporter/otap'] } } }
  };
}

export function cloudCollector() {
  const config = collector();
  delete config.extensions;
  delete config.receivers.otlp.protocols.http.auth;
  config.service.extensions = [];
  return config;
}

function evidenceCollector() {
  return {
    receivers: { otlp: { protocols: { http: { endpoint: '0.0.0.0:4318' } } } },
    processors: { memory_limiter: { check_interval: '1s', limit_mib: 128, spike_limit_mib: 32 }, batch: { timeout: '1s', send_batch_size: 128 } },
    exporters: { 'file/evidence': { path: '/evidence/traces.json', flush_interval: '1s' } },
    service: { telemetry: { metrics: { level: 'none' }, logs: { level: 'warn' } }, pipelines: { traces: { receivers: ['otlp'], processors: ['memory_limiter', 'batch'], exporters: ['file/evidence'] } } }
  };
}

function valueOf(any) {
  if (!any || typeof any !== 'object') return null;
  for (const [key, value] of Object.entries(any)) {
    if (key.endsWith('Value')) return value;
  }
  return null;
}

function attributes(list) {
  const result = {};
  for (const item of list ?? []) {
    if (!item || typeof item.key !== 'string' || forbiddenAttribute.test(item.key) || own(result, item.key)) fail('Unsafe or duplicate span attribute');
    const value = valueOf(item.value);
    if (typeof value === 'object' && value !== null) fail('Nested attribute values are unsupported');
    result[item.key] = value;
  }
  return Object.fromEntries(Object.entries(result).sort(([a], [b]) => a.localeCompare(b)));
}

function canonical(value) { return JSON.stringify(value, Object.keys(value).sort()); }

export function expectedRows(batches, options) {
  if (!Array.isArray(batches) || !options?.service || !options?.scenarios || !/^[1-9][0-9]*$/.test(String(options.repeat))) fail('Invalid evidence options');
  const all = [];
  for (const batch of batches) {
    if (batch.resourceMetrics || batch.resourceLogs) fail('Only traces are allowed');
    for (const resource of batch.resourceSpans ?? []) {
      const resourceMap = attributes(resource.resource?.attributes);
      if (resourceMap['service.name'] !== options.service) continue;
      for (const scope of resource.scopeSpans ?? []) {
        if (scope.scope?.name !== 'com.microsoft.sqlserver.jdbc') fail('Unexpected instrumentation scope');
        for (const span of scope.spans ?? []) {
          if (!validId(span.traceId, 32) || !validId(span.spanId, 16)) fail('Invalid span identity');
          const parent = span.parentSpanId ?? '';
          if (parent && !/^0{16}$/.test(parent) && !validId(parent, 16)) fail('Invalid parent identity');
          const start = BigInt(span.startTimeUnixNano);
          const end = BigInt(span.endTimeUnixNano);
          if (end < start) fail('Invalid span duration');
          const attrs = attributes(span.attributes);
          if (span.status?.message) fail('Status free text is forbidden');
          all.push({
            application: options.service, trace_id: span.traceId.toLowerCase(), span_id: span.spanId.toLowerCase(),
            parent_span_id: !parent || /^0{16}$/.test(parent) ? '' : parent.toLowerCase(), name: span.name,
            status_code: Number(span.status?.code ?? 0),
            attributes: JSON.stringify(Object.fromEntries(
              Object.entries(attrs).map(([key, value]) => [key, String(value)]))),
            start_us: String(start / 1000n), duration_us: String((end - start) / 1000n)
          });
        }
      }
    }
  }
  if (!all.length) fail('No matching trace evidence');
  const ids = new Set(all.map(row => `${row.trace_id}/${row.span_id}`));
  for (const row of all) if (row.parent_span_id && !ids.has(`${row.trace_id}/${row.parent_span_id}`)) fail('Orphaned span');
  return all.sort((a, b) => a.trace_id.localeCompare(b.trace_id) || a.start_us.localeCompare(b.start_us) || a.span_id.localeCompare(b.span_id));
}

export function compare(expected, actual) {
  if (!Array.isArray(expected) || !Array.isArray(actual?.rows) || !Array.isArray(actual?.unexpected)) fail('Invalid comparison payload');
  if (actual.unexpected.length) fail('Unexpected telemetry tables found');
  if (actual.rows.length > expected.length) fail('Unexpected telemetry rows found');
  if (actual.rows.length < expected.length) return false;
  const normalize = rows => rows.map(row => {
    const copy = {};
    for (const key of ['application', 'trace_id', 'span_id', 'parent_span_id', 'name', 'status_code', 'attributes', 'start_us', 'duration_us']) {
      if (!own(row, key)) fail(`Missing ${key}`);
      copy[key] = key === 'status_code' ? Number(row[key]) : String(row[key]);
    }
    try { copy.attributes = JSON.stringify(JSON.parse(copy.attributes)); } catch { fail('Invalid attributes'); }
    return copy;
  }).sort((a, b) => a.trace_id.localeCompare(b.trace_id) || a.start_us.localeCompare(b.start_us) || a.span_id.localeCompare(b.span_id));
  const left = normalize(expected); const right = normalize(actual.rows);
  for (let i = 0; i < left.length; i++) {
    for (const key of Object.keys(left[i])) if (left[i][key] !== right[i][key]) fail(`Mismatch in ${key}`);
  }
  return true;
}

export async function poll(fn, maxMs, intervalMs) {
  if (typeof fn !== 'function' || !Number.isFinite(maxMs) || maxMs <= 0 || !Number.isFinite(intervalMs) || intervalMs < 0) fail('Invalid polling bounds');
  const deadline = Date.now() + maxMs;
  while (true) {
    if (await fn()) return;
    if (Date.now() >= deadline) fail('Polling deadline exceeded');
    await new Promise(resolveDelay => setTimeout(resolveDelay, Math.min(intervalMs, Math.max(0, deadline - Date.now()))));
  }
}

function target(refId, query, format = 'table') {
  return { refId, scenarioId: 'raw_frame', datasource: { type: 'grafana-azure-data-explorer-datasource', uid: 'adx-jdbc-errors' }, queryType: 'KQL', querySource: 'raw', rawMode: true, resultFormat: format, query };
}

function attributeValue() {
  return `case(type == 1, str, type == 2, tostring(['int']), type == 3, tostring(['double']), type == 4, tolower(tostring(['bool'])), type == 5, bytes, ser)`;
}

function rootAttributes(service) {
  const escaped = service.replaceAll("'", "''");
  return `let RootAttrs=span_attrs | where application == '${escaped}' | extend attribute_value=${attributeValue()} | summarize connection_guid=anyif(attribute_value,key=='mssql.connection.guid'), failure_phase=anyif(attribute_value,key=='mssql.connection.failure_phase'), error_category=anyif(attribute_value,key=='mssql.error.category'), error_type=anyif(attribute_value,key=='error.type'), outcome=anyif(attribute_value,key=='mssql.connection.outcome'), attempt_count=anyif(attribute_value,key=='mssql.connection.attempt_count'), retry_count=anyif(attribute_value,key=='mssql.connection.retry_count'), redirect_count=anyif(attribute_value,key=='mssql.connection.redirect_count'), budget_exhausted=anyif(attribute_value,key=='mssql.connection.budget_exhausted') by parent_id, export_time_unix_nano;`;
}

function rootConnections(service) {
  const escaped = service.replaceAll("'", "''");
  return `${rootAttributes(service)} spans | where application == '${escaped}' and name == 'mssql.driver.connection.open' | join kind=inner RootAttrs on $left.id==$right.parent_id, $left.export_time_unix_nano==$right.export_time_unix_nano`;
}

function queryManifest(service) {
  const joined = rootConnections(service);
  return Object.freeze({
    failuresByPhase: `${joined} | summarize failures=count() by failure_phase | order by failures desc`,
    failuresByCategory: `${joined} | summarize failures=count() by error_category | order by failures desc`,
    failedConnectionDuration: `${joined} | project start_time_unix_nano, duration_ms=toreal(duration_time_unix_nano)/1000.0, failure_phase | order by start_time_unix_nano asc`,
    individualConnections: `${joined} | project start_time_unix_nano, connection_guid, failure_phase, error_category, error_type, duration_ms=toreal(duration_time_unix_nano)/1000.0, attempt_count, retry_count, budget_exhausted | order by start_time_unix_nano desc | take 500`
  });
}

function drilldownQueries(service) {
  const escaped = service.replaceAll("'", "''");
  const filters = `| where ('\${failure_phase:raw}' == '*' or failure_phase == '\${failure_phase:raw}') and ('\${error_category:raw}' == '*' or error_category == '\${error_category:raw}') and ('\${error_type:raw}' == '*' or error_type == '\${error_type:raw}') and ('\${connection_guid:raw}' == '*' or connection_guid == '\${connection_guid:raw}')`;
  const selectedRoots = `${rootConnections(service)} ${filters}`;
  const selectedSpans = `${selectedRoots} | project selected_trace_id=trace_id, selected_connection_guid=connection_guid | join kind=inner (spans | where application == '${escaped}' | project child_id=id, child_trace_id=trace_id, child_span_id=span_id, child_parent_span_id=parent_span_id, child_start=start_time_unix_nano, child_duration=duration_time_unix_nano, child_name=name, child_status=status_code, child_export=export_time_unix_nano) on $left.selected_trace_id == $right.child_trace_id`;
  return {
    failuresByPhase: `${selectedRoots} | summarize failures=count() by failure_phase | order by failures desc`,
    failuresByCategory: `${selectedRoots} | summarize failures=count() by error_category | order by failures desc`,
    failedConnectionDuration: `${selectedRoots} | project start_time_unix_nano, duration_ms=toreal(duration_time_unix_nano)/1000.0, failure_phase | order by start_time_unix_nano asc`,
    individualConnections: `${selectedRoots} | project start_time_unix_nano, connection_guid, failure_phase, error_category, error_type, outcome, duration_ms=round(toreal(duration_time_unix_nano)/1000.0,2), attempt_count, retry_count, redirect_count, budget_exhausted, trace_id, span_id | order by start_time_unix_nano desc | take 500`,
    selectedAttributes: `${selectedRoots} | project selected_id=id, selected_export=export_time_unix_nano, connection_guid | join kind=inner (span_attrs | where application == '${escaped}' | extend value=${attributeValue()} | project attr_parent_id=parent_id, attr_export=export_time_unix_nano, attribute=key, value, otap_type=type) on $left.selected_id == $right.attr_parent_id, $left.selected_export == $right.attr_export | project connection_guid, attribute, value, otap_type | order by attribute asc | take 2000`,
    selectedSpanTree: `${selectedSpans} | project connection_guid=selected_connection_guid, start_time_unix_nano=child_start, span_name=child_name, duration_ms=round(toreal(child_duration)/1000.0,2), status_code=child_status, span_id=child_span_id, parent_span_id=child_parent_span_id | order by start_time_unix_nano asc | take 2000`,
    selectedEvents: `${selectedSpans} | project selected_connection_guid, child_id, child_span_name=child_name, child_export | join kind=inner (span_events | where application == '${escaped}' | project event_id=id, event_parent_id=parent_id, event_time=time_unix_nano, event_name=name, event_export=export_time_unix_nano) on $left.child_id == $right.event_parent_id, $left.child_export == $right.event_export | project connection_guid=selected_connection_guid, event_time, span_name=child_span_name, event_name, event_id, event_export | join kind=leftouter (span_event_attrs | where application == '${escaped}' | extend value=${attributeValue()} | project attr_event_id=parent_id, attr_event_export=export_time_unix_nano, key, value) on $left.event_id == $right.attr_event_id, $left.event_export == $right.attr_event_export | summarize attributes=make_bag(pack(key,value)) by connection_guid, event_time, span_name, event_name | project connection_guid, event_time, span_name, event_name, attributes=tostring(attributes) | order by event_time asc | take 2000`
  };
}

function variable(name, label, query, dependencies = []) {
  return {
    name, label, type: 'query', datasource: { type: 'grafana-azure-data-explorer-datasource', uid: 'adx-jdbc-errors' },
    query, definition: query, refresh: 1, sort: 1, multi: false, includeAll: true, allValue: '*',
    current: { selected: true, text: 'All', value: '$__all' }, options: [], skipUrlSync: false,
    regex: '', hide: 0, description: dependencies.length ? `Filtered by ${dependencies.join(', ')}` : ''
  };
}

function connectionTableOverrides() {
  return [{
    matcher: { id: 'byName', options: 'connection_guid' },
    properties: [{ id: 'links', value: [{
      title: 'Drill into this failed connection',
      url: '/d/jdbc-connection-errors/jdbc-connection-error-telemetry?orgId=1&from=now-24h&to=now&timezone=browser&var-failure_phase=*&var-error_category=*&var-error_type=*&var-connection_guid=\${__value.raw}'
    }] }]
  }];
}

export function dashboard(_rows, service) {
  if (typeof service !== 'string' || !service) fail('Dashboard service is required');
  const queries = drilldownQueries(service);
  const roots = rootConnections(service);
  const variableFilter = `| where ('\${failure_phase:raw}' == '*' or failure_phase == '\${failure_phase:raw}') and ('\${error_category:raw}' == '*' or error_category == '\${error_category:raw}') and ('\${error_type:raw}' == '*' or error_type == '\${error_type:raw}')`;
  return {
    id: null, uid: 'jdbc-connection-errors', title: 'JDBC connection error telemetry', tags: ['jdbc', 'opentelemetry', 'connection-errors'], timezone: 'browser', schemaVersion: 41, version: 3, refresh: '30s', time: { from: 'now-24h', to: 'now' },
    templating: { list: [
      variable('failure_phase', 'Failure phase', `${roots} | distinct failure_phase | order by failure_phase asc`),
      variable('error_category', 'Error category', `${roots} | where ('\${failure_phase:raw}' == '*' or failure_phase == '\${failure_phase:raw}') | distinct error_category | order by error_category asc`, ['failure phase']),
      variable('error_type', 'Error type', `${roots} | where ('\${failure_phase:raw}' == '*' or failure_phase == '\${failure_phase:raw}') and ('\${error_category:raw}' == '*' or error_category == '\${error_category:raw}') | distinct error_type | order by error_type asc`, ['phase', 'category']),
      variable('connection_guid', 'Connection GUID', `${roots} ${variableFilter} | distinct connection_guid | order by connection_guid asc`, ['phase', 'category', 'error type'])
    ] },
    panels: [
      { id: 1, title: 'Failures by phase', type: 'barchart', gridPos: { x: 0, y: 0, w: 8, h: 8 }, targets: [target('A', queries.failuresByPhase)] },
      { id: 2, title: 'Failures by category', type: 'piechart', gridPos: { x: 8, y: 0, w: 8, h: 8 }, targets: [target('A', queries.failuresByCategory)] },
      { id: 3, title: 'Failed connection duration', type: 'timeseries', gridPos: { x: 16, y: 0, w: 8, h: 8 }, targets: [target('A', queries.failedConnectionDuration, 'time_series')] },
      { id: 4, title: 'Failed connections — click a GUID to drill down', type: 'table', gridPos: { x: 0, y: 8, w: 24, h: 10 }, fieldConfig: { defaults: {}, overrides: connectionTableOverrides() }, options: { showHeader: true, cellHeight: 'sm' }, targets: [target('A', queries.individualConnections)] },
      { id: 5, title: 'Selected connection — all root span attributes', description: 'Select or click one Connection GUID. Sensitive payload fields are intentionally never collected.', type: 'table', gridPos: { x: 0, y: 18, w: 12, h: 12 }, options: { showHeader: true, cellHeight: 'sm' }, targets: [target('A', queries.selectedAttributes)] },
      { id: 6, title: 'Selected connection — complete span tree', type: 'table', gridPos: { x: 12, y: 18, w: 12, h: 12 }, options: { showHeader: true, cellHeight: 'sm' }, targets: [target('A', queries.selectedSpanTree)] },
      { id: 7, title: 'Selected connection — sanitized errors and retry decisions', type: 'table', gridPos: { x: 0, y: 30, w: 24, h: 12 }, options: { showHeader: true, cellHeight: 'sm' }, targets: [target('A', queries.selectedEvents)] }
    ]
  };
}

function yamlScalar(value) {
  if (value === null) return 'null';
  if (typeof value === 'boolean' || typeof value === 'number') return String(value);
  return JSON.stringify(String(value));
}
function toYaml(value, indent = 0) {
  const pad = ' '.repeat(indent);
  if (Array.isArray(value)) return value.map(item => typeof item === 'object' ? `${pad}-\n${toYaml(item, indent + 2)}` : `${pad}- ${yamlScalar(item)}`).join('\n');
  return Object.entries(value).map(([key, item]) => {
    const safeKey = /^[A-Za-z_][A-Za-z0-9_/-]*$/.test(key) ? key : JSON.stringify(key);
    if (item && typeof item === 'object') return `${pad}${safeKey}:\n${toYaml(item, indent + 2)}`;
    return `${pad}${safeKey}: ${yamlScalar(item)}`;
  }).join('\n');
}

function cli() {
  const [command, envPath, outputPath, actualPath] = process.argv.slice(2);
  if (!['config', 'generate', 'expected', 'compare'].includes(command) || !envPath) fail('Usage: cloud-e2e.mjs config|generate|expected|compare ENV_FILE OUTPUT [ACTUAL]');
  const config = configure(parseEnv(readFileSync(resolve(envPath), 'utf8')));
  if (command === 'config') {
    process.stdout.write(JSON.stringify({ run: config.run, owner: config.owner, project: config.project, service: config.service, cluster: config.cluster, database: config.database, rootPath: config.rootPath, consumerGroup: config.consumerGroup, customerUi: 'http://127.0.0.1:3001' }, null, 2) + '\n');
    return;
  }
  if (!outputPath) fail('Output directory is required');
  if (command === 'expected') {
    const batches = readFileSync(resolve(outputPath), 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    process.stdout.write(JSON.stringify(expectedRows(batches, { service: config.service, scenarios: config.env.DEMO_SCENARIOS || 'config,dns,login,success', repeat: config.env.DEMO_REPEAT || '1' })) + '\n');
    return;
  }
  if (command === 'compare') {
    if (!actualPath) fail('Actual snapshot is required');
    const expected = JSON.parse(readFileSync(resolve(outputPath), 'utf8'));
    const actual = JSON.parse(readFileSync(resolve(actualPath), 'utf8'));
    if (!compare(expected, actual)) fail('Cloud snapshot is incomplete');
    process.stdout.write(JSON.stringify({ status: 'verified', rows: expected.length }) + '\n');
    return;
  }
  const directory = resolve(outputPath); mkdirSync(directory, { recursive: true, mode: 0o700 });
  const writeArtifact = (name, contents) => {
    const path = resolve(directory, name);
    writeFileSync(path, contents, { mode: 0o644 });
    chmodSync(path, 0o644);
  };
  writeArtifact('config.json', JSON.stringify({
    run: config.run,
    owner: config.owner,
    project: config.project,
    service: config.service,
    subscription: config.env.AZURE_SUBSCRIPTION_ID,
    eventHubResourceGroup: config.env.EVENT_HUB_RESOURCE_GROUP,
    eventHubNamespace: config.env.EVENT_HUB_NAMESPACE,
    eventHubName: config.env.EVENT_HUB_NAME,
    consumerGroup: config.consumerGroup,
    storageAccount: config.env.STORAGE_ACCOUNT,
    storageContainer: config.env.STORAGE_CONTAINER,
    rootPath: config.rootPath,
    checkpointStorageAccount: config.env.CHECKPOINT_STORAGE_ACCOUNT,
    checkpointStorageContainer: config.env.CHECKPOINT_STORAGE_CONTAINER,
    cluster: config.cluster,
    database: config.database,
    scenarioCounts: config.counts
  }, null, 2) + '\n');
  // These generated files contain no credentials. The collector, Grafana and
  // verifier deliberately run as different non-root UIDs and need read access.
  writeArtifact('otelcol.yaml', toYaml(cloudCollector()) + '\n');
  writeArtifact('evidence.yaml', toYaml(evidenceCollector()) + '\n');
  writeArtifact('dashboard.json', JSON.stringify(dashboard([], config.service), null, 2) + '\n');
  writeArtifact('queries.json', JSON.stringify(queryManifest(config.service), null, 2) + '\n');
  writeArtifact('runtime.env', [
    `COMPOSE_PROJECT_NAME=${config.project}`,
    `CLOUD_SERVICE_NAME=${config.service}`,
    `CLOUD_ARTIFACT_DIR=${directory}`,
    `OTEL_SERVICE_NAME=${config.service}`,
    'OTEL_AUTH_MODE=none',
    'OTEL_ALLOW_INSECURE_DEVELOPMENT_ENDPOINT=true',
    'OTEL_EXPORTER_OTLP_ENDPOINT=http://otelcol:4318',
    `DEMO_SCENARIOS=${config.env.DEMO_SCENARIOS || 'config,dns,login,success'}`,
    `DEMO_REPEAT=${config.env.DEMO_REPEAT || '1'}`,
    `DEMO_PAUSE_SECONDS=${config.env.DEMO_PAUSE_SECONDS || '0'}`,
    `DEMO_CONFIG_COUNT=${config.counts.config}`,
    `DEMO_DNS_COUNT=${config.counts.dns}`,
    `DEMO_LOGIN_COUNT=${config.counts.login}`,
    `DEMO_SUCCESS_COUNT=${config.counts.success}`,
    `DEMO_EXPECTED_FAILURE_COUNTS=configuration:${config.counts.config},dns:${config.counts.dns},login:${config.counts.login}`,
    ''
  ].join('\n'));
  process.stdout.write(JSON.stringify({ status: 'generated', directory }) + '\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { cli(); } catch { console.error('Cloud E2E configuration rejected.'); process.exitCode = 1; }
}
