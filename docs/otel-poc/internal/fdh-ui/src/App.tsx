import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Button,
  Input,
  Select,
  Tab,
  TabList,
  Tooltip,
} from '@fluentui/react-components';
import {
  AlertRegular,
  ArrowClockwiseRegular,
  ChevronRightRegular,
  DatabaseRegular,
  DismissRegular,
  FilterRegular,
  HomeRegular,
  LineHorizontal3Regular,
  MoreHorizontalRegular,
  SearchRegular,
  SettingsRegular,
  ShieldErrorRegular,
  WeatherMoonRegular,
} from '@fluentui/react-icons';
import type {
  ConnectionFailure,
  FailurePhase,
  PerformanceMetric,
  StatementFailure,
  ThroughputPoint,
  OutcomeDistribution,
  RetryMetric,
  ErrorEvent,
} from './types';
import { emptyTelemetry, loadTelemetry } from './live';

const formatNumber = (value: number | null | undefined, digits = 1) =>
  value == null || !Number.isFinite(value) ? '—' : value.toLocaleString(undefined, { maximumFractionDigits: digits });
const milliseconds = (value: number | null | undefined) => value == null ? '—' : `${formatNumber(value)} ms`;
const utcTime = (time: string) => new Date(time).toLocaleTimeString('en-GB', { timeZone: 'UTC', hour12: false });
const statementLabel = (kind: string) => ({ statement: 'Statement', prepared_statement: 'PreparedStatement', callable_statement: 'CallableStatement' } as Record<string, string>)[kind] ?? kind;

const phaseColor: Record<FailurePhase, string> = {
  DNS: '#117865',
  Configuration: '#5b5fc7',
  Login: '#c239b3',
};

const statementCategoryColor: Record<string, string> = {
  'Constraint violation': '#c50f1f',
  'Query syntax & semantics': '#5b5fc7',
};

function Logo() {
  return (
    <div className="logo-mark" aria-label="FDH">
      <span />
      <span />
      <span />
    </div>
  );
}

function MetricCard({ label, value, delta, tone }: { label: string; value: string; delta: string; tone: string }) {
  return (
    <div className="metric-card">
      <div className="metric-accent" style={{ background: tone }} />
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      <div className="metric-delta">{delta}</div>
    </div>
  );
}

function Distribution({ data }: { data: ConnectionFailure[] }) {
  const counts = [...new Set(data.map(item => item.phase))].map(phase => ({
    phase,
    count: data.filter(item => item.phase === phase).length,
  }));
  const top = [...counts].sort((a, b) => b.count - a.count)[0];
  const max = Math.max(...counts.map(item => item.count), 1);
  return (
    <div className="panel distribution-panel">
      <div className="panel-title-row">
        <div>
          <h3>Failures by connection phase</h3>
          <p>Failed opens grouped by the terminal lifecycle phase</p>
        </div>
        <Button appearance="subtle" icon={<MoreHorizontalRegular />} />
      </div>
      <div className="distribution-content">
        <div className="bars" aria-label="Failure distribution">
          {counts.map(item => (
            <div className="bar-group" key={item.phase}>
              <div className="bar-value">{item.count}</div>
              <div className="bar-track">
                <div className="bar-fill" style={{ height: `${(item.count / max) * 100}%`, background: phaseColor[item.phase] ?? '#0f6cbd' }} />
              </div>
              <div className="bar-label">{item.phase}</div>
            </div>
          ))}
        </div>
        <div className="insight-card">
          <span className="insight-kicker">Top issue</span>
          <strong>{top?.phase ?? 'No failures'}</strong>
          <span>{top ? `${Math.round((top.count / data.length) * 100)}% of filtered failures` : 'No matching traces'}</span>
          <div className="mini-rule" />
          <span className="insight-note">Local Jaeger traces</span>
        </div>
      </div>
    </div>
  );
}

function Activity({ data }: { data: ConnectionFailure[] }) {
  const recent = data.slice(0, 32).reverse();
  const maxDuration = Math.max(...recent.map(item => item.duration), 1);
  const points = recent.map((item, index) => ({
    x: index * (560 / Math.max(recent.length - 1, 1)),
    y: 112 - (item.duration / maxDuration) * 90,
  }));
  const line = points.map(point => `${point.x},${point.y}`).join(' ');
  return (
    <div className="panel activity-panel">
      <div className="panel-title-row">
        <div>
          <h3>Connection failure duration</h3>
          <p>End-to-end latency for recent failed connection attempts</p>
        </div>
        <span className="period-chip">0–{formatNumber(maxDuration)} ms</span>
      </div>
      <svg className="activity-chart" viewBox="0 0 560 130" preserveAspectRatio="none" role="img" aria-label="Connection duration chart">
        <defs>
          <linearGradient id="area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="#5b5fc7" stopOpacity=".28" />
            <stop offset="1" stopColor="#5b5fc7" stopOpacity="0" />
          </linearGradient>
        </defs>
        {[25, 65, 105].map(y => <line key={y} x1="0" x2="560" y1={y} y2={y} className="chart-grid" />)}
        <polygon points={`0,120 ${line} 560,120`} fill="url(#area)" />
        <polyline points={line} fill="none" stroke="#5b5fc7" strokeWidth="2.3" />
        {points.map((point, index) => <circle key={index} cx={point.x} cy={point.y} r="3" fill="#5b5fc7"><title>{milliseconds(recent[index].duration)}</title></circle>)}
      </svg>
      <div className="chart-axis"><span>Earlier</span><span>Recent</span></div>
    </div>
  );
}

function StatementDistribution({ data }: { data: StatementFailure[] }) {
  const categories = [...new Set(data.map(item => item.category))];
  const counts = categories.map(category => ({
    category,
    count: data.filter(item => item.category === category).length,
  }));
  const top = [...counts].sort((a, b) => b.count - a.count)[0];
  const max = Math.max(...counts.map(item => item.count), 1);
  return (
    <div className="panel distribution-panel statement-distribution-panel">
      <div className="panel-title-row">
        <div>
          <h3>Statement failures by category</h3>
          <p>Failed Statement and PreparedStatement executions grouped by driver category</p>
        </div>
        <Button appearance="subtle" icon={<MoreHorizontalRegular />} />
      </div>
      <div className="distribution-content">
        <div className="bars statement-bars" aria-label="Statement error category distribution">
          {counts.map(item => (
            <div className="bar-group" key={item.category}>
              <div className="bar-value">{item.count}</div>
              <div className="bar-track">
                <div
                  className="bar-fill"
                  style={{
                    height: `${Math.max((item.count / max) * 100, 4)}%`,
                    background: statementCategoryColor[item.category] ?? '#5b5fc7',
                  }}
                />
              </div>
              <div className="bar-label">{item.category}</div>
            </div>
          ))}
        </div>
        <div className="insight-card statement-insight-card">
          <span className="insight-kicker">Top category</span>
          <strong>{top?.category ?? 'No failures'}</strong>
          <span>{top ? `${Math.round((top.count / data.length) * 100)}% of statement failures` : 'No matching traces'}</span>
          <div className="mini-rule" />
          <span className="insight-note">Stable, message-independent classification</span>
        </div>
      </div>
    </div>
  );
}

function StatementActivity({ data }: { data: StatementFailure[] }) {
  const recent = data.slice(0, 32).reverse();
  const maxDuration = Math.max(...recent.map(item => item.duration), 1);
  const points = recent.map((item, index) => ({
    x: index * (560 / Math.max(recent.length - 1, 1)),
    y: 112 - (item.duration / maxDuration) * 90,
  }));
  const line = points.map(point => `${point.x},${point.y}`).join(' ');
  return (
    <div className="panel activity-panel">
      <div className="panel-title-row">
        <div>
          <h3>Statement failure duration</h3>
          <p>End-to-end latency for recent failed SQL executions</p>
        </div>
        <span className="period-chip">0–{formatNumber(maxDuration)} ms</span>
      </div>
      <svg className="activity-chart" viewBox="0 0 560 130" preserveAspectRatio="none" role="img" aria-label="Statement failure duration chart">
        <defs>
          <linearGradient id="statement-area" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0" stopColor="#c50f1f" stopOpacity=".22" />
            <stop offset="1" stopColor="#c50f1f" stopOpacity="0" />
          </linearGradient>
        </defs>
        {[25, 65, 105].map(y => <line key={y} x1="0" x2="560" y1={y} y2={y} className="chart-grid" />)}
        <polygon points={`0,120 ${line} 560,120`} fill="url(#statement-area)" />
        <polyline points={line} fill="none" stroke="#c50f1f" strokeWidth="2.3" />
        {points.map((point, index) => <circle key={index} cx={point.x} cy={point.y} r="3" fill="#c50f1f"><title>{milliseconds(recent[index].duration)}</title></circle>)}
      </svg>
      <div className="chart-axis"><span>Earlier</span><span>Recent</span></div>
    </div>
  );
}

function PerformanceThroughput({ throughputPoints }: { throughputPoints: ThroughputPoint[] }) {
  const max = Math.max(...throughputPoints.flatMap(point => [point.statements ?? 0, point.connections ?? 0]), 1);
  const path = (kind: 'statements' | 'connections') => {
    let gap = true;
    return throughputPoints.map((point, index) => {
      const value = point[kind];
      if (value === null) { gap = true; return ''; }
      const command = gap ? 'M' : 'L';
      gap = false;
      return `${command}${index * 560 / Math.max(throughputPoints.length - 1, 1)},${112 - value / max * 82}`;
    }).join(' ');
  };
  return (
    <div className="panel activity-panel metrics-throughput-panel">
      <div className="panel-title-row">
        <div><h3>Operation throughput</h3><p>All successful and failed root operations per minute</p></div>
        <div className="chart-legend"><span><i className="legend-statements" />Statements</span><span><i className="legend-connections" />Connections</span></div>
      </div>
      <svg className="activity-chart" viewBox="0 0 560 130" preserveAspectRatio="none" role="img" aria-label="Connection and statement throughput chart">
        {[25, 65, 105].map(y => <line key={y} x1="0" x2="560" y1={y} y2={y} className="chart-grid" />)}
        <path d={path('statements')} fill="none" stroke="#5b5fc7" strokeWidth="2.4" />
        <path d={path('connections')} fill="none" stroke="#117865" strokeWidth="2.4" />
      </svg>
      <div className="chart-axis"><span>Last 24 hours · 5-minute rate · shared scale 0–{formatNumber(max)} / min</span><span>Recent</span></div>
    </div>
  );
}

function ActivityLatency({ title, subtitle, metrics, tone }: {
  title: string;
  subtitle: string;
  metrics: PerformanceMetric[];
  tone: string;
}) {
  const max = Math.max(...metrics.map(metric => metric.p95 ?? 0), 1);
  return (
    <div className="panel metrics-latency-panel">
      <div className="panel-title-row"><div><h3>{title}</h3><p>{subtitle}</p></div><span className="period-chip">p95 · ms</span></div>
      <div className="latency-bars">
        {metrics.map(metric => (
          <div className="latency-row" key={metric.activity}>
            <span>{metric.label}</span>
            <div className="latency-track"><i style={{ width: `${((metric.p95 ?? 0) / max) * 100}%`, background: tone }} /></div>
            <strong>{formatNumber(metric.p95)}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function EventDetails({ event }: { event: ErrorEvent }) {
  return <><strong>{event.name}</strong><p>{event.phase} · {event.source}</p>
    <div className="event-tags">{Object.entries(event.attributes ?? {}).map(([key, value]) => <span key={key}>{key}: {value}</span>)}</div></>;
}

function OutcomeChart({ data }: { data: OutcomeDistribution[] }) {
  const keys = ['success', 'failure', 'timeout', 'canceled'] as const;
  const tones = ['#117865', '#c50f1f', '#c239b3', '#5b5fc7'];
  const max = Math.max(1, ...data.flatMap(item => keys.map(key => item[key] ?? 0)));
  return <div className="panel metrics-latency-panel outcome-panel">
    <div className="panel-title-row"><div><h3>Root operation outcomes</h3><p>24h count increases by actual outcome · — means not observed</p></div></div>
    <div className="latency-bars">{data.flatMap(item => keys.map((key, index) => <div className="latency-row" key={`${item.activity}-${key}`}>
      <span>{item.activity} · {key}</span><div className="latency-track"><i style={{ width: `${(item[key] ?? 0) / max * 100}%`, background: tones[index] }} /></div>
      <strong>{formatNumber(item[key])}</strong>
    </div>))}{!data.length && <p>No root outcome counts observed.</p>}</div>
  </div>;
}

function PerformanceMetricsDashboard({ performanceMetrics, throughputPoints, outcomeDistribution, retries }: {
  performanceMetrics: PerformanceMetric[];
  throughputPoints: ThroughputPoint[];
  outcomeDistribution: OutcomeDistribution[];
  retries: RetryMetric[];
}) {
  const connectionRoot = performanceMetrics.find(metric => metric.activity === 'connection.open');
  const statementRoot = performanceMetrics.find(metric => metric.activity === 'statement.execute');
  const connectionStages = performanceMetrics.filter(metric => metric.kind === 'Connection' && !metric.activity.endsWith('.open'));
  const statementStages = performanceMetrics.filter(metric => metric.kind === 'Statement'
    && !['statement.execute', 'statement.attempt'].includes(metric.activity));
  const current = throughputPoints.at(-1);
  return (
    <section className="performance-dashboard">
      <div className="statement-analysis-header performance-analysis-header">
        <div><h2>Performance metrics</h2><p>SDK-aggregated metrics from existing lifecycle END events—not only failures.</p></div>
        <span className="period-chip">Live Prometheus · last 24 hours</span>
      </div>
      <section className="metrics performance-metrics">
        <MetricCard label="Connection opens / min" value={formatNumber(current?.connections)} delta={`${formatNumber(connectionRoot?.successRate)}% successful · 24h`} tone="#117865" />
        <MetricCard label="Statement executions / min" value={formatNumber(current?.statements)} delta={`${formatNumber(statementRoot?.successRate)}% successful · 24h`} tone="#5b5fc7" />
        <MetricCard label="Connection p95" value={milliseconds(connectionRoot?.p95)} delta="Physical open latency · estimate" tone="#c239b3" />
        <MetricCard label="Statement p95" value={milliseconds(statementRoot?.p95)} delta="JDBC execute invocation · estimate" tone="#c50f1f" />
      </section>
      <section className="charts-grid performance-top-grid">
        <PerformanceThroughput throughputPoints={throughputPoints} />
        <ActivityLatency title="Connection lifecycle latency" subtitle="Independent p95 for each measured phase" metrics={connectionStages} tone="#117865" />
      </section>
      <section className="charts-grid performance-bottom-grid">
        <ActivityLatency title="Statement pipeline latency" subtitle="Independent p95; nested phase durations are not added" metrics={statementStages} tone="#5b5fc7" />
        <OutcomeChart data={outcomeDistribution} />
      </section>
      <section className="charts-grid performance-bottom-grid">
        <div className="panel metrics-latency-panel retry-panel">
          <div className="panel-title-row"><div><h3>Additional attempts begun</h3><p>Root retry counter · 24h increases, not retry decisions</p></div></div>
          <table className="data-table"><thead><tr><th>Activity</th><th>Reason</th><th>Retries</th></tr></thead>
            <tbody>{retries.map(item => <tr key={`${item.activity}-${item.reason}`}><td>{item.activity}</td><td>{item.reason}</td><td>{formatNumber(item.count)}</td></tr>)}</tbody></table>
          {!retries.length && <p>No retry series observed; this does not establish zero retries.</p>}
        </div>
        <div className="panel metric-principles-panel">
          <div className="panel-title-row"><div><h3>Metric signal contract</h3><p>Existing END boundaries and bounded dimensions</p></div></div>
          <div className="principle-list">
            <div><strong>All operations</strong><span>Successes, failures, timeouts, and cancellations</span></div>
            <div><strong>OpenTelemetry SDK aggregation</strong><span>One count and duration observation per completed activity</span></div>
            <div><strong>Bounded cardinality</strong><span>No SQL, IDs, server names, exception text, or trace context</span></div>
            <div><strong>Failure diagnostics stay separate</strong><span>Only failed root traces are displayed above</span></div>
            <div><strong>Exact maximum unavailable</strong><span>Classic histogram buckets and sums cannot recover the largest observation</span></div>
          </div>
        </div>
      </section>
      <section className="panel table-panel aggregate-metrics-panel">
        <div className="panel-title-row table-title-row">
          <div><h3>Performance activity aggregates</h3><p>Counts and percentiles calculated independently for each activity.</p></div>
          <span className="period-chip">Last 24 hours · histogram estimates</span>
        </div>
        <div className="data-table-wrap aggregate-table-wrap">
          <table className="data-table aggregate-table">
            <thead><tr><th>Activity</th><th>Kind</th><th>Count</th><th>Success rate</th><th>Errors</th><th>Mean</th><th>p50</th><th>p95</th><th>p99</th><th>Max</th></tr></thead>
            <tbody>{performanceMetrics.map(metric => (
              <tr key={metric.activity}>
                <td><code className="metric-activity">{metric.activity}</code></td><td>{metric.kind}</td><td>{formatNumber(metric.count)}</td>
                <td>{metric.successRate === null ? '—' : `${formatNumber(metric.successRate, 3)}%`}</td><td>{formatNumber(metric.errors)}</td>
                <td title="Histogram sum / observed count; never a sum of nested phase durations">{milliseconds(metric.meanDurationMs)}</td><td>{milliseconds(metric.p50)}</td><td><strong>{milliseconds(metric.p95)}</strong></td><td>{milliseconds(metric.p99)}</td><td title="Exact maximum is not preserved by classic histogram buckets or sums">{milliseconds(metric.max)}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        <div className="table-footer"><span>{performanceMetrics.length} bounded activity series</span><span>Counter increases · includes successes · — = unavailable</span></div>
      </section>
    </section>
  );
}

function SpanWaterfall({ connection }: { connection: Pick<ConnectionFailure, 'duration' | 'spans'> }) {
  const total = Math.max(connection.duration, 1);
  return (
    <div className="span-list">
      <div className="span-scale"><span>0 ms</span><span>{Math.round(total / 2)} ms</span><span>{total} ms</span></div>
      {connection.spans.map((span, index) => (
        <div className="span-row" key={`${span.name}-${index}`}>
          <div className="span-name" style={{ paddingLeft: `${span.depth * 14}px` }}>
            <span className={`status-dot ${span.status.toLowerCase()}`} />
            <span>{span.name.replace(/^mssql\.driver\.(?:connection|statement)\./, '')}</span>
          </div>
          <div className="waterfall">
            <div
              className={`waterfall-bar ${span.status.toLowerCase()}`}
              style={{ left: `${(span.start / total) * 100}%`, width: `${Math.max((span.duration / total) * 100, 1.5)}%` }}
            />
          </div>
          <div className="span-duration">{span.duration.toFixed(1)} ms</div>
        </div>
      ))}
    </div>
  );
}

function DetailDrawer({ connection, onClose }: { connection: ConnectionFailure; onClose: () => void }) {
  const [tab, setTab] = useState('trace');
  return (
    <div className="drawer-backdrop" onMouseDown={event => event.currentTarget === event.target && onClose()}>
      <aside className="detail-drawer">
        <header className="drawer-header">
          <div>
            <div className="eyebrow">Failed connection</div>
            <h2>{connection.phase} failure</h2>
            <code>{connection.id}</code>
          </div>
          <Button appearance="subtle" icon={<DismissRegular />} onClick={onClose} aria-label="Close details" />
        </header>
        <div className="drawer-summary">
          <div><span>Status</span><strong className="error-text">Error</strong></div>
          <div><span>Duration</span><strong>{connection.duration} ms</strong></div>
          <div><span>Attempts</span><strong>{connection.attempts}</strong></div>
          <div><span>Region</span><strong>{connection.region}</strong></div>
        </div>
        <TabList selectedValue={tab} onTabSelect={(_, data) => setTab(String(data.value))} className="drawer-tabs">
          <Tab value="trace">Trace</Tab>
          <Tab value="attributes">Attributes</Tab>
          <Tab value="events">Events</Tab>
        </TabList>
        <div className="drawer-body">
          {tab === 'trace' && <SpanWaterfall connection={connection} />}
          {tab === 'attributes' && (
            <div className="attribute-grid">
              {Object.entries(connection.attributes).map(([key, value]) => (
                <div className="attribute-row" key={key}><code>{key}</code><span>{value}</span></div>
              ))}
            </div>
          )}
          {tab === 'events' && (
            <div className="event-list">
              {connection.events.map((event, index) => (
                <div className="event-card" key={`${event.name}-${index}`}>
                  <div className="event-icon"><ShieldErrorRegular /></div>
                  <div>
                    <EventDetails event={event} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}

function StatementDetailDrawer({ statement, onClose }: { statement: StatementFailure; onClose: () => void }) {
  const [tab, setTab] = useState('trace');
  return (
    <div className="drawer-backdrop" onMouseDown={event => event.currentTarget === event.target && onClose()}>
      <aside className="detail-drawer">
        <header className="drawer-header">
          <div><div className="eyebrow">Failed SQL execution</div><h2>{statementLabel(statement.statementType)}</h2><code>{statement.id}</code></div>
          <Button appearance="subtle" icon={<DismissRegular />} onClick={onClose} aria-label="Close details" />
        </header>
        <div className="drawer-summary">
          <div><span>Status</span><strong className="error-text">Error</strong></div>
          <div><span>Duration</span><strong>{statement.duration} ms</strong></div>
          <div><span>Operation</span><strong>{statement.operation}</strong></div>
          <div><span>Category</span><strong>{statement.category}</strong></div>
        </div>
        <div className="masked-query"><span>Masked SQL</span><code>{statement.maskedSql}</code></div>
        <TabList selectedValue={tab} onTabSelect={(_, data) => setTab(String(data.value))} className="drawer-tabs">
          <Tab value="trace">Trace</Tab><Tab value="attributes">Attributes</Tab><Tab value="events">Events</Tab>
        </TabList>
        <div className="drawer-body">
          {tab === 'trace' && <SpanWaterfall connection={statement} />}
          {tab === 'attributes' && <div className="attribute-grid">{Object.entries(statement.attributes).map(([key, value]) => (
            <div className="attribute-row" key={key}><code>{key}</code><span>{value}</span></div>
          ))}</div>}
          {tab === 'events' && <div className="event-list">{statement.events.map((event, index) => (
            <div className="event-card" key={`${event.name}-${index}`}><div className="event-icon"><ShieldErrorRegular /></div><div>
              <EventDetails event={event} />
            </div></div>
          ))}</div>}
        </div>
      </aside>
    </div>
  );
}

export function App() {
  const [telemetry, setTelemetry] = useState(emptyTelemetry);
  const [loading, setLoading] = useState(true);
  const [notice, setNotice] = useState('');
  const refreshing = useRef(false);
  const refresh = useCallback(async () => {
    if (refreshing.current) return;
    refreshing.current = true;
    setLoading(true);
    try { setTelemetry(await loadTelemetry()); }
    catch { setNotice('Telemetry could not be loaded. Refresh to retry.'); }
    finally { refreshing.current = false; setLoading(false); }
  }, []);
  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => void refresh(), 30000);
    return () => window.clearInterval(timer);
  }, [refresh]);
  useEffect(() => {
    const close = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { setSelected(null); setSelectedStatement(null); }
    };
    window.addEventListener('keydown', close);
    return () => window.removeEventListener('keydown', close);
  }, []);
  const { failures, statementFailures, performanceMetrics, throughputPoints, outcomeDistribution, retries } = telemetry;
  const tracesReady = Boolean(telemetry.loadedAt) && !telemetry.warnings.some(warning => warning.startsWith('Jaeger traces unavailable'));
  const [phase, setPhase] = useState('All');
  const [region, setRegion] = useState('All');
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<ConnectionFailure | null>(null);
  const [selectedStatement, setSelectedStatement] = useState<StatementFailure | null>(null);
  const [nav, setNav] = useState('Connections');

  const filtered = useMemo(() => failures.filter(item =>
    (phase === 'All' || item.phase === phase)
    && (region === 'All' || item.region === region)
    && (!search || `${item.id} ${item.errorType} ${item.category}`.toLowerCase().includes(search.toLowerCase())),
  ), [failures, phase, region, search]);

  const loginCount = filtered.filter(item => item.phase === 'Login').length;
  const avgDuration = filtered.length ? Math.round(filtered.reduce((sum, item) => sum + item.duration, 0) / filtered.length) : 0;
  const preparedStatementCount = statementFailures.filter(item => item.statementType === 'prepared_statement').length;
  const statementAvgDuration = statementFailures.length
    ? Math.round(statementFailures.reduce((sum, item) => sum + item.duration, 0) / statementFailures.length)
    : 0;
  const statementCategoryCount = new Set(statementFailures.map(item => item.category)).size;

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand"><Logo /><strong>FDH</strong><span>Fleet diagnostics hub</span></div>
        <div className="top-search"><SearchRegular /><span>Search resources, incidents, and telemetry</span><kbd>Ctrl + K</kbd></div>
        <div className="top-actions">
          <Tooltip content="Theme preview" relationship="label"><Button appearance="subtle" icon={<WeatherMoonRegular />} /></Tooltip>
          <Tooltip content="Settings" relationship="label"><Button appearance="subtle" icon={<SettingsRegular />} /></Tooltip>
          <div className="avatar">MC</div>
        </div>
      </header>
      <nav className="rail">
        <Button appearance="subtle" icon={<LineHorizontal3Regular />} className="rail-menu" />
        {[
          ['Overview', <HomeRegular />],
          ['Connections', <DatabaseRegular />],
          ['Incidents', <AlertRegular />],
          ['Diagnostics', <ShieldErrorRegular />],
        ].map(([label, icon]) => (
          <button key={String(label)} className={`rail-item ${nav === label ? 'active' : ''}`} onClick={() => setNav(String(label))}>
            {icon}<span>{label}</span>
          </button>
        ))}
        <button className="rail-item rail-bottom"><SettingsRegular /><span>Settings</span></button>
      </nav>
      <main className="workspace">
        <div className="breadcrumb"><span>SQL drivers</span><ChevronRightRegular /><span>Local SQL Server demo</span><ChevronRightRegular /><strong>Connection telemetry</strong></div>
        <section className="page-header">
          <div>
            <div className="title-line"><h1>Connection telemetry</h1><span className="preview-badge">Customer preview</span></div>
            <p>Explore failed SQL connection attempts, lifecycle timing, and sanitized driver diagnostics.</p>
          </div>
          <div className="header-actions"><span className="updated" aria-live="polite">{loading ? 'Refreshing…' : `Updated ${utcTime(telemetry.loadedAt || new Date().toISOString())} UTC`}</span><Button disabled={loading} onClick={() => void refresh()} icon={<ArrowClockwiseRegular />}>Refresh</Button><Button appearance="primary" onClick={() => { void navigator.clipboard.writeText(window.location.href).then(() => setNotice('View link copied. This dashboard is local to this machine.'), () => setNotice('Could not copy the link. Copy the address from the browser.')); }}>Share view</Button></div>
        </section>
        <details className="telemetry-note" open={telemetry.warnings.some(warning => warning.includes('unavailable')) || undefined}><summary>Live local telemetry · last 24 hours · refreshes every 30 seconds · collection limits</summary>{telemetry.warnings.map(warning => <p key={warning}>{warning}</p>)}<p>Only provider-masked root db.query.text is displayed when emitted and lexically safe. Missing SQL and regions remain unreported. No mock values are displayed.</p></details>
        {notice && <div className="telemetry-note" role="status">{notice}<Button appearance="subtle" onClick={() => setNotice('')} icon={<DismissRegular />} aria-label="Dismiss message" /></div>}
        <section className="metrics">
          <MetricCard label="Failed connections" value={tracesReady ? filtered.length.toLocaleString() : '—'} delta="Returned traces · last 24 hours" tone="#c50f1f" />
          <MetricCard label="Login failures" value={tracesReady ? loginCount.toLocaleString() : '—'} delta={`${Math.round(loginCount / Math.max(filtered.length, 1) * 100)}% of returned failures`} tone="#c239b3" />
          <MetricCard label="Average duration" value={tracesReady && filtered.length ? `${avgDuration} ms` : '—'} delta="Across filtered attempts" tone="#5b5fc7" />
          <MetricCard label="Affected regions" value={filtered.some(item => item.region !== 'Not emitted') ? String(new Set(filtered.filter(item => item.region !== 'Not emitted').map(item => item.region)).size) : '—'} delta="Only emitted cloud.region values" tone="#117865" />
        </section>
        <section className="charts-grid"><Distribution data={filtered} /><Activity data={filtered} /></section>
        <section className="panel table-panel">
          <div className="panel-title-row table-title-row">
            <div><h3>Failed connections</h3><p>Select a connection to inspect the full driver trace.</p></div>
            <div className="filters">
              <Input value={search} onChange={(_, data) => setSearch(data.value)} contentBefore={<SearchRegular />} placeholder="Search GUID or error" />
              <FilterRegular />
              <Select value={phase} onChange={(_, data) => setPhase(data.value)} aria-label="Failure phase">
                <option>All</option>{[...new Set(failures.map(item => item.phase))].map(value => <option key={value}>{value}</option>)}
              </Select>
              <Select value={region} onChange={(_, data) => setRegion(data.value)} aria-label="Region">
                <option>All</option>{[...new Set(failures.map(item => item.region))].map(value => <option key={value}>{value}</option>)}
              </Select>
            </div>
          </div>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead><tr><th>Time (UTC)</th><th>Connection ID</th><th>Phase</th><th>Error type</th><th>Source</th><th>Region</th><th>Duration</th><th /></tr></thead>
              <tbody>
                {filtered.slice(0, 40).map(item => (
                  <tr key={`${item.id}-${item.time}`} tabIndex={0} role="button" aria-label={`Inspect ${item.phase} connection failure`} onKeyDown={event => (event.key === 'Enter' || event.key === ' ') && (event.preventDefault(), setSelected(item))} onClick={() => setSelected(item)}>
                    <td title={item.time}>{utcTime(item.time)}</td>
                    <td><code className="guid">{item.id}</code></td>
                    <td><span className="phase-pill"><i style={{ background: phaseColor[item.phase] ?? '#0f6cbd' }} />{item.phase}</span></td>
                    <td><span className="error-type">{item.errorType}</span></td>
                    <td>{item.source}</td><td>{item.region}</td><td>{item.duration} ms</td><td><ChevronRightRegular /></td>
                  </tr>
                ))}
                {!filtered.length && <tr><td colSpan={8} className="empty-state">{loading ? 'Loading connection failures…' : 'No matching failed connection traces returned.'}</td></tr>}
              </tbody>
            </table>
          </div>
          <div className="table-footer"><span>Showing {Math.min(filtered.length, 40)} of {filtered.length} returned failures</span><span>Live Jaeger data · last 24 hours</span></div>
        </section>
        <section className="statement-analysis-header">
          <div><h2>Statement error analysis</h2><p>Category trends for failed Statement and PreparedStatement executions.</p></div>
          <span className="period-chip">{statementFailures.length} failures</span>
        </section>
        <section className="metrics statement-metrics">
          <MetricCard label="Failed statements" value={tracesReady ? statementFailures.length.toLocaleString() : '—'} delta="Returned traces · last 24 hours" tone="#c50f1f" />
          <MetricCard label="PreparedStatement failures" value={tracesReady ? preparedStatementCount.toLocaleString() : '—'} delta={`${Math.round(preparedStatementCount / Math.max(statementFailures.length, 1) * 100)}% of returned failures`} tone="#5b5fc7" />
          <MetricCard label="Average duration" value={tracesReady && statementFailures.length ? `${statementAvgDuration} ms` : '—'} delta="Across failed executions" tone="#c239b3" />
          <MetricCard label="Error categories" value={tracesReady ? String(statementCategoryCount) : '—'} delta="Stable driver classification" tone="#117865" />
        </section>
        <section className="charts-grid statement-charts-grid">
          <StatementDistribution data={statementFailures} />
          <StatementActivity data={statementFailures} />
        </section>
        <section className="panel table-panel statement-panel">
          <div className="panel-title-row table-title-row">
            <div><h3>Statement and prepared-statement errors</h3><p>Live driver failures. Only emitted, provider-masked root SQL is displayed.</p></div>
            <span className="period-chip">{statementFailures.length} failures</span>
          </div>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead><tr><th>Time (UTC)</th><th>Statement type</th><th>Operation</th><th>Masked SQL</th><th>Category</th><th>Error type</th><th>Duration</th><th /></tr></thead>
              <tbody>{statementFailures.slice(0, 40).map(item => (
                <tr key={`${item.id}-${item.time}`} tabIndex={0} role="button" aria-label={`Inspect ${item.statementType} failure`} onKeyDown={event => (event.key === 'Enter' || event.key === ' ') && (event.preventDefault(), setSelectedStatement(item))} onClick={() => setSelectedStatement(item)}>
                  <td title={item.time}>{utcTime(item.time)}</td>
                  <td><span className="phase-pill"><i style={{ background: item.statementType === 'prepared_statement' ? '#5b5fc7' : '#117865' }} />{statementLabel(item.statementType)}</span></td>
                  <td>{item.operation}</td><td><code className="query-text">{item.maskedSql}</code></td><td>{item.category}</td>
                  <td><span className="error-type">{item.errorType}</span></td><td>{item.duration} ms</td><td><ChevronRightRegular /></td>
                </tr>
              ))}{!statementFailures.length && <tr><td colSpan={8} className="empty-state">{loading ? 'Loading statement failures…' : 'No failed statement traces returned.'}</td></tr>}</tbody>
            </table>
          </div>
          <div className="table-footer"><span>Showing {Math.min(statementFailures.length, 40)} of {statementFailures.length} returned failures</span><span>Masked root SQL only · last 24 hours</span></div>
        </section>
        <PerformanceMetricsDashboard performanceMetrics={performanceMetrics} throughputPoints={throughputPoints} outcomeDistribution={outcomeDistribution} retries={retries} />
      </main>
      {selected && <DetailDrawer connection={selected} onClose={() => setSelected(null)} />}
      {selectedStatement && <StatementDetailDrawer statement={selectedStatement} onClose={() => setSelectedStatement(null)} />}
    </div>
  );
}