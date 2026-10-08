import { useMemo, useState } from 'react';
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
import {
  ConnectionFailure,
  FailurePhase,
  PerformanceMetric,
  StatementFailure,
  failures,
  performanceMetrics,
  statementFailures,
  throughputPoints,
} from './data';

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
  const counts = (['DNS', 'Configuration', 'Login'] as FailurePhase[]).map(phase => ({
    phase,
    count: data.filter(item => item.phase === phase).length,
  }));
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
                <div className="bar-fill" style={{ height: `${Math.max((item.count / max) * 100, 4)}%`, background: phaseColor[item.phase] }} />
              </div>
              <div className="bar-label">{item.phase}</div>
            </div>
          ))}
        </div>
        <div className="insight-card">
          <span className="insight-kicker">Top issue</span>
          <strong>{counts.sort((a, b) => b.count - a.count)[0].phase}</strong>
          <span>{Math.round((counts[0].count / Math.max(data.length, 1)) * 100)}% of filtered failures</span>
          <div className="mini-rule" />
          <span className="insight-note">Customer-facing mock data</span>
        </div>
      </div>
    </div>
  );
}

function Activity({ data }: { data: ConnectionFailure[] }) {
  const points = data.slice(0, 32).reverse().map((item, index) => ({
    x: index * (560 / 31),
    y: 104 - Math.min(item.duration / 2.5, 90),
  }));
  const line = points.map(point => `${point.x},${point.y}`).join(' ');
  return (
    <div className="panel activity-panel">
      <div className="panel-title-row">
        <div>
          <h3>Connection failure duration</h3>
          <p>End-to-end latency for recent failed connection attempts</p>
        </div>
        <span className="period-chip">Last 24 hours</span>
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
      </svg>
      <div className="chart-axis"><span>Earlier</span><span>Recent</span></div>
    </div>
  );
}

function StatementDistribution({ data }: { data: StatementFailure[] }) {
  const categories = ['Constraint violation', 'Query syntax & semantics'];
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
                    background: statementCategoryColor[item.category],
                  }}
                />
              </div>
              <div className="bar-label">{item.category}</div>
            </div>
          ))}
        </div>
        <div className="insight-card statement-insight-card">
          <span className="insight-kicker">Top category</span>
          <strong>{top.category}</strong>
          <span>{Math.round((top.count / Math.max(data.length, 1)) * 100)}% of statement failures</span>
          <div className="mini-rule" />
          <span className="insight-note">Stable, message-independent classification</span>
        </div>
      </div>
    </div>
  );
}

function StatementActivity({ data }: { data: StatementFailure[] }) {
  const points = data.slice(0, 32).reverse().map((item, index) => ({
    x: index * (560 / 31),
    y: 108 - Math.min(item.duration, 94),
  }));
  const line = points.map(point => `${point.x},${point.y}`).join(' ');
  return (
    <div className="panel activity-panel">
      <div className="panel-title-row">
        <div>
          <h3>Statement failure duration</h3>
          <p>End-to-end latency for recent failed SQL executions</p>
        </div>
        <span className="period-chip">Last 24 hours</span>
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
      </svg>
      <div className="chart-axis"><span>Earlier</span><span>Recent</span></div>
    </div>
  );
}

function PerformanceThroughput() {
  const maxStatements = Math.max(...throughputPoints.map(point => point.statements), 1);
  const maxConnections = Math.max(...throughputPoints.map(point => point.connections), 1);
  const statementLine = throughputPoints.map((point, index) =>
    `${index * (560 / 31)},${112 - (point.statements / maxStatements) * 82}`,
  ).join(' ');
  const connectionLine = throughputPoints.map((point, index) =>
    `${index * (560 / 31)},${112 - (point.connections / maxConnections) * 82}`,
  ).join(' ');
  return (
    <div className="panel activity-panel metrics-throughput-panel">
      <div className="panel-title-row">
        <div><h3>Operation throughput</h3><p>All successful and failed root operations per minute</p></div>
        <div className="chart-legend"><span><i className="legend-statements" />Statements</span><span><i className="legend-connections" />Connections</span></div>
      </div>
      <svg className="activity-chart" viewBox="0 0 560 130" preserveAspectRatio="none" role="img" aria-label="Connection and statement throughput chart">
        {[25, 65, 105].map(y => <line key={y} x1="0" x2="560" y1={y} y2={y} className="chart-grid" />)}
        <polyline points={statementLine} fill="none" stroke="#5b5fc7" strokeWidth="2.4" />
        <polyline points={connectionLine} fill="none" stroke="#117865" strokeWidth="2.4" />
      </svg>
      <div className="chart-axis"><span>Earlier</span><span>Recent</span></div>
    </div>
  );
}

function ActivityLatency({ title, subtitle, metrics, tone }: {
  title: string;
  subtitle: string;
  metrics: PerformanceMetric[];
  tone: string;
}) {
  const max = Math.max(...metrics.map(metric => metric.p95), 1);
  return (
    <div className="panel metrics-latency-panel">
      <div className="panel-title-row"><div><h3>{title}</h3><p>{subtitle}</p></div><span className="period-chip">p95 · ms</span></div>
      <div className="latency-bars">
        {metrics.map(metric => (
          <div className="latency-row" key={metric.activity}>
            <span>{metric.label}</span>
            <div className="latency-track"><i style={{ width: `${Math.max((metric.p95 / max) * 100, 2)}%`, background: tone }} /></div>
            <strong>{metric.p95.toFixed(metric.p95 < 10 ? 1 : 0)}</strong>
          </div>
        ))}
      </div>
    </div>
  );
}

function PerformanceMetricsDashboard() {
  const connectionRoot = performanceMetrics.find(metric => metric.activity === 'connection.open')!;
  const statementRoot = performanceMetrics.find(metric => metric.activity === 'statement.execute')!;
  const connectionStages = performanceMetrics.filter(metric => metric.kind === 'Connection' && !metric.activity.endsWith('.open'));
  const statementStages = performanceMetrics.filter(metric => metric.kind === 'Statement'
    && !['statement.execute', 'statement.attempt'].includes(metric.activity));
  const current = throughputPoints.at(-1)!;
  return (
    <section className="performance-dashboard">
      <div className="statement-analysis-header performance-analysis-header">
        <div><h2>Performance metrics</h2><p>Pre-aggregated interval metrics for all connections and statements—not only failures.</p></div>
        <span className="period-chip">60-second windows</span>
      </div>
      <section className="metrics performance-metrics">
        <MetricCard label="Connection opens / min" value={current.connections.toLocaleString()} delta={`${connectionRoot.successRate.toFixed(2)}% successful`} tone="#117865" />
        <MetricCard label="Statement executions / min" value={current.statements.toLocaleString()} delta={`${statementRoot.successRate.toFixed(3)}% successful`} tone="#5b5fc7" />
        <MetricCard label="Connection p95" value={`${connectionRoot.p95} ms`} delta="Physical open latency" tone="#c239b3" />
        <MetricCard label="Statement p95" value={`${statementRoot.p95} ms`} delta="JDBC execute invocation" tone="#c50f1f" />
      </section>
      <section className="charts-grid performance-top-grid">
        <PerformanceThroughput />
        <ActivityLatency title="Connection lifecycle latency" subtitle="Independent p95 for each measured phase" metrics={connectionStages} tone="#117865" />
      </section>
      <section className="charts-grid performance-bottom-grid">
        <ActivityLatency title="Statement pipeline latency" subtitle="Independent p95; nested phase durations are not added" metrics={statementStages} tone="#5b5fc7" />
        <div className="panel metric-principles-panel">
          <div className="panel-title-row"><div><h3>Aggregate signal contract</h3><p>Bounded dimensions and interval snapshots</p></div></div>
          <div className="principle-list">
            <div><strong>All operations</strong><span>Successes, failures, timeouts, and cancellations</span></div>
            <div><strong>Driver pre-aggregation</strong><span>Counts and fixed duration buckets per 60-second window</span></div>
            <div><strong>Bounded cardinality</strong><span>No SQL, IDs, server names, exception text, or trace context</span></div>
            <div><strong>Failure diagnostics stay separate</strong><span>Detailed traces above remain failure-only</span></div>
          </div>
        </div>
      </section>
      <section className="panel table-panel aggregate-metrics-panel">
        <div className="panel-title-row table-title-row">
          <div><h3>Performance activity aggregates</h3><p>Counts and percentiles calculated independently for each activity.</p></div>
          <span className="period-chip">Last 60 minutes</span>
        </div>
        <div className="data-table-wrap aggregate-table-wrap">
          <table className="data-table aggregate-table">
            <thead><tr><th>Activity</th><th>Kind</th><th>Count</th><th>Success rate</th><th>Errors</th><th>p50</th><th>p95</th><th>p99</th><th>Max</th></tr></thead>
            <tbody>{performanceMetrics.map(metric => (
              <tr key={metric.activity}>
                <td><code className="metric-activity">{metric.activity}</code></td><td>{metric.kind}</td><td>{metric.count.toLocaleString()}</td>
                <td>{metric.successRate.toFixed(metric.successRate > 99.99 ? 3 : 2)}%</td><td>{metric.errors.toLocaleString()}</td>
                <td>{metric.p50} ms</td><td><strong>{metric.p95} ms</strong></td><td>{metric.p99} ms</td><td>{metric.max} ms</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        <div className="table-footer"><span>{performanceMetrics.length} bounded activity series</span><span>Mock aggregate data · includes successful operations</span></div>
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
                    <strong>{event.name}</strong>
                    <p>{event.phase} · {event.source}</p>
                    <div className="event-tags"><span>{event.code}</span><span>{event.decision}</span></div>
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
          <div><div className="eyebrow">Failed SQL execution</div><h2>{statement.statementType}</h2><code>{statement.id}</code></div>
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
              <strong>{event.name}</strong><p>{event.phase} · {event.source}</p><div className="event-tags"><span>{event.code}</span><span>{event.decision}</span></div>
            </div></div>
          ))}</div>}
        </div>
      </aside>
    </div>
  );
}

export function App() {
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
  ), [phase, region, search]);

  const loginCount = filtered.filter(item => item.phase === 'Login').length;
  const avgDuration = filtered.length ? Math.round(filtered.reduce((sum, item) => sum + item.duration, 0) / filtered.length) : 0;
  const preparedStatementCount = statementFailures.filter(item => item.statementType === 'PreparedStatement').length;
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
        <div className="breadcrumb"><span>SQL drivers</span><ChevronRightRegular /><span>Customer orders API</span><ChevronRightRegular /><strong>Connection telemetry</strong></div>
        <section className="page-header">
          <div>
            <div className="title-line"><h1>Connection telemetry</h1><span className="preview-badge">Customer preview</span></div>
            <p>Explore failed SQL connection attempts, lifecycle timing, and sanitized driver diagnostics.</p>
          </div>
          <div className="header-actions"><span className="updated">Updated just now</span><Button icon={<ArrowClockwiseRegular />}>Refresh</Button><Button appearance="primary">Share view</Button></div>
        </section>
        <section className="metrics">
          <MetricCard label="Failed connections" value={filtered.length.toLocaleString()} delta="Last 24 hours" tone="#c50f1f" />
          <MetricCard label="Login failures" value={loginCount.toLocaleString()} delta={`${Math.round(loginCount / Math.max(filtered.length, 1) * 100)}% of failures`} tone="#c239b3" />
          <MetricCard label="Average duration" value={`${avgDuration} ms`} delta="Across filtered attempts" tone="#5b5fc7" />
          <MetricCard label="Affected regions" value={String(new Set(filtered.map(item => item.region)).size)} delta="Mock customer estate" tone="#117865" />
        </section>
        <section className="charts-grid"><Distribution data={filtered} /><Activity data={filtered} /></section>
        <section className="panel table-panel">
          <div className="panel-title-row table-title-row">
            <div><h3>Failed connections</h3><p>Select a connection to inspect the full driver trace.</p></div>
            <div className="filters">
              <Input value={search} onChange={(_, data) => setSearch(data.value)} contentBefore={<SearchRegular />} placeholder="Search GUID or error" />
              <FilterRegular />
              <Select value={phase} onChange={(_, data) => setPhase(data.value)} aria-label="Failure phase">
                <option>All</option><option>Configuration</option><option>DNS</option><option>Login</option>
              </Select>
              <Select value={region} onChange={(_, data) => setRegion(data.value)} aria-label="Region">
                <option>All</option><option>East US</option><option>West Europe</option><option>Southeast Asia</option>
              </Select>
            </div>
          </div>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead><tr><th>Time (UTC)</th><th>Connection ID</th><th>Phase</th><th>Error type</th><th>Source</th><th>Region</th><th>Duration</th><th /></tr></thead>
              <tbody>
                {filtered.slice(0, 40).map(item => (
                  <tr key={item.id} onClick={() => setSelected(item)}>
                    <td>{new Date(item.time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}</td>
                    <td><code className="guid">{item.id}</code></td>
                    <td><span className="phase-pill"><i style={{ background: phaseColor[item.phase] }} />{item.phase}</span></td>
                    <td><span className="error-type">{item.errorType}</span></td>
                    <td>{item.source}</td><td>{item.region}</td><td>{item.duration} ms</td><td><ChevronRightRegular /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="table-footer"><span>Showing {Math.min(filtered.length, 40)} of {filtered.length} failures</span><span>Mock data · no customer identifiers</span></div>
        </section>
        <section className="statement-analysis-header">
          <div><h2>Statement error analysis</h2><p>Category trends for failed Statement and PreparedStatement executions.</p></div>
          <span className="period-chip">{statementFailures.length} failures</span>
        </section>
        <section className="metrics statement-metrics">
          <MetricCard label="Failed statements" value={statementFailures.length.toLocaleString()} delta="Last 24 hours" tone="#c50f1f" />
          <MetricCard label="PreparedStatement failures" value={preparedStatementCount.toLocaleString()} delta={`${Math.round(preparedStatementCount / Math.max(statementFailures.length, 1) * 100)}% of failures`} tone="#5b5fc7" />
          <MetricCard label="Average duration" value={`${statementAvgDuration} ms`} delta="Across failed executions" tone="#c239b3" />
          <MetricCard label="Error categories" value={String(statementCategoryCount)} delta="Stable driver classification" tone="#117865" />
        </section>
        <section className="charts-grid statement-charts-grid">
          <StatementDistribution data={statementFailures} />
          <StatementActivity data={statementFailures} />
        </section>
        <section className="panel table-panel statement-panel">
          <div className="panel-title-row table-title-row">
            <div><h3>Statement and prepared-statement errors</h3><p>Masked SQL preserves query shape while removing literal values and comments.</p></div>
            <span className="period-chip">{statementFailures.length} failures</span>
          </div>
          <div className="data-table-wrap">
            <table className="data-table">
              <thead><tr><th>Time (UTC)</th><th>Statement type</th><th>Operation</th><th>Masked SQL</th><th>Category</th><th>Error type</th><th>Duration</th><th /></tr></thead>
              <tbody>{statementFailures.slice(0, 40).map(item => (
                <tr key={item.id} onClick={() => setSelectedStatement(item)}>
                  <td>{new Date(item.time).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })}</td>
                  <td><span className="phase-pill"><i style={{ background: item.statementType === 'PreparedStatement' ? '#5b5fc7' : '#117865' }} />{item.statementType}</span></td>
                  <td>{item.operation}</td><td><code className="query-text">{item.maskedSql}</code></td><td>{item.category}</td>
                  <td><span className="error-type">{item.errorType}</span></td><td>{item.duration} ms</td><td><ChevronRightRegular /></td>
                </tr>
              ))}</tbody>
            </table>
          </div>
          <div className="table-footer"><span>Showing {Math.min(statementFailures.length, 40)} of {statementFailures.length} failures</span><span>Literal values and comments masked</span></div>
        </section>
        <PerformanceMetricsDashboard />
      </main>
      {selected && <DetailDrawer connection={selected} onClose={() => setSelected(null)} />}
      {selectedStatement && <StatementDetailDrawer statement={selectedStatement} onClose={() => setSelectedStatement(null)} />}
    </div>
  );
}