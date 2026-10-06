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
import { ConnectionFailure, FailurePhase, failures } from './data';

const phaseColor: Record<FailurePhase, string> = {
  DNS: '#117865',
  Configuration: '#5b5fc7',
  Login: '#c239b3',
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

function SpanWaterfall({ connection }: { connection: ConnectionFailure }) {
  const total = Math.max(connection.duration, 1);
  return (
    <div className="span-list">
      <div className="span-scale"><span>0 ms</span><span>{Math.round(total / 2)} ms</span><span>{total} ms</span></div>
      {connection.spans.map((span, index) => (
        <div className="span-row" key={`${span.name}-${index}`}>
          <div className="span-name" style={{ paddingLeft: `${span.depth * 14}px` }}>
            <span className={`status-dot ${span.status.toLowerCase()}`} />
            <span>{span.name.replace('mssql.driver.connection.', '')}</span>
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

export function App() {
  const [phase, setPhase] = useState('All');
  const [region, setRegion] = useState('All');
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState<ConnectionFailure | null>(null);
  const [nav, setNav] = useState('Connections');

  const filtered = useMemo(() => failures.filter(item =>
    (phase === 'All' || item.phase === phase)
    && (region === 'All' || item.region === region)
    && (!search || `${item.id} ${item.errorType} ${item.category}`.toLowerCase().includes(search.toLowerCase())),
  ), [phase, region, search]);

  const loginCount = filtered.filter(item => item.phase === 'Login').length;
  const avgDuration = filtered.length ? Math.round(filtered.reduce((sum, item) => sum + item.duration, 0) / filtered.length) : 0;

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
      </main>
      {selected && <DetailDrawer connection={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}