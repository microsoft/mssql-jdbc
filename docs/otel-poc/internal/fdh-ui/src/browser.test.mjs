import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

// Optional live end-to-end check: requires a running dashboard and local Chromium/Edge.
// Uses built-in CDP/WebSocket support instead of adding a browser dependency.
const browser = process.env.BROWSER_PATH ?? (process.platform === 'win32'
  ? 'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe' : '');
const url = process.env.DASHBOARD_URL ?? 'http://127.0.0.1:3002';
const fixtures = process.env.DASHBOARD_FIXTURES === '1';
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

test('live dashboard: failure tables, trace/attributes/events drawers, metrics and refresh', {
  skip: !existsSync(browser) || typeof WebSocket === 'undefined', timeout: 90000,
}, async () => {
  const profile = await mkdtemp(path.join(tmpdir(), 'jdbc-dashboard-browser-'));
  const child = spawn(browser, ['--headless=new', '--disable-gpu', '--no-first-run',
    '--no-default-browser-check', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'],
  { stdio: ['ignore', 'ignore', 'pipe'] });
  let socket;
  try {
    const endpoint = await new Promise((resolve, reject) => {
      let output = '';
      const timer = setTimeout(() => reject(new Error('Browser CDP startup timed out')), 20000);
      child.once('error', error => { clearTimeout(timer); reject(error); });
      child.stderr.on('data', chunk => {
        output += chunk;
        const found = output.match(/DevTools listening on (ws:\/\/[^\s]+)/);
        if (found) { clearTimeout(timer); resolve(found[1]); }
      });
    });
    const port = new URL(endpoint).port;
    const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
    socket = new WebSocket(targets.find(target => target.type === 'page').webSocketDebuggerUrl);
    await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject; });
    let id = 0;
    const pending = new Map();
    const errors = [];
    socket.onmessage = event => {
      const message = JSON.parse(event.data);
      if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text);
      const request = pending.get(message.id);
      if (request) {
        clearTimeout(request.timer);
        pending.delete(message.id);
        message.error ? request.reject(new Error(message.error.message)) : request.resolve(message.result);
      }
    };
    const send = (method, params = {}) => new Promise((resolve, reject) => {
      const requestId = ++id;
      const timer = setTimeout(() => { pending.delete(requestId); reject(new Error(`CDP timeout: ${method}`)); }, 15000);
      pending.set(requestId, { resolve, reject, timer });
      socket.send(JSON.stringify({ id: requestId, method, params }));
    });
    const evaluate = async expression => {
      const result = await send('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
      if (result.exceptionDetails) throw new Error(`${expression}: ${result.exceptionDetails.exception?.description ?? result.exceptionDetails.text}`);
      return result.result.value;
    };
    const until = async expression => {
      for (let attempt = 0; attempt < 120; attempt++) {
        if (await evaluate(expression)) return;
        await wait(250);
      }
      throw new Error(`DOM condition timed out: ${expression}`);
    };
    await send('Runtime.enable');
    await send('Page.enable');
    if (fixtures) await send('Page.addScriptToEvaluateOnNewDocument', { source: `
      const tags = values => Object.entries(values).map(([key,value]) => ({key,value}));
      const start = Date.now() * 1000;
      const makeSpan = (spanID, operationName, parent, values, logs=[]) => ({spanID, traceID:'abc123',
        operationName:'mssql.driver.'+operationName,startTime:start,duration:2500,processID:'p1',
        tags:tags(values),logs,references:parent?[{refType:'CHILD_OF',traceID:'abc123',spanID:parent}]:[]});
      const spans = [makeSpan('c','connection.open',null,{error:true,'mssql.connection.failure_phase':'dns'}),
        makeSpan('d','connection.dns','c',{},[{timestamp:start,fields:tags({event:'mssql.driver.error','mssql.error.phase':'dns'})}]),
        makeSpan('s','statement.execute',null,{error:true,'mssql.statement.type':'prepared_statement',
          'db.query.text':'SELECT ? FROM dbo.demo WHERE id = ?'},[{timestamp:start,fields:tags({
            event:'mssql.driver.statement.retry_decision','mssql.retry.delay':0.5,'mssql.error.retry_decision':'limit_reached'})}]),
        makeSpan('a','statement.attempt','s',{})];
      const metric = (activity, value, extra={}) => ({metric:{mssql_performance_activity:activity,
        mssql_operation_kind:activity.startsWith('connection.')?'connection':'statement',...extra}, value:[1,String(value)]});
      const realFetch=window.fetch;
      window.fetch=async (input,options) => {
        const url=new URL(input,location.href); const q=url.searchParams.get('query')??'';
        let payload;
        if(url.pathname==='/jaeger/api/traces') payload={data:[{traceID:'abc123',spans}]};
        else if(url.pathname.endsWith('query_range')) payload={status:'success',data:{result:[]}};
        else if(url.pathname==='/prometheus/api/v1/query') {
          const result=q.includes('retry_count')?[metric('statement.execute',2,{mssql_retry_reason:'configurable_retry'})]
            :q.includes('count_total')&&!q.includes('error_count')?[
              metric('connection.open',8,{mssql_operation_outcome:'success'}),metric('connection.open',2,{mssql_operation_outcome:'failure'}),
              metric('statement.execute',9,{mssql_operation_outcome:'success'}),metric('statement.execute',1,{mssql_operation_outcome:'timeout'})]
            :[metric('connection.open',q.includes('histogram_quantile')?0.025:1),metric('statement.execute',q.includes('histogram_quantile')?0.05:1)];
          payload={status:'success',data:{result}};
        } else return realFetch(input,options);
        return {ok:true,json:async()=>payload};
      };` });
    await send('Emulation.setDeviceMetricsOverride', { width: 1600, height: 1100, deviceScaleFactor: 1, mobile: false });
    await send('Page.navigate', { url });
    await until(`document.querySelector('.updated')?.textContent.startsWith('Updated')`);
    assert.equal(await evaluate(`document.querySelector('h1').textContent`), 'Connection telemetry');
    assert.ok(await evaluate(`document.querySelectorAll('tr[role="button"]').length > 0`), 'Real failure rows must exist');
    assert.equal(await evaluate(`document.body.innerText.includes('Customer-facing mock data')`), false);
    assert.equal(await evaluate(`document.body.innerText.includes('Mock aggregate data')`), false);
    assert.ok(await evaluate(`document.querySelectorAll('.aggregate-table tbody tr').length > 0`),
      'Metric series must exist: ' + await evaluate(`document.querySelector('.telemetry-note').innerText`));
    assert.ok(await evaluate(`document.querySelector('.outcome-panel') !== null`));
    assert.ok(await evaluate(`document.querySelector('.retry-panel') !== null`));
    if (fixtures) {
      assert.ok(await evaluate(`document.querySelector('.statement-panel .query-text').textContent.includes('SELECT ?')`));
      assert.ok(await evaluate(`document.querySelector('.outcome-panel').innerText.includes('timeout')`));
      assert.ok(await evaluate(`document.querySelector('.retry-panel').innerText.includes('configurable_retry')`));
      assert.ok(await evaluate(`document.querySelector('.aggregate-table').innerText.includes('80%')`));
    }
    const firstTable = `.table-panel:not(.statement-panel):not(.aggregate-metrics-panel)`;
    const inspect = async selector => {
      await evaluate(`document.querySelector(${JSON.stringify(selector + ' tr[role="button"]')}).click()`);
      await until(`document.querySelector('.detail-drawer .span-row') !== null`);
      assert.ok(await evaluate(`document.querySelectorAll('.span-row').length >= 2`));
      await evaluate(`[...document.querySelectorAll('[role="tab"]')].find(tab => tab.textContent.includes('Attributes')).click()`);
      await until(`document.querySelector('.attribute-row') !== null`);
      await evaluate(`[...document.querySelectorAll('[role="tab"]')].find(tab => tab.textContent.includes('Events')).click()`);
      await until(`document.querySelector('.event-card') !== null`);
      assert.ok(await evaluate(`[...document.querySelectorAll('.event-card')].some(card => /mssql\\.driver\\.(error|statement\\.retry_decision|timeout|redirect|authentication|retry|connection\\.retry_decision)/.test(card.innerText))`));
      await evaluate(`document.querySelector('[aria-label="Close details"]').click()`);
    };
    await inspect(firstTable);
    await inspect('.statement-panel');
    await evaluate(`document.querySelector('input[placeholder="Search GUID or error"]').focus()`);
    await send('Input.insertText', { text: 'no-such-guid-for-browser-test' });
    await until(`document.querySelector('.empty-state') !== null`);
    assert.equal(await evaluate(`document.querySelector(${JSON.stringify(firstTable)}).querySelectorAll('tr[role="button"]').length`), 0);
    await evaluate(`document.querySelector('input[placeholder="Search GUID or error"]').focus()`);
    await send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'a', code: 'KeyA', modifiers: 2, windowsVirtualKeyCode: 65 });
    await send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
    await send('Input.dispatchKeyEvent', { type: 'keyUp', key: 'Backspace', code: 'Backspace', windowsVirtualKeyCode: 8 });
    await evaluate(`[...document.querySelectorAll('.header-actions button')].find(button => button.textContent === 'Refresh').click()`);
    await until(`document.querySelector('.updated').textContent.startsWith('Updated')`);
    assert.deepEqual(errors, [], 'No uncaught browser runtime errors');
    const screenshot = await send('Page.captureScreenshot', { format: 'png', captureBeyondViewport: false });
    const screenshotPath = path.join(tmpdir(), 'jdbc-live-dashboard.png');
    await writeFile(screenshotPath, Buffer.from(screenshot.data, 'base64'));
    console.log(`Live screenshot saved to ${screenshotPath}`);
    // CDP may close its socket before acknowledging Browser.close. Do not retain a
    // pending request/timer for shutdown after all interaction assertions passed.
    socket.send(JSON.stringify({ id: ++id, method: 'Browser.close', params: {} }));
  } finally {
    socket?.close();
    child.kill();
    // Chromium subprocesses can hold profile files briefly after shutdown.
    await rm(profile, { recursive: true, force: true, maxRetries: 1, retryDelay: 100 }).catch(() => {});
  }
});