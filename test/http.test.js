import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { startServer } from '../src/server.js';

/**
 * End-to-end over real HTTP: identity, read, cancellation, receiver pushes,
 * SSE and recovery. Uses a temp database per test and the local source.
 */

let dir;
let server;

beforeEach(async () => {
  dir = mkdtempSync(join(tmpdir(), 'qdw-e2e-'));
  server = await startServer({
    port: 0,
    dbFile: join(dir, 'workbench.db'),
    receiverFile: join(dir, 'receiver.db'),
  });
  // Refreshes are driven explicitly by the tests; the background timer would
  // otherwise race assertions. Delivery draining stays on so queued pushes
  // actually reach the receiver over real HTTP.
  server.refresher.stop();
});

afterEach(async () => {
  await server.close();
  rmSync(dir, { recursive: true, force: true });
});

async function call(path, { method = 'GET', token, body, headers = {} } = {}) {
  const response = await fetch(`${server.url}${path}`, {
    method,
    headers: {
      ...(body === undefined ? {} : { 'content-type': 'application/json' }),
      ...(token ? { 'x-viewer-token': token } : {}),
      ...headers,
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  let payload = null;
  try { payload = text ? JSON.parse(text) : null; } catch { payload = text; }
  return { status: response.status, payload };
}

async function makeViewer(label, role) {
  const { payload } = await call('/api/viewers', { method: 'POST', body: { label, role } });
  return payload;
}

const readBody = (overrides = {}) => ({
  source_id: 'local-library',
  query: 'e2e query',
  max_age_ms: 60_000,
  stale_if_error_ms: 0,
  ...overrides,
});

/* ------------------------------------------------------------------ */

test('meta reports the sharing policy and live sources', async () => {
  const { status, payload } = await call('/api/meta');
  assert.equal(status, 200);
  assert.ok(payload.sources.some((source) => source.id === 'wikipedia'));
  assert.ok(payload.sources.some((source) => source.id === 'local-library'));
  assert.equal(payload.policy.sharing_unit, '(source, normalized query text, locale)');
});

test('reads require a viewer token', async () => {
  const anonymous = await call('/api/read', { method: 'POST', body: readBody() });
  assert.equal(anonymous.status, 401);
  const bogus = await call('/api/read', { method: 'POST', body: readBody(), token: 'vt_nope' });
  assert.equal(bogus.status, 401);
});

test('concurrent HTTP reads coalesce and expose different fields per role', async () => {
  const analyst = await makeViewer('Ava', 'analyst');
  const publicViewer = await makeViewer('Pat', 'public');
  const query = 'slow 500 http coalesce';

  const started = Date.now();
  const [a, b] = await Promise.all([
    call('/api/read', { method: 'POST', body: readBody({ query }), token: analyst.token }),
    call('/api/read', { method: 'POST', body: readBody({ query, client_token: 'second' }), token: publicViewer.token }),
  ]);
  const elapsed = Date.now() - started;

  assert.equal(a.status, 200);
  assert.equal(b.status, 200);
  assert.ok(elapsed < 1_600, `expected one fetch, took ${elapsed}ms`);
  assert.equal(a.payload.admission.share_token, b.payload.admission.share_token);
  assert.deepEqual(Object.keys(a.payload.result), ['title', 'url', 'snippet', 'published_at', 'score', 'crawl_meta']);
  assert.deepEqual(Object.keys(b.payload.result), ['title', 'url', 'snippet', 'published_at']);
  assert.ok(b.payload.withheld_fields.some((entry) => entry.field === 'contact_email'));

  const { payload: state } = await call('/api/state');
  assert.equal(state.stats.upstream_requests, 1);
  assert.equal(state.stats.upstream_requests_avoided, 1);
});

test('detaching one viewer over HTTP keeps the shared task alive for the other', async () => {
  const first = await makeViewer('First', 'analyst');
  const second = await makeViewer('Second', 'analyst');
  const query = 'slow 900 http detach';

  const a = call('/api/read', { method: 'POST', body: readBody({ query, client_token: 'a' }), token: first.token });
  await new Promise((resolve) => setTimeout(resolve, 150));
  const b = call('/api/read', { method: 'POST', body: readBody({ query, client_token: 'b' }), token: second.token });
  await new Promise((resolve) => setTimeout(resolve, 150));

  const { payload: state } = await call('/api/state');
  const task = state.tasks.find((entry) => entry.query === query);
  assert.ok(task, 'the shared task is visible');
  assert.equal(task.ref_count, 2);

  const left = await call(`/api/tasks/${task.id}/leave`, { method: 'POST', body: {}, token: first.token });
  assert.equal(left.payload.stillWaiting, 1);
  assert.equal(left.payload.aborted, false);

  const [aResult, bResult] = await Promise.all([a, b]);
  assert.equal(aResult.status, 409, 'the viewer that left no longer waits on the task');
  assert.equal(aResult.payload.code, 'abandoned');
  assert.equal(bResult.status, 200, 'the remaining viewer still gets the source result');
  assert.equal(bResult.payload.admission.served, 'source');
});

test('a second viewer with a larger max age is served from cache while the first refetches', async () => {
  const viewer = await makeViewer('Age', 'analyst');
  const first = await call('/api/read', { method: 'POST', body: readBody({ query: 'age over http', max_age_ms: 1_000 }), token: viewer.token });
  assert.equal(first.payload.admission.served, 'source');
  const second = await call('/api/read', { method: 'POST', body: readBody({ query: 'age over http', max_age_ms: 600_000 }), token: viewer.token });
  assert.equal(second.payload.admission.served, 'cache');
  assert.equal(second.payload.admission.share_token, first.payload.admission.share_token);
});

test('a subscription is pushed to the receiver, signed, and projected for its role', async () => {
  const viewer = await makeViewer('Sub', 'public');
  const created = await call('/api/subscriptions', {
    method: 'POST',
    token: viewer.token,
    body: { source_id: 'local-library', query: 'http push', max_age_ms: 30_000, stale_if_error_ms: 300_000 },
  });
  assert.equal(created.status, 201);
  assert.match(created.payload.subscription.callback_url, /\/receiver\/sub_/);
  assert.ok(created.payload.secret.startsWith('whsec_'));

  const tick = await call('/api/subscriptions/tick', { method: 'POST', body: {}, token: viewer.token });
  assert.equal(tick.payload.started.length, 1);

  await waitFor(async () => {
    const { payload } = await call('/api/receiver');
    return payload.inbox.length > 0 ? payload : null;
  });

  const { payload: receiverState } = await call('/api/receiver');
  assert.equal(receiverState.inbox.length, 1);
  assert.equal(receiverState.inbox[0].role, 'public');
  assert.equal(receiverState.inbox[0].subscription_id, created.payload.subscription.id);
  assert.deepEqual(Object.keys(receiverState.inbox[0].payload.result), ['title', 'url', 'snippet', 'published_at']);
  assert.equal(receiverState.stats.rejected, 0);
});

test('the receiver rejects a push whose signature does not verify', async () => {
  const viewer = await makeViewer('Forged', 'analyst');
  const created = await call('/api/subscriptions', {
    method: 'POST',
    token: viewer.token,
    body: { source_id: 'local-library', query: 'forgery', max_age_ms: 30_000 },
  });
  const subId = created.payload.subscription.id;
  const body = JSON.stringify({ dedupe_key: 'k', subscription_id: subId, role: 'analyst', result: {}, query: { key: 'q' }, task_id: 't', generation: 1 });
  const response = await fetch(`${server.url}/receiver/${subId}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-workbench-subscription': subId,
      'x-workbench-timestamp': String(Date.now()),
      'x-workbench-signature': 'not-a-signature',
      'x-workbench-dedupe': 'k',
    },
    body,
  });
  assert.equal(response.status, 401);
  const { payload } = await call('/api/receiver');
  assert.equal(payload.stats.rejected, 1);
  assert.equal(payload.inbox.length, 0);
});

test('a cancelled subscription ejects queued deliveries and future fan-out', async () => {
  const viewer = await makeViewer('Cancel', 'analyst');
  // Produce a generation first so subscribing backfills a real delivery.
  const read = await call('/api/read', { method: 'POST', body: readBody({ query: 'cancel feed' }), token: viewer.token });
  assert.equal(read.payload.admission.served, 'source');
  const created = await call('/api/subscriptions', {
    method: 'POST',
    token: viewer.token,
    body: { source_id: 'local-library', query: 'cancel feed', max_age_ms: 60_000 },
  });
  const subId = created.payload.subscription.id;
  assert.equal(created.payload.subscription.deliveries.length, 1);
  assert.equal(created.payload.subscription.deliveries[0].state, 'pending');
  const cancelled = await call(`/api/subscriptions/${subId}/cancel`, { method: 'POST', body: {}, token: viewer.token });
  assert.equal(cancelled.payload.subscription.active, false);
  assert.equal(cancelled.payload.subscription.deliveries[0].state, 'dropped');
});

test('crash and recover over HTTP adopt the original task without a new generation', async () => {
  const viewer = await makeViewer('Crash', 'analyst');
  const query = 'slow 1200 crash over http';
  const read = call('/api/read', { method: 'POST', body: readBody({ query }), token: viewer.token });
  await new Promise((resolve) => setTimeout(resolve, 200));
  const before = server.db.prepare('SELECT id, generation FROM source_tasks').all();

  const crash = await call('/api/demo/crash', { method: 'POST', body: {}, token: viewer.token });
  assert.equal(crash.payload.detached.length, 1);
  assert.equal(server.db.prepare('SELECT state FROM source_tasks').get().state, 'running');
  const readOutcome = await read;
  assert.equal(readOutcome.status, 503, 'the caller whose process died sees an interruption');
  assert.equal(readOutcome.payload.code, 'process_lost');

  const recovery = await call('/api/demo/recover', { method: 'POST', body: { max_adoptions: 25 }, token: viewer.token });
  assert.equal(recovery.payload.new_tasks_created, 0);
  assert.equal(recovery.payload.adopted.length, 1);
  await waitFor(() => server.coordinator.taskById(before[0].id).state === 'succeeded');

  assert.deepEqual(server.db.prepare('SELECT id, generation FROM source_tasks').all(), before);
  assert.equal(server.db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
});

test('lose-notification then recover finishes the original task with one upstream request', async () => {
  const viewer = await makeViewer('Lose', 'analyst');
  await call('/api/demo/lose-notification', { method: 'POST', body: {}, token: viewer.token });

  const read = await call('/api/read', { method: 'POST', body: readBody({ query: 'lost over http' }), token: viewer.token });
  assert.equal(read.status, 503);

  const recovery = await call('/api/demo/recover', { method: 'POST', body: {}, token: viewer.token });
  assert.equal(recovery.payload.receipts_finished.length, 1);

  const { payload: state } = await call('/api/state');
  const task = state.tasks.find((entry) => entry.query === 'lost over http');
  assert.equal(task.state, 'succeeded');
  assert.equal(state.stats.upstream_requests, 1);
});

test('the event stream emits state frames after a read', async () => {
  const viewer = await makeViewer('Live', 'analyst');
  const controller = new AbortController();
  const response = await fetch(`${server.url}/api/stream`, { signal: controller.signal });
  assert.equal(response.status, 200);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  const readUntil = async (needle) => {
    const deadline = Date.now() + 4_000;
    while (Date.now() < deadline) {
      if (buffer.includes(needle)) return;
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
    }
    throw new Error(`stream never produced ${needle}; saw: ${buffer.slice(0, 200)}`);
  };
  await readUntil('event: ready');
  await call('/api/read', { method: 'POST', body: readBody({ query: 'stream event' }), token: viewer.token });
  await readUntil('event: state');
  controller.abort();
});

test('the static workbench page and its assets are served', async () => {
  const page = await fetch(`${server.url}/`);
  assert.equal(page.status, 200);
  const html = await page.text();
  assert.match(html, /Query Dedupe Workbench/);
  for (const asset of ['/styles.css', '/app.js']) {
    const response = await fetch(`${server.url}${asset}`);
    assert.equal(response.status, 200, `${asset} should be served`);
  }
});

async function waitFor(predicate, timeoutMs = 4_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 30));
  }
  throw new Error('waitFor timed out');
}
