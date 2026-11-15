import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { openStore } from '../src/store.js';
import { Coordinator } from '../src/coordinator.js';
import { createViewer } from '../src/viewers.js';
import { Receiver, newSecret } from '../src/receiver.js';
import { Dispatcher } from '../src/dispatcher.js';
import { logicalKey } from '../src/keys.js';
import { projectDocument } from '../src/permissions.js';

/**
 * Durable subscription semantics: at-least-once push, per-subscription
 * projection, receiver idempotency, retry bounds, and failure isolation.
 */

let db;
let coordinator;
let receiver;
let dispatcher;
let viewer;

const views = {};

beforeEach(() => {
  db = openStore(':memory:');
  coordinator = new Coordinator({ db, fetchTimeoutMs: 6_000 });
  receiver = new Receiver({ filename: ':memory:' });
  dispatcher = new Dispatcher({ db });
  views.analyst = createViewer(db, { label: 'Analyst Ava', role: 'analyst' }, Date.now());
  views.public = createViewer(db, { label: 'Public Pat', role: 'public' }, Date.now());
  views.steward = createViewer(db, { label: 'Steward Sam', role: 'steward' }, Date.now());
  viewer = views.analyst;
});

afterEach(() => {
  dispatcher.stop();
  coordinator.simulateCrash();
  db.close();
  receiver.db.close();
});

function subscribe(viewerFor, overrides = {}) {
  const sub = coordinator.upsertSubscription({
    viewer: viewerFor,
    sourceId: 'local-library',
    query: overrides.query ?? 'subscription feed',
    maxAgeMs: overrides.maxAgeMs ?? 60_000,
    staleIfErrorMs: overrides.staleIfErrorMs ?? 300_000,
    callbackUrl: 'http://receiver.test/push',
    secret: overrides.secret ?? newSecret(),
    deliverLatest: false,
  });
  return sub;
}

/** Wire the dispatcher's fetch to the in-process receiver. */
function wireReceiver() {
  dispatcher.fetchImpl = async (url, init) => {
    const subscription = coordinator.subscriptionById(init.headers['x-workbench-subscription']);
    const secretRow = db.prepare('SELECT secret FROM subscriptions WHERE id = ?').get(subscription.id);
    const outcome = receiver.receive({
      rawBody: init.body,
      headers: init.headers,
      subscription: { id: subscription.id, secret: secretRow.secret, role: subscription.role },
    });
    return new Response(outcome.body === null ? null : JSON.stringify(outcome.body), {
      status: outcome.status,
      headers: { 'content-type': 'application/json' },
    });
  };
}

async function produceGeneration(query) {
  const read = await coordinator.read({
    viewer,
    sourceId: 'local-library',
    query,
    maxAgeMs: 60_000,
    staleIfErrorMs: 0,
    clientToken: 'test',
  });
  return read;
}

/* ------------------------------------------------------------------ */

test('one shared fetch fans out to every subscriber with role-specific fields', async () => {
  const query = 'fanned out query';
  subscribe(views.public, { query });
  subscribe(views.analyst, { query });
  subscribe(views.steward, { query });
  wireReceiver();

  const read = await produceGeneration(query);
  assert.equal(read.admission.served, 'source');

  await dispatcher.drain();
  const inbox = receiver.snapshot().inbox;
  assert.equal(inbox.length, 3);

  const byRole = Object.fromEntries(inbox.map((row) => [row.role, row]));
  assert.deepEqual(Object.keys(byRole.public.payload.result), ['title', 'url', 'snippet', 'published_at']);
  assert.deepEqual(Object.keys(byRole.analyst.payload.result), ['title', 'url', 'snippet', 'published_at', 'score', 'crawl_meta']);
  assert.equal(Object.keys(byRole.steward.payload.result).length, 8);
  assert.equal(new Set(inbox.map((row) => row.share_token)).size, 1, 'all subscribers share one document');
  assert.equal(receiver.snapshot().stats.duplicates, 0);
});

test('a delivery is retried until acknowledged and the receiver dedupes the retry', async () => {
  const query = 'retry until ack';
  subscribe(viewer, { query });
  receiver.armFaults({ failNext: 2 });
  wireReceiver();

  await produceGeneration(query);
  await dispatcher.drain();
  let delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'pending');
  assert.equal(delivery.attempts, 1);

  // Backoff is real time; move the due time forward between attempts.
  dispatcher.makeAllDue();
  await dispatcher.drain();
  delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'pending');
  assert.equal(delivery.attempts, 2);

  dispatcher.makeAllDue();
  await dispatcher.drain();
  delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'acked');
  assert.equal(delivery.attempts, 3);
  assert.equal(receiver.snapshot().stats.accepted, 1);
});

test('a lost ack causes a retry that arrives as a duplicate and is absorbed', async () => {
  const query = 'lost ack query';
  subscribe(viewer, { query });
  receiver.armFaults({ loseNextAck: true });
  wireReceiver();

  await produceGeneration(query);
  await dispatcher.drain();
  let delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'pending', 'the sender did not see the ack');

  dispatcher.makeAllDue();
  await dispatcher.drain();
  delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'acked');
  const snapshot = receiver.snapshot();
  assert.equal(snapshot.inbox.length, 1, 'the receiver kept exactly one copy');
  assert.equal(snapshot.inbox[0].delivery_count, 2, 'the retry was observed as a duplicate');
  assert.equal(snapshot.stats.duplicates, 1);
});

test('retries stop at the cap and the delivery is dropped with its last error', async () => {
  const query = 'give up eventually';
  subscribe(viewer, { query });
  receiver.armFaults({ failNext: 20 });
  wireReceiver();

  await produceGeneration(query);
  for (let i = 0; i < 8; i += 1) {
    dispatcher.makeAllDue();
    await dispatcher.drain();
  }
  const delivery = coordinator.subscriptionById(coordinator.listSubscriptions()[0].id).deliveries[0];
  assert.equal(delivery.state, 'dropped');
  assert.ok(delivery.attempts >= 2);
  assert.match(delivery.last_error, /500|receiver/i);
});

test('one receiver failure does not block another subscriptions delivery', async () => {
  const query = 'isolation query';
  const failing = subscribe(views.public, { query });
  const healthy = subscribe(views.analyst, { query });
  wireReceiver();
  const originalFetch = dispatcher.fetchImpl;
  dispatcher.fetchImpl = async (url, init) => {
    if (init.headers['x-workbench-subscription'] === failing.id) {
      return new Response('{"error":"down"}', { status: 500 });
    }
    return originalFetch(url, init);
  };

  await produceGeneration(query);
  await dispatcher.drain();

  const failingDelivery = coordinator.subscriptionById(failing.id).deliveries[0];
  const healthyDelivery = coordinator.subscriptionById(healthy.id).deliveries[0];
  assert.equal(failingDelivery.state, 'pending');
  assert.equal(healthyDelivery.state, 'acked');
});

test('an unsigned or wrongly signed push is rejected and no inbox row is written', () => {
  const sub = subscribe(viewer);
  const secretRow = db.prepare('SELECT secret FROM subscriptions WHERE id = ?').get(sub.id);
  const body = JSON.stringify({
    dedupe_key: 'k1', subscription_id: sub.id, role: 'analyst',
    result: { title: 'x' }, query: { key: 'q' }, task_id: 't', generation: 1, stale: false,
  });

  const bad = receiver.receive({
    rawBody: body,
    headers: {
      'x-workbench-subscription': sub.id,
      'x-workbench-timestamp': String(Date.now()),
      'x-workbench-signature': 'deadbeef',
      'x-workbench-dedupe': 'k1',
    },
    subscription: { id: sub.id, secret: secretRow.secret, role: sub.role },
  });
  assert.equal(bad.status, 401);
  assert.equal(receiver.snapshot().inbox.length, 0);
});

test('the receiver projects a widened payload back down to the subscriptions clearance', () => {
  const sub = subscribe(views.public);
  const secretRow = db.prepare('SELECT secret FROM subscriptions WHERE id = ?').get(sub.id);
  const signed = (body) => {
    const timestamp = String(Date.now());
    return {
      'x-workbench-subscription': sub.id,
      'x-workbench-timestamp': timestamp,
      'x-workbench-signature': receiver.sign(secretRow.secret, timestamp, body),
      'x-workbench-dedupe': JSON.parse(body).dedupe_key,
    };
  };
  // A buggy sender includes restricted fields for a public subscription.
  const body = JSON.stringify({
    dedupe_key: 'k2', subscription_id: sub.id, role: 'public',
    result: {
      title: 'leaky', contact_email: 'secret@example.invalid', matched_terms: ['x'],
      score: 0.9, crawl_meta: { bytes: 1 },
    },
    query: { key: 'q', text: 'leaky', source_id: 'local-library' },
    task_id: 't', generation: 1, stale: false,
  });
  const outcome = receiver.receive({
    rawBody: body,
    headers: signed(body),
    subscription: { id: sub.id, secret: secretRow.secret, role: sub.role },
  });
  assert.equal(outcome.status, 201);
  const stored = receiver.snapshot().inbox[0].payload;
  assert.deepEqual(Object.keys(stored.result), ['title']);
  assert.equal(JSON.stringify(stored.result).includes('secret@example.invalid'), false);
});

test('the delivery dedupe key is stable per subscription and generation', async () => {
  const query = 'dedupe key stability';
  subscribe(viewer, { query });
  wireReceiver();
  await produceGeneration(query);
  const sub = coordinator.listSubscriptions()[0];
  const firstKey = sub.deliveries[0].id;

  // Re-running fan-out for the same generation must not create a second row.
  const before = db.prepare('SELECT COUNT(*) AS n FROM deliveries').get().n;
  const task = db.prepare('SELECT * FROM source_tasks ORDER BY created_at DESC LIMIT 1').get();
  coordinator.insertDeliveryFanout(task, { stale: false, reason: 'duplicate test' });
  const after = db.prepare('SELECT COUNT(*) AS n FROM deliveries').get().n;
  assert.equal(after, before);
  assert.ok(firstKey);
  assert.equal(
    db.prepare('SELECT dedupe_key FROM deliveries').get().dedupe_key,
    `${logicalKey({ sourceId: 'local-library', query, locale: 'en' }).key}:g${task.generation}`,
  );
});

test('deleting a subscription drops its queued deliveries and stops future fan-out', async () => {
  const query = 'cancel my feed';
  const sub = subscribe(viewer, { query });
  await produceGeneration(query);
  assert.equal(coordinator.subscriptionById(sub.id).deliveries[0].state, 'pending');

  coordinator.cancelSubscription({ viewer, subscriptionId: sub.id });
  const after = coordinator.subscriptionById(sub.id);
  assert.equal(after.active, false);
  assert.equal(after.deliveries[0].state, 'dropped');

  const task = db.prepare('SELECT * FROM source_tasks ORDER BY created_at DESC LIMIT 1').get();
  const created = coordinator.insertDeliveryFanout(task, { stale: false, reason: 'after cancel' });
  assert.equal(created, 0);
});

test('subscribing backfills the latest usable generation without an upstream request', async () => {
  const query = 'backfill me';
  await produceGeneration(query);
  const before = db.prepare('SELECT COUNT(*) AS n FROM deliveries').get().n;
  const sub = coordinator.upsertSubscription({
    viewer,
    sourceId: 'local-library',
    query,
    maxAgeMs: 60_000,
    staleIfErrorMs: 0,
    callbackUrl: 'http://receiver.test/push',
    secret: newSecret(),
  });
  const withBackfill = coordinator.subscriptionById(sub.id);
  assert.equal(withBackfill.deliveries.length, before + 1);
  const latest = coordinator.subscriptionById(sub.id).deliveries[0];
  assert.equal(latest.state, 'pending');
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
});

test('the same subscription key is reused on resubscribe rather than duplicated', () => {
  const a = subscribe(viewer);
  const b = subscribe(viewer);
  assert.equal(a.id, b.id);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM subscriptions').get().n, 1);
});

test('subscriptions with different max ages are distinct and get their own deliveries', async () => {
  const query = 'different ages';
  const fast = subscribe(viewer, { query, maxAgeMs: 1_000 });
  const slow = subscribe(viewer, { query, maxAgeMs: 60_000 });
  assert.notEqual(fast.id, slow.id);
  await produceGeneration(query);
  assert.equal(coordinator.subscriptionById(fast.id).deliveries.length, 1);
  assert.equal(coordinator.subscriptionById(slow.id).deliveries.length, 1);
});

test('only a privileged viewer may cancel another viewers subscription', () => {
  const sub = subscribe(viewer);
  assert.throws(
    () => coordinator.cancelSubscription({ viewer: views.public, subscriptionId: sub.id }),
    (err) => err.statusCode === 403,
  );
  const cancelled = coordinator.cancelSubscription({ viewer: views.steward, subscriptionId: sub.id });
  assert.equal(cancelled.active, false);
});

test('projection is recomputed from the stored document, not the subscribers role at one moment', async () => {
  const query = 'projection time';
  subscribe(views.public, { query });
  wireReceiver();
  await produceGeneration(query);
  const document = JSON.parse(
    db.prepare('SELECT payload_json FROM documents ORDER BY fetched_at DESC LIMIT 1').get().payload_json,
  );
  // The stored document is full fidelity.
  assert.ok(document.contact_email);
  // A subscriber's payload is a projection of it.
  await dispatcher.drain();
  const stored = receiver.snapshot().inbox[0].payload;
  assert.equal('contact_email' in stored.result, false);
  assert.deepEqual(
    Object.keys(stored.result),
    Object.keys(projectDocument(document, 'public').result),
  );
});
