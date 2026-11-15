import { test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { openStore } from '../src/store.js';
import { Coordinator } from '../src/coordinator.js';
import { createViewer } from '../src/viewers.js';
import { logicalKey, normalizeQuery, normalizeLocale, QueryValidationError } from '../src/keys.js';
import { projectDocument, FIELD_CATALOG, canReadTier } from '../src/permissions.js';
import { getSource, SourceError } from '../src/sources/index.js';

/**
 * These tests pin the coordination invariants. They use the local source, whose
 * timing is controlled by the query text, so they need no network.
 */

let db;
let coordinator;
let viewers;

function makeViewer(role = 'analyst', label = role) {
  return createViewer(db, { label, role }, Date.now());
}

beforeEach(() => {
  db = openStore(':memory:');
  coordinator = new Coordinator({ db, fetchTimeoutMs: 6_000 });
  viewers = {
    public: makeViewer('public', 'Public Pat'),
    analyst: makeViewer('analyst', 'Analyst Ava'),
    steward: makeViewer('steward', 'Steward Sam'),
  };
});

afterEach(() => {
  try { coordinator.simulateCrash(); } catch { /* nothing in flight */ }
  db.close();
});

const readOptions = (overrides = {}) => ({
  sourceId: 'local-library',
  maxAgeMs: 60_000,
  staleIfErrorMs: 0,
  clientToken: `t:${Math.random().toString(36).slice(2)}`,
  ...overrides,
});

/* ------------------------------------------------------------------ */
/* Key derivation                                                      */
/* ------------------------------------------------------------------ */

test('normalization folds whitespace, case and unicode so equal queries share a key', () => {
  const a = logicalKey({ sourceId: 'local-library', query: 'Web   Crawler', locale: 'EN' });
  const b = logicalKey({ sourceId: 'local-library', query: ' web crawler ', locale: 'en' });
  assert.equal(a.key, b.key);
  assert.equal(a.query, 'web crawler');
  assert.equal(a.locale, 'en');
});

test('every dimension that can change source bytes is part of the key', () => {
  const base = { sourceId: 'local-library', query: 'crawler', locale: 'en' };
  const keys = [
    logicalKey(base).key,
    logicalKey({ ...base, sourceId: 'wikipedia' }).key,
    logicalKey({ ...base, query: 'crawler 2' }).key,
    logicalKey({ ...base, locale: 'de' }).key,
  ];
  assert.equal(new Set(keys).size, 4);
});

test('case folding is per source: free text folds, a URL query does not', () => {
  // Search text is case-insensitive, so the same words share one entry.
  assert.equal(
    logicalKey({ sourceId: 'wikipedia', query: 'Web Crawler', locale: 'en' }).key,
    logicalKey({ sourceId: 'wikipedia', query: 'web crawler', locale: 'en' }).key,
  );
  // A page URL is not: these are two different pages and must not coalesce.
  const upper = logicalKey({ sourceId: 'page', query: 'https://en.wikipedia.org/wiki/Web_crawler', locale: 'en', caseSensitiveQuery: true });
  const lower = logicalKey({ sourceId: 'page', query: 'https://en.wikipedia.org/wiki/web_crawler', locale: 'en', caseSensitiveQuery: true });
  assert.equal(upper.query, 'https://en.wikipedia.org/wiki/Web_crawler');
  assert.notEqual(upper.key, lower.key);
  // Whitespace around it is still collapsed, so a pasted URL with a trailing
  // newline is the same entry.
  assert.equal(
    upper.key,
    logicalKey({ sourceId: 'page', query: '  https://en.wikipedia.org/wiki/Web_crawler \n', locale: 'en', caseSensitiveQuery: true }).key,
  );
});

test('invalid queries and locales are rejected before any work is scheduled', () => {
  assert.throws(() => normalizeQuery('   '), QueryValidationError);
  assert.throws(() => normalizeQuery('x'.repeat(241)), QueryValidationError);
  assert.throws(() => normalizeLocale('not a locale!'), QueryValidationError);
  assert.equal(normalizeLocale(undefined), 'en');
  assert.equal(normalizeLocale('PT_br'), 'pt-br');
});

/* ------------------------------------------------------------------ */
/* Permissions                                                         */
/* ------------------------------------------------------------------ */

test('the same document projects differently per clearance and never leaks values', () => {
  const full = {
    title: 'T', url: 'u', snippet: 's', published_at: 'p',
    score: 1, crawl_meta: { bytes: 1 }, contact_email: 'secret@example.invalid', matched_terms: ['x'],
  };
  // projectDocument takes a clearance tier, not a role name.
  const publicView = projectDocument(full, 'public');
  const analystView = projectDocument(full, 'internal');
  const stewardView = projectDocument(full, 'restricted');

  assert.deepEqual(Object.keys(publicView.result), ['title', 'url', 'snippet', 'published_at']);
  assert.equal('contact_email' in analystView.result, false);
  assert.equal(stewardView.result.contact_email, 'secret@example.invalid');
  assert.ok(publicView.withheld.some((entry) => entry.field === 'contact_email'));
  // Withheld fields are omitted, not nulled, so even their values cannot leak.
  assert.equal(JSON.stringify(publicView.result).includes('secret@example.invalid'), false);
  assert.equal(Object.keys(FIELD_CATALOG).length, 8);
  assert.equal(canReadTier('restricted', 'internal'), false);
});

/* ------------------------------------------------------------------ */
/* Single flight and caching                                           */
/* ------------------------------------------------------------------ */

test('concurrent readers of one query cause exactly one upstream fetch', async () => {
  const query = 'slow 600 concurrent readers';
  const started = Date.now();
  const results = await Promise.all([
    coordinator.read({ viewer: viewers.public, ...readOptions({ query }) }),
    coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) }),
    coordinator.read({ viewer: viewers.steward, ...readOptions({ query }) }),
  ]);
  const elapsed = Date.now() - started;

  assert.ok(elapsed < 1_800, `expected one 600ms fetch, took ${elapsed}ms`);
  assert.equal(new Set(results.map((r) => r.admission.share_token)).size, 1);
  assert.equal(new Set(results.map((r) => r.admission.generation)).size, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM documents').get().n, 1);

  // Different roles, different fields, identical bytes.
  assert.deepEqual(Object.keys(results[0].result), ['title', 'url', 'snippet', 'published_at']);
  assert.deepEqual(Object.keys(results[1].result), ['title', 'url', 'snippet', 'published_at', 'score', 'crawl_meta']);
  assert.deepEqual(Object.keys(results[2].result), Object.keys(FIELD_CATALOG));
});

test('a repeat read inside the viewer max age is a cache hit and is counted as avoided', async () => {
  const query = 'cache me';
  const first = await coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  const second = await coordinator.read({
    viewer: viewers.public, ...readOptions({ query }), maxAgeMs: 60_000,
  });

  assert.equal(first.admission.served, 'source');
  assert.equal(second.admission.served, 'cache');
  assert.equal(second.admission.share_token, first.admission.share_token);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
  assert.equal(coordinator.stats().upstream_requests_avoided, 1);
});

test('freshness is judged per viewer, not per document', async () => {
  const query = 'per viewer age';
  await coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }), maxAgeMs: 60_000 });
  // Move the clock forward for one viewer's read by using a tiny max age. The
  // document is milliseconds old, so use a fake clock on a second coordinator
  // to make the distinction deterministic instead of sleeping.
  let now = Date.now();
  const timed = new Coordinator({ db, clock: () => now, fetchTimeoutMs: 6_000 });
  now += 5_000;
  const strict = await timed.read({
    viewer: viewers.public, ...readOptions({ query }), maxAgeMs: 1_000, staleIfErrorMs: 0,
  });
  assert.equal(strict.admission.served, 'source');
  now += 10;
  const lenient = await timed.read({
    viewer: viewers.analyst, ...readOptions({ query }), maxAgeMs: 60_000,
  });
  assert.equal(lenient.admission.served, 'cache');
  assert.equal(lenient.admission.generation, strict.admission.generation);
  coordinator.simulateCrash();
  timed.simulateCrash();
  await timed.close?.();
});

/* ------------------------------------------------------------------ */
/* Cancellation                                                        */
/* ------------------------------------------------------------------ */

test('one viewer leaving does not kill work another viewer is waiting on', async () => {
  const query = 'slow 900 shared work';
  const key = logicalKey({ sourceId: 'local-library', query, locale: 'en' }).key;
  const first = coordinator.read({ viewer: viewers.public, ...readOptions({ query }) });
  const second = coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  first.catch(() => {});

  await waitFor(() => coordinator.activeTaskFor(key)?.ref_count === 2);
  const taskId = coordinator.activeTaskFor(key).id;
  const left = coordinator.leaveTask({ viewer: viewers.public, taskId, reason: 'closed tab' });

  assert.equal(left.closed, true);
  assert.equal(left.stillWaiting, 1);
  assert.equal(left.aborted, false);
  assert.equal(coordinator.taskById(taskId).state, 'running');

  const result = await second;
  assert.equal(result.admission.served, 'source');
  assert.equal(coordinator.taskById(taskId).state, 'succeeded');
});

test('the last viewer leaving cancels the task, and a late completion is stored but not usable', async () => {
  const query = 'late 500 last one out';
  const key = logicalKey({ sourceId: 'local-library', query, locale: 'en' }).key;
  const pending = coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  pending.catch(() => {});

  await waitFor(() => coordinator.activeTaskFor(key));
  const taskId = coordinator.activeTaskFor(key).id;
  const left = coordinator.leaveTask({ viewer: viewers.analyst, taskId, reason: 'navigated away' });
  assert.equal(left.aborted, true);
  assert.equal(left.task.state, 'cancelled');

  await assert.rejects(pending, (err) => err.code === 'cancelled');

  // The local "late" source finishes anyway.
  await waitFor(() => db.prepare(
    'SELECT 1 AS ok FROM events WHERE kind = ? AND task_id = ?',
  ).get('task.late-completion', taskId), 2_500);

  const document = db.prepare(
    'SELECT generation, usable FROM documents WHERE query_key = ?',
  ).get(key);
  assert.ok(document, 'the late bytes are stored for audit');
  assert.equal(document.usable, 0);
  assert.equal(coordinator.taskById(taskId).state, 'cancelled');

  // A later read must not serve the late document as fresh; it starts a new
  // generation instead.
  const next = await coordinator.read({
    viewer: viewers.analyst, ...readOptions({ query: 'late 0 last one out' }),
  });
  assert.equal(next.admission.served, 'source');
  assert.equal(next.admission.usable_as_fresh, true);
});

test('only the starter or a privileged role may cancel a whole task', async () => {
  const query = 'slow 1200 permission to cancel';
  const key = logicalKey({ sourceId: 'local-library', query, locale: 'en' }).key;
  const owner = viewers.analyst;
  const pending = coordinator.read({ viewer: owner, ...readOptions({ query }) });
  pending.catch(() => {});
  await waitFor(() => coordinator.activeTaskFor(key));
  const taskId = coordinator.activeTaskFor(key).id;

  assert.throws(
    () => coordinator.cancelTask({ viewer: viewers.public, taskId }),
    (err) => err.code === 'not_your_task' && err.statusCode === 403,
  );
  // A privileged viewer may cancel someone else's work, explicitly.
  const cancelled = coordinator.cancelTask({ viewer: viewers.steward, taskId, reason: 'drill' });
  assert.equal(cancelled.changed, true);
  assert.equal(cancelled.task.state, 'cancelled');
});

/* ------------------------------------------------------------------ */
/* Expiry and stale policy                                             */
/* ------------------------------------------------------------------ */

test('a rebuild creates the next generation and keeps the old document readable', async () => {
  const query = 'generation growth';
  const first = await coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  const rebuilt = coordinator.rebuild({
    viewer: viewers.analyst, sourceId: 'local-library', query, locale: 'en',
  });
  const finished = await rebuilt.done;

  assert.equal(rebuilt.generation, first.admission.generation + 1);
  assert.equal(first.admission.generation, 1);
  assert.equal(finished.generation, 2);
  const generations = db.prepare(
    'SELECT generation FROM documents WHERE query_key = ? ORDER BY generation',
  ).all(first.query.key).map((row) => row.generation);
  assert.deepEqual(generations, [1, 2]);

  const historical = coordinator.readGeneration({
    viewer: viewers.public,
    queryKey: first.query.key,
    generation: 1,
  });
  assert.equal(historical.admission.served, 'stale');
  assert.equal(historical.admission.stale, true);
  assert.equal(historical.admission.generation, 1);
  assert.equal(historical.admission.share_token, first.admission.share_token);
});

test('a failed rebuild serves the previous generation inside the stale window, flagged', async () => {
  const query = 'stale fallback pinned';
  const first = await coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  // Age the document so the next read wants a rebuild.
  db.prepare('UPDATE documents SET fetched_at = fetched_at - ?').run(5_000);

  const source = getSource('local-library');
  const originalFetch = source.fetch;
  source.fetch = async () => {
    throw new SourceError('synthetic outage', { retryable: false });
  };
  try {
    const result = await coordinator.read({
      viewer: viewers.public,
      ...readOptions({ query }),
      maxAgeMs: 1_000,
      staleIfErrorMs: 60_000,
    });
    assert.equal(result.admission.served, 'stale');
    assert.equal(result.admission.stale, true);
    assert.equal(result.admission.generation, first.admission.generation);
    assert.equal(result.admission.usable_as_fresh, false);
    assert.match(result.note, /stale window/i);
    assert.ok(result.withheld_fields.length > 0, 'stale reads still enforce permissions');
  } finally {
    source.fetch = originalFetch;
  }

  // With no stale window the same failure must surface, not be papered over.
  db.prepare('UPDATE documents SET fetched_at = fetched_at - ?').run(5_000);
  source.fetch = async () => { throw new SourceError('synthetic outage', { retryable: false }); };
  try {
    await assert.rejects(
      coordinator.read({
        viewer: viewers.public, ...readOptions({ query }), maxAgeMs: 1_000, staleIfErrorMs: 0,
      }),
      (err) => err.code === 'source_failed',
    );
  } finally {
    source.fetch = originalFetch;
  }
});

/* ------------------------------------------------------------------ */
/* Recovery                                                            */
/* ------------------------------------------------------------------ */

test('recovery adopts an interrupted task instead of starting a second one', async () => {
  const query = 'slow 1200 interrupted';
  const pending = coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  pending.catch(() => {});
  await waitFor(() => coordinator.activeTaskFor(logicalKey({ sourceId: 'local-library', query, locale: 'en' }).key));
  const before = db.prepare('SELECT id, generation FROM source_tasks').all();

  coordinator.simulateCrash();
  assert.equal(db.prepare('SELECT state FROM source_tasks').get().state, 'running');

  const report = coordinator.recover({ maxAdoptions: 25 });
  assert.equal(report.new_tasks_created, 0);
  assert.equal(report.adopted.length, 1);
  assert.equal(report.adopted[0].taskId, before[0].id);

  await waitFor(() => coordinator.taskById(before[0].id).state === 'succeeded', 4_000);
  assert.deepEqual(
    db.prepare('SELECT id, generation FROM source_tasks').all(),
    before,
    'recovery must not create a generation',
  );
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
});

test('a lost success notification is finished from the stored receipt without refetching', async () => {
  const query = 'lost notification';
  coordinator.armLostNotification();
  const pending = coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) });
  await assert.rejects(pending, (err) => err.code === 'process_lost' || err.message.includes('process loss'));

  const task = db.prepare('SELECT * FROM source_tasks').get();
  assert.equal(task.state, 'running');
  const receipt = db.prepare('SELECT * FROM source_receipts WHERE task_id = ?').get(task.id);
  assert.ok(receipt, 'the source answer was receipted before the notification was lost');
  assert.equal(receipt.consumed, 0);

  const report = coordinator.recover();
  assert.equal(report.receipts_finished.length, 1);
  assert.equal(report.receipts_finished[0].taskId, task.id);
  assert.equal(coordinator.taskById(task.id).state, 'succeeded');
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM source_receipts').get().n, 1);
  assert.equal(db.prepare('SELECT consumed FROM source_receipts').get().consumed, 1);
});

test('recovery is bounded and fails a task that outlived its deadline', async () => {
  const at = Date.now();
  const logical = logicalKey({ sourceId: 'local-library', query: 'deadlined', locale: 'en' });
  db.prepare(`
    INSERT INTO source_tasks (id, query_key, generation, source_id, query_text, locale,
      state, origin, ref_count, attempt, max_attempts, created_at, deadline_at,
      started_by, updated_at)
    VALUES ('task_dead', ?, 1, 'local-library', 'deadlined', 'en',
      'running', 'live', 1, 1, 3, ?, ?, NULL, ?)
  `).run(logical.key, at, at - 1_000, at);

  const report = coordinator.recover();
  assert.equal(report.adopted.length, 0);
  assert.equal(report.failed.length, 1);
  assert.equal(coordinator.taskById('task_dead').state, 'failed');
});

test('recovery caps how many tasks it adopts in one pass', async () => {
  const at = Date.now();
  for (let i = 0; i < 5; i += 1) {
    const logical = logicalKey({ sourceId: 'local-library', query: `cap query ${i}`, locale: 'en' });
    db.prepare(`
      INSERT INTO source_tasks (id, query_key, generation, source_id, query_text, locale,
        state, origin, ref_count, attempt, max_attempts, created_at, deadline_at,
        started_by, updated_at)
      VALUES (?, ?, 1, 'local-library', ?, 'en', 'running', 'live', 1, 1, 3, ?, ?, NULL, ?)
    `).run(`task_cap_${i}`, logical.key, `cap query ${i}`, at - i, at + 60_000, at);
  }
  const report = coordinator.recover({ maxAdoptions: 2 });
  assert.equal(report.adopted.length, 2);
  assert.equal(report.skipped.length, 3);
  // Exactly the cap were started, not all five.
  assert.equal(coordinator.flights.size, 2);
});

/* ------------------------------------------------------------------ */
/* Savings accounting                                                  */
/* ------------------------------------------------------------------ */

test('the ledger attributes one upstream request to many viewers', async () => {
  const query = 'ledger query';
  const [a, b, c] = await Promise.all([
    coordinator.read({ viewer: viewers.analyst, ...readOptions({ query }) }),
    coordinator.read({ viewer: viewers.steward, ...readOptions({ query }) }),
    coordinator.read({ viewer: viewers.public, ...readOptions({ query }) }),
  ]);
  assert.equal(new Set([a, b, c].map((r) => r.admission.share_token)).size, 1);
  await coordinator.read({ viewer: viewers.public, ...readOptions({ query }) });

  const insight = coordinator.insights().find((entry) => entry.query === query);
  assert.equal(insight.upstream_requests, 1);
  assert.equal(insight.tasks_created, 1);
  assert.equal(insight.readers_coalesced, 2);
  assert.equal(insight.cache_reads, 1);
  assert.equal(insight.upstream_requests_avoided, 3);
  assert.equal(insight.distinct_viewers, 3);

  const stats = coordinator.stats();
  assert.equal(stats.upstream_requests, 1);
  assert.equal(stats.upstream_requests_avoided, 3);
  assert.equal(stats.hit_rate, 0.25);
});

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

async function waitFor(predicate, timeoutMs = 3_000) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = predicate();
      if (value) return value;
    } catch (err) { last = err; }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw last ?? new Error('waitFor timed out');
}
