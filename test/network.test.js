import { test } from 'node:test';
import assert from 'node:assert/strict';
import { openStore } from '../src/store.js';
import { Coordinator } from '../src/coordinator.js';
import { createViewer } from '../src/viewers.js';
import { getSource } from '../src/sources/index.js';

/**
 * Real sources over the network. These are gated: if the environment cannot
 * reach the public internet the tests are skipped, never failed, so the suite
 * stays useful offline.
 */

async function reachable(url, timeoutMs = 15_000) {
  try {
    // GET, not HEAD: some origins reject HEAD, and this environment needs a
    // slow TLS handshake before the first request succeeds.
    const response = await fetch(url, {
      method: 'GET',
      signal: AbortSignal.timeout(timeoutMs),
      headers: { 'user-agent': 'QueryDedupeWorkbench/1.0 (connectivity probe)' },
    });
    await response.body?.cancel().catch(() => {});
    return response.status < 500;
  } catch {
    return false;
  }
}

const wikipediaUp = await reachable('https://en.wikipedia.org/wiki/Main_Page');
const duckduckgoUp = await reachable('https://html.duckduckgo.com/html/');

test('the Wikipedia adapter returns parsed search results over HTTPS', { skip: !wikipediaUp }, async () => {
  const db = openStore(':memory:');
  const coordinator = new Coordinator({ db, fetchTimeoutMs: 25_000 });
  const viewer = createViewer(db, { label: 'Net', role: 'public' }, Date.now());
  try {
    const read = await coordinator.read({
      viewer,
      sourceId: 'wikipedia',
      query: 'web crawler',
      maxAgeMs: 600_000,
      staleIfErrorMs: 0,
      clientToken: 'net-wikipedia',
    });
    assert.equal(read.admission.served, 'source');
    assert.equal(read.query.source_id, 'wikipedia');
    assert.equal(typeof read.result.title, 'string');
    assert.ok(read.result.title.length > 0);
    assert.match(read.result.url, /^https?:\/\//);
    // Public clearance means the full document exists but restricted fields
    // were never even present in the projection.
    assert.equal('contact_email' in read.result, false);

    // The same logical query served again must come from cache, not the network.
    const again = await coordinator.read({
      viewer,
      sourceId: 'wikipedia',
      query: '  WEB   crawler ',
      maxAgeMs: 600_000,
      staleIfErrorMs: 0,
      clientToken: 'net-wikipedia-2',
    });
    assert.equal(again.admission.served, 'cache');
    assert.equal(again.admission.share_token, read.admission.share_token);
    assert.equal(coordinator.stats().upstream_requests, 1);
  } finally {
    coordinator.simulateCrash();
    db.close();
  }
});

test('the DuckDuckGo HTML adapter returns results with unwrapped links', { skip: !duckduckgoUp }, async () => {
  const db = openStore(':memory:');
  const coordinator = new Coordinator({ db, fetchTimeoutMs: 25_000 });
  const viewer = createViewer(db, { label: 'Net', role: 'analyst' }, Date.now());
  try {
    const read = await coordinator.read({
      viewer,
      sourceId: 'duckduckgo',
      query: 'node.js sqlite',
      maxAgeMs: 600_000,
      staleIfErrorMs: 0,
      clientToken: 'net-ddg',
    });
    assert.equal(read.admission.served, 'source');
    assert.ok(read.result.title.length > 0);
    if (read.result.url) {
      assert.equal(read.result.url.includes('uddg='), false, 'redirect links are unwrapped');
    }
  } finally {
    coordinator.simulateCrash();
    db.close();
  }
});

test('the page adapter fetches and extracts a real page', { skip: !wikipediaUp }, async () => {
  const db = openStore(':memory:');
  const coordinator = new Coordinator({ db, fetchTimeoutMs: 25_000 });
  const viewer = createViewer(db, { label: 'Net', role: 'public' }, Date.now());
  try {
    const read = await coordinator.read({
      viewer,
      sourceId: 'page',
      query: 'https://en.wikipedia.org/wiki/Web_crawler',
      maxAgeMs: 600_000,
      staleIfErrorMs: 0,
      clientToken: 'net-page',
    });
    assert.equal(read.result.url, 'https://en.wikipedia.org/wiki/Web_crawler');
    assert.match(read.result.title, /crawler/i);
    assert.ok(getSource('page'));
  } finally {
    coordinator.simulateCrash();
    db.close();
  }
});
