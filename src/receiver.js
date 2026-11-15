import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { newId } from './ids.js';
import { projectDocument, roleProfile } from './permissions.js';
import { deliveryDedupeKey } from './keys.js';

/**
 * Receiver. This is the other end of a durable subscription: an HTTP endpoint
 * the dispatcher pushes to. One receiver can host many subscriptions with
 * different clearances, so it is deliberately a separate SQLite file from the
 * server: a receiver that was down, slow, or restarted cannot corrupt the
 * sender's state and vice versa.
 *
 * Properties that make it useful for testing push semantics:
 *  - Idempotent on (subscription_id, dedupe_key). The dispatcher guarantees
 *    at-least-once, so the receiver must tolerate duplicates. Dedupe is scoped
 *    per subscription: two subscriptions to the same query legitimately receive
 *    the same generation and must not swallow each other's copy.
 *  - HMAC-authenticated per subscription, so one subscription's payload cannot
 *    be forged into another's inbox.
 *  - Each payload is projected for that subscription's role at the sender. The
 *    receiver re-projects for the SAME subscription's clearance as a defensive
 *    check, so a misconfigured or replaying sender still cannot widen what that
 *    subscription may see. It never projects down to a global sink clearance:
 *    a steward subscription's fields must not be stripped by a public one.
 *  - Fault injection is armed through the UI: "fail next N deliveries" answers
 *    500 so the dispatcher retries, and "lose next ack" stores the notification
 *    but reports failure, so the sender retries and the duplicate is visible.
 */

const RECEIVER_SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 4000;

CREATE TABLE IF NOT EXISTS inbox (
  subscription_id   TEXT NOT NULL,
  dedupe_key        TEXT NOT NULL,
  task_id           TEXT NOT NULL,
  generation        INTEGER NOT NULL,
  share_token       TEXT,
  query_key         TEXT NOT NULL,
  query_text        TEXT NOT NULL,
  source_id         TEXT NOT NULL,
  role              TEXT NOT NULL,
  stale             INTEGER NOT NULL DEFAULT 0,
  payload_json      TEXT NOT NULL,
  first_received_at INTEGER NOT NULL,
  last_received_at  INTEGER NOT NULL,
  delivery_count    INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (subscription_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_inbox_order ON inbox(last_received_at DESC);

CREATE TABLE IF NOT EXISTS receiver_events (
  seq      INTEGER PRIMARY KEY AUTOINCREMENT,
  at       INTEGER NOT NULL,
  kind     TEXT NOT NULL,
  dedupe_key TEXT,
  subscription_id TEXT,
  detail   TEXT
);
`;

const HMAC_WINDOW_MS = 5 * 60_000;

export class Receiver {
  constructor({ filename, clock = Date.now }) {
    if (filename !== ':memory:') mkdirSync(dirname(filename), { recursive: true });
    this.db = new DatabaseSync(filename);
    this.db.exec(RECEIVER_SCHEMA);
    this.clock = clock;
    this.faults = { failNext: 0, loseNextAck: false };
  }

  armFaults({ failNext = 0, loseNextAck = false }) {
    this.faults.failNext = Math.max(0, Math.min(Number(failNext) || 0, 20));
    this.faults.loseNextAck = Boolean(loseNextAck);
    return { ...this.faults };
  }

  sign(secret, timestamp, body) {
    return createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
  }

  verify(secret, timestamp, body, signature) {
    const ts = Number(timestamp);
    if (!Number.isFinite(ts) || Math.abs(this.clock() - ts) > HMAC_WINDOW_MS) return false;
    const expected = Buffer.from(this.sign(secret, timestamp, body), 'utf8');
    const given = Buffer.from(String(signature ?? ''), 'utf8');
    return expected.length === given.length && timingSafeEqual(expected, given);
  }

  event(kind, detail, extra = {}) {
    this.db.prepare(`
      INSERT INTO receiver_events (at, kind, dedupe_key, subscription_id, detail)
      VALUES (?, ?, ?, ?, ?)
    `).run(this.clock(), kind, extra.dedupeKey ?? null, extra.subscriptionId ?? null, JSON.stringify(detail));
  }

  /**
   * Accept one push. Returns { status, body } so the HTTP layer stays thin.
   * The caller must pass the raw body string used for the HMAC check.
   */
  receive({ rawBody, headers, subscription, fault = null }) {
    const at = this.clock();
    const subscriptionId = headers['x-workbench-subscription'];
    const timestamp = headers['x-workbench-timestamp'];
    const signature = headers['x-workbench-signature'];
    const dedupeKey = headers['x-workbench-dedupe'];

    if (!subscription || !subscription.secret) {
      this.event('reject.unknown-subscription', { subscriptionId });
      return { status: 404, body: { error: 'unknown subscription' } };
    }
    if (subscription.id !== subscriptionId) {
      this.event('reject.subscription-mismatch', { subscriptionId, expected: subscription.id });
      return { status: 409, body: { error: 'subscription header does not match the signed subscription' } };
    }
    if (!this.verify(subscription.secret, timestamp, rawBody, signature)) {
      this.event('reject.bad-signature', { subscriptionId });
      return { status: 401, body: { error: 'bad or expired signature' } };
    }

    let payload;
    try {
      payload = JSON.parse(rawBody);
    } catch {
      this.event('reject.bad-json', { subscriptionId });
      return { status: 400, body: { error: 'body is not JSON' } };
    }
    if (!dedupeKey || dedupeKey !== payload.dedupe_key) {
      this.event('reject.dedupe-mismatch', { subscriptionId, header: dedupeKey });
      return { status: 400, body: { error: 'dedupe key missing or mismatched' } };
    }

    if (fault === '500' || this.faults.failNext > 0) {
      if (fault !== '500') this.faults.failNext -= 1;
      this.event('fault.injected-500', { dedupeKey }, { dedupeKey, subscriptionId });
      return { status: 500, body: { error: 'injected receiver failure; the sender should retry' } };
    }

    const existing = this.db.prepare(
      'SELECT * FROM inbox WHERE subscription_id = ? AND dedupe_key = ?',
    ).get(subscription.id, dedupeKey);
    if (existing) {
      this.db.prepare(`
        UPDATE inbox SET delivery_count = delivery_count + 1, last_received_at = ?
        WHERE subscription_id = ? AND dedupe_key = ?
      `).run(at, subscription.id, dedupeKey);
      this.event('receive.duplicate', {
        dedupeKey,
        deliveryCount: existing.delivery_count + 1,
        firstReceivedAt: existing.first_received_at,
      }, { dedupeKey, subscriptionId });
      return {
        status: 200,
        body: {
          accepted: true,
          duplicate: true,
          dedupe_key: dedupeKey,
          delivery_count: existing.delivery_count + 1,
          first_received_at: existing.first_received_at,
        },
      };
    }

    const expectedRole = subscription.role;
    if (payload.role !== expectedRole) {
      this.event('reject.role-mismatch', {
        dedupeKey,
        payloadRole: payload.role,
        subscriptionRole: expectedRole,
      }, { dedupeKey, subscriptionId });
      return { status: 409, body: { error: 'payload role does not match the subscription role' } };
    }

    // Defence in depth: project again at the SUBSCRIPTION's own clearance. If a
    // sender ever broadens a payload, the sink still refuses fields that
    // subscription may not see. Never project down to a single sink-wide
    // clearance, or a public subscription would strip a steward's fields.
    const projected = projectDocument(payload.result ?? {}, roleProfile(subscription.role).clearance);
    this.db.prepare(`
      INSERT INTO inbox (
        subscription_id, dedupe_key, task_id, generation, share_token, query_key,
        query_text, source_id, role, stale, payload_json, first_received_at,
        last_received_at, delivery_count
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    `).run(
      subscription.id, dedupeKey, payload.task_id, payload.generation,
      payload.share_token ?? null, payload.query?.key ?? 'unknown',
      payload.query?.text ?? '', payload.query?.source_id ?? 'unknown',
      payload.role, payload.stale ? 1 : 0,
      JSON.stringify({ ...payload, result: projected.result, withheld_fields: projected.withheld }),
      at, at,
    );
    this.event('receive.accepted', {
      dedupeKey,
      generation: payload.generation,
      stale: Boolean(payload.stale),
      shareToken: payload.share_token,
    }, { dedupeKey, subscriptionId });

    if (this.faults.loseNextAck) {
      // Stored, but the ack is lost: answer as if the request failed. The
      // sender retries with the same dedupe key; the duplicate surfaces above.
      this.faults.loseNextAck = false;
      this.event('fault.ack-lost', { dedupeKey }, { dedupeKey, subscriptionId });
      return { status: 503, body: { error: 'stored, but this ack was injected as lost', stored: true } };
    }

    return {
      status: 201,
      body: {
        accepted: true,
        duplicate: false,
        dedupe_key: dedupeKey,
        generation: payload.generation,
      },
    };
  }

  snapshot() {
    const rows = this.db.prepare(`
      SELECT * FROM inbox ORDER BY last_received_at DESC LIMIT 60
    `).all().map((row) => ({
      subscription_id: row.subscription_id,
      dedupe_key: row.dedupe_key,
      subscription_id: row.subscription_id,
      task_id: row.task_id,
      generation: row.generation,
      share_token: row.share_token,
      query_key: row.query_key,
      query: row.query_text,
      source_id: row.source_id,
      role: row.role,
      stale: Boolean(row.stale),
      delivery_count: row.delivery_count,
      first_received_at: row.first_received_at,
      last_received_at: row.last_received_at,
      payload: JSON.parse(row.payload_json),
    }));
    const events = this.db.prepare(`
      SELECT seq, at, kind, dedupe_key, subscription_id, detail
      FROM receiver_events ORDER BY seq DESC LIMIT 80
    `).all().map((row) => ({
      seq: row.seq,
      at: row.at,
      kind: row.kind,
      dedupe_key: row.dedupe_key,
      subscription_id: row.subscription_id,
      detail: row.detail ? JSON.parse(row.detail) : null,
    }));
    const stats = {
      accepted: this.db.prepare(`SELECT COUNT(*) AS n FROM inbox`).get().n,
      duplicates: this.db.prepare(`
        SELECT COALESCE(SUM(delivery_count - 1), 0) AS n FROM inbox
      `).get().n,
      rejected: this.db.prepare(`
        SELECT COUNT(*) AS n FROM receiver_events WHERE kind LIKE 'reject.%'
      `).get().n,
      faults: this.db.prepare(`
        SELECT COUNT(*) AS n FROM receiver_events WHERE kind LIKE 'fault.%'
      `).get().n,
    };
    return { at: this.clock(), inbox: rows, events, stats };
  }

  clear() {
    this.db.exec('DELETE FROM inbox; DELETE FROM receiver_events;');
  }
}

export function newSecret() {
  return `whsec_${randomBytes(24).toString('base64url')}`;
}

export function newDedupeProbe() {
  return deliveryDedupeKey(newId('probe'), 1);
}
