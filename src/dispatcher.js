import { createHmac } from 'node:crypto';
import { logEvent } from './events.js';
import { projectDocument, roleProfile } from './permissions.js';

/**
 * Delivery dispatcher.
 *
 * Semantics deliberately chosen:
 *  - At-least-once, never at-most-once. A delivery row is only marked acked
 *    when the receiver answers 2xx *and* we saw that answer. If the ack is lost,
 *    the retry carries the same dedupe key, and the receiver absorbs the
 *    duplicate. A subscription therefore never silently misses a generation.
 *  - Retries are bounded per delivery (maxAttempts) with exponential backoff.
 *    After the cap the delivery is marked dropped with the last error, not
 *    retried forever.
 *  - Every push is projected for the subscription's role immediately before
 *    sending. The projection is recomputed from the stored full document, so a
 *    viewer's clearance is enforced on the outbound path too, not only on reads.
 *  - Each push is HMAC-signed over timestamp + raw body with the
 *    subscription's secret, and names its subscription in a header. The receiver
 *    verifies all three.
 *  - A subscription's own failure never blocks another subscription's delivery:
 *    each delivery is sent independently and failures are recorded per row.
 */

const MAX_ATTEMPTS = 6;
const BACKOFF_MS = [250, 750, 2_000, 5_000, 15_000, 45_000];

export class Dispatcher {
  constructor({ db, clock = Date.now, fetchImpl = fetch, onSent = null }) {
    this.db = db;
    this.clock = clock;
    this.fetchImpl = fetchImpl;
    this.onSent = onSent;
    this.timer = null;
    this.running = false;
    this.inFlight = false;
  }

  start({ intervalMs = 400 } = {}) {
    if (this.timer) return;
    this.running = true;
    this.timer = setInterval(() => {
      this.drain().catch(() => { /* individual failures are recorded */ });
    }, intervalMs);
    this.timer.unref?.();
  }

  stop() {
    this.running = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  /** Make every pending delivery immediately due (used after recovery). */
  makeAllDue(at = this.clock()) {
    return this.db.prepare(`
      UPDATE deliveries SET next_attempt_at = ? WHERE state = 'pending'
    `).run(at).changes;
  }

  dueDeliveries(limit = 8, at = this.clock()) {
    return this.db.prepare(`
      SELECT d.*, s.viewer_id, s.viewer_label, s.role, s.callback_url, s.secret,
             s.query_key AS sub_query_key, s.stale_if_error_ms, s.active AS sub_active
      FROM deliveries d JOIN subscriptions s ON s.id = d.subscription_id
      WHERE d.state = 'pending' AND d.next_attempt_at <= ?
      ORDER BY d.created_at ASC LIMIT ?
    `).all(at, limit);
  }

  async drain({ limit = 8 } = {}) {
    if (this.inFlight) return { skipped: 'already draining' };
    this.inFlight = true;
    const results = [];
    try {
      // Drain whatever is due right now. This is independent of the repeating
      // timer: tests and recovery call it directly, and it must still work when
      // the background loop has not been started.
      for (;;) {
        const batch = this.dueDeliveries(limit);
        if (batch.length === 0) break;
        // Each delivery is independent; one receiver's outage must not hold up
        // another subscription's queue.
        const settled = await Promise.allSettled(batch.map((row) => this.deliver(row)));
        results.push(...settled.map((outcome) => (
          outcome.status === 'fulfilled' ? outcome.value : { error: outcome.reason?.message ?? 'failed' }
        )));
        if (batch.length < limit) break;
      }
    } finally {
      this.inFlight = false;
    }
    return { delivered: results };
  }

  buildPayload(delivery, subscription) {
    const document = this.db.prepare(
      'SELECT * FROM documents WHERE query_key = ? AND generation = ?',
    ).get(delivery.sub_query_key, delivery.generation);
    if (!document) throw new Error(`document for generation ${delivery.generation} is missing`);
    const full = JSON.parse(document.payload_json);
    const profile = roleProfile(subscription.role);
    const projected = projectDocument(full, profile.clearance);
    return {
      dedupe_key: delivery.dedupe_key,
      subscription_id: subscription.id,
      task_id: delivery.task_id,
      generation: delivery.generation,
      share_token: document.share_token,
      stale: Boolean(delivery.stale),
      role: subscription.role,
      clearance: profile.clearance,
      query: {
        key: document.query_key,
        text: document.query_text,
        source_id: document.source_id,
        locale: document.locale,
      },
      result: projected.result,
      withheld_fields: projected.withheld,
      fetched_at: document.fetched_at,
      sent_at: this.clock(),
    };
  }

  sign(secret, timestamp, body) {
    return createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex');
  }

  async deliver(delivery) {
    const at = this.clock();
    if (!delivery.sub_active) {
      this.db.prepare(`
        UPDATE deliveries SET state = 'dropped', last_error = 'subscription inactive'
        WHERE id = ? AND state = 'pending'
      `).run(delivery.id);
      return { id: delivery.id, state: 'dropped', reason: 'subscription inactive' };
    }

    let payload;
    try {
      payload = this.buildPayload(delivery, delivery);
    } catch (err) {
      this.db.prepare(`
        UPDATE deliveries SET state = 'dropped', attempts = attempts + 1, last_error = ?
        WHERE id = ?
      `).run(err.message.slice(0, 400), delivery.id);
      logEvent(this.db, at, {
        kind: 'delivery.dropped',
        taskId: delivery.task_id,
        viewerId: delivery.viewer_id,
        queryKey: delivery.sub_query_key,
        detail: { deliveryId: delivery.id, reason: 'payload unavailable', error: err.message },
      });
      return { id: delivery.id, state: 'dropped', error: err.message };
    }

    const body = JSON.stringify(payload);
    const timestamp = String(at);
    const signature = this.sign(delivery.secret, timestamp, body);
    const attempts = delivery.attempts + 1;

    let response;
    let error = null;
    try {
      response = await this.fetchImpl(delivery.callback_url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-workbench-subscription': delivery.subscription_id,
          'x-workbench-dedupe': delivery.dedupe_key,
          'x-workbench-timestamp': timestamp,
          'x-workbench-signature': signature,
          'x-workbench-attempt': String(attempts),
        },
        body,
        signal: AbortSignal.timeout(8_000),
      });
    } catch (err) {
      error = err?.message ?? 'receiver unreachable';
    }

    if (response && response.ok) {
      let reply = null;
      try { reply = await response.json(); } catch { /* 204 or non-JSON is fine */ }
      this.db.prepare(`
        UPDATE deliveries SET state = 'acked', attempts = ?, sent_at = ?, acked_at = ?,
          last_error = NULL, next_attempt_at = ?
        WHERE id = ?
      `).run(attempts, at, this.clock(), this.clock(), delivery.id);
      logEvent(this.db, this.clock(), {
        kind: 'delivery.acked',
        taskId: delivery.task_id,
        viewerId: delivery.viewer_id,
        queryKey: delivery.sub_query_key,
        detail: {
          deliveryId: delivery.id,
          subscriptionId: delivery.subscription_id,
          generation: delivery.generation,
          attempts,
          duplicate: reply?.duplicate ?? null,
        },
      });
      this.onSent?.({ delivery, payload, response: reply });
      return { id: delivery.id, state: 'acked', attempts, duplicate: reply?.duplicate ?? null };
    }

    const status = response?.status ?? null;
    const detail = error ?? `receiver answered ${status}`;
    if (response) {
      try { await response.text(); } catch { /* ignore */ }
    }
    this.finishAttempt(delivery, at, { ok: false, error: detail, status, attempts, payload });
    return { id: delivery.id, state: attempts >= MAX_ATTEMPTS ? 'dropped' : 'retrying', error: detail, attempts };
  }

  finishAttempt(delivery, at, { ok, error, status = null, attempts = delivery.attempts + 1 }) {
    if (ok) return;
    const capped = attempts >= MAX_ATTEMPTS;
    const backoff = BACKOFF_MS[Math.min(attempts - 1, BACKOFF_MS.length - 1)];
    const next = at + backoff;
    this.db.prepare(`
      UPDATE deliveries SET state = ?, attempts = ?, last_error = ?, next_attempt_at = ?
      WHERE id = ?
    `).run(capped ? 'dropped' : 'pending', attempts, `${status ? `${status}: ` : ''}${error}`.slice(0, 400), next, delivery.id);
    logEvent(this.db, at, {
      kind: capped ? 'delivery.dropped' : 'delivery.retry-scheduled',
      taskId: delivery.task_id,
      viewerId: delivery.viewer_id,
      queryKey: delivery.sub_query_key,
      detail: {
        deliveryId: delivery.id,
        subscriptionId: delivery.subscription_id,
        generation: delivery.generation,
        attempts,
        nextAttemptAt: next,
        error: String(error).slice(0, 300),
      },
    });
  }
}

/**
 * Subscription refresher. Each tick, finds active subscriptions whose cached
 * document is older than their own max age (or which have no document), and
 * starts a keep-alive rebuild for that logical key. Starting a rebuild when an
 * identical one is already running just coalesces onto it, so N subscribers to
 * the same key cause at most one upstream request per generation.
 */
export class Refresher {
  constructor({ coordinator, clock = Date.now, intervalMs = 1_000 }) {
    this.coordinator = coordinator;
    this.clock = clock;
    this.intervalMs = intervalMs;
    this.timer = null;
  }

  start() {
    if (this.timer) return;
    this.timer = setInterval(() => {
      try { this.tick(); } catch { /* retried next tick */ }
    }, this.intervalMs);
    this.timer.unref?.();
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  dueSubscriptions(at = this.clock()) {
    return this.coordinator.db.prepare(`
      SELECT s.*,
             (SELECT d.fetched_at FROM documents d
              WHERE d.query_key = s.query_key AND d.usable = 1
              ORDER BY d.generation DESC LIMIT 1) AS last_fetched_at
      FROM subscriptions s
      WHERE s.active = 1
      ORDER BY s.created_at ASC
    `).all().filter((row) => (
      row.last_fetched_at === null || at - row.last_fetched_at > row.max_age_ms
    ));
  }

  tick() {
    const at = this.clock();
    const due = this.dueSubscriptions(at);
    const started = [];
    const coalesced = [];
    const seen = new Set();
    for (const sub of due) {
      if (seen.has(sub.query_key)) {
        coalesced.push({ subscriptionId: sub.id, queryKey: sub.query_key, reason: 'same key already refreshed this tick' });
        continue;
      }
      seen.add(sub.query_key);
      const viewer = {
        id: sub.viewer_id,
        label: sub.viewer_label,
        role: sub.role,
      };
      const result = this.coordinator.rebuild({
        viewer,
        sourceId: sub.source_id,
        query: sub.query_text,
        locale: sub.locale,
        origin: 'subscription',
        clientToken: `sub:${sub.id}`,
      });
      if (result.startedNew) {
        started.push({ subscriptionId: sub.id, taskId: result.taskId, generation: result.generation });
      } else {
        coalesced.push({ subscriptionId: sub.id, queryKey: sub.query_key, generation: result.generation });
      }
      this.coordinator.db.prepare(
        'UPDATE subscriptions SET next_refresh_at = ?, last_seen_at = ? WHERE id = ?',
      ).run(at + sub.max_age_ms, at, sub.id);
    }
    if (started.length) {
      logEvent(this.coordinator.db, at, {
        kind: 'refresh.tick',
        detail: {
          started: started.length,
          coalesced: coalesced.length,
          due: due.length,
        },
      });
    }
    return { started, coalesced, due: due.length };
  }
}
