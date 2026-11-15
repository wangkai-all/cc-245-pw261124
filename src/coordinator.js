import { hostname } from 'node:os';
import { newId } from './ids.js';
import { logEvent, recentEvents } from './events.js';
import { transaction } from './store.js';
import {
  buildDocument,
  projectDocument,
  roleProfile,
  visibleFields,
  withheldFields,
} from './permissions.js';
import {
  deliveryDedupeKey,
  logicalKey,
  QueryValidationError,
} from './keys.js';
import {
  getSource,
  sourceDefaultMaxAgeMs,
  SourceCancelledError,
  SourceError,
} from './sources/index.js';

/**
 * Task coordination.
 *
 * Sharing unit: (source, normalized query, locale) — the logical key — plus a
 * generation number for the bytes produced by one upstream task.
 *
 *  - Live work is single-flight per logical key: while a generation is queued
 *    or running, new readers attach to it instead of starting another fetch.
 *  - A completed document is cached per generation. A read is a cache hit when
 *    now - fetched_at <= the *viewer's own* max age, so two viewers can share
 *    the same bytes and still disagree about whether those bytes are fresh.
 *  - A rebuild creates generation + 1. The previous generation's document stays
 *    cached and remains readable under the stale policy if the rebuild fails.
 *  - Cancellation is per subscriber. A viewer leaving closes only its own row.
 *    The task is aborted only when nobody is waiting and it is not keep-alive.
 *    A cancelled task that completes anyway is a "late completion": its bytes
 *    are stored, marked unusable and never served as fresh cache, and the task
 *    stays cancelled, so the next reader starts a new generation instead of
 *    silently consuming work everyone had already given up on.
 *
 * This module owns the DB and the in-process flight map. The server owns HTTP.
 */

export const MAX_STALE_MS = 6 * 60 * 60_000;
const MAX_MAX_AGE_MS = 30 * 24 * 60 * 60_000;

export class CoordinatorError extends Error {
  constructor(message, { status = 400, code = 'bad_request' } = {}) {
    super(message);
    this.name = 'CoordinatorError';
    this.statusCode = status;
    this.code = code;
  }
}

/** Thrown by the crash failpoint to model a process dying mid-task. */
export class CrashSignal extends Error {
  constructor(taskId) {
    super(`simulated process loss during task ${taskId}`);
    this.name = 'CrashSignal';
    this.taskId = taskId;
    // A caller whose read was interrupted by the process dying should learn
    // that the work is recoverable, not receive a generic internal error.
    this.statusCode = 503;
    this.code = 'process_lost';
  }
}

function requireSource(sourceId) {
  const source = getSource(sourceId);
  if (!source) throw new CoordinatorError(`unknown source: ${sourceId}`, { status: 404, code: 'unknown_source' });
  return source;
}

function parseMaxAge(raw, sourceId) {
  if (raw === undefined || raw === null || raw === '') return sourceDefaultMaxAgeMs(sourceId);
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0 || value > MAX_MAX_AGE_MS) {
    throw new CoordinatorError(`max_age_ms must be between 0 and ${MAX_MAX_AGE_MS}`);
  }
  return Math.round(value);
}

function parseStaleIfError(raw) {
  if (raw === undefined || raw === null || raw === '') return 0;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0 || value > MAX_STALE_MS) {
    throw new CoordinatorError(`stale_if_error_ms must be between 0 and ${MAX_STALE_MS}`);
  }
  return Math.round(value);
}

function shapeTask(row) {
  if (!row) return null;
  return {
    id: row.id,
    query_key: row.query_key,
    generation: row.generation,
    source_id: row.source_id,
    query: row.query_text,
    locale: row.locale,
    state: row.state,
    origin: row.origin,
    keep_alive: Boolean(row.keep_alive),
    ref_count: row.ref_count,
    attempt: row.attempt,
    max_attempts: row.max_attempts,
    created_at: row.created_at,
    started_at: row.started_at,
    finished_at: row.finished_at,
    deadline_at: row.deadline_at,
    cancel_reason: row.cancel_reason,
    error: row.error,
    started_by: row.started_by,
  };
}

function shapeSubscriber(row) {
  return {
    subscriber_id: row.subscriber_id,
    viewer_id: row.viewer_id,
    label: row.viewer_label,
    role: row.role,
    kind: row.kind,
    joined_at: row.joined_at,
    left_at: row.left_at,
    left_reason: row.left_reason,
  };
}

/** Must match the subscriber_id built by joinOrStartTask. */
function subscriberKey(viewerId, clientToken) {
  return `${viewerId}:${clientToken || 'default'}`;
}

export class Coordinator {
  constructor({ db, clock = Date.now, fetchTimeoutMs = 25_000 }) {
    this.db = db;
    this.clock = clock;
    this.fetchTimeoutMs = fetchTimeoutMs;
    this.flights = new Map(); // taskId -> { controller, promise, resolve, reject }
    // taskId -> Map(subscriberId -> { resolve, reject }). A subscriber can stop
    // waiting without cancelling the shared task, so its caller needs a way to
    // be released even while the task keeps running for everyone else.
    this.waiters = new Map();
    this.listeners = new Set();
    this.instance = `${hostname()}:${process.pid}`;
    this.epoch = 0;
    this.suppressWrites = false;
    this.loseNotificationFor = null; // one-shot task id; 'next' arms the next task
    this.afterReceiptHook = null;
  }

  /* ------------------------------------------------------------------ */
  /* Read path                                                           */
  /* ------------------------------------------------------------------ */

  async read({ viewer, sourceId, query, locale, maxAgeMs, staleIfErrorMs, clientToken }) {
    const source = requireSource(sourceId);
    const logical = logicalKey({
      sourceId, query, locale, caseSensitiveQuery: Boolean(source.caseSensitiveQuery),
    });
    const maxAge = parseMaxAge(maxAgeMs, source.defaultMaxAgeMs);
    const staleWindow = parseStaleIfError(staleIfErrorMs);
    const at = this.clock();

    const cached = this.cachedDocument(logical.key);
    if (cached && at - cached.fetched_at <= maxAge) {
      this.recordAvoided({
        viewer, logical, taskId: cached.task_id, generation: cached.generation, at,
        reason: 'fresh-cache-hit', ageMs: at - cached.fetched_at, maxAgeMs: maxAge,
      });
      return this.buildReadResult({
        viewer, logical, document: cached, served: 'cache', stale: false, late: false,
        ageMs: at - cached.fetched_at, maxAgeMs: maxAge, staleWindow,
        task: null, coalescedWith: 0, at,
      });
    }

    const join = this.joinOrStartTask({
      viewer, logical, source, origin: 'live', at, clientToken,
    });

    try {
      const finished = await this.waitForSubscriber({
        taskId: join.taskId,
        subscriberId: subscriberKey(viewer.id, clientToken),
        done: join.done,
      });
      const document = this.documentForTask(finished.id);
      if (!document) {
        throw new CoordinatorError('task finished without a document', { status: 500, code: 'missing_document' });
      }
      return this.buildReadResult({
        viewer, logical, document, served: 'source', stale: false, late: false,
        ageMs: this.clock() - document.fetched_at, maxAgeMs: maxAge, staleWindow,
        task: finished, coalescedWith: join.sharedWith, at: this.clock(),
      });
    } catch (err) {
      if (err instanceof CrashSignal) throw err;
      // Leaving a task is not a source failure: the viewer is gone, so there is
      // no one to serve a stale fallback to.
      if (err.code === 'abandoned') throw err;
      const fallback = this.cachedDocument(logical.key);
      const fallbackAge = fallback ? this.clock() - fallback.fetched_at : null;
      const canServeStale = fallback
        && staleWindow > 0
        && fallbackAge !== null
        && fallbackAge <= staleWindow
        && fallback.generation !== join.generation;
      if (canServeStale) {
        this.emitEvent({
          at: this.clock(),
          kind: 'read.stale-fallback',
          viewerId: viewer.id,
          queryKey: logical.key,
          detail: {
            requestedGeneration: join.generation,
            servedGeneration: fallback.generation,
            ageMs: fallbackAge,
            staleIfErrorMs: staleWindow,
            error: err?.message ?? String(err),
          },
        });
        return this.buildReadResult({
          viewer, logical, document: fallback, served: 'stale', stale: true, late: false,
          ageMs: fallbackAge, maxAgeMs: maxAge, staleWindow,
          task: this.taskById(fallback.task_id), coalescedWith: 0, at: this.clock(),
          note: `The rebuild failed. This is generation ${fallback.generation}, `
            + `${Math.round(fallbackAge / 1000)}s old, inside the ${Math.round(staleWindow / 1000)}s stale window.`,
        });
      }
      throw err;
    }
  }

  /** Explicit, clearly-labelled read of an older generation. */
  readGeneration({ viewer, queryKey, generation }) {
    const row = this.db.prepare(
      'SELECT * FROM documents WHERE query_key = ? AND generation = ?',
    ).get(queryKey, Number(generation));
    if (!row) throw new CoordinatorError('unknown generation', { status: 404, code: 'unknown_generation' });
    const document = this.shapeDocumentRow(row);
    const at = this.clock();
    const age = at - document.fetched_at;
    const task = this.taskById(document.task_id);
    return this.buildReadResult({
      viewer,
      logical: {
        key: document.query_key,
        sourceId: document.source_id,
        query: document.query,
        locale: document.locale,
      },
      document,
      served: 'stale',
      stale: true,
      late: !document.usable,
      ageMs: age,
      maxAgeMs: 0,
      staleWindow: 0,
      task,
      coalescedWith: 0,
      at,
      note: document.usable
        ? `Historical generation ${document.generation}, read explicitly. It is ${Math.round(age / 1000)}s old and is not a freshness claim.`
        : `Generation ${document.generation} completed after its task was cancelled. It is stored for audit only and is never served as fresh cache.`,
    });
  }

  cachedDocument(queryKey) {
    const row = this.db.prepare(`
      SELECT * FROM documents WHERE query_key = ? AND usable = 1
      ORDER BY generation DESC LIMIT 1
    `).get(queryKey);
    return row ? this.shapeDocumentRow(row) : null;
  }

  documentForTask(taskId) {
    const row = this.db.prepare('SELECT * FROM documents WHERE task_id = ?').get(taskId);
    return row ? this.shapeDocumentRow(row) : null;
  }

  shapeDocumentRow(row) {
    return {
      query_key: row.query_key,
      generation: row.generation,
      task_id: row.task_id,
      source_id: row.source_id,
      query: row.query_text,
      locale: row.locale,
      share_token: row.share_token,
      fetched_at: row.fetched_at,
      expires_at: row.expires_at,
      usable: Boolean(row.usable),
      payload: JSON.parse(row.payload_json),
    };
  }

  buildReadResult({
    viewer, logical, document, served, stale, late, ageMs, maxAgeMs,
    staleWindow, task, coalescedWith, at, note,
  }) {
    const profile = roleProfile(viewer.role);
    const projected = projectDocument(document.payload, profile.clearance);
    const result = {
      query: {
        key: logical.key,
        source_id: logical.sourceId,
        text: logical.query,
        locale: logical.locale,
      },
      viewer: {
        id: viewer.id,
        label: viewer.label,
        role: viewer.role,
        clearance: profile.clearance,
      },
      admission: {
        served,
        stale,
        late_completion: late,
        generation: document.generation,
        share_token: document.share_token,
        task_id: document.task_id,
        fetched_at: document.fetched_at,
        age_ms: ageMs,
        max_age_ms: maxAgeMs,
        stale_if_error_ms: staleWindow,
        coalesced_with: coalescedWith,
        usable_as_fresh: document.usable && !stale,
      },
      result: projected.result,
      withheld_fields: projected.withheld,
      visible_fields: visibleFields(profile.clearance),
      withheld_catalog: withheldFields(profile.clearance),
      task: task ? shapeTask(task) : null,
      note: note ?? null,
    };
    logEvent(this.db, at, {
      kind: served === 'cache' ? 'read.cache' : served === 'stale' ? 'read.stale' : 'read.source',
      taskId: document.task_id,
      viewerId: viewer.id,
      queryKey: logical.key,
      detail: {
        served,
        stale,
        lateCompletion: late,
        ageMs,
        maxAgeMs,
        generation: document.generation,
        shareToken: document.share_token,
        withheld: projected.withheld.map((entry) => entry.field),
        coalescedWith,
      },
    });
    return result;
  }

  recordAvoided({ viewer, logical, taskId, generation, at, reason, ageMs, maxAgeMs }) {
    logEvent(this.db, at, {
      kind: 'savings.upstream-request-avoided',
      taskId,
      viewerId: viewer.id,
      queryKey: logical.key,
      detail: { reason, generation, ageMs, maxAgeMs, viewerRole: viewer.role },
    });
  }

  /* ------------------------------------------------------------------ */
  /* Task lifecycle                                                      */
  /* ------------------------------------------------------------------ */

  activeTaskFor(queryKey) {
    return this.db.prepare(`
      SELECT * FROM source_tasks WHERE query_key = ? AND state IN ('queued','running')
      ORDER BY generation DESC LIMIT 1
    `).get(queryKey) ?? null;
  }

  taskById(id) {
    return this.db.prepare('SELECT * FROM source_tasks WHERE id = ?').get(id) ?? null;
  }

  nextGeneration(queryKey) {
    const row = this.db.prepare(
      'SELECT MAX(generation) AS gen FROM source_tasks WHERE query_key = ?',
    ).get(queryKey);
    return (row?.gen ?? 0) + 1;
  }

  joinOrStartTask({ viewer, logical, source, origin, at, clientToken, keepAlive = false }) {
    const subscriberId = `${viewer.id}:${clientToken || 'default'}`;
    let taskRow;
    let created = false;
    let attached = false;
    let priorGeneration = null;

    transaction(this.db, () => {
      const active = this.activeTaskFor(logical.key);
      if (active) {
        const existing = this.db.prepare(
          'SELECT * FROM task_subscribers WHERE task_id = ? AND subscriber_id = ?',
        ).get(active.id, subscriberId);
        if (existing) {
          if (existing.left_at !== null) {
            this.db.prepare(`
              UPDATE task_subscribers SET left_at = NULL, left_reason = NULL, joined_at = ?
              WHERE task_id = ? AND subscriber_id = ?
            `).run(at, active.id, subscriberId);
            attached = true;
          }
        } else {
          this.insertSubscriber(active.id, subscriberId, viewer, origin === 'rebuild' ? 'rebuild' : 'read', at);
          attached = true;
        }
        this.recountRefs(active.id);
        priorGeneration = active.generation;
        taskRow = this.taskById(active.id);
        return;
      }

      const generation = this.nextGeneration(logical.key);
      const taskId = newId('task');
      this.db.prepare(`
        INSERT INTO source_tasks (
          id, query_key, generation, source_id, query_text, locale, state, origin,
          keep_alive, ref_count, attempt, max_attempts, created_at, deadline_at,
          started_by, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, 0, 3, ?, ?, ?, ?)
      `).run(
        taskId, logical.key, generation, source.debugId ?? logical.sourceId, logical.query,
        logical.locale, origin, keepAlive ? 1 : 0, at,
        at + this.fetchTimeoutMs * 4, viewer.id, at,
      );
      this.insertSubscriber(taskId, subscriberId, viewer, origin === 'rebuild' ? 'rebuild' : 'read', at);
      this.recountRefs(taskId);
      taskRow = this.taskById(taskId);
      created = true;
      logEvent(this.db, at, {
        kind: 'task.created',
        taskId,
        viewerId: viewer.id,
        queryKey: logical.key,
        detail: {
          generation, origin, sourceId: logical.sourceId,
          query: logical.query, locale: logical.locale,
        },
      });
    });

    if (attached) {
      logEvent(this.db, this.clock(), {
        kind: 'savings.upstream-request-avoided',
        taskId: taskRow.id,
        viewerId: viewer.id,
        queryKey: logical.key,
        detail: {
          reason: 'coalesced-onto-active-task',
          generation: taskRow.generation,
          viewerRole: viewer.role,
          priorGeneration,
        },
      });
      logEvent(this.db, this.clock(), {
        kind: 'task.joined',
        taskId: taskRow.id,
        viewerId: viewer.id,
        queryKey: logical.key,
        detail: {
          generation: taskRow.generation,
          subscriberId,
          refCount: this.refreshRefCount(taskRow.id),
        },
      });
    }

    if (created && !activeFlightMissing(this, taskRow.id)) this.startFlight(taskRow.id);

    return {
      taskId: taskRow.id,
      generation: taskRow.generation,
      subscriberId,
      created,
      attached,
      done: this.waitForTask(taskRow.id),
      sharedWith: Math.max(0, this.refreshRefCount(taskRow.id) - 1),
    };
  }

  insertSubscriber(taskId, subscriberId, viewer, kind, at) {
    this.db.prepare(`
      INSERT INTO task_subscribers
        (task_id, subscriber_id, viewer_id, viewer_label, role, kind, joined_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(taskId, subscriberId, viewer.id, viewer.label, viewer.role, kind, at);
  }

  recountRefs(taskId) {
    const count = this.db.prepare(
      'SELECT COUNT(*) AS n FROM task_subscribers WHERE task_id = ? AND left_at IS NULL',
    ).get(taskId).n;
    this.db.prepare('UPDATE source_tasks SET ref_count = ?, updated_at = ? WHERE id = ?')
      .run(count, this.clock(), taskId);
    return count;
  }

  refreshRefCount(taskId) {
    return this.db.prepare('SELECT ref_count FROM source_tasks WHERE id = ?').get(taskId)?.ref_count ?? 0;
  }

  terminalError(row) {
    if (row.state === 'cancelled') {
      return new CoordinatorError(row.cancel_reason ?? 'task cancelled', { status: 409, code: 'cancelled' });
    }
    return new CoordinatorError(row.error ?? 'source task failed', { status: 502, code: 'source_failed' });
  }

  waitForTask(taskId) {
    const flight = this.flights.get(taskId);
    if (flight) return flight.promise;
    const row = this.taskById(taskId);
    if (!row) return Promise.reject(new CoordinatorError('task vanished', { status: 500 }));
    if (row.state === 'succeeded') return Promise.resolve(row);
    if (row.state === 'failed' || row.state === 'cancelled') return Promise.reject(this.terminalError(row));
    // A live task without a flight in this process can only have been adopted
    // or resumed; start it lazily. startFlight is idempotent.
    this.startFlight(taskId);
    return this.flights.get(taskId)?.promise
      ?? Promise.reject(new CoordinatorError('task is not runnable', { status: 500 }));
  }

  /**
   * Wait for one subscriber's view of a task. The task-level promise settles
   * when the task does; this wrapper additionally lets leaveTask release just
   * this caller while the shared task keeps running for everyone else.
   */
  waitForSubscriber({ taskId, subscriberId, done }) {
    return new Promise((resolve, reject) => {
      const entry = {
        resolve: (value) => {
          this.dropWaiter(taskId, subscriberId, entry);
          resolve(value);
        },
        reject: (err) => {
          this.dropWaiter(taskId, subscriberId, entry);
          reject(err);
        },
      };
      let waiting = this.waiters.get(taskId);
      if (!waiting) this.waiters.set(taskId, (waiting = new Map()));
      waiting.set(subscriberId, entry);
      done.then(entry.resolve, entry.reject);
    });
  }

  dropWaiter(taskId, subscriberId, entry) {
    const waiting = this.waiters.get(taskId);
    if (!waiting || waiting.get(subscriberId) !== entry) return;
    waiting.delete(subscriberId);
    if (waiting.size === 0) this.waiters.delete(taskId);
  }

  /**
   * Release a subscriber that detached. Returns true if a caller was actually
   * waiting, so the HTTP layer can answer 409 instead of leaving the request
   * open until the shared task finishes.
   */
  abandonWaiter(taskId, subscriberId, message = 'you left this task; it continues for other readers') {
    const waiting = this.waiters.get(taskId);
    const entry = waiting?.get(subscriberId);
    if (!entry) return false;
    waiting.delete(subscriberId);
    if (waiting.size === 0) this.waiters.delete(taskId);
    entry.reject(new CoordinatorError(message, { status: 409, code: 'abandoned' }));
    return true;
  }

  startFlight(taskId) {
    if (this.flights.has(taskId)) return this.flights.get(taskId);
    const row = this.taskById(taskId);
    if (!row || row.state === 'succeeded' || row.state === 'failed' || row.state === 'cancelled') {
      return null;
    }
    const controller = new AbortController();
    const flight = { controller };
    flight.promise = new Promise((resolve, reject) => {
      flight.resolve = resolve;
      flight.reject = reject;
    });
    flight.promise.catch(() => {});
    this.flights.set(taskId, flight);
    this.runTask(taskId, controller.signal)
      .then((row) => {
        if (this.flights.get(taskId) === flight) this.flights.delete(taskId);
        // A task can end terminally without throwing (markFailed/markCancelled
        // return the row). Callers waiting on it must see a rejection, so the
        // read path can fall back to a stale generation and rebuild() can
        // report the failure.
        if (row && (row.state === 'failed' || row.state === 'cancelled')) {
          flight.reject(this.terminalError(row));
        } else {
          flight.resolve(row);
        }
      })
      .catch((err) => {
        if (this.flights.get(taskId) === flight) this.flights.delete(taskId);
        flight.reject(err);
      });
    return flight;
  }

  async runTask(taskId, signal) {
    const epoch = this.epoch;
    let task = this.taskById(taskId);
    if (!task) throw new CoordinatorError('task vanished', { status: 500 });
    if (task.state === 'succeeded') return task;
    if (task.state === 'failed' || task.state === 'cancelled') throw this.terminalError(task);

    // A receipt from a previous attempt means the source already answered. Use
    // it instead of asking again. This is the "lost success notification" path.
    const pendingReceipt = this.db.prepare(
      'SELECT * FROM source_receipts WHERE task_id = ? AND consumed = 0',
    ).get(taskId);
    if (pendingReceipt) {
      return this.finalizeTask({
        taskId,
        raw: JSON.parse(pendingReceipt.payload_json),
        receivedAt: pendingReceipt.received_at,
      });
    }

    const at = this.clock();
    if (task.state === 'queued') {
      this.db.prepare(`
        UPDATE source_tasks SET state = 'running', started_at = COALESCE(started_at, ?),
          attempt = CASE WHEN attempt = 0 THEN 1 ELSE attempt END, updated_at = ?
        WHERE id = ?
      `).run(at, at, taskId);
      this.emitEvent({ at, kind: 'task.started', taskId, queryKey: task.query_key, detail: { generation: task.generation, attempt: 1 } });
    }

    const source = requireSource(task.source_id);
    let attempt = Math.max(1, task.attempt);

    while (true) {
      task = this.taskById(taskId);
      if (!task || task.state === 'succeeded') return task;
      if (task.state === 'failed' || task.state === 'cancelled') throw this.terminalError(task);

      const localAbort = new AbortController();
      const composed = AbortSignal.any([signal, localAbort.signal]);
      const deadlineTimer = setTimeout(
        () => localAbort.abort(new Error('attempt deadline exceeded')),
        this.fetchTimeoutMs,
      );
      deadlineTimer.unref?.();

      try {
        const raw = await source.fetch({
          query: task.query_text,
          locale: task.locale,
          signal: composed,
        });
        clearTimeout(deadlineTimer);
        if (epoch !== this.epoch) throw new CrashSignal(taskId);

        const receivedAt = this.clock();
        // Persist the source receipt BEFORE any post-processing. If the process
        // dies from here on, recovery finishes this task from the receipt
        // instead of issuing a second upstream request.
        this.db.prepare(`
          INSERT INTO source_receipts (task_id, attempt, payload_json, received_at, consumed)
          VALUES (?, ?, ?, ?, 0)
          ON CONFLICT(task_id) DO UPDATE SET
            attempt = excluded.attempt, payload_json = excluded.payload_json,
            received_at = excluded.received_at, consumed = 0
        `).run(taskId, attempt, JSON.stringify(raw), receivedAt);

        if (this.afterReceiptHook) await this.afterReceiptHook(taskId);
        if (epoch !== this.epoch) throw new CrashSignal(taskId);
        if (this.loseNotificationFor === taskId || this.loseNotificationFor === 'next') {
          this.loseNotificationFor = this.loseNotificationFor === 'next'
            ? 'next-consumed'
            : null;
          throw new CrashSignal(taskId);
        }

        return this.finalizeTask({ taskId, raw, receivedAt });
      } catch (err) {
        clearTimeout(deadlineTimer);
        if (err instanceof CrashSignal) {
          // Leave the task running with its receipt unconsumed: exactly the
          // state a process death would leave behind.
          throw err;
        }
        if (this.suppressWrites || epoch !== this.epoch) throw new CrashSignal(taskId);

        const latest = this.taskById(taskId);
        if (!latest || latest.state === 'succeeded') return latest;
        if (latest.state === 'cancelled') return latest;

        const abortedByCaller = signal.aborted;
        const abortedByDeadline = localAbort.signal.aborted;
        const cancelledBySource = err instanceof SourceCancelledError;

        if (cancelledBySource || abortedByCaller) {
          return this.markCancelled(taskId, latest.cancel_reason ?? 'all-readers-left', this.clock());
        }
        if (abortedByDeadline) {
          return this.markFailed(taskId, 'source deadline exceeded', this.clock());
        }

        const message = err?.message ?? String(err);
        const retryable = err instanceof SourceError ? err.retryable : true;
        if (retryable && attempt < latest.max_attempts) {
          attempt += 1;
          this.db.prepare('UPDATE source_tasks SET attempt = ?, updated_at = ? WHERE id = ?')
            .run(attempt, this.clock(), taskId);
          logEvent(this.db, this.clock(), {
            kind: 'task.retry',
            taskId,
            queryKey: latest.query_key,
            detail: { attempt, maxAttempts: latest.max_attempts, error: message.slice(0, 300) },
          });
          continue;
        }
        return this.markFailed(taskId, message, this.clock());
      }
    }
  }

  /**
   * Turn a source receipt into the generation's document. Idempotent: recovery
   * and the live path may both call it; the second call returns the existing
   * terminal task.
   */
  finalizeTask({ taskId, raw, receivedAt }) {
    const task = this.taskById(taskId);
    if (!task) throw new CoordinatorError('task vanished', { status: 500 });
    if (task.state === 'succeeded') return task;

    const finishedAt = this.clock();
    const completedAfterCancel = task.state === 'cancelled';
    const shareToken = `st_${task.query_key.slice(-10)}_g${task.generation}_${(receivedAt >>> 0).toString(36)}`;
    const document = buildDocument({
      raw,
      logical: {
        key: task.query_key,
        sourceId: task.source_id,
        query: task.query_text,
        locale: task.locale,
      },
      shareToken,
      fetchedAt: receivedAt,
    });
    const expiresAt = receivedAt + sourceDefaultMaxAgeMs(task.source_id);

    transaction(this.db, () => {
      this.db.prepare(`
        INSERT INTO documents
          (query_key, generation, task_id, source_id, query_text, locale, share_token,
           payload_json, fetched_at, expires_at, usable)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(query_key, generation) DO UPDATE SET
          task_id = excluded.task_id, share_token = excluded.share_token,
          payload_json = excluded.payload_json, fetched_at = excluded.fetched_at,
          expires_at = excluded.expires_at, usable = excluded.usable
      `).run(
        task.query_key, task.generation, task.id, task.source_id, task.query_text,
        task.locale, shareToken, JSON.stringify(document), receivedAt, expiresAt,
        completedAfterCancel ? 0 : 1,
      );
      this.db.prepare('UPDATE source_receipts SET consumed = 1 WHERE task_id = ?').run(taskId);

      if (completedAfterCancel) {
        // Everyone stopped waiting before the source answered. Keep the bytes
        // (the request was already paid for) but never present them as fresh.
        this.db.prepare(`
          UPDATE source_tasks SET finished_at = ?, updated_at = ?,
            error = COALESCE(error, 'source completed after cancellation')
          WHERE id = ? AND state = 'cancelled'
        `).run(finishedAt, finishedAt, taskId);
        logEvent(this.db, finishedAt, {
          kind: 'task.late-completion',
          taskId,
          queryKey: task.query_key,
          detail: {
            generation: task.generation,
            receivedAt,
            cancelledAt: task.finished_at ?? null,
            charge: 'upstream request already issued',
          },
        });
      } else {
        this.db.prepare(`
          UPDATE source_tasks SET state = 'succeeded', finished_at = ?, updated_at = ?
          WHERE id = ? AND state IN ('queued','running')
        `).run(finishedAt, finishedAt, taskId);
        this.insertDeliveryFanout(task, { stale: false, reason: 'new-generation' });
        logEvent(this.db, finishedAt, {
          kind: 'task.succeeded',
          taskId,
          queryKey: task.query_key,
          detail: {
            generation: task.generation,
            fetchedAt: receivedAt,
            durationMs: finishedAt - (task.started_at ?? task.created_at),
            shareToken,
          },
        });
      }
      this.db.prepare(`
        UPDATE task_subscribers SET left_at = ?, left_reason = 'task-finished'
        WHERE task_id = ? AND left_at IS NULL
      `).run(finishedAt, taskId);
      this.db.prepare('UPDATE source_tasks SET ref_count = 0 WHERE id = ?').run(taskId);
    });

    this.emitChange();
    return this.taskById(taskId);
  }

  markCancelled(taskId, reason, at) {
    const task = this.taskById(taskId);
    if (!task) return null;
    if (task.state === 'succeeded' || task.state === 'failed' || task.state === 'cancelled') return task;
    this.db.prepare(`
      UPDATE source_tasks SET state = 'cancelled', finished_at = ?, cancel_reason = ?,
        ref_count = 0, updated_at = ?
      WHERE id = ?
    `).run(at, reason, at, taskId);
    this.db.prepare(`
      UPDATE task_subscribers SET left_at = ?, left_reason = ?
      WHERE task_id = ? AND left_at IS NULL
    `).run(at, `cancelled:${reason}`, taskId);
    logEvent(this.db, at, {
      kind: 'task.cancelled',
      taskId,
      queryKey: task.query_key,
      detail: { generation: task.generation, reason },
    });
    this.emitEvent({
      at, kind: 'task.cancelled', taskId, queryKey: task.query_key,
      detail: { generation: task.generation, reason },
    });
    this.emitChange();
    return this.taskById(taskId);
  }

  markFailed(taskId, message, at) {
    const task = this.taskById(taskId);
    if (!task) return null;
    if (task.state === 'succeeded' || task.state === 'failed' || task.state === 'cancelled') return task;
    this.db.prepare(`
      UPDATE source_tasks SET state = 'failed', finished_at = ?, error = ?,
        ref_count = 0, updated_at = ?
      WHERE id = ? AND state IN ('queued','running')
    `).run(at, message.slice(0, 500), at, taskId);
    this.db.prepare(`
      UPDATE task_subscribers SET left_at = ?, left_reason = 'task-failed'
      WHERE task_id = ? AND left_at IS NULL
    `).run(at, taskId);
    logEvent(this.db, at, {
      kind: 'task.failed',
      taskId,
      queryKey: task.query_key,
      detail: { generation: task.generation, error: message.slice(0, 500), attempt: task.attempt },
    });
    this.emitEvent({
      at, kind: 'task.failed', taskId, queryKey: task.query_key,
      detail: { generation: task.generation, error: message.slice(0, 300) },
    });
    this.emitChange();
    return this.taskById(taskId);
  }

  /* ------------------------------------------------------------------ */
  /* Cancellation / detaching                                            */
  /* ------------------------------------------------------------------ */

  /**
   * A viewer stops waiting on a task. Closes only that viewer's attachment; the
   * task is aborted only when no other viewer remains and it is not keep-alive.
   */
  leaveTask({ viewer, taskId, reason = 'left' }) {
    const task = this.taskById(taskId);
    if (!task) throw new CoordinatorError('unknown task', { status: 404, code: 'unknown_task' });
    const at = this.clock();
    let stillWaiting = 0;
    let closed = false;
    let releasedSubscriberId = null;

    transaction(this.db, () => {
      const row = this.db.prepare(`
        SELECT * FROM task_subscribers
        WHERE task_id = ? AND viewer_id = ? AND left_at IS NULL
        ORDER BY joined_at DESC LIMIT 1
      `).get(taskId, viewer.id);
      if (row) {
        this.db.prepare(`
          UPDATE task_subscribers SET left_at = ?, left_reason = ?
          WHERE task_id = ? AND subscriber_id = ?
        `).run(at, reason, taskId, row.subscriber_id);
        closed = true;
        releasedSubscriberId = row.subscriber_id;
      }
      stillWaiting = this.recountRefs(taskId);
      logEvent(this.db, at, {
        kind: closed ? 'task.left' : 'task.leave-noop',
        taskId,
        viewerId: viewer.id,
        queryKey: task.query_key,
        detail: { reason, stillWaiting, keepAlive: Boolean(task.keep_alive) },
      });
    });

    // The caller that left must not keep its HTTP request open until the shared
    // task finishes: the answer is no longer wanted. Only release it here when
    // the task continues for someone else. If this was the last reader, the
    // abort below rejects the flight with the task's cancellation, which is the
    // more accurate outcome for this caller.
    const shouldAbort = closed
      && stillWaiting === 0
      && !task.keep_alive
      && (task.state === 'queued' || task.state === 'running');
    if (shouldAbort) {
      this.markCancelled(taskId, 'all-readers-left', at);
      const flight = this.flights.get(taskId);
      if (flight) flight.controller.abort(new Error('all readers left'));
    } else if (closed) {
      this.abandonWaiter(
        taskId,
        releasedSubscriberId,
        `you left this task (${reason}); it continues for other readers`,
      );
    }
    this.emitChange();
    return {
      taskId,
      closed,
      stillWaiting,
      aborted: shouldAbort,
      task: shapeTask(this.taskById(taskId)),
    };
  }

  /** Deliberate cancellation of an entire task, never implicit. */
  cancelTask({ viewer, taskId, reason }) {
    const task = this.taskById(taskId);
    if (!task) throw new CoordinatorError('unknown task', { status: 404, code: 'unknown_task' });
    if (task.state === 'succeeded' || task.state === 'failed' || task.state === 'cancelled') {
      return { changed: false, task: shapeTask(task) };
    }
    const profile = roleProfile(viewer.role);
    if (task.started_by !== viewer.id && !profile.canCancelOthers) {
      throw new CoordinatorError('you may only cancel work you started', {
        status: 403,
        code: 'not_your_task',
      });
    }
    const at = this.clock();
    const row = this.markCancelled(taskId, reason ?? `cancelled by ${viewer.label}`, at);
    const flight = this.flights.get(taskId);
    if (flight) flight.controller.abort(new Error('cancelled by viewer'));
    logEvent(this.db, at, {
      kind: 'task.cancel-requested',
      taskId,
      viewerId: viewer.id,
      queryKey: task.query_key,
      detail: { generation: task.generation, byRole: viewer.role, reason: reason ?? null },
    });
    return { changed: true, task: shapeTask(row) };
  }

  /* ------------------------------------------------------------------ */
  /* Rebuild / history                                                   */
  /* ------------------------------------------------------------------ */

  rebuild({ viewer, sourceId, query, locale, clientToken, origin = 'rebuild' }) {
    const source = requireSource(sourceId);
    const logical = logicalKey({
      sourceId, query, locale, caseSensitiveQuery: Boolean(source.caseSensitiveQuery),
    });
    const at = this.clock();
    const previous = this.cachedDocument(logical.key);
    const join = this.joinOrStartTask({
      viewer, logical, source, origin, at, clientToken, keepAlive: origin === 'subscription',
    });
    logEvent(this.db, at, {
      kind: join.created ? 'rebuild.started' : 'rebuild.coalesced',
      taskId: join.taskId,
      viewerId: viewer.id,
      queryKey: logical.key,
      detail: {
        generation: join.generation,
        previousGeneration: previous?.generation ?? null,
        previousAgeMs: previous ? at - previous.fetched_at : null,
        origin,
      },
    });
    return {
      task: shapeTask(join.task),
      taskId: join.taskId,
      generation: join.generation,
      startedNew: join.created,
      previousGeneration: previous?.generation ?? null,
      previousAgeMs: previous ? at - previous.fetched_at : null,
      done: join.done,
    };
  }

  /**
   * Start a task without a calling reader; used for subscription refresh and
   * the lost-notification demo. Keep-alive prevents cancellation by absence.
   */
  startBackgroundTask({ viewer, sourceId, query, locale, origin = 'subscription', clientToken }) {
    return this.rebuild({ viewer, sourceId, query, locale, origin, clientToken });
  }

  /* ------------------------------------------------------------------ */
  /* Subscriptions                                                       */
  /* ------------------------------------------------------------------ */

  upsertSubscription({
    viewer, sourceId, query, locale, maxAgeMs, staleIfErrorMs, callbackUrl, secret, deliverLatest = true,
  }) {
    const source = requireSource(sourceId);
    const logical = logicalKey({
      sourceId, query, locale, caseSensitiveQuery: Boolean(source.caseSensitiveQuery),
    });
    const at = this.clock();
    const maxAge = parseMaxAge(maxAgeMs, source.defaultMaxAgeMs);
    const staleWindow = parseStaleIfError(staleIfErrorMs);
    const existing = this.db.prepare(`
      SELECT * FROM subscriptions
      WHERE viewer_id = ? AND source_id = ? AND query_key = ? AND max_age_ms = ?
    `).get(viewer.id, sourceId, logical.key, maxAge);
    let id;
    let created = false;
    if (existing) {
      id = existing.id;
      this.db.prepare(`
        UPDATE subscriptions SET active = 1, updated_at = ?, last_seen_at = ?,
          stale_if_error_ms = ?, callback_url = ?, secret = ?
        WHERE id = ?
      `).run(at, at, staleWindow, callbackUrl, secret, id);
    } else {
      id = newId('sub');
      created = true;
      this.db.prepare(`
        INSERT INTO subscriptions (
          id, viewer_id, viewer_label, role, source_id, query_text, query_key, locale,
          max_age_ms, stale_if_error_ms, callback_url, secret, active, created_at,
          updated_at, last_seen_at, next_refresh_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
      `).run(
        id, viewer.id, viewer.label, viewer.role, sourceId, logical.query, logical.key,
        logical.locale, maxAge, staleWindow, callbackUrl, secret, at, at, at, at,
      );
      logEvent(this.db, at, {
        kind: 'subscription.created',
        viewerId: viewer.id,
        queryKey: logical.key,
        detail: {
          subscriptionId: id, sourceId, query: logical.query, locale: logical.locale,
          maxAge, staleWindow, callbackUrl,
        },
      });
    }

    // Deliver whatever is already cached, if it is usable, rather than waiting
    // for the next refresh. Freshness is judged per subscription when sent.
    if (deliverLatest) {
      const latest = this.cachedDocument(logical.key);
      if (latest) this.enqueueDelivery({
        subscription: this.subscriptionRow(id),
        task: this.taskById(latest.task_id),
        generation: latest.generation,
        stale: false,
        reason: created ? 'backfill-on-subscribe' : 'resubscribe-backfill',
      });
    }

    this.emitChange();
    return this.subscriptionById(id);
  }

  subscriptionRow(id) {
    return this.db.prepare('SELECT * FROM subscriptions WHERE id = ?').get(id) ?? null;
  }

  subscriptionById(id) {
    const row = this.subscriptionRow(id);
    if (!row) return null;
    const deliveries = this.db.prepare(`
      SELECT * FROM deliveries WHERE subscription_id = ? ORDER BY created_at DESC LIMIT 30
    `).all(id).map((delivery) => ({
      id: delivery.id,
      generation: delivery.generation,
      state: delivery.state,
      attempts: delivery.attempts,
      stale: Boolean(delivery.stale),
      last_error: delivery.last_error,
      next_attempt_at: delivery.next_attempt_at,
      created_at: delivery.created_at,
      sent_at: delivery.sent_at,
      acked_at: delivery.acked_at,
    }));
    return {
      id: row.id,
      viewer_id: row.viewer_id,
      viewer_label: row.viewer_label,
      role: row.role,
      source_id: row.source_id,
      query: row.query_text,
      query_key: row.query_key,
      locale: row.locale,
      max_age_ms: row.max_age_ms,
      stale_if_error_ms: row.stale_if_error_ms,
      callback_url: row.callback_url,
      active: Boolean(row.active),
      created_at: row.created_at,
      updated_at: row.updated_at,
      last_seen_at: row.last_seen_at,
      next_refresh_at: row.next_refresh_at,
      last_delivered_gen: row.last_delivered_gen,
      deliveries,
    };
  }

  listSubscriptions(viewer = null) {
    const rows = viewer
      ? this.db.prepare('SELECT id FROM subscriptions WHERE viewer_id = ? ORDER BY created_at DESC').all(viewer.id)
      : this.db.prepare('SELECT id FROM subscriptions ORDER BY created_at DESC').all();
    return rows.map((row) => this.subscriptionById(row.id));
  }

  cancelSubscription({ viewer, subscriptionId }) {
    const row = this.subscriptionRow(subscriptionId);
    if (!row) throw new CoordinatorError('unknown subscription', { status: 404, code: 'unknown_subscription' });
    if (row.viewer_id !== viewer.id && !roleProfile(viewer.role).canCancelOthers) {
      throw new CoordinatorError('you may only cancel your own subscriptions', {
        status: 403,
        code: 'not_your_subscription',
      });
    }
    const at = this.clock();
    this.db.prepare('UPDATE subscriptions SET active = 0, updated_at = ? WHERE id = ?').run(at, subscriptionId);
    this.db.prepare(`
      UPDATE deliveries SET state = 'dropped', next_attempt_at = ?, last_error = ?
      WHERE subscription_id = ? AND state = 'pending'
    `).run(at, 'subscription cancelled', subscriptionId);
    logEvent(this.db, at, {
      kind: 'subscription.cancelled',
      viewerId: viewer.id,
      queryKey: row.query_key,
      detail: { subscriptionId, byRole: viewer.role, owner: row.viewer_id },
    });
    this.emitChange();
    return this.subscriptionById(subscriptionId);
  }

  enqueueDelivery({ subscription, task, generation, stale, reason }) {
    if (!subscription || !task) return null;
    const at = this.clock();
    const dedupeKey = deliveryDedupeKey(task.query_key, generation);
    const id = newId('dlv');
    const result = this.db.prepare(`
      INSERT INTO deliveries (
        id, subscription_id, task_id, generation, dedupe_key, state,
        attempts, next_attempt_at, stale, created_at
      ) VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
      ON CONFLICT(subscription_id, dedupe_key) DO NOTHING
    `).run(id, subscription.id, task.id, generation, dedupeKey, at, stale ? 1 : 0, at);
    if (result.changes === 0) return null;
    this.db.prepare(`
      UPDATE subscriptions SET last_delivered_gen = MAX(COALESCE(last_delivered_gen, 0), ?), updated_at = ?
      WHERE id = ?
    `).run(generation, at, subscription.id);
    logEvent(this.db, at, {
      kind: 'delivery.enqueued',
      taskId: task.id,
      viewerId: subscription.viewer_id,
      queryKey: subscription.query_key,
      detail: { subscriptionId: subscription.id, generation, stale, reason, dedupeKey },
    });
    return id;
  }

  insertDeliveryFanout(task, { stale, reason }) {
    if (!task) return 0;
    const subs = this.db.prepare(`
      SELECT * FROM subscriptions WHERE query_key = ? AND active = 1
    `).all(task.query_key);
    let created = 0;
    for (const sub of subs) {
      const id = this.enqueueDelivery({
        subscription: sub,
        task,
        generation: task.generation,
        stale,
        reason,
      });
      if (id) created += 1;
    }
    return created;
  }

  /* ------------------------------------------------------------------ */
  /* Recovery                                                            */
  /* ------------------------------------------------------------------ */

  /**
   * Reconcile after a restart. Rules, in order:
   *
   *  1. Never create work before checking the original task for the same key.
   *     Tasks found in a live state are adopted under their existing id and
   *     generation, never duplicated.
   *  2. A task whose source receipt exists but was never consumed is finalized
   *     from the receipt. No upstream request is made.
   *  3. A live task with no receipt is resumed in place.
   *  4. A task past its deadline is failed rather than restarted forever.
   *  5. Pending deliveries are made due immediately; the dispatcher's attempt
   *     cap stops endless retries, and receiver-side dedupe absorbs duplicates.
   *  6. Adoption is capped per pass, so a damaged table cannot turn one restart
   *     into an unbounded fetch storm.
   */
  recover({ maxAdoptions = 25 } = {}) {
    const at = this.clock();
    const report = {
      at,
      instance: this.instance,
      adopted: [],
      receipts_finished: [],
      failed: [],
      skipped: [],
      retries_made_due: 0,
      new_tasks_created: 0,
      cap: maxAdoptions,
    };
    const live = this.db.prepare(`
      SELECT * FROM source_tasks WHERE state IN ('queued','running')
      ORDER BY created_at ASC
    `).all();

    for (const task of live) {
      if (report.adopted.length + report.receipts_finished.length >= maxAdoptions) {
        report.skipped.push({ taskId: task.id, reason: 'adoption cap reached' });
        continue;
      }
      if (this.flights.has(task.id)) {
        report.skipped.push({ taskId: task.id, reason: 'already running in this process' });
        continue;
      }
      const receipt = this.db.prepare(
        'SELECT * FROM source_receipts WHERE task_id = ?',
      ).get(task.id);

      if (receipt && !receipt.consumed) {
        // Rule 2: the source answered but the success notification was lost.
        this.db.prepare(`
          UPDATE source_tasks SET state = 'running', updated_at = ?
          WHERE id = ? AND state = 'queued'
        `).run(at, task.id);
        const finished = this.finalizeTask({
          taskId: task.id,
          raw: JSON.parse(receipt.payload_json),
          receivedAt: receipt.received_at,
        });
        report.receipts_finished.push({
          taskId: task.id,
          generation: task.generation,
          state: finished?.state ?? null,
        });
        logEvent(this.db, this.clock(), {
          kind: 'recovery.receipt-consumed',
          taskId: task.id,
          queryKey: task.query_key,
          detail: {
            generation: task.generation,
            receivedAt: receipt.received_at,
            upstreamRequestReissued: false,
          },
        });
        continue;
      }

      if (at > task.deadline_at) {
        // Rule 4: do not restart a task that outlived its deadline.
        this.markFailed(task.id, `deadline exceeded before restart (${new Date(task.deadline_at).toISOString()})`, at);
        report.failed.push({ taskId: task.id, generation: task.generation, reason: 'deadline' });
        continue;
      }

      // Rules 1 & 3: adopt and resume the original task.
      this.db.prepare(`
        UPDATE source_tasks SET state = 'queued', started_at = NULL, updated_at = ?
        WHERE id = ?
      `).run(at, task.id);
      report.adopted.push({ taskId: task.id, generation: task.generation, origin: task.origin });
      logEvent(this.db, at, {
        kind: 'recovery.task-adopted',
        taskId: task.id,
        queryKey: task.query_key,
        detail: {
          generation: task.generation,
          priorAttempt: task.attempt,
          deadlineAt: task.deadline_at,
          reusedOriginalTask: true,
        },
      });
      const flight = this.startFlight(task.id);
      flight?.promise.catch(() => {});
    }

    // Rule 5: make pending deliveries due immediately after a restart.
    report.retries_made_due = this.db.prepare(`
      SELECT COUNT(*) AS n FROM deliveries WHERE state = 'pending'
    `).get().n;

    logEvent(this.db, at, {
      kind: 'recovery.completed',
      detail: {
        instance: this.instance,
        liveTasksAtStart: live.length,
        adopted: report.adopted.length,
        receiptsFinished: report.receipts_finished.length,
        failed: report.failed.length,
        skipped: report.skipped.length,
        retriesMadeDue: report.retries_made_due,
        cap: maxAdoptions,
      },
    });
    this.emitChange();
    return report;
  }

  /**
   * Model a crash of the whole process: abort in-flight fetches, drop the
   * flight map, and suppress terminal writes so tasks stay live in the DB.
   * Recovery then has real work to reconcile.
   */
  simulateCrash() {
    const ids = [...this.flights.keys()];
    this.suppressWrites = true;
    this.epoch += 1;
    const flights = [...this.flights.values()];
    this.flights.clear();
    for (const flight of flights) {
      flight.controller.abort(new Error('simulated process loss'));
      flight.reject(new CoordinatorError('simulated process loss', {
        status: 503,
        code: 'process_lost',
      }));
    }
    // runTask's catch sees the epoch change and returns without writing. The
    // flag is cleared once those handlers have had a chance to run.
    const epoch = this.epoch;
    setTimeout(() => {
      if (this.epoch === epoch) this.suppressWrites = false;
    }, 250).unref?.();
    return { detached: ids, liveTasks: ids.map((id) => shapeTask(this.taskById(id))) };
  }

  /** Arm the next source completion to lose its success notification. */
  armLostNotification() {
    this.loseNotificationFor = 'next';
  }

  /* ------------------------------------------------------------------ */
  /* Observation                                                         */
  /* ------------------------------------------------------------------ */

  emitChange() {
    for (const listener of this.listeners) {
      try { listener(); } catch { /* a broken listener must not break the writer */ }
    }
  }

  onChange(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emitEvent(payload) {
    for (const listener of this.listeners) {
      try { listener(payload); } catch { /* ignore */ }
    }
  }

  snapshot() {
    const at = this.clock();
    const tasks = this.db.prepare(`
      SELECT * FROM source_tasks ORDER BY created_at DESC LIMIT 60
    `).all().map((row) => {
      const task = shapeTask(row);
      const subscribers = this.db.prepare(`
        SELECT * FROM task_subscribers WHERE task_id = ? ORDER BY joined_at ASC
      `).all(row.id).map(shapeSubscriber);
      const document = this.db.prepare(
        'SELECT generation, fetched_at, share_token, usable FROM documents WHERE task_id = ?',
      ).get(row.id);
      return {
        ...task,
        subscribers,
        document: document
          ? {
            generation: document.generation,
            fetched_at: document.fetched_at,
            share_token: document.share_token,
            usable: Boolean(document.usable),
            age_ms: at - document.fetched_at,
          }
          : null,
      };
    });

    const documents = this.db.prepare(`
      SELECT d.query_key, d.generation, d.task_id, d.source_id, d.query_text, d.locale,
             d.share_token, d.fetched_at, d.usable,
             t.state AS task_state, t.cancel_reason,
             (SELECT COUNT(*) FROM events e
               WHERE e.kind IN ('read.cache','read.source','read.stale')
                 AND json_extract(e.detail, '$.shareToken') = d.share_token) AS reads
      FROM documents d LEFT JOIN source_tasks t ON t.id = d.task_id
      ORDER BY d.fetched_at DESC LIMIT 40
    `).all().map((row) => ({
      query_key: row.query_key,
      generation: row.generation,
      task_id: row.task_id,
      source_id: row.source_id,
      query: row.query_text,
      locale: row.locale,
      share_token: row.share_token,
      fetched_at: row.fetched_at,
      age_ms: at - row.fetched_at,
      usable: Boolean(row.usable),
      task_state: row.task_state,
      cancel_reason: row.cancel_reason,
      reads: row.reads,
    }));

    const subscriptions = this.listSubscriptions(null);

    return {
      at,
      instance: this.instance,
      tasks,
      documents,
      subscriptions,
      events: recentEvents(this.db, { limit: 160 }),
      insights: this.insights(at),
      stats: this.stats(at),
    };
  }

  /**
   * The savings and stale-read ledger, grouped by logical query. Numbers are
   * counted from durable events, so they survive restarts and are explainable.
   */
  insights(at = this.clock()) {
    const queries = this.db.prepare(`
      SELECT query_key,
             MIN(source_id) AS source_id,
             MIN(query_text) AS query_text,
             MIN(locale) AS locale
      FROM source_tasks GROUP BY query_key ORDER BY MAX(created_at) DESC LIMIT 30
    `).all();
    return queries.map((entry) => {
      const row = this.db.prepare(`
        SELECT
          COUNT(*) FILTER (WHERE kind = 'task.created') AS tasks_created,
          COUNT(*) FILTER (WHERE kind = 'task.joined') AS readers_coalesced,
          COUNT(*) FILTER (WHERE kind = 'task.succeeded') AS tasks_succeeded,
          COUNT(*) FILTER (WHERE kind = 'task.failed') AS tasks_failed,
          COUNT(*) FILTER (WHERE kind = 'task.cancelled') AS tasks_cancelled,
          COUNT(*) FILTER (WHERE kind = 'task.late-completion') AS late_completions,
          COUNT(*) FILTER (WHERE kind = 'read.cache') AS cache_reads,
          COUNT(*) FILTER (WHERE kind = 'read.source') AS source_reads,
          COUNT(*) FILTER (WHERE kind = 'read.stale') AS stale_reads,
          COUNT(*) FILTER (WHERE kind = 'savings.upstream-request-avoided') AS avoided,
          COUNT(*) FILTER (WHERE kind = 'delivery.enqueued') AS deliveries_enqueued
        FROM events WHERE query_key = ?
      `).get(entry.query_key);
      const upstream = this.db.prepare(`
        SELECT COUNT(*) AS n FROM source_receipts r
        JOIN source_tasks t ON t.id = r.task_id WHERE t.query_key = ?
      `).get(entry.query_key).n;
      const viewers = this.db.prepare(`
        SELECT COUNT(DISTINCT viewer_id) AS n FROM task_subscribers WHERE task_id IN (
          SELECT id FROM source_tasks WHERE query_key = ?
        )
      `).get(entry.query_key).n;
      const generations = this.db.prepare(`
        SELECT generation, usable, fetched_at FROM documents WHERE query_key = ?
        ORDER BY generation ASC
      `).all(entry.query_key).map((doc) => ({
        generation: doc.generation,
        usable: Boolean(doc.usable),
        fetched_at: doc.fetched_at,
        age_ms: at - doc.fetched_at,
      }));
      return {
        query_key: entry.query_key,
        source_id: entry.source_id,
        query: entry.query_text,
        locale: entry.locale,
        upstream_requests: upstream,
        tasks_created: row.tasks_created,
        tasks_succeeded: row.tasks_succeeded,
        tasks_failed: row.tasks_failed,
        tasks_cancelled: row.tasks_cancelled,
        late_completions: row.late_completions,
        cache_reads: row.cache_reads,
        source_reads: row.source_reads,
        stale_reads: row.stale_reads,
        readers_coalesced: row.readers_coalesced,
        upstream_requests_avoided: row.avoided,
        deliveries_enqueued: row.deliveries_enqueued,
        distinct_viewers: viewers,
        generations,
      };
    });
  }

  stats(at = this.clock()) {
    const one = (sql, ...args) => this.db.prepare(sql).get(...args).n;
    const upstream = one('SELECT COUNT(*) AS n FROM source_receipts');
    const cacheHits = one("SELECT COUNT(*) AS n FROM events WHERE kind = 'read.cache'");
    const sourceReads = one("SELECT COUNT(*) AS n FROM events WHERE kind = 'read.source'");
    const staleServes = one("SELECT COUNT(*) AS n FROM events WHERE kind = 'read.stale'");
    const coalesced = one("SELECT COUNT(*) AS n FROM events WHERE kind = 'savings.upstream-request-avoided'");
    const list = {
      upstream_requests: upstream,
      tasks_succeeded: one("SELECT COUNT(*) AS n FROM source_tasks WHERE state = 'succeeded'"),
      tasks_failed: one("SELECT COUNT(*) AS n FROM source_tasks WHERE state = 'failed'"),
      tasks_cancelled: one("SELECT COUNT(*) AS n FROM source_tasks WHERE state = 'cancelled'"),
      reads_from_cache: cacheHits,
      reads_from_source: sourceReads,
      reads_stale: staleServes,
      upstream_requests_avoided: coalesced,
      late_completions: one("SELECT COUNT(*) AS n FROM events WHERE kind = 'task.late-completion'"),
      recoveries: one("SELECT COUNT(*) AS n FROM events WHERE kind = 'recovery.completed'"),
      deliveries_pending: one("SELECT COUNT(*) AS n FROM deliveries WHERE state = 'pending'"),
      deliveries_acked: one("SELECT COUNT(*) AS n FROM deliveries WHERE state = 'acked'"),
      deliveries_dropped: one("SELECT COUNT(*) AS n FROM deliveries WHERE state = 'dropped'"),
      documents_cached: one('SELECT COUNT(*) AS n FROM documents'),
      documents_unusable: one('SELECT COUNT(*) AS n FROM documents WHERE usable = 0'),
      viewers_waiting: one(`
        SELECT COUNT(DISTINCT viewer_id) AS n FROM task_subscribers WHERE left_at IS NULL
      `),
      hit_rate: cacheHits + sourceReads > 0
        ? Number((cacheHits / (cacheHits + sourceReads)).toFixed(3))
        : 0,
    };
    list.methods = {
      upstream_requests: 'one row per source answer (source_receipts)',
      upstream_requests_avoided: 'read served from a fresh cached document, or coalesced onto an in-flight task',
      reads_stale: 'read served from an older generation under the stale policy, clearly flagged',
      hit_rate: 'reads_from_cache / (reads_from_cache + reads_from_source)',
    };
    return list;
  }
}

function activeFlightMissing(coordinator, taskId) {
  return coordinator.flights.has(taskId);
}

export { QueryValidationError };
