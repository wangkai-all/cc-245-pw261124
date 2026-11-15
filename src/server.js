import express from 'express';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openStore } from './store.js';
import { Coordinator, CoordinatorError } from './coordinator.js';
import { createViewer, listViewers, viewerFromToken } from './viewers.js';
import { listSources, getSource } from './sources/index.js';
import { Receiver, newSecret } from './receiver.js';
import { Dispatcher, Refresher } from './dispatcher.js';
import { FIELD_CATALOG, VIEWER_ROLES } from './permissions.js';
import { recentEvents } from './events.js';

const here = dirname(fileURLToPath(import.meta.url));
const ROOT = join(here, '..');

export function createApp({
  db,
  receiver,
  coordinator,
  dispatcher,
  refresher,
  clock = Date.now,
  publicDir = join(ROOT, 'public'),
  autoRecover = true,
  baseUrlProvider = () => null,
}) {
  const app = express();
  app.disable('x-powered-by');

  // HMAC verification needs the exact bytes the sender signed, so receiver
  // pushes are parsed as raw buffers. This must be scoped to the push path
  // only: control endpoints under /api keep the normal JSON parser, and the
  // parser must not run on pushes or it would consume the stream first.
  const rawPush = express.raw({ type: '*/*', limit: '128kb' });
  app.use((req, res, next) => {
    if (/^\/receiver\/[^/]+\/?$/.test(req.path)) return rawPush(req, res, next);
    return next();
  });
  app.use(express.json({ limit: '128kb' }));

  /* ---------------------------------------------------------------- */
  /* Identity                                                           */
  /* ---------------------------------------------------------------- */

  function resolveViewer(req) {
    const token = req.get('x-viewer-token') ?? req.query.token ?? null;
    if (token) {
      const viewer = viewerFromToken(db, token);
      if (viewer) return viewer;
    }
    return null;
  }

  function requireViewer(req, _res, next) {
    const viewer = resolveViewer(req);
    if (!viewer) {
      const err = new CoordinatorError('missing or unknown viewer token', {
        status: 401,
        code: 'unauthenticated',
      });
      return next(err);
    }
    req.viewer = viewer;
    return next();
  }

  /* ---------------------------------------------------------------- */
  /* Meta                                                               */
  /* ---------------------------------------------------------------- */

  app.get('/api/meta', (req, res) => {
    const viewer = resolveViewer(req);
    res.json({
      sources: listSources(),
      roles: VIEWER_ROLES,
      fields: FIELD_CATALOG,
      viewer,
      instance: coordinator.instance,
      counts: {
        tasks: db.prepare('SELECT COUNT(*) AS n FROM source_tasks').get().n,
        documents: db.prepare('SELECT COUNT(*) AS n FROM documents').get().n,
        subscriptions: db.prepare('SELECT COUNT(*) AS n FROM subscriptions WHERE active = 1').get().n,
      },
      policy: {
        sharing_unit: '(source, normalized query text, locale)',
        generation_note: 'One upstream task per generation; a rebuild creates the next generation and keeps the old one readable.',
        permission_note: 'Fields are projected per viewer at read time from one full-fidelity shared document.',
        at_least_once: 'Deliveries retry until the receiver acknowledges; receivers dedupe on the delivery key.',
      },
    });
  });

  app.post('/api/viewers', (req, res, next) => {
    try {
      const { label, role } = req.body ?? {};
      if (typeof label !== 'string' || !label.trim()) {
        throw new CoordinatorError('label is required');
      }
      if (!(role in VIEWER_ROLES)) throw new CoordinatorError('unknown role');
      const viewer = createViewer(db, { label: label.trim().slice(0, 48), role }, clock());
      res.status(201).json({ viewer, token: viewer.token });
    } catch (err) { next(err); }
  });

  app.get('/api/viewers', (_req, res) => {
    res.json({ viewers: listViewers(db) });
  });

  app.post('/api/viewers/login', (req, res, next) => {
    try {
      const { token } = req.body ?? {};
      const viewer = viewerFromToken(db, token);
      if (!viewer) throw new CoordinatorError('unknown token', { status: 401, code: 'unauthenticated' });
      res.json({ viewer });
    } catch (err) { next(err); }
  });

  /* ---------------------------------------------------------------- */
  /* Reads / rebuilds                                                   */
  /* ---------------------------------------------------------------- */

  app.post('/api/read', requireViewer, async (req, res, next) => {
    try {
      const result = await coordinator.read({
        viewer: req.viewer,
        sourceId: req.body?.source_id,
        query: req.body?.query,
        locale: req.body?.locale,
        maxAgeMs: req.body?.max_age_ms,
        staleIfErrorMs: req.body?.stale_if_error_ms,
        clientToken: req.body?.client_token ?? req.get('x-client-token'),
      });
      res.json(result);
    } catch (err) { next(err); }
  });

  app.post('/api/rebuild', requireViewer, async (req, res, next) => {
    try {
      const result = coordinator.rebuild({
        viewer: req.viewer,
        sourceId: req.body?.source_id,
        query: req.body?.query,
        locale: req.body?.locale,
        clientToken: req.body?.client_token ?? `rebuild:${req.viewer.id}`,
      });
      const wait = req.body?.wait !== false;
      if (!wait) {
        res.status(202).json({
          task: result.task,
          generation: result.generation,
          started_new: result.startedNew,
          previous_generation: result.previousGeneration,
          previous_age_ms: result.previousAgeMs,
        });
        return;
      }
      const finished = await result.done;
      const doc = coordinator.documentForTask(finished.id);
      res.json({
        task: finished,
        generation: result.generation,
        started_new: result.startedNew,
        previous_generation: result.previousGeneration,
        previous_age_ms: result.previousAgeMs,
        share_token: doc?.share_token ?? null,
      });
    } catch (err) { next(err); }
  });

  app.get('/api/generations', requireViewer, (req, res, next) => {
    try {
      const queryKey = String(req.query.query_key ?? '');
      if (!queryKey) throw new CoordinatorError('query_key is required');
      const rows = db.prepare(`
        SELECT d.generation, d.fetched_at, d.share_token, d.usable, d.task_id,
               t.state AS task_state, t.cancel_reason, t.origin
        FROM documents d LEFT JOIN source_tasks t ON t.id = d.task_id
        WHERE d.query_key = ? ORDER BY d.generation DESC
      `).all(queryKey);
      res.json({
        generations: rows.map((row) => ({
          generation: row.generation,
          fetched_at: row.fetched_at,
          share_token: row.share_token,
          usable: Boolean(row.usable),
          task_id: row.task_id,
          task_state: row.task_state,
          cancel_reason: row.cancel_reason,
          origin: row.origin,
        })),
      });
    } catch (err) { next(err); }
  });

  app.post('/api/read-generation', requireViewer, (req, res, next) => {
    try {
      const result = coordinator.readGeneration({
        viewer: req.viewer,
        queryKey: req.body?.query_key,
        generation: req.body?.generation,
      });
      res.json(result);
    } catch (err) { next(err); }
  });

  /* ---------------------------------------------------------------- */
  /* Tasks, cancellation, crash/recovery                                */
  /* ---------------------------------------------------------------- */

  app.get('/api/state', (req, res) => {
    const viewer = resolveViewer(req);
    const snapshot = coordinator.snapshot();
    res.json({
      ...snapshot,
      viewer,
      received: receiver.snapshot(),
      server_events: recentEvents(db, { limit: 160 }),
      viewer_scoped: Boolean(viewer),
    });
  });

  app.post('/api/tasks/:id/leave', requireViewer, (req, res, next) => {
    try {
      const result = coordinator.leaveTask({
        viewer: req.viewer,
        taskId: req.params.id,
        reason: req.body?.reason ?? 'left',
      });
      res.json(result);
    } catch (err) { next(err); }
  });

  app.post('/api/tasks/:id/cancel', requireViewer, (req, res, next) => {
    try {
      const result = coordinator.cancelTask({
        viewer: req.viewer,
        taskId: req.params.id,
        reason: req.body?.reason,
      });
      res.json(result);
    } catch (err) { next(err); }
  });

  app.post('/api/demo/crash', requireViewer, async (req, res, next) => {
    try {
      const result = coordinator.simulateCrash();
      res.json({
        crashed: true,
        detached: result.detached,
        live_tasks: result.liveTasks,
        note: 'In-flight fetches were aborted and the flight map was dropped without marking tasks terminal. These tasks are now live rows with no process working them, exactly as after a kill -9.',
      });
    } catch (err) { next(err); }
  });

  app.post('/api/demo/recover', requireViewer, async (req, res, next) => {
    try {
      const maxAdoptions = Number(req.body?.max_adoptions ?? 25);
      const report = coordinator.recover({ maxAdoptions });
      // Deliveries that were pending across the crash become due immediately.
      dispatcher.makeAllDue();
      res.json(report);
    } catch (err) { next(err); }
  });

  app.post('/api/demo/lose-notification', requireViewer, (_req, res) => {
    coordinator.armLostNotification();
    res.json({
      armed: true,
      note: 'The next source response will be written as an unconsumed receipt, then the task will be left running. Press Recover to watch recovery finish the original task without a new upstream request.',
    });
  });

  app.post('/api/demo/expire', requireViewer, (req, res, next) => {
    try {
      // Age documents logically by moving their fetched_at back. This changes
      // only the deterministic demo store, not the fetch history.
      const queryKey = req.body?.query_key ?? null;
      const ageMs = Math.max(1_000, Number(req.body?.age_ms ?? 3_600_000));
      const at = clock();
      const changed = queryKey
        ? db.prepare('UPDATE documents SET fetched_at = fetched_at - ? WHERE query_key = ?').run(ageMs, queryKey).changes
        : db.prepare('UPDATE documents SET fetched_at = fetched_at - ?').run(ageMs).changes;
      res.json({ expired: changed, aged_by_ms: ageMs, query_key: queryKey, at });
    } catch (err) { next(err); }
  });

  /* ---------------------------------------------------------------- */
  /* Subscriptions                                                      */
  /* ---------------------------------------------------------------- */

  app.get('/api/subscriptions', requireViewer, (req, res) => {
    res.json({ subscriptions: coordinator.listSubscriptions(req.viewer) });
  });

  app.post('/api/subscriptions', requireViewer, (req, res, next) => {
    try {
      const body = req.body ?? {};
      const secret = newSecret();
      // The callback must name the subscription, not the source. Build it from
      // the bound address unless the caller provides an explicit URL.
      const callbackBase = body.callback_url
        ?? `${baseUrlProvider() ?? `http://127.0.0.1:${req.socket.localPort}`}/receiver/{sub}`;
      const sub = coordinator.upsertSubscription({
        viewer: req.viewer,
        sourceId: body.source_id,
        query: body.query,
        locale: body.locale,
        maxAgeMs: body.max_age_ms,
        staleIfErrorMs: body.stale_if_error_ms,
        callbackUrl: callbackBase,
        secret,
      });
      if (callbackBase.includes('{sub}')) {
        const resolved = callbackBase.replace('{sub}', sub.id);
        coordinator.db.prepare('UPDATE subscriptions SET callback_url = ? WHERE id = ?')
          .run(resolved, sub.id);
        sub.callback_url = resolved;
      }
      // The secret is returned exactly once, at creation, so the UI can show it
      // in the receiver log. It is never returned by list/get endpoints.
      res.status(201).json({ subscription: sub, secret });
    } catch (err) { next(err); }
  });

  app.post('/api/subscriptions/:id/cancel', requireViewer, (req, res, next) => {
    try {
      const sub = coordinator.cancelSubscription({
        viewer: req.viewer,
        subscriptionId: req.params.id,
      });
      res.json({ subscription: sub });
    } catch (err) { next(err); }
  });

  app.post('/api/subscriptions/tick', requireViewer, (req, res) => {
    const result = refresher.tick();
    res.json(result);
  });

  /* ---------------------------------------------------------------- */
  /* Receiver                                                           */
  /* ---------------------------------------------------------------- */

  app.post('/receiver/:subId', (req, res) => {
    const subscription = coordinator.subscriptionById(req.params.subId);
    if (!subscription) {
      return res.status(404).json({ error: 'unknown subscription' });
    }
    const secretRow = db.prepare('SELECT secret, role FROM subscriptions WHERE id = ?').get(subscription.id);
    const subForVerify = { id: subscription.id, secret: secretRow.secret, role: secretRow.role };
    const rawBody = Buffer.isBuffer(req.body)
      ? req.body.toString('utf8')
      : (req.rawBody ?? JSON.stringify(req.body ?? {}));
    const outcome = receiver.receive({
      rawBody,
      headers: req.headers,
      subscription: subForVerify,
      fault: req.query.fault ?? null,
    });
    return res.status(outcome.status).json(outcome.body);
  });

  app.get('/api/receiver', (_req, res) => {
    res.json(receiver.snapshot());
  });

  app.post('/api/receiver/faults', (req, res) => {
    const faults = receiver.armFaults(req.body ?? {});
    res.json({ faults });
  });

  app.post('/api/receiver/clear', (_req, res) => {
    receiver.clear();
    res.json({ cleared: true });
  });

  /* ---------------------------------------------------------------- */
  /* Live updates                                                       */
  /* ---------------------------------------------------------------- */

  app.get('/api/stream', (req, res) => {
    res.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no',
    });
    res.write(`retry: 2000\n\n`);
    let closed = false;
    const send = (event, data) => {
      if (closed) return;
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    send('ready', { at: clock(), instance: coordinator.instance });
    const off = coordinator.onChange(() => {
      const snapshot = coordinator.snapshot();
      send('state', {
        at: snapshot.at,
        stats: snapshot.stats,
        insights: snapshot.insights,
        tasks: snapshot.tasks,
        documents: snapshot.documents,
        subscriptions: snapshot.subscriptions,
        events: snapshot.events,
        received: receiver.snapshot(),
      });
    });
    const heartbeat = setInterval(() => send('ping', { at: clock() }), 15_000);
    heartbeat.unref?.();
    req.on('close', () => {
      closed = true;
      clearInterval(heartbeat);
      off();
    });
  });

  /* ---------------------------------------------------------------- */
  /* Static + errors                                                    */
  /* ---------------------------------------------------------------- */

  app.use(express.static(publicDir, { extensions: ['html'] }));

  app.use('/api', (req, res) => {
    res.status(404).json({ error: 'not found', path: req.path });
  });

  app.use((err, _req, res, _next) => {
    const status = err.statusCode ?? err.status ?? 500;
    const payload = {
      error: err.message ?? 'internal error',
      code: err.code ?? 'error',
    };
    if (status >= 500) {
      // Simulated process loss is an expected drill outcome, not a server bug;
      // keep the log useful by not crying over it.
      if (err.code !== 'process_lost') console.error('[server]', err);
      payload.error = err.code === 'process_lost'
        ? err.message
        : 'internal error';
    }
    res.status(status).json(payload);
  });

  if (autoRecover) {
    // Run once at boot. It is safe to call on a fresh database (no live tasks).
    coordinator.recover();
    dispatcher.makeAllDue();
  }

  return app;
}

export async function startServer({
  port = Number(process.env.PORT ?? 4180),
  dbFile = process.env.WORKBENCH_DB ?? join(ROOT, 'data', 'workbench.db'),
  receiverFile = process.env.RECEIVER_DB ?? join(ROOT, 'data', 'receiver.db'),
} = {}) {
  const db = openStore(dbFile);
  const receiver = new Receiver({ filename: receiverFile });
  const coordinator = new Coordinator({ db });
  const dispatcher = new Dispatcher({ db });
  const refresher = new Refresher({ coordinator });
  let baseUrl = null;
  const app = createApp({
    db,
    receiver,
    coordinator,
    dispatcher,
    refresher,
    autoRecover: true,
    baseUrlProvider: () => baseUrl,
  });
  const server = await new Promise((resolve) => {
    const s = app.listen(port, '127.0.0.1', () => resolve(s));
  });
  const actualPort = server.address().port;
  baseUrl = `http://127.0.0.1:${actualPort}`;
  dispatcher.start();
  refresher.start();
  return {
    app, server, db, receiver, coordinator, dispatcher, refresher,
    port: actualPort,
    url: baseUrl,
    async close() {
      refresher.stop();
      dispatcher.stop();
      await new Promise((resolve) => server.close(resolve));
      db.close();
      receiver.db.close();
    },
  };
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1];
if (isMain) {
  const started = await startServer();
  console.log(`query-dedupe workbench listening on ${started.url}`);
  const shutdown = async () => {
    await started.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
