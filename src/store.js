import { DatabaseSync } from 'node:sqlite';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

/**
 * Durable state. SQLite in WAL mode, so tasks, documents, subscriptions and
 * deliveries survive a process kill.
 *
 * Invariants the schema enforces:
 *  - source_tasks (query_key, generation) is UNIQUE. Recovery re-checks a task
 *    before doing anything, and the constraint is the backstop against
 *    duplicate rebuilds after overlapping recoveries.
 *  - source_receipts is written the instant a source resolves, before any
 *    post-processing. If the process dies after the source succeeded but before
 *    the task was finalized ("success notification lost"), recovery finds the
 *    receipt and finishes the original task instead of issuing a new request.
 *  - task_subscribers is the per-viewer attachment. One viewer leaving closes
 *    only its own row; the task keeps running while any viewer remains, or when
 *    the task is keep-alive (background rebuild / subscription refresh).
 *  - documents are stored once per (query_key, generation), full fidelity.
 *    Field permissions are applied on every read, never stored per viewer.
 *  - deliveries (subscription_id, dedupe_key) is UNIQUE, giving at-least-once
 *    push with receiver-side idempotency. A delivery is only marked acked after
 *    the receiver answers 2xx, so a lost ack causes a retry and a duplicate,
 *    never a silent loss.
 *  - events is append-only audit history: savings, stale reads, cancels,
 *    recovery decisions and delivery retries.
 */
export const SCHEMA = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 4000;

CREATE TABLE IF NOT EXISTS viewers (
  id          TEXT PRIMARY KEY,
  label       TEXT NOT NULL,
  role        TEXT NOT NULL CHECK (role IN ('public','analyst','steward')),
  token_hash  TEXT NOT NULL,
  created_at  INTEGER NOT NULL,
  updated_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_viewers_token ON viewers(token_hash);

CREATE TABLE IF NOT EXISTS source_tasks (
  id             TEXT PRIMARY KEY,
  query_key      TEXT NOT NULL,
  generation     INTEGER NOT NULL,
  source_id      TEXT NOT NULL,
  query_text     TEXT NOT NULL,
  locale         TEXT NOT NULL,
  state          TEXT NOT NULL CHECK (state IN
                   ('queued','running','succeeded','failed','cancelled')),
  origin         TEXT NOT NULL CHECK (origin IN ('live','rebuild','subscription','recovery')),
  keep_alive     INTEGER NOT NULL DEFAULT 0,
  ref_count      INTEGER NOT NULL DEFAULT 0,
  attempt        INTEGER NOT NULL DEFAULT 0,
  max_attempts   INTEGER NOT NULL DEFAULT 3,
  created_at     INTEGER NOT NULL,
  started_at     INTEGER,
  finished_at    INTEGER,
  deadline_at    INTEGER NOT NULL,
  cancel_reason  TEXT,
  error          TEXT,
  started_by     TEXT,
  updated_at     INTEGER NOT NULL,
  UNIQUE (query_key, generation)
);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON source_tasks(state, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_key ON source_tasks(query_key, generation DESC);

CREATE TABLE IF NOT EXISTS task_subscribers (
  task_id       TEXT NOT NULL REFERENCES source_tasks(id) ON DELETE CASCADE,
  subscriber_id TEXT NOT NULL,
  viewer_id     TEXT NOT NULL,
  viewer_label  TEXT NOT NULL,
  role          TEXT NOT NULL,
  kind          TEXT NOT NULL CHECK (kind IN ('read','rebuild','subscription')),
  joined_at     INTEGER NOT NULL,
  left_at       INTEGER,
  left_reason   TEXT,
  PRIMARY KEY (task_id, subscriber_id)
);
CREATE INDEX IF NOT EXISTS idx_task_subs_sub ON task_subscribers(viewer_id, left_at);
CREATE INDEX IF NOT EXISTS idx_task_subs_active ON task_subscribers(task_id, left_at);

CREATE TABLE IF NOT EXISTS source_receipts (
  task_id      TEXT PRIMARY KEY REFERENCES source_tasks(id) ON DELETE CASCADE,
  attempt      INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  received_at  INTEGER NOT NULL,
  consumed     INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS documents (
  query_key    TEXT NOT NULL,
  generation   INTEGER NOT NULL,
  task_id      TEXT NOT NULL,
  source_id    TEXT NOT NULL,
  query_text   TEXT NOT NULL,
  locale       TEXT NOT NULL,
  share_token  TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  fetched_at   INTEGER NOT NULL,
  expires_at   INTEGER NOT NULL,
  usable       INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (query_key, generation)
);
CREATE INDEX IF NOT EXISTS idx_docs_fetched ON documents(query_key, fetched_at DESC);

CREATE TABLE IF NOT EXISTS subscriptions (
  id                 TEXT PRIMARY KEY,
  viewer_id          TEXT NOT NULL,
  viewer_label       TEXT NOT NULL,
  role               TEXT NOT NULL,
  source_id          TEXT NOT NULL,
  query_text         TEXT NOT NULL,
  query_key          TEXT NOT NULL,
  locale             TEXT NOT NULL,
  max_age_ms         INTEGER NOT NULL,
  stale_if_error_ms  INTEGER NOT NULL DEFAULT 0,
  callback_url       TEXT NOT NULL,
  secret             TEXT NOT NULL,
  active             INTEGER NOT NULL DEFAULT 1,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL,
  last_seen_at       INTEGER NOT NULL,
  next_refresh_at    INTEGER NOT NULL,
  last_delivered_gen INTEGER,
  UNIQUE (viewer_id, source_id, query_key, max_age_ms)
);
CREATE INDEX IF NOT EXISTS idx_subs_key ON subscriptions(query_key, active);
CREATE INDEX IF NOT EXISTS idx_subs_active ON subscriptions(active, next_refresh_at);
CREATE INDEX IF NOT EXISTS idx_subs_viewer ON subscriptions(viewer_id, active);

CREATE TABLE IF NOT EXISTS deliveries (
  id              TEXT PRIMARY KEY,
  subscription_id TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
  task_id         TEXT NOT NULL REFERENCES source_tasks(id) ON DELETE CASCADE,
  generation      INTEGER NOT NULL,
  dedupe_key      TEXT NOT NULL,
  state           TEXT NOT NULL CHECK (state IN ('pending','acked','dropped')),
  attempts        INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL,
  last_error      TEXT,
  stale           INTEGER NOT NULL DEFAULT 0,
  created_at      INTEGER NOT NULL,
  sent_at         INTEGER,
  acked_at        INTEGER,
  UNIQUE (subscription_id, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries(state, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_sub ON deliveries(subscription_id, created_at DESC);

CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,
  at         INTEGER NOT NULL,
  kind       TEXT NOT NULL,
  task_id    TEXT,
  viewer_id  TEXT,
  query_key  TEXT,
  detail     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_at ON events(at);
CREATE INDEX IF NOT EXISTS idx_events_kind ON events(kind, at);
CREATE INDEX IF NOT EXISTS idx_events_key ON events(query_key, at);
`;

export function openStore(filename) {
  if (filename !== ':memory:') mkdirSync(dirname(filename), { recursive: true });
  const db = new DatabaseSync(filename);
  db.exec(SCHEMA);
  return db;
}

export function transaction(db, fn) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const value = fn();
    db.exec('COMMIT');
    return value;
  } catch (err) {
    try { db.exec('ROLLBACK'); } catch { /* already unwound */ }
    throw err;
  }
}
