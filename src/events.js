import { newId } from './ids.js';

/**
 * Append-only event log. Every decision worth explaining later is written here,
 * in the same transaction as the state change where possible, so the UI can
 * reconstruct why a request was served from cache, served stale, cancelled, or
 * rebuilt.
 */
export function logEvent(db, at, event) {
  db.prepare(`
    INSERT INTO events (at, kind, task_id, viewer_id, query_key, detail)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    at,
    event.kind,
    event.taskId ?? null,
    event.viewerId ?? null,
    event.queryKey ?? null,
    event.detail === undefined ? null : JSON.stringify(event.detail),
  );
}

export function recentEvents(db, { limit = 100, kind = null, queryKey = null, afterSeq = 0 } = {}) {
  const clauses = ['seq > ?'];
  const args = [afterSeq];
  if (kind) { clauses.push('kind = ?'); args.push(kind); }
  if (queryKey) { clauses.push('query_key = ?'); args.push(queryKey); }
  const rows = db.prepare(`
    SELECT seq, at, kind, task_id, viewer_id, query_key, detail
    FROM events WHERE ${clauses.join(' AND ')} ORDER BY seq DESC LIMIT ?
  `).all(...args, Math.min(Math.max(limit, 1), 500));
  return rows.map((row) => ({
    seq: row.seq,
    at: row.at,
    kind: row.kind,
    taskId: row.task_id,
    viewerId: row.viewer_id,
    queryKey: row.query_key,
    detail: row.detail ? JSON.parse(row.detail) : null,
  }));
}

export function eventId() {
  return newId('evt');
}
