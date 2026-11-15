import { createHash, randomBytes, timingSafeEqual } from 'node:crypto';
import { newId } from './ids.js';
import { roleProfile, VIEWER_ROLES } from './permissions.js';

/**
 * Viewer identity. Each browser holds a capability token; only its SHA-256 is
 * stored. Role decides clearance (which fields are projected into responses)
 * and whether the viewer may cancel work started by someone else. Because
 * permissions live on the viewer, two viewers sharing one cached document can
 * still receive different field sets.
 */
function hashToken(token) {
  return createHash('sha256').update(token).digest('hex');
}

export function tokenMatches(token, hash) {
  const a = Buffer.from(hashToken(token));
  const b = Buffer.from(hash);
  return a.length === b.length && timingSafeEqual(a, b);
}

export function createViewer(db, { label, role }, at) {
  const profile = roleProfile(role);
  const id = newId('vw');
  const token = `vt_${randomBytes(24).toString('base64url')}`;
  db.prepare(`
    INSERT INTO viewers (id, label, role, token_hash, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(id, label, role in VIEWER_ROLES ? role : 'public', hashToken(token), at, at);
  return { id, token, label, role: role in VIEWER_ROLES ? role : 'public', clearance: profile.clearance };
}

export function viewerFromToken(db, token) {
  if (typeof token !== 'string' || !token.startsWith('vt_')) return null;
  const hash = hashToken(token);
  const row = db.prepare('SELECT * FROM viewers WHERE token_hash = ?').get(hash);
  if (!row) {
    // Fall back to a scan only if a hash collision-free lookup failed; keeps the
    // fast path a single indexed query in practice.
    const all = db.prepare('SELECT * FROM viewers').all();
    const match = all.find((candidate) => tokenMatches(token, candidate.token_hash));
    if (!match) return null;
    return shapeViewer(match);
  }
  return shapeViewer(row);
}

export function viewerById(db, id) {
  const row = db.prepare('SELECT * FROM viewers WHERE id = ?').get(id);
  return row ? shapeViewer(row) : null;
}

function shapeViewer(row) {
  const profile = roleProfile(row.role);
  return {
    id: row.id,
    label: row.label,
    role: row.role,
    clearance: profile.clearance,
    canCancelOthers: profile.canCancelOthers,
  };
}

export function listViewers(db) {
  return db.prepare('SELECT id, label, role, created_at FROM viewers ORDER BY created_at ASC').all()
    .map((row) => ({ ...row, clearance: roleProfile(row.role).clearance }));
}
