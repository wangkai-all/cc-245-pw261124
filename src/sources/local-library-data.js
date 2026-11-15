import { createHash } from 'node:crypto';

/**
 * Deterministic local source records. This is the "local source" used to make
 * concurrency, cancellation, expiry and recovery reproducible without depending
 * on the public internet. Query flags understood by the adapter:
 *
 *   slow <ms>   delay before answering (honors cancellation)
 *   late <ms>   delay before answering, but ignores cancellation; used to
 *               simulate a source that completes after everyone stopped waiting
 *   fail        answer with a retryable failure
 *
 * Anything else returns a stable, synthesized catalog record.
 */

const SHELVES = ['systems', 'networks', 'indexing', 'archives', 'interfaces'];

function hashInt(text, mod) {
  const digest = createHash('sha256').update(text).digest();
  return digest.readUInt32BE(0) % mod;
}

export function libraryRecord(query) {
  const cleaned = query.replace(/^(slow|late)\s*\d*\s*/i, '').replace(/\bfail\b/gi, '').trim()
    || 'untitled inquiry';
  const shelf = SHELVES[hashInt(cleaned.toLowerCase(), SHELVES.length)];
  const code = String(100 + hashInt(`code:${cleaned.toLowerCase()}`, 900));
  const title = `${cleaned.replace(/\b\w/g, (c) => c.toUpperCase())} — field notes`;
  const snippet = [
    `A local catalog entry on ${cleaned}.`,
    `Shelf ${shelf}, call number L-${code}.`,
    'The record is deterministic so repeated runs can be compared byte for byte.',
  ].join(' ');
  return {
    title,
    url: `local://library/${shelf}/L-${code}`,
    snippet,
    published_at: `2026-0${1 + hashInt(cleaned, 9)}-1${hashInt(`d:${cleaned}`, 9)}`,
    shelf,
  };
}

export function libraryHost() {
  return 'local-library.invalid';
}
