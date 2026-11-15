import { createHash } from 'node:crypto';

/**
 * What is shared, and what is not.
 *
 * A "logical query" is the cache key:
 *
 *     (source id, normalized query text, locale)
 *
 * Everything that can change the bytes coming back from a source is in the key.
 * Everything that is viewer-specific is not:
 *
 *   max age / freshness tolerance  -> evaluated against the cache entry's age
 *   allowed fields / clearance     -> projected when a viewer reads a result
 *   cancellation                   -> per-subscriber attachment to one task
 *
 * That split is the whole point. If freshness were in the key, two viewers with
 * different tolerances could never share one upstream fetch. If permissions were
 * in the key, the same bytes would be fetched and stored once per clearance
 * tier. If permissions were *ignored* when reading, a low-clearance viewer could
 * read restricted fields out of a shared cache entry. So permissions are applied
 * at read time, on every path: direct reads, cached reads, stale reads and push
 * notifications.
 *
 * A "source task" is one attempt to fill a logical query. Tasks are numbered by
 * generation per logical key. Concurrent readers join the active task instead of
 * starting another. A rebuild creates a new generation; the previous generation
 * keeps its cache entry and stays readable under the stale policy.
 *
 * The key deliberately has no timestamp. Freshness buckets were considered and
 * rejected: wall-clock buckets split readers who arrive either side of a
 * boundary, and make "rebuild now" harder to express. Instead, one active task
 * per logical key coalesces concurrent work, and completed entries are aged
 * against each viewer's max age.
 */

export const MAX_QUERY_LENGTH = 240;
export const MAX_LOCALE_LENGTH = 16;

export class QueryValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'QueryValidationError';
    this.statusCode = 400;
  }
}

/**
 * Normalize query text into the form used as the shared identity.
 *
 * `caseSensitive` exists for sources whose query is not free text — the `page`
 * adapter treats the query as a URL, and URL paths are case-sensitive, so
 * lowercasing would silently fetch the wrong page. Every free-text source is
 * case-insensitive, which is the default.
 */
export function normalizeQuery(raw, { caseSensitive = false } = {}) {
  if (typeof raw !== 'string') throw new QueryValidationError('query must be a string');
  const collapsed = raw
    .normalize('NFKC')
    .replace(/[\u0000-\u001f\u007f]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
  // Case folding is deliberate: every free-text source treats query text
  // case-insensitively, so "Web Crawler" and "web crawler" would return the
  // same bytes. Without folding they would be two cache entries and two
  // upstream requests for identical results.
  const normalized = caseSensitive ? collapsed : collapsed.toLowerCase();
  if (!normalized) throw new QueryValidationError('query must not be empty');
  if (normalized.length > MAX_QUERY_LENGTH) {
    throw new QueryValidationError(`query must be at most ${MAX_QUERY_LENGTH} characters`);
  }
  return normalized;
}

export function normalizeLocale(raw) {
  if (raw === undefined || raw === null || raw === '') return 'en';
  if (typeof raw !== 'string') throw new QueryValidationError('locale must be a string');
  const tag = raw.trim().toLowerCase().replace(/_/g, '-');
  if (!/^[a-z]{2,3}(-[a-z0-9]{1,8})?$/.test(tag) || tag.length > MAX_LOCALE_LENGTH) {
    throw new QueryValidationError('locale must look like "en" or "pt-br"');
  }
  return tag;
}

function sha256(text) {
  return createHash('sha256').update(text).digest('hex');
}

/**
 * `caseSensitiveQuery` must be supplied by the adapter when its query is not
 * free text (the page adapter's query is a URL path). Everything else about the
 * key is unchanged, so the sharing unit is still
 * (source, normalized query text, locale).
 */
export function logicalKey({ sourceId, query, locale, caseSensitiveQuery = false }) {
  const normalizedQuery = normalizeQuery(query, { caseSensitive: caseSensitiveQuery });
  const normalizedLocale = normalizeLocale(locale);
  const canonical = JSON.stringify({
    v: 1,
    sourceId,
    query: normalizedQuery,
    locale: normalizedLocale,
    ...(caseSensitiveQuery ? { caseSensitive: true } : {}),
  });
  return {
    key: `qk_${sha256(canonical).slice(0, 32)}`,
    sourceId,
    query: normalizedQuery,
    locale: normalizedLocale,
    canonical,
  };
}

/**
 * Identity of a push notification. Generation is unique per logical key, so one
 * delivery per subscription per generation is the at-least-once unit. The
 * receiver dedupes on it.
 */
export function deliveryDedupeKey(logicalKeyValue, generation) {
  return `${logicalKeyValue}:g${generation}`;
}
