import { newId } from './ids.js';

export const FIELD_CATALOG = Object.freeze({
  title: { tier: 'public', label: 'Title' },
  url: { tier: 'public', label: 'URL' },
  snippet: { tier: 'public', label: 'Snippet' },
  published_at: { tier: 'public', label: 'Published' },
  score: { tier: 'internal', label: 'Relevance score' },
  crawl_meta: { tier: 'internal', label: 'Crawl metadata' },
  contact_email: { tier: 'restricted', label: 'Contact email' },
  matched_terms: { tier: 'restricted', label: 'Matched query terms' },
});

export const TIER_RANK = Object.freeze({ public: 0, internal: 1, restricted: 2 });

export const VIEWER_ROLES = Object.freeze({
  public: { label: 'Public', clearance: 'public', canCancelOthers: false },
  analyst: { label: 'Analyst', clearance: 'internal', canCancelOthers: true },
  steward: { label: 'Steward', clearance: 'restricted', canCancelOthers: true },
});

export function roleProfile(role) {
  return VIEWER_ROLES[role] ?? VIEWER_ROLES.public;
}

export function canReadTier(tier, clearance) {
  return TIER_RANK[tier] <= TIER_RANK[clearance ?? 'public'];
}

export function visibleFields(clearance) {
  return Object.entries(FIELD_CATALOG)
    .filter(([, spec]) => canReadTier(spec.tier, clearance))
    .map(([field]) => field);
}

export function withheldFields(clearance) {
  return Object.entries(FIELD_CATALOG)
    .filter(([, spec]) => !canReadTier(spec.tier, clearance))
    .map(([field, spec]) => ({ field, tier: spec.tier, label: spec.label }));
}

/**
 * Project one stored, full-fidelity payload for one viewer.
 *
 * `document` is the shared, complete result record. The projection is applied at
 * read time and never stored per-viewer, so upgrading a viewer's clearance
 * exposes fields on the very next read without a refetch, and downgrading it
 * hides them immediately even if the bytes are still in the shared cache.
 */
export function projectDocument(document, clearance) {
  if (!document) return { result: null, withheld: [] };
  const result = {};
  const withheld = [];
  for (const [field, spec] of Object.entries(FIELD_CATALOG)) {
    if (!(field in document)) continue;
    if (canReadTier(spec.tier, clearance)) result[field] = document[field];
    else withheld.push({ field, tier: spec.tier, label: spec.label });
  }
  return { result, withheld };
}

/**
 * Build the shared document from raw source output. This is the only place
 * sensitive fields are added; everything downstream reads from store and then
 * projects. `shareToken` is issued per (logical key, generation) and is what the
 * UI compares to prove two viewers consumed the same upstream bytes.
 */
export function buildDocument({ raw, logical, shareToken, fetchedAt }) {
  return {
    id: newId('doc'),
    logical_key: logical.key,
    source_id: logical.sourceId,
    query: logical.query,
    locale: logical.locale,
    share_token: shareToken,
    fetched_at: fetchedAt,
    title: raw.title,
    url: raw.url,
    snippet: raw.snippet,
    published_at: raw.published_at,
    score: raw.score,
    crawl_meta: raw.crawl_meta,
    contact_email: raw.contact_email,
    matched_terms: raw.matched_terms,
  };
}
