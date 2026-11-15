import { load } from 'cheerio';
import { normalizeLocale } from '../keys.js';

/**
 * Source adapters. An adapter receives a validated logical query and an
 * AbortSignal, and returns raw source output at full fidelity. It must honor
 * the signal promptly: the task layer relies on abort to stop upstream work
 * once nobody is waiting, and must not swallow the resulting AbortError.
 *
 * Which fields a viewer may see is decided later, by src/permissions.js.
 */

export const USER_AGENT =
  'QueryDedupeWorkbench/1.0 (local research demo; contact: workbench@example.invalid)';

const REQUEST_TIMEOUT_MS = 20_000;

export class SourceError extends Error {
  constructor(message, { status = 502, retryable = true, cause } = {}) {
    super(message);
    this.name = 'SourceError';
    this.statusCode = status;
    this.retryable = retryable;
    if (cause) this.cause = cause;
  }
}

export class SourceCancelledError extends Error {
  constructor() {
    super('source fetch was cancelled');
    this.name = 'SourceCancelledError';
    this.cancelled = true;
  }
}

function composeSignal(signal) {
  const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

async function fetchText(url, { signal, headers = {}, accept = 'text/html' } = {}) {
  let response;
  try {
    response = await fetch(url, {
      signal: composeSignal(signal),
      redirect: 'follow',
      headers: {
        'user-agent': USER_AGENT,
        accept,
        'accept-language': 'en;q=0.9,*;q=0.5',
        ...headers,
      },
    });
  } catch (err) {
    if (err?.name === 'AbortError' || err?.name === 'TimeoutError') {
      if (signal?.aborted) throw new SourceCancelledError();
      throw new SourceError('upstream request timed out', { status: 504, retryable: true, cause: err });
    }
    throw new SourceError(`upstream request failed: ${err?.message ?? 'unknown error'}`, {
      status: 502,
      retryable: true,
      cause: err,
    });
  }
  if (!response.ok) {
    const retryable = response.status >= 500 || response.status === 429;
    throw new SourceError(`upstream returned HTTP ${response.status}`, {
      status: retryable ? 503 : 502,
      retryable,
    });
  }
  const text = await response.text();
  if (signal?.aborted) throw new SourceCancelledError();
  return { text, contentType: response.headers.get('content-type') ?? '', finalUrl: response.url };
}

export function stableNoiseSeed(text) {
  let hash = 2166136261;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

export function pseudoScore(seed, title) {
  return Number((0.35 + (((seed ^ stableNoiseSeed(title)) >>> 0) % 6000) / 10000).toFixed(4));
}

const STOPWORDS = new Set([
  'the', 'and', 'for', 'with', 'from', 'that', 'this', 'are', 'was', 'were',
  'a', 'an', 'of', 'to', 'in', 'on', 'is', 'it', 'as', 'at', 'by', 'or',
]);

export function matchedTerms(query) {
  const seen = new Set();
  for (const word of query.toLowerCase().split(/[^\p{L}\p{N}]+/u)) {
    if (word.length > 2 && !STOPWORDS.has(word)) seen.add(word);
  }
  return [...seen].slice(0, 12);
}

export function contactEmail(host) {
  return `research+${host.replace(/[^a-z0-9]/gi, '-').toLowerCase()}@example.invalid`;
}

export function crawlMeta(bytes, extra = {}) {
  return { bytes, fetched_via: 'server-fetch', ...extra };
}

const ADAPTERS = new Map();

export function registerSource(adapter) {
  ADAPTERS.set(adapter.id, adapter);
}

export function getSource(id) {
  return ADAPTERS.get(id) ?? null;
}

export function listSources() {
  return [...ADAPTERS.values()].map(
    ({ id, label, description, kind, supportsLocale, homepage, defaultMaxAgeMs, caseSensitiveQuery }) => ({
      id, label, description, kind, supportsLocale, homepage, defaultMaxAgeMs,
      case_sensitive_query: Boolean(caseSensitiveQuery),
    }),
  );
}

export function sourceDefaultMaxAgeMs(sourceId) {
  return ADAPTERS.get(sourceId)?.defaultMaxAgeMs ?? 60_000;
}

function asSourceFailure(err, signal) {
  if (signal?.aborted || err?.cancelled) return new SourceCancelledError();
  if (err instanceof SourceError) return err;
  return new SourceError(err?.message ?? 'source failed', { retryable: true, cause: err });
}

registerSource({
  id: 'wikipedia',
  label: 'Wikipedia search',
  description: 'Live MediaWiki full-text search. Returns article titles, snippets and last-modified data.',
  kind: 'real-web',
  supportsLocale: true,
  homepage: 'https://en.wikipedia.org',
  defaultMaxAgeMs: 10 * 60_000,
  async fetch({ query, locale, signal }) {
    const lang = normalizeLocale(locale).split('-')[0];
    const params = new URLSearchParams({
      search: query,
      title: 'Special:Search',
      fulltext: '1',
      ns0: '1',
      limit: '8',
    });
    const url = `https://${lang}.wikipedia.org/w/index.php?${params}`;
    let text;
    try {
      ({ text } = await fetchText(url, { signal }));
    } catch (err) {
      throw asSourceFailure(err, signal);
    }
    const $ = load(text);
    const results = $('li.mw-search-result').toArray().slice(0, 8).map((element) => {
      const anchor = $(element).find('.mw-search-result-heading a').first();
      const rawHref = anchor.attr('href') ?? '';
      return {
        title: anchor.text().replace(/\s+/g, ' ').trim(),
        url: rawHref.startsWith('http') ? rawHref : `https://${lang}.wikipedia.org${rawHref}`,
        snippet: $(element).find('.searchresult').text().replace(/\s+/g, ' ').trim().slice(0, 280),
        published_at: $(element).find('.mw-search-result-data').text().trim() || null,
      };
    }).filter((result) => result.title && result.url && !result.url.endsWith('/'));
    const first = results[0];
    const noMatches = !first && /There were no results matching|no results matching/i.test(text);
    if (!first && !noMatches) {
      throw new SourceError('could not parse the Wikipedia search results page', {
        status: 502,
        retryable: true,
      });
    }
    const seed = stableNoiseSeed(query);
    return {
      title: first?.title ?? `No Wikipedia matches for “${query}”`,
      url: first?.url ?? url,
      snippet: first?.snippet
        ?? 'The source answered successfully and reported zero matching articles.',
      published_at: first?.published_at ?? null,
      score: first ? pseudoScore(seed, first.title) : null,
      crawl_meta: crawlMeta(text.length, {
        host: `${lang}.wikipedia.org`,
        results: results.length,
      }),
      contact_email: contactEmail(`${lang}.wikipedia.org`),
      matched_terms: matchedTerms(query),
      result_count: results.length,
      results,
    };
  },
});

registerSource({
  id: 'duckduckgo',
  label: 'DuckDuckGo HTML',
  description: 'Live HTML search endpoint. Public and rate-limited; failures are retried with backoff.',
  kind: 'real-web',
  supportsLocale: true,
  homepage: 'https://duckduckgo.com',
  defaultMaxAgeMs: 5 * 60_000,
  async fetch({ query, locale, signal }) {
    const localeTag = normalizeLocale(locale);
    const params = new URLSearchParams({ q: query, kl: localeTag === 'en' ? 'us-en' : localeTag });
    const url = `https://html.duckduckgo.com/html/?${params}`;
    let text;
    try {
      ({ text } = await fetchText(url, {
        signal,
        headers: {
          'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        },
      }));
    } catch (err) {
      throw asSourceFailure(err, signal);
    }
    const $ = load(text);
    const results = $('.result').toArray().slice(0, 8).map((element) => {
      const anchor = $(element).find('.result__a').first();
      let href = anchor.attr('href') ?? '';
      try {
        const wrapped = new URL(href, 'https://html.duckduckgo.com').searchParams.get('uddg');
        if (wrapped) href = wrapped;
      } catch { /* keep the original href */ }
      return {
        title: anchor.text().replace(/\s+/g, ' ').trim(),
        url: href,
        snippet: $(element).find('.result__snippet').text().replace(/\s+/g, ' ').trim().slice(0, 280),
        published_at: $(element).find('.result__timestamp').text().trim() || null,
      };
    }).filter((result) => result.title && /^https?:\/\//.test(result.url));
    const first = results[0];
    if (!first) {
      throw new SourceError('DuckDuckGo returned no parseable results (usually rate limiting)', {
        status: 503,
        retryable: true,
      });
    }
    return {
      title: first.title,
      url: first.url,
      snippet: first.snippet,
      published_at: first.published_at,
      score: pseudoScore(stableNoiseSeed(query), first.title),
      crawl_meta: crawlMeta(text.length, { host: 'html.duckduckgo.com', results: results.length }),
      contact_email: contactEmail('duckduckgo.com'),
      matched_terms: matchedTerms(query),
      result_count: results.length,
      results,
    };
  },
});

registerSource({
  id: 'page',
  label: 'Fetch one page',
  description: 'Fetches a real http(s) URL (or example.com) and extracts its title and description.',
  kind: 'real-web',
  supportsLocale: false,
  // The query is a URL, and URL paths are case-sensitive: folding case would
  // fetch a different page than the caller asked for.
  caseSensitiveQuery: true,
  homepage: 'https://example.com',
  defaultMaxAgeMs: 30 * 60_000,
  async fetch({ query, signal }) {
    const candidate = query.trim();
    const target = /^https?:\/\//i.test(candidate)
      ? candidate
      : candidate.toLowerCase() === 'example.com' || candidate.toLowerCase() === 'example'
        ? 'https://example.com'
        : null;
    if (!target) {
      throw new SourceError('this source expects a full http(s) URL or "example.com"', {
        status: 400,
        retryable: false,
      });
    }
    let parsed;
    try { parsed = new URL(target); } catch {
      throw new SourceError('not a valid URL', { status: 400, retryable: false });
    }
    let text;
    try {
      ({ text } = await fetchText(parsed.href, { signal, accept: 'text/html,application/xhtml+xml' }));
    } catch (err) {
      throw asSourceFailure(err, signal);
    }
    const $ = load(text);
    $('script, style, noscript, template').remove();
    const title = $('title').first().text().replace(/\s+/g, ' ').trim() || parsed.hostname;
    const description = ($('meta[name="description"]').attr('content') ?? '').trim()
      || $('main').first().text().replace(/\s+/g, ' ').trim().slice(0, 280)
      || $('body').first().text().replace(/\s+/g, ' ').trim().slice(0, 280);
    return {
      title,
      url: parsed.href,
      snippet: description,
      published_at: ($('meta[property="article:modified_time"]').attr('content')
        ?? $('meta[name="last-modified"]').attr('content')
        ?? null),
      score: null,
      crawl_meta: crawlMeta(Buffer.byteLength(text), { host: parsed.hostname, content_type: 'text/html' }),
      contact_email: contactEmail(parsed.hostname),
      matched_terms: matchedTerms(title),
      result_count: 1,
      results: [{ title, url: parsed.href, snippet: description, published_at: null }],
    };
  },
});

registerSource({
  id: 'local-library',
  label: 'Local library',
  description: 'Deterministic local catalog. Add “slow 4000”, “late 4000” or “fail” to simulate timing and errors.',
  kind: 'local',
  supportsLocale: false,
  homepage: null,
  defaultMaxAgeMs: 60_000,
  async fetch({ query, signal }) {
    const lowered = query.toLowerCase();
    if (/\bfail\b/.test(lowered)) {
      throw new SourceError('the local library refused this request (query contains “fail”)', {
        status: 503,
        retryable: true,
      });
    }
    // "late" intentionally ignores cancellation: it models a source that
    // completes after every subscriber left, which recovery and the
    // late-completion rule must handle. "slow" honors cancellation.
    const slow = /\bslow\s*(\d{1,6})?\b/.exec(lowered);
    const late = /\blate\s*(\d{1,6})?\b/.exec(lowered);
    const delay = Number(slow?.[1] ?? (slow ? 3000 : late?.[1] ?? (late ? 3000 : 0)));
    if (delay > 0) {
      await new Promise((resolve) => {
        const timer = setTimeout(resolve, Math.min(delay, 60_000));
        if (!late) {
          signal?.addEventListener('abort', () => {
            clearTimeout(timer);
            resolve('aborted');
          }, { once: true });
        }
      });
      if (!late && signal?.aborted) throw new SourceCancelledError();
    }
    if (signal?.aborted && !late) throw new SourceCancelledError();
    const { libraryRecord } = await import('./local-library-data.js');
    const record = libraryRecord(query);
    return {
      title: record.title,
      url: record.url,
      snippet: record.snippet,
      published_at: record.published_at,
      score: pseudoScore(stableNoiseSeed(query), record.title),
      crawl_meta: crawlMeta(record.snippet.length, { host: 'local-library.invalid', shelf: record.shelf }),
      contact_email: `library-${record.shelf}@example.invalid`,
      matched_terms: matchedTerms(query),
      result_count: 1,
      results: [{ title: record.title, url: record.url, snippet: record.snippet, published_at: record.published_at }],
    };
  },
});
