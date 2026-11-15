# Query dedupe workbench

One enriched query answer from a source is expensive. Many people ask the same
question, but each of them has their own cancellation, their own freshness
tolerance, and their own field permissions. This workbench ties those concerns
together:

- **query dedupe** — identical queries share one upstream fetch
- **source tasks** — one durable task per attempt, with generations
- **cache** — completed answers stay readable, including after a rebuild
- **subscriptions** — durable, signed, at-least-once push to a receiver
- **real web pages** — the UI is a real server-rendered app, and two of the
  source adapters hit the public internet

It runs from a local SQLite file, has no build step, and needs Node 22.5+ for
the built-in `node:sqlite`.

```bash
npm install
npm start            # http://127.0.0.1:4180
npm test             # 47 tests, including real-network sources
```

Open the page, create two viewers with different roles, and press **Read** in
two tabs. The source task count stays at one; the two results carry the same
`share_token` and different fields.

---

## 1. Sharing unit and cache key

The decision that everything else follows from:

> **Sharing unit = `(source id, normalized query text, locale)`, plus a
> generation number for the bytes one upstream task produced.**

Implemented in `src/keys.js` as `logicalKey()`, which hashes that triple into a
stable `qk_…` key. The key is the cache key **and** the single-flight key.

Normalization: Unicode NFKC, control characters → space, whitespace collapsed,
trimmed, and case folded. So `" Web   Crawler "`, `"web crawler"` and
`"WEB CRAWLER"` are one entry. Case folding is per source: the `page` adapter's
query is a URL, and URL paths are case sensitive, so it declares
`caseSensitiveQuery` and its key keeps the case. Locales are normalized to a
lowercase BCP-47-ish tag (`PT_br` → `pt-br`) and default to `en`.

### What is deliberately *not* in the key

| Concern | Where it lives instead | Why |
| --- | --- | --- |
| freshness / max age | evaluated against the cached entry's age, per viewer | otherwise two viewers with different tolerances could never share one fetch |
| field permissions | projection at read time, per viewer | otherwise the same bytes are stored once per clearance tier |
| cancellation | per-subscriber attachment to one task | one person leaving must not kill the fetch |
| timestamp buckets | rejected | readers either side of a bucket boundary would split, and "rebuild now" becomes impossible to express |

The cost of this choice is visible and intentional: freshness is a *read-time*
judgement, so a stale tolerance can never be part of dedupe, and a viewer's
clearance is applied to cached bytes on every read.

### How coalescing works

`Coordinator.joinOrStartTask()` looks for a `queued`/`running` task for the key.
If one exists, the viewer is attached as a subscriber and the existing flight's
promise is awaited; if not, generation `max(generation) + 1` is created and a
flight started. Each avoided fetch writes a durable
`savings.upstream-request-avoided` event, tagged with why
(`coalesced-onto-active-task` or `fresh-cache-hit`), so the ledger is
explainable rather than a counter.

The key has no per-viewer data, so `share_token` is issued once per
`(logical key, generation)`. Two viewers seeing the same token is proof they
read the same upstream bytes.

---

## 2. Permissions: one document, many projections

A source returns a **full-fidelity** document (all eight fields in
`src/permissions.js`) and it is stored once. `projectDocument()` runs on every
read path — cache, source, stale, historical, and subscription push — and
omits, rather than nulls, fields above the viewer's clearance:

| Tier | Fields | Visible to |
| --- | --- | --- |
| public | `title`, `url`, `snippet`, `published_at` | everyone |
| internal | `score`, `crawl_meta` | analyst, steward |
| restricted | `contact_email`, `matched_terms` | steward |

Consequences that the tests pin:

- identical query text does **not** mean identical bytes on the wire: a
  public viewer gets four fields while an analyst gets six from the same
  `share_token`;
- withheld field *values* never appear in the serialized result at all, so a
  filtering bug cannot leak them as `null` vs present;
- upgrading a role exposes restricted fields on the next read with no refetch,
  and downgrading hides them immediately;
- the sender projects again before each push, and the receiver projects a third
  time from the stored document, so a widened payload is cut back down at the
  subscription's role.

---

## 3. Cancellation

Cancellation is per subscriber, not per task. Every read attaches a
`task_subscribers` row keyed by `(viewer id, client token)`.

- `POST /api/tasks/:id/leave` closes only that attachment. If anyone else is
  still waiting, the task keeps running and the leaver's HTTP request is
  released with `409 { code: "abandoned" }` instead of hanging until the shared
  fetch finishes.
- Only when the last subscriber leaves, and the task is not keep-alive, is the
  task cancelled: the row becomes `cancelled` and the source's abort signal
  fires.
- `POST /api/tasks/:id/cancel` is a *deliberate* whole-task cancel. It is
  allowed for the starter, or for a role with `canCancelOthers`; anyone else
  gets `403 not_your_task`.

### Late completion

Sources can ignore cancellation and finish anyway (`late 4000` in the local
library). The bytes are then written as a document with `usable = 0`, the task
stays `cancelled`, and a `task.late-completion` event is recorded. It is
readable for audit via `POST /api/read-generation`, but it is never served as
fresh cache; the next read starts a new generation.

---

## 4. Expiry, rebuild, and reading old data

Generations are strictly increasing per logical key, with a
`UNIQUE(query_key, generation)` guard so a retry or recovery can never create a
duplicate.

- A read older than the viewer's `max_age_ms` starts the next generation.
- A rebuild never deletes the previous generation. Old documents stay in
  `documents` and `POST /api/read-generation` reads them explicitly, marked
  `served: "stale"` with a note saying it is not a freshness claim.
- If the rebuild fails and the viewer supplied `stale_if_error_ms`, the
  previous generation is served **flagged**: `admission.served = "stale"`,
  `admission.stale = true`, `usable_as_fresh = false`, and a human-readable
  note. Permissions are still projected on that path. With no stale window,
  the failure surfaces rather than being papered over.

`POST /api/demo/expire` ages cached documents by an hour so this can be
exercised from the UI without waiting.

---

## 5. Sources, failures, and recovery

Adapters (`src/sources/index.js`): `wikipedia` (live MediaWiki search),
`duckduckgo` (live HTML endpoint), `page` (fetch a real URL), and
`local-library` (deterministic, with `slow N`, `late N`, and `fail` query
flags so every failure mode is reproducible offline).

A source answer is written to `source_receipts` **before** any post-processing.
That single ordering decision gives two recoveries:

- **Lost success notification.** The source answered, then the process died
  before the task was finalised. `recover()` finds the unconsumed receipt and
  finishes the original task from it — no second upstream request.
- **Interrupted task.** No receipt: `recover()` adopts the still-live task and
  resumes it in place. It does not create a new generation or a new row.

`recover()` is bounded and deadline-aware:

- tasks past `deadline_at` are failed, not adopted;
- at most `max_adoptions` tasks are adopted per pass, and the rest are reported
  in `skipped` rather than silently dropped;
- pending deliveries are made due immediately after recovery.

The UI's **Failure drills** call the same endpoints the tests do:
`POST /api/demo/crash`, `/api/demo/recover`, `/api/demo/lose-notification`,
`/api/demo/expire`, plus receiver faults.

---

## 6. Durable subscriptions and the receiver

Subscriptions are keyed by `(viewer, source, query key, max_age_ms)` and are
persisted. The refresher ticks, finds subscriptions whose usable document is
older than *their own* max age, and rebuilds the key as keep-alive work;
subscribers to the same key coalesce in the same tick.

Delivery is **at-least-once**:

- completing a generation fans out one `deliveries` row per active
  subscription, deduped on `(subscription_id, dedupe_key)` where
  `dedupe_key = queryKey:gGeneration`;
- the dispatcher retries with exponential backoff, only marking `acked` when it
  sees a 2xx *and* reads the acknowledgement; after the cap the row is
  `dropped` with its last error;
- a lost ack retries with the same dedupe key, and the receiver's inbox has a
  `(subscription_id, dedupe_key)` primary key, so the duplicate is absorbed and
  counted as a duplicate rather than double-delivered;
- each push is HMAC-SHA256 signed over `timestamp.rawBody` with the
  subscription's secret plus a timestamp window check;
- one receiver's failure is recorded on its own rows and does not block another
  subscription's delivery.

The receiver is a real second SQLite database with its own inbox, fault
injection (`/api/receiver/faults`), and projection. `GET /api/receiver` shows
what actually arrived, including `delivery_count` for absorbed retries.

---

## 7. Savings and old-data impact in the UI

The right-hand column is the accounting the request asked for:

- **Savings & stale reads** — upstream requests, requests avoided, cache reads,
  stale reads, late completions, pending/acked/dropped deliveries, and a hit
  rate. `GET /api/state` → `stats`; every number is derived from durable
  `events`, not an in-memory counter, so it survives a restart.
- **Per-query ledger** — per logical query: tasks created, readers coalesced,
  source/cache/stale reads, upstream requests avoided, late completions,
  distinct viewers, and each generation with its age and `usable` flag.
- **Shared cache** — every document with its `share_token`, age, read count,
  task state, and whether a cancelled task's late bytes are stored but
  unusable.

The saved-request count answers "what did dedupe buy"; the stale and
late-completion counts, plus the `usable: false` markers, answer "what did
reading old data cost".

---

## 8. API sketch

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/meta` | sources, roles, field catalog, sharing policy |
| POST | `/api/viewers` | create a viewer, returns the token once |
| POST | `/api/viewers/login` | resolve a stored token |
| POST | `/api/read` | deduped read with per-viewer `max_age_ms` / `stale_if_error_ms` |
| POST | `/api/rebuild` | force the next generation (`wait: false` to poll) |
| GET | `/api/generations?query_key=` | generation history |
| POST | `/api/read-generation` | read an older generation explicitly |
| POST | `/api/tasks/:id/leave` | detach this viewer only |
| POST | `/api/tasks/:id/cancel` | cancel the whole task (authorised) |
| GET/POST | `/api/subscriptions` | list / create durable subscriptions |
| POST | `/api/subscriptions/:id/cancel` | deactivate and drop queued pushes |
| POST | `/api/subscriptions/tick` | run one refresh tick manually |
| GET | `/api/state` | tasks, cache, ledger, events, receiver summary |
| GET | `/api/stream` | SSE live updates |
| POST | `/receiver/:subId` | signed push target |
| GET | `/api/receiver` | receiver inbox and counters |

`x-viewer-token` (or `?token=`) identifies the caller; reads require it.

---

## 9. Tests

`npm test` runs 47 tests across four files:

- `test/coordinator.test.js` — key derivation (including per-source case
  handling), projection, single-flight, per-viewer aging, leave/cancel,
  generations, stale fallback, recovery bounds, receipts, ledger.
- `test/subscriptions.test.js` — fan-out, retry-to-ack, lost ack dedupe, retry
  cap, failure isolation, signature rejection, re-projection, backfill.
- `test/http.test.js` — the same guarantees over real HTTP: auth, coalescing,
  detach isolation, SSE, signed push to the real receiver, crash/recover.
- `test/network.test.js` — live Wikipedia, DuckDuckGo and page fetch. Skipped
  automatically when the network is unavailable; never failed for that reason.

Determinism comes from the local source's query flags and a fake clock in the
coordinator tests, so the suite needs no sleeps for correctness, only for
exercising real timing in a handful of HTTP tests.

---

## 10. Layout

```
src/
  keys.js          sharing unit, normalization, dedupe key
  permissions.js   field catalog, tiers, read-time projection
  store.js         SQLite schema (WAL), transactions
  coordinator.js   single-flight, tasks, generations, cancellation, recovery
  dispatcher.js    delivery retries, backoff, HMAC signing, refresher
  receiver.js      durable inbox, signature verification, idempotency
  server.js        HTTP API, SSE, static UI
  sources/         wikipedia, duckduckgo, page, local-library
public/            real workbench UI (no framework, no build)
test/              coordinator, subscriptions, http, network
```

The data directory holds `workbench.db` (tasks, documents, subscriptions) and
`receiver.db` (the sink's inbox). Both are SQLite in WAL mode and are ignored
by git.
