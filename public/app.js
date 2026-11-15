/* Query Dedupe Workbench — client.
   No framework: the state is small and the interesting behaviour lives on the
   server. Renders are full redraws of small regions, throttled to animation
   frames, so SSE bursts cannot cause layout thrash. */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const state = {
  token: null,
  viewer: null,
  meta: null,
  viewers: [],
  snapshot: null,
  lastRead: null,
  lastReadError: null,
  readInFlight: false,
  filter: '',
  statsMethodOpen: false,
  knownEventSeq: 0,
  pendingRender: false,
};

/* ------------------------------------------------------------------ */
/* API                                                                 */
/* ------------------------------------------------------------------ */

class ApiError extends Error {
  constructor(message, status, code) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

async function api(path, { method = 'GET', body, token = state.token, raw = false } = {}) {
  const headers = {};
  if (body !== undefined) headers['content-type'] = 'application/json';
  if (token) headers['x-viewer-token'] = token;
  let response;
  try {
    response = await fetch(path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (err) {
    throw new ApiError(`network error: ${err.message}`, 0, 'network');
  }
  if (raw) return response;
  let payload = null;
  try { payload = await response.json(); } catch { /* empty body */ }
  if (!response.ok) {
    throw new ApiError(payload?.error ?? `request failed (${response.status})`, response.status, payload?.code);
  }
  return payload;
}

/* ------------------------------------------------------------------ */
/* Persistence of the active identity                                  */
/* ------------------------------------------------------------------ */

const TOKEN_KEY = 'qdw.viewer.token';
const TOKENS_KEY = 'qdw.viewer.tokens';

function storedToken() {
  try { return localStorage.getItem(TOKEN_KEY); } catch { return null; }
}
function storeToken(token) {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch { /* storage may be blocked; the UI still works for this page */ }
}

/**
 * Token vault, keyed by viewer id. The server only ever reveals a capability
 * token once, at creation, so the browser keeps them to let you switch between
 * the viewers you made in this browser. This is a demo convenience, not a
 * security boundary: anyone with a token has that viewer's clearance.
 */
function readTokenVault() {
  try { return JSON.parse(localStorage.getItem(TOKENS_KEY) ?? '{}'); } catch { return {}; }
}
function rememberToken(viewerId, token) {
  try {
    const vault = readTokenVault();
    vault[viewerId] = token;
    localStorage.setItem(TOKENS_KEY, JSON.stringify(vault));
  } catch { /* ignore */ }
}
function tokenForViewer(viewerId) {
  return readTokenVault()[viewerId] ?? null;
}

/* ------------------------------------------------------------------ */
/* Formatting                                                          */
/* ------------------------------------------------------------------ */

function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function fmtMs(ms) {
  if (ms === null || ms === undefined) return '—';
  if (ms < 0) return 'future';
  if (ms < 1_000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1_000).toFixed(ms < 10_000 ? 1 : 0)}s`;
  if (ms < 3_600_000) return `${Math.round(ms / 60_000)}m`;
  if (ms < 86_400_000) return `${(ms / 3_600_000).toFixed(1)}h`;
  return `${(ms / 86_400_000).toFixed(1)}d`;
}

function fmtTime(epoch) {
  if (!epoch) return '—';
  const d = new Date(epoch);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function fmtClockShort(epoch) {
  if (!epoch) return '—';
  return new Date(epoch).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function plural(n, one, many = `${one}s`) {
  return `${n} ${n === 1 ? one : many}`;
}

function truncate(text, n) {
  const str = String(text ?? '');
  return str.length > n ? `${str.slice(0, n - 1)}…` : str;
}

function toast(message, kind = '') {
  const host = $('#toasts');
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = message;
  host.append(el);
  const remove = () => el.remove();
  setTimeout(() => {
    el.style.opacity = '0';
    el.style.transition = 'opacity 180ms';
    setTimeout(remove, 200);
  }, kind === 'error' ? 6_000 : 3_600);
}

/* ------------------------------------------------------------------ */
/* Rendering                                                           */
/* ------------------------------------------------------------------ */

function scheduleRender() {
  if (state.pendingRender) return;
  state.pendingRender = true;
  requestAnimationFrame(() => {
    state.pendingRender = false;
    render();
  });
}

function render() {
  renderIdentity();
  renderViewers();
  renderReadResult();
  renderTasks();
  renderDocuments();
  renderStats();
  renderInsights();
  renderSubscriptions();
  renderReceiver();
  renderEvents();
}

function renderIdentity() {
  const chip = $('#viewer-chip');
  const dot = $('#viewer-dot');
  const name = $('#viewer-name');
  const role = $('#viewer-role');
  if (state.viewer) {
    chip.classList.add('active');
    name.textContent = state.viewer.label;
    role.textContent = state.viewer.role;
    role.className = `role-pill ${state.viewer.role}`;
    chip.title = `Viewer ${state.viewer.id} · clearance ${state.viewer.clearance}`;
  } else {
    chip.classList.remove('active');
    name.textContent = 'No viewer';
    role.textContent = '—';
    role.className = 'role-pill';
  }
  $('#instance-label').textContent = state.snapshot?.instance ?? '';
}

function renderViewers() {
  const host = $('#viewers-list');
  if (state.viewers.length === 0) {
    host.innerHTML = '<p class="viewer-empty">No viewers yet. Add one to start reading.</p>';
  } else {
    host.innerHTML = state.viewers.map((viewer) => {
      const active = state.viewer?.id === viewer.id;
      return `
        <div class="viewer-row ${active ? 'active' : ''}" data-viewer="${esc(viewer.id)}">
          <span class="name">${esc(viewer.label)}</span>
          <span class="role-pill ${esc(viewer.role)}">${esc(viewer.role)}</span>
          <span class="clearance">${esc(viewer.clearance)}</span>
          ${active
            ? '<span class="use">active</span>'
            : `<button class="use" type="button" data-use-viewer="${esc(viewer.id)}">use</button>`}
        </div>`;
    }).join('');
  }

  const legend = [
    ['public', 'title, url, snippet, published'],
    ['analyst', '+ score, crawl metadata'],
    ['steward', '+ contact email, matched terms'],
  ];
  $('#clearance-legend').innerHTML = legend.map(
    ([role, fields]) => `<span class="legend-item"><b>${role}</b> ${esc(fields)}</span>`,
  ).join('');
}

function renderReadResult() {
  const badge = $('#result-badge');
  const note = $('#result-note');
  const host = $('#result');

  if (state.readInFlight) {
    badge.className = 'badge';
    badge.textContent = 'fetching';
  }

  if (state.lastReadError) {
    badge.className = 'badge error';
    badge.textContent = 'error';
    note.className = 'notice error';
    note.textContent = state.lastReadError;
    note.classList.remove('hidden');
    host.innerHTML = '<p class="empty">No result for the last attempt.</p>';
    return;
  }

  const read = state.lastRead;
  if (!read) {
    badge.className = 'badge';
    badge.textContent = 'idle';
    note.classList.add('hidden');
    host.innerHTML = '<p class="empty">Run a read to see what this viewer may observe.</p>';
    return;
  }

  const { admission, result, withheld_fields: withheld, query, viewer } = read;
  const served = read.admission.late_completion ? 'late' : admission.served;
  badge.className = `badge ${served === 'cache' ? 'cache' : served === 'source' ? 'source' : 'stale'}`;
  badge.textContent = served === 'cache' ? 'shared cache' : served === 'source' ? 'fresh fetch' : served === 'late' ? 'late completion' : 'stale';

  if (read.note) {
    note.className = `notice ${admission.stale ? 'stale' : 'good'}`;
    note.textContent = read.note;
    note.classList.remove('hidden');
  } else {
    note.classList.add('hidden');
  }

  const facts = [];
  if (served === 'cache') {
    facts.push(`<span class="fact good">avoided <b>1</b> upstream request</span>`);
  } else if (served === 'source') {
    facts.push(`<span class="fact ${admission.coalesced_with > 0 ? 'good' : ''}">joined <b>${admission.coalesced_with}</b> other reader${admission.coalesced_with === 1 ? '' : 's'}</span>`);
  }
  facts.push(`<span class="fact">generation <b>${admission.generation}</b></span>`);
  facts.push(`<span class="fact">age <b>${fmtMs(admission.age_ms)}</b> / max <b>${fmtMs(admission.max_age_ms)}</b></span>`);
  if (admission.stale_if_error_ms > 0) {
    facts.push(`<span class="fact">stale window <b>${fmtMs(admission.stale_if_error_ms)}</b></span>`);
  }
  if (admission.stale) facts.push('<span class="fact warn">older generation, flagged</span>');

  const rows = Object.entries(result ?? {}).map(([key, value]) => {
    const display = typeof value === 'object' && value !== null
      ? JSON.stringify(value, null, 0)
      : String(value);
    return `<div class="kv-row"><span class="kv-key">${esc(key)}</span><span class="kv-val">${esc(display)}</span></div>`;
  }).join('');

  const withheldChips = withheld.length
    ? withheld.map((entry) => `<span class="chip denied" title="${esc(entry.tier)} clearance required">${esc(entry.field)}</span>`).join('')
    : '<span class="muted" style="font-size:11px">none — this viewer sees every stored field</span>';

  host.innerHTML = `
    <div class="result-headline">
      <div>
        <div class="result-title">${esc(result?.title ?? 'Untitled result')}</div>
        ${result?.url ? `<div class="result-url">${esc(result.url)}</div>` : ''}
      </div>
      <span class="chip">${esc(viewer.role)}</span>
    </div>
    <p class="result-snippet">${esc(result?.snippet ?? '')}</p>
    <div class="result-facts">${facts.join('')}</div>
    <div class="result-split">
      <div>
        <div class="kv">
          <div class="kv-row"><span class="kv-key">query key</span><span class="kv-val" style="font-family:var(--mono);font-size:10.5px">${esc(query.key)}</span></div>
          <div class="kv-row"><span class="kv-key">share token</span><span class="kv-val" style="font-family:var(--mono);font-size:10.5px">${esc(admission.share_token)}</span></div>
          ${rows}
        </div>
      </div>
      <div class="withheld-box">
        <h3>Withheld for ${esc(viewer.role)} clearance</h3>
        <div class="withheld-list">${withheldChips}</div>
        <p class="hint" style="margin-top:7px">Same document, same share token. Fields are removed at read time, never shipped and hidden in the browser.</p>
      </div>
    </div>`;
}

function renderTasks() {
  const host = $('#tasks');
  const tasks = state.snapshot?.tasks ?? [];
  if (tasks.length === 0) {
    host.innerHTML = '<p class="empty">No source tasks yet.</p>';
    return;
  }
  host.innerHTML = tasks.map((task) => {
    const activeSubs = task.subscribers.filter((s) => !s.left_at);
    const leftSubs = task.subscribers.filter((s) => s.left_at);
    const subs = [...activeSubs, ...leftSubs].map((sub) => {
      const left = Boolean(sub.left_at);
      return `<span class="sub-chip ${left ? 'left' : ''}" title="${esc(sub.kind)}${left ? ` · left: ${esc(sub.left_reason)}` : ''}">
        ${esc(sub.label)} <span class="mini-role">${esc(sub.role)}</span>
      </span>`;
    }).join('');
    const live = task.state === 'queued' || task.state === 'running';
    const canCancelHere = state.viewer && live
      && (task.started_by === state.viewer.id || state.viewer.role !== 'public');
    const isMine = state.viewer && activeSubs.some((s) => s.viewer_id === state.viewer.id);
    return `
      <div class="task ${task.state}">
        <div class="task-top">
          <span class="state-pill ${task.state}">${esc(task.state)}</span>
          <span class="task-query" title="${esc(task.query)}">${esc(task.query)}</span>
          <span class="task-id">${esc(task.id.slice(-8))}</span>
        </div>
        <div class="task-meta">
          <span>gen ${task.generation}</span>
          <span>${esc(task.source_id)}</span>
          <span>${task.origin}</span>
          <span>waiting ${task.ref_count}</span>
          <span>attempt ${task.attempt}/${task.max_attempts}</span>
          ${task.document ? `<span>doc age ${fmtMs(task.document.age_ms)}</span>` : ''}
          ${task.keep_alive ? '<span>keep-alive</span>' : ''}
        </div>
        ${subs ? `<div class="task-subscribers">${subs}</div>` : ''}
        ${task.error ? `<div class="event-detail">${esc(task.error)}</div>` : ''}
        <div class="task-actions">
          ${isMine ? `<button class="btn btn-tiny" data-leave-task="${esc(task.id)}">detach me</button>` : ''}
          ${canCancelHere ? `<button class="btn btn-tiny btn-danger" data-cancel-task="${esc(task.id)}">cancel for everyone</button>` : ''}
        </div>
      </div>`;
  }).join('');
}

function renderDocuments() {
  const host = $('#documents');
  const docs = state.snapshot?.documents ?? [];
  if (docs.length === 0) {
    host.innerHTML = '<p class="empty">Nothing cached yet.</p>';
    return;
  }
  host.innerHTML = docs.slice(0, 12).map((doc) => `
    <div class="doc ${doc.usable ? '' : 'unusable'}">
      <div class="doc-top">
        <span class="gen-pill">g${doc.generation}</span>
        <span class="doc-query" title="${esc(doc.query)}">${esc(doc.query)}</span>
        <span class="chip">${esc(doc.source_id)}</span>
        ${doc.usable ? '' : '<span class="chip denied">late completion</span>'}
      </div>
      <div class="task-meta">
        <span>${fmtMs(doc.age_ms)} old</span>
        <span>${plural(doc.reads, 'read')}</span>
        <span>${esc(doc.task_state ?? '')}</span>
        ${doc.cancel_reason ? `<span>${esc(doc.cancel_reason)}</span>` : ''}
      </div>
      <div class="doc-share">${esc(doc.share_token)}</div>
      <div class="doc-actions">
        <button class="btn btn-tiny" data-read-gen="${esc(doc.query_key)}" data-gen="${doc.generation}">read this generation</button>
      </div>
    </div>`).join('');
}

function renderStats() {
  const stats = state.snapshot?.stats;
  const grid = $('#stat-grid');
  if (!stats) {
    grid.innerHTML = '<p class="empty">Waiting for state…</p>';
    return;
  }
  const cards = [
    { value: stats.upstream_requests_avoided, label: 'requests avoided', cls: 'accent', sub: 'cache hits + coalesced readers' },
    { value: stats.upstream_requests, label: 'upstream requests', cls: '', sub: 'one per source answer' },
    { value: stats.reads_from_cache, label: 'reads from cache', cls: '', sub: `hit rate ${(stats.hit_rate * 100).toFixed(0)}%` },
    { value: stats.reads_stale, label: 'stale reads', cls: stats.reads_stale > 0 ? 'amber' : '', sub: 'older generation, flagged' },
    { value: stats.late_completions, label: 'late completions', cls: stats.late_completions > 0 ? 'amber' : '', sub: 'finished after cancellation' },
    { value: stats.tasks_cancelled, label: 'cancelled tasks', cls: '', sub: `${stats.tasks_succeeded} succeeded · ${stats.tasks_failed} failed` },
    { value: stats.deliveries_acked, label: 'deliveries acked', cls: '', sub: `${stats.deliveries_pending} pending · ${stats.deliveries_dropped} dropped` },
    { value: stats.recoveries, label: 'recoveries', cls: '', sub: 'restart reconciliations' },
  ];
  grid.innerHTML = cards.map((card) => `
    <div class="stat ${card.cls}">
      <div class="value">${card.value}</div>
      <div class="label">${esc(card.label)}</div>
      <div class="sub">${esc(card.sub)}</div>
    </div>`).join('');
  $('#stats-method').textContent = state.statsMethodOpen
    ? Object.entries(stats.methods).map(([k, v]) => `${k}: ${v}`).join(' · ')
    : 'Avoided requests = fresh cache reads + readers coalesced onto an in-flight task.';
}

function renderInsights() {
  const host = $('#insights');
  const insights = state.snapshot?.insights ?? [];
  if (insights.length === 0) {
    host.innerHTML = '<p class="empty">No queries yet.</p>';
    return;
  }
  host.innerHTML = insights.slice(0, 8).map((insight) => {
    const gens = insight.generations.map((gen) => (
      `<span class="gen-dot ${gen.usable ? '' : 'bad'}" title="generation ${gen.generation}${gen.usable ? '' : ' (unusable late completion)'}">g${gen.generation}</span>`
    )).join('');
    return `
      <div class="insight">
        <div class="insight-head">
          <span class="insight-q" title="${esc(insight.query)}">${esc(truncate(insight.query, 42))}</span>
          <span class="chip">${esc(insight.locale)}</span>
        </div>
        <div class="insight-nums">
          <span><b>${insight.upstream_requests}</b> upstream</span>
          <span><b>${insight.upstream_requests_avoided}</b> avoided</span>
          <span><b>${insight.readers_coalesced}</b> coalesced</span>
          <span><b>${insight.distinct_viewers}</b> viewers</span>
          <span><b>${insight.cache_reads}</b> cache</span>
          <span><b>${insight.stale_reads}</b> stale</span>
          ${insight.late_completions ? `<span><b>${insight.late_completions}</b> late</span>` : ''}
        </div>
        <div class="gen-strip">${gens}</div>
      </div>`;
  }).join('');
}

function renderSubscriptions() {
  const host = $('#subscriptions');
  const subs = state.snapshot?.subscriptions ?? [];
  if (subs.length === 0) {
    host.innerHTML = '<p class="empty">No subscriptions yet.</p>';
    return;
  }
  host.innerHTML = subs.map((sub) => {
    const deliveries = sub.deliveries.slice(0, 4).map((delivery) => `
      <div class="delivery-row">
        <span class="delivery-state ${delivery.state}">${esc(delivery.state)}</span>
        <span>g${delivery.generation}</span>
        <span>${delivery.attempts} attempt${delivery.attempts === 1 ? '' : 's'}</span>
        ${delivery.stale ? '<span>stale</span>' : ''}
        ${delivery.last_error ? `<span title="${esc(delivery.last_error)}">⚠</span>` : ''}
      </div>`).join('');
    const mine = state.viewer && sub.viewer_id === state.viewer.id;
    return `
      <div class="sub ${sub.active ? '' : 'inactive'}">
        <div class="sub-top">
          <span class="role-pill ${esc(sub.role)}">${esc(sub.role)}</span>
          <span class="sub-q" title="${esc(sub.query)}">${esc(truncate(sub.query, 34))}</span>
          ${sub.active
            ? `<button class="btn btn-tiny" data-cancel-sub="${esc(sub.id)}" ${mine || state.viewer?.role !== 'public' ? '' : 'disabled'}>cancel</button>`
            : '<span class="badge">inactive</span>'}
        </div>
        <div class="sub-meta">
          <span>every ${fmtMs(sub.max_age_ms)}</span>
          <span>stale ${fmtMs(sub.stale_if_error_ms)}</span>
          <span>${esc(sub.viewer_label)}</span>
          <span>gen ${sub.last_delivered_gen ?? '—'}</span>
        </div>
        ${deliveries}
      </div>`;
  }).join('');
}

function renderReceiver() {
  const received = state.snapshot?.received;
  if (!received) {
    $('#receiver-stats').innerHTML = '';
    $('#receiver-inbox').innerHTML = '';
    return;
  }
  const stats = received.stats;
  $('#receiver-stats').innerHTML = `
    <span class="mini-stat">accepted <b>${stats.accepted}</b></span>
    <span class="mini-stat">duplicates absorbed <b>${stats.duplicates}</b></span>
    <span class="mini-stat">rejected <b>${stats.rejected}</b></span>
    <span class="mini-stat">faults <b>${stats.faults}</b></span>`;
  $('#receiver-inbox').innerHTML = received.inbox.length === 0
    ? '<p class="empty">Nothing received yet. Subscribe and let the refresher tick.</p>'
    : received.inbox.slice(0, 8).map((item) => {
      const fields = Object.keys(item.payload?.result ?? {});
      const withheld = item.payload?.withheld_fields ?? [];
      return `
        <div class="inbox-item ${item.delivery_count > 1 ? 'dup' : ''}">
          <div class="inbox-top">
            <span class="inbox-role ${esc(item.role)}">${esc(item.role)}</span>
            <span class="inbox-q" title="${esc(item.query)}">${esc(truncate(item.query, 26))}</span>
            <span class="chip">g${item.generation}</span>
            ${item.delivery_count > 1 ? `<span class="chip denied">×${item.delivery_count}</span>` : ''}
            ${item.stale ? '<span class="chip">stale</span>' : ''}
          </div>
          <div class="inbox-fields">
            ${fields.map((f) => `<span class="chip">${esc(f)}</span>`).join('')}
            ${withheld.map((w) => `<span class="chip denied" title="withheld">${esc(w.field)}</span>`).join('')}
          </div>
        </div>`;
    }).join('');
}

function renderEvents() {
  const host = $('#events');
  const events = [...(state.snapshot?.events ?? [])]
    .filter((event) => !state.filter || event.kind.startsWith(`${state.filter}.`) || event.kind.includes(state.filter))
    .slice(0, 70);
  if (events.length === 0) {
    host.innerHTML = '<li class="empty">No events.</li>';
    return;
  }
  host.innerHTML = events.map((event) => {
    const category = event.kind.split('.')[0];
    const detail = event.detail
      ? Object.entries(event.detail)
        .filter(([, value]) => value !== null && value !== undefined && value !== '')
        .slice(0, 5)
        .map(([key, value]) => `${key}=${typeof value === 'object' ? JSON.stringify(value) : value}`)
        .join(' · ')
      : '';
    return `
      <li class="event">
        <span class="event-time">${fmtTime(event.at)}</span>
        <span class="event-body">
          <span class="event-kind ${category}">${esc(event.kind)}</span>
          <span class="event-detail">${esc(truncate(detail, 150))}</span>
          ${event.taskId ? `<span class="event-task">${esc(event.taskId.slice(-10))}</span>` : ''}
        </span>
      </li>`;
  }).join('');
}

/* ------------------------------------------------------------------ */
/* Actions                                                             */
/* ------------------------------------------------------------------ */

function applyViewer(viewer, token) {
  state.viewer = viewer ?? null;
  if (token !== undefined) state.token = token;
  storeToken(state.token);
  scheduleRender();
}

async function loadMeta() {
  state.meta = await api('/api/meta', { token: null });
  const options = state.meta.sources.map(
    (source) => `<option value="${esc(source.id)}">${esc(source.label)}${source.kind === 'local' ? ' · local' : ' · live web'}</option>`,
  ).join('');
  $('#read-source').innerHTML = options;
  $('#sub-source').innerHTML = options;
  $('#read-source').value = 'wikipedia';
  $('#sub-source').value = 'local-library';
  updateSourceHint();
}

function updateSourceHint() {
  const id = $('#read-source').value;
  const source = state.meta?.sources.find((entry) => entry.id === id);
  const hint = $('#source-hint');
  if (!source) { hint.textContent = ''; return; }
  const bits = [source.description];
  if (id === 'local-library') bits.push('Try “slow 3000”, “late 3000”, or “fail”.');
  if (id === 'page') bits.push('Use a full URL or “example.com”.');
  hint.textContent = bits.join(' ');
}

async function loadViewers() {
  const { viewers } = await api('/api/viewers', { token: null });
  state.viewers = viewers;
  scheduleRender();
}

async function loadState() {
  const snapshot = await api('/api/state');
  state.snapshot = snapshot;
  scheduleRender();
}

async function createViewer(event) {
  event.preventDefault();
  const label = $('#viewer-label').value.trim();
  const role = $('#viewer-role').value;
  if (!label) return;
  try {
    const created = await api('/api/viewers', { method: 'POST', body: { label, role }, token: null });
    $('#viewer-label').value = '';
    rememberToken(created.viewer.id, created.token);
    applyViewer(created.viewer, created.token);
    await loadViewers();
    toast(`Viewer “${created.viewer.label}” added (${created.viewer.role}).`, 'good');
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function useViewer(id) {
  const viewer = state.viewers.find((entry) => entry.id === id);
  if (!viewer) return;
  const token = tokenForViewer(id);
  if (!token) {
    toast('No token stored in this browser for that viewer. Tokens are only issued at creation.', 'error');
    return;
  }
  try {
    const session = await api('/api/viewers/login', { method: 'POST', body: { token }, token: null });
    applyViewer(session.viewer, token);
    await loadState();
    toast(`Now acting as ${session.viewer.label} (${session.viewer.role}).`, 'good');
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function performRead(event) {
  event.preventDefault();
  if (!state.viewer) { toast('Add or select a viewer first.', 'error'); return; }
  if (state.readInFlight) return;
  const payload = {
    source_id: $('#read-source').value,
    query: $('#read-query').value,
    max_age_ms: Number($('#read-max-age').value) * 1_000,
    stale_if_error_ms: Number($('#read-stale').value) * 1_000,
    locale: $('#read-locale').value,
    client_token: `ui:${state.viewer.id}`,
  };
  const parallel = $('#read-parallel').checked;
  state.readInFlight = true;
  state.lastReadError = null;
  $('#read-button').disabled = true;
  scheduleRender();
  const started = performance.now();
  try {
    if (parallel) {
      const requests = [state.viewer.id, `${state.viewer.id}:b`, `${state.viewer.id}:c`].map(
        (token) => api('/api/read', { method: 'POST', body: { ...payload, client_token: token } }),
      );
      const results = await Promise.all(requests);
      state.lastRead = results[0];
      const elapsed = Math.round(performance.now() - started);
      const coalesced = results[0].admission.coalesced_with;
      toast(`3 reads finished in ${elapsed}ms · ${coalesced} coalesced onto one fetch.`, 'good');
    } else {
      state.lastRead = await api('/api/read', { method: 'POST', body: payload });
    }
    await loadState();
  } catch (err) {
    state.lastReadError = err.message;
    toast(`Read failed: ${err.message}`, 'error');
  } finally {
    state.readInFlight = false;
    $('#read-button').disabled = false;
    scheduleRender();
  }
}

async function rebuildQuery() {
  if (!state.viewer) { toast('Add a viewer first.', 'error'); return; }
  try {
    const result = await api('/api/rebuild', {
      method: 'POST',
      body: {
        source_id: $('#read-source').value,
        query: $('#read-query').value,
        locale: $('#read-locale').value,
      },
    });
    toast(result.started_new
      ? `Started generation ${result.generation}.`
      : `Joined the in-flight generation ${result.generation}; no second fetch.`, 'good');
    await loadState();
  } catch (err) {
    toast(`Rebuild failed: ${err.message}`, 'error');
  }
}

async function readGeneration(queryKey, generation) {
  if (!state.viewer) { toast('Add a viewer first.', 'error'); return; }
  try {
    state.lastRead = await api('/api/read-generation', {
      method: 'POST',
      body: { query_key: queryKey, generation: Number(generation) },
    });
    state.lastReadError = null;
    toast(`Read generation ${generation} explicitly.`, 'good');
    scheduleRender();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function leaveTask(taskId) {
  try {
    const result = await api(`/api/tasks/${encodeURIComponent(taskId)}/leave`, { method: 'POST', body: { reason: 'viewer detached from UI' } });
    toast(result.aborted
      ? 'You were the last reader; the task was cancelled.'
      : `Detached. ${result.stillWaiting} other reader${result.stillWaiting === 1 ? '' : 's'} still waiting.`,
    result.aborted ? '' : 'good');
    await loadState();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function cancelTask(taskId) {
  try {
    const result = await api(`/api/tasks/${encodeURIComponent(taskId)}/cancel`, { method: 'POST', body: { reason: 'cancelled from workbench' } });
    toast(result.changed ? 'Task cancelled for everyone attached to it.' : 'Task had already finished.', result.changed ? '' : 'error');
    await loadState();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function createSubscription(event) {
  event.preventDefault();
  if (!state.viewer) { toast('Add a viewer first.', 'error'); return; }
  const payload = {
    source_id: $('#sub-source').value,
    query: $('#sub-query').value,
    max_age_ms: Number($('#sub-max-age').value) * 1_000,
    stale_if_error_ms: Number($('#sub-stale').value) * 1_000,
  };
  try {
    const created = await api('/api/subscriptions', { method: 'POST', body: payload });
    toast(`Subscribed as ${created.subscription.role}. Callback is the receiver.`, 'good');
    await loadState();
  } catch (err) {
    toast(err.message, 'error');
  }
}

async function cancelSubscription(id) {
  try {
    await api(`/api/subscriptions/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
    toast('Subscription cancelled. Pending deliveries dropped.');
    await loadState();
  } catch (err) {
    toast(err.message, 'error');
  }
}

/* ------------------------------------------------------------------ */
/* Failure drills                                                      */
/* ------------------------------------------------------------------ */

const drills = {
  async crash() {
    const result = await api('/api/demo/crash', { method: 'POST' });
    return [
      `Simulated process loss: ${result.detached.length} in-flight task(s) detached.`,
      'Tasks remain “running” in the database with no worker — the state a kill -9 leaves.',
      'Press Recover to reconcile. Recovery adopts the original task and never creates a duplicate.',
    ].join('\n');
  },
  async recover() {
    const report = await api('/api/demo/recover', { method: 'POST', body: { max_adoptions: 25 } });
    return [
      `Recovery pass at ${new Date(report.at).toLocaleTimeString()}`,
      `adopted in place: ${report.adopted.length}`,
      `finished from a stored receipt (no refetch): ${report.receipts_finished.length}`,
      `failed past deadline: ${report.failed.length}`,
      `skipped: ${report.skipped.length} · cap ${report.cap}`,
      `new tasks created: ${report.new_tasks_created}`,
      `pending deliveries made due: ${report.retries_made_due}`,
    ].join('\n');
  },
  async lose() {
    const result = await api('/api/demo/lose-notification', { method: 'POST' });
    return [
      'Armed: the next source answer will be stored as an unconsumed receipt.',
      'The task will be left running, as if the success notification was lost.',
      'Run a query, then press Recover. Recovery finishes the original task from the receipt — the upstream request is not repeated.',
    ].join('\n');
  },
  async expire() {
    const result = await api('/api/demo/expire', { method: 'POST', body: { age_ms: 3_600_000 } });
    return `${result.expired} cached document(s) aged by 1h.\nRead again to trigger an expiry rebuild; set a stale window to read the old generation if the rebuild fails.`;
  },
  async fault500() {
    await api('/api/receiver/faults', { method: 'POST', body: { failNext: 2 } });
    return 'The receiver will answer 500 for its next 2 deliveries. The dispatcher retries with backoff; deliveries stay pending until acked.';
  },
  async faultAck() {
    await api('/api/receiver/faults', { method: 'POST', body: { loseNextAck: true } });
    return 'The receiver will store the next notification but report failure, losing the ack.\nThe dispatcher retries with the same dedupe key; the duplicate is absorbed and counted.';
  },
};

async function runDrill(name) {
  const output = $('#drill-output');
  const button = $(`[data-drill="${name}"]`);
  button.disabled = true;
  output.textContent = 'running…';
  try {
    const lines = await drills[name]();
    output.textContent = lines;
    if (name === 'crash' || name === 'recover' || name === 'expire') await loadState();
  } catch (err) {
    output.textContent = `failed: ${err.message}`;
    toast(err.message, 'error');
  } finally {
    button.disabled = false;
  }
}

/* ------------------------------------------------------------------ */
/* Live updates                                                        */
/* ------------------------------------------------------------------ */

function connectStream() {
  const dot = $('#live-dot');
  const source = new EventSource('/api/stream');
  source.addEventListener('ready', () => dot.classList.add('on'));
  source.addEventListener('state', (event) => {
    try {
      const payload = JSON.parse(event.data);
      state.snapshot = {
        ...(state.snapshot ?? {}),
        ...payload,
        received: payload.received ?? state.snapshot?.received,
      };
      scheduleRender();
    } catch { /* ignore malformed frames */ }
  });
  source.addEventListener('error', () => dot.classList.remove('on'));
  source.addEventListener('open', () => dot.classList.add('on'));
}

/* ------------------------------------------------------------------ */
/* Wiring                                                              */
/* ------------------------------------------------------------------ */

function wire() {
  $('#viewer-form').addEventListener('submit', createViewer);
  $('#read-form').addEventListener('submit', performRead);
  $('#read-source').addEventListener('change', updateSourceHint);
  $('#sub-form').addEventListener('submit', createSubscription);
  $('#stats-info').addEventListener('click', () => {
    state.statsMethodOpen = !state.statsMethodOpen;
    renderStats();
  });
  $('#receiver-clear').addEventListener('click', async () => {
    try {
      await api('/api/receiver/clear', { method: 'POST' });
      await loadState();
    } catch (err) { toast(err.message, 'error'); }
  });
  $('#event-filter').addEventListener('change', (event) => {
    state.filter = event.target.value;
    renderEvents();
  });
  $$('[data-drill]').forEach((button) => {
    button.addEventListener('click', () => runDrill(button.dataset.drill));
  });

  document.addEventListener('click', (event) => {
    const target = event.target.closest('[data-leave-task],[data-cancel-task],[data-cancel-sub],[data-read-gen],[data-use-viewer]');
    if (!target) return;
    if (target.dataset.leaveTask) leaveTask(target.dataset.leaveTask);
    else if (target.dataset.cancelTask) cancelTask(target.dataset.cancelTask);
    else if (target.dataset.cancelSub) cancelSubscription(target.dataset.cancelSub);
    else if (target.dataset.readGen) readGeneration(target.dataset.readGen, target.dataset.gen);
    else if (target.dataset.useViewer) useViewer(target.dataset.useViewer);
  });

  // Keep a keyboard route to rebuilding the active query.
  document.addEventListener('keydown', (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      rebuildQuery();
    }
  });
}

async function boot() {
  wire();
  try {
    await loadMeta();
    await loadViewers();
  } catch (err) {
    toast(`Could not load workbench: ${err.message}`, 'error');
    return;
  }

  const token = storedToken();
  if (token) {
    try {
      const { viewer } = await api('/api/viewers/login', { method: 'POST', body: { token }, token: null });
      rememberToken(viewer.id, token);
      applyViewer(viewer, token);
    } catch {
      storeToken(null);
      state.token = null;
    }
  }

  try {
    await loadState();
  } catch (err) {
    toast(`Could not load state: ${err.message}`, 'error');
  }
  connectStream();
  setInterval(() => {
    // Slow reconciliation in case an SSE frame was missed while the tab slept.
    if (document.visibilityState === 'visible') loadState().catch(() => {});
  }, 15_000);
}

boot();
