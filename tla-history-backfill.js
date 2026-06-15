// =============================================================================
// tla-history-backfill.js  —  vote + lock EVENT history (behavioral, ungameable)
// =============================================================================
//
// The TLA analogue of the proven NFT pending-claims / provenance backfill:
// reconstruct *behavioral* history from the permanent transaction log, because
// public Terra LCDs prune STATE (~100 blocks) but keep the TX log forever.
//
//   ✅ backfillable (these are transactions):  votes, lock create/extend/relock/
//      merge/withdraw — i.e. WHAT members DID and WHEN.
//   ❌ NOT backfillable (derived state, pruned):  position USD over time, past
//      APR, past pending rewards. Those stay forward-only (daily adao-positions).
//
// Feeds Vote Intelligence (vote-change frequency, voting on inactive LPs) and
// member tenure/behavior — the data nobody else has.
//
// MODEL (mirrors nft-inventory pending-claims):
//   • First run with no committed state  → SEED: replay the full tx history to
//     the LCD's reachable horizon, publish ONCE.
//   • Every run after  → FORWARD: scan only blocks above lastScannedHeight,
//     append, never-shrink guard.
//
// Reliability checklist applied (cron-scripts/README → F1–F8):
//   F1 pagination: publicnode IGNORES offset → page= + ORDER_BY_DESC, MAX_PAGES.
//   F2 null≠[]:    a failed page returns null (retry/abort), NOT "no data".
//   F3 no-shrink:  history is append-only; fewer events than committed ⇒ abort.
//   F7 heartbeat:  status flips to partial/error on any incomplete scan.
//   F8 horizon:    record the earliest reachable height honestly ("history from
//                  height H"), never a false "from genesis".
//
// Outputs (to GITHUB_REPO, default tla-history-data_2026, 2026/ year-folder):
//   2026/data/vote-events.json   append-only vote log + scan state
//   2026/data/lock-events.json   append-only lock log + scan state
//   2026/data/rollups.json       per-wallet derived: vote churn, tenure, timelines
//   2026/heartbeat.json          freshness + discovered_actions + horizon
//   2026/daily/YYYY-MM-DD.json   per-day delta summary (append-only archive)
//
// RUN MODE:  RUN_MODE=sample (default) scans a few recent txs per contract and
// prints the distinct execute-msg action keys + one sample each — WRITES NOTHING.
// Use this once to confirm the action-key map below. RUN_MODE=full does the real
// seed/forward and writes. (Mirrors the nft backfills' sample/full convention.)
//
// Env:  GITHUB_TOKEN, GITHUB_REPO (default defipatriot/tla-history-data_2026),
//       GITHUB_BRANCH (default main), RUN_MODE (sample|full),
//       LCD_PRIMARY / LCD_FALLBACK (defaults below are correct),
//       SEED_MAX_PAGES (default 400 per contract).
// =============================================================================

'use strict';

const https = require('https');

let ErrorLog;
try { ({ ErrorLog } = require('../lib/error-reporter.js')); }
catch { // standalone fallback so this file runs even if lib path differs
    ErrorLog = class { constructor(){ this._e = []; } add(step,e){ this._e.push({ step, message: String(e && e.message || e) }); } list(){ return this._e; } count(){ return this._e.length; } };
}

// ----------------------------------------------------------------------------- constants
// Env names match the proven nft backfills: LCD_PRIMARY / LCD_FALLBACK,
// RUN_MODE = sample (dry-run, writes nothing) | full (seed/forward + write).
const TERRA_LCD_PRIMARY  = process.env.LCD_PRIMARY  || process.env.TERRA_LCD_PRIMARY  || 'https://terra-lcd.publicnode.com';
const TERRA_LCD_FALLBACK = process.env.LCD_FALLBACK || process.env.TERRA_LCD_FALLBACK || 'https://terra-rest.publicnode.com';

const TLA_GAUGE_CONTROLLER = 'terra1hfksrhchkmsj4qdq33wkksrslnfles6y2l77fmmzeep0xmq24l2smsd3lj';
const TLA_VOTING_ESCROW    = 'terra1uqhj8agyeaz8fu6mdggfuwr3lp32jlrx5hqag4jxexde92rzkamq3l62zg';

const GITHUB_TOKEN  = process.env.GITHUB_TOKEN;
const GITHUB_REPO   = process.env.GITHUB_REPO   || 'defipatriot/tla-history-data_2026';
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';
const YEAR_DIR      = '2026';

const EPOCH_DATES_URL = 'https://raw.githubusercontent.com/defipatriot/website-adao-core/main/epoch_1-300_date.json';

const RUN_MODE      = (process.env.RUN_MODE || 'sample').toLowerCase(); // 'sample' = dry-run/probe (no write) | 'full' = seed/forward + write
const PROBE_ONLY    = RUN_MODE === 'sample' || process.env.PROBE_ONLY === '1' || process.env.PROBE_ONLY === 'true';
const SEED_MAX_PAGES = Number(process.env.SEED_MAX_PAGES || 400); // hard page ceiling per contract during seed
const PAGE_LIMIT    = 100;
const HTTP_TIMEOUT_MS = 25000;
const RETRIES       = 4;
const WATCHDOG_MS   = 9 * 60 * 1000; // 9-min hard ceiling (Render-cost guard)
const SCHEMA_VERSION = 1;
const FORWARD_CADENCE_HOURS = 6;     // forward maintenance cadence (tune in Render)

// Known execute-msg keys → normalized event type. Anything NOT here is still
// captured losslessly (raw_msg kept) and counted in discovered_actions, so an
// unexpected key surfaces in the heartbeat instead of being silently dropped.
// (Eris ve3 naming; confirm via PROBE_ONLY before the first seed.)
const VOTE_ACTION_KEYS = {
    vote: 'vote', place_vote: 'vote', place_votes: 'vote', update_vote: 'vote', update_votes: 'vote',
};
const LOCK_ACTION_KEYS = {
    create_lock: 'lock_create', deposit_for: 'lock_deposit_for',
    extend_lock_amount: 'lock_extend_amount', extend_lock_time: 'lock_extend_time',
    relock: 'relock', merge_lock: 'merge', merge: 'merge',
    withdraw: 'withdraw', unlock: 'withdraw',
};
// CW20 send-hook inner keys (when a lock is funded by an LST cw20 `send`):
const LOCK_HOOK_KEYS = { create_lock: 'lock_create', deposit_for: 'lock_deposit_for', extend_lock_amount: 'lock_extend_amount' };

// ----------------------------------------------------------------------------- http
async function fetchJson(url, label = url, timeoutMs = HTTP_TIMEOUT_MS) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(url, { signal: controller.signal, headers: { 'Accept': 'application/json', 'User-Agent': 'aDAO-tla-history-backfill/1.0' } });
        if (!res.ok) { const body = await res.text().catch(() => ''); throw new Error(`HTTP ${res.status} ${body.slice(0, 120)}`); }
        return await res.json();
    } catch (e) {
        if (e.name === 'AbortError') throw new Error(`Timeout (${label})`);
        throw e;
    } finally { clearTimeout(timeout); }
}
async function fetchJsonWithRetry(url, label, maxTries = RETRIES) {
    let lastErr;
    for (let attempt = 1; attempt <= maxTries; attempt++) {
        try { return await fetchJson(url, label); }
        catch (e) { lastErr = e; if (attempt < maxTries) await new Promise(r => setTimeout(r, Math.pow(3, attempt - 1) * 500)); }
    }
    throw lastErr;
}
async function tryFetchJson(url, label) { try { return await fetchJson(url, label); } catch (e) { console.warn(`  ⚠ ${label} fetch failed (non-fatal): ${e.message}`); return null; } }

// ----------------------------------------------------------------------------- tx_search
// One page of txs for a contract, NEWEST-FIRST. Returns:
//   { txs: [...] }        success (possibly empty array = genuine end)
//   null                  query FAILED (both LCDs) — caller must NOT treat as end (F2)
async function txSearchPage(contract, page) {
    const q = `wasm._contract_address='${contract}'`;
    const path = `/cosmos/tx/v1beta1/txs?query=${encodeURIComponent(q)}&order_by=ORDER_BY_DESC&page=${page}&limit=${PAGE_LIMIT}`;
    for (const base of [TERRA_LCD_PRIMARY, TERRA_LCD_FALLBACK]) {
        try {
            const res = await fetchJsonWithRetry(base + path, `tx_search ${contract.slice(0,12)} p${page}`);
            // success shape — distinguish "no data" ([]) from a malformed/failed body (null)
            const txs = Array.isArray(res?.tx_responses) ? res.tx_responses : null;
            if (txs === null) continue; // try fallback LCD before declaring failure
            return { txs, total: res.total != null ? Number(res.total) : null };
        } catch (e) { /* try fallback */ }
    }
    return null; // F2: both LCDs failed → null, NOT []
}

// Scan a contract NEWEST-FIRST, stopping when we drop to/below sinceHeight.
// Returns { txs, complete, horizonHeight }. complete=false means a page failed
// or we hit the page ceiling (=> partial; do NOT advance scan height past a gap).
async function scanContract(contract, sinceHeight, deadline) {
    const collected = [];
    let complete = true, horizonHeight = null, total = null;
    const maxPages = sinceHeight > 0 ? 60 : SEED_MAX_PAGES; // forward runs need few pages; seed needs many
    for (let page = 1; page <= maxPages; page++) {
        if (Date.now() > deadline) { complete = false; console.warn(`  ⚠ ${contract.slice(0,12)}: watchdog hit on page ${page}`); break; }
        const res = await txSearchPage(contract, page);
        if (res === null) { complete = false; console.warn(`  ⚠ ${contract.slice(0,12)}: page ${page} FAILED (both LCDs) — partial scan`); break; }
        if (total == null) total = res.total;
        const txs = res.txs;
        if (txs.length === 0) break;                 // genuine end of history
        for (const tr of txs) {
            const h = Number(tr.height);
            if (horizonHeight == null || h < horizonHeight) horizonHeight = h;
            if (h > sinceHeight) collected.push(tr);
        }
        const pageMin = Math.min(...txs.map(tr => Number(tr.height)));
        if (pageMin <= sinceHeight) break;           // newest-first: covered the new region
        if (txs.length < PAGE_LIMIT) break;          // last page
        if (page === maxPages) { complete = false; console.warn(`  ⚠ ${contract.slice(0,12)}: hit page ceiling ${maxPages} — horizon = height ${horizonHeight}`); }
    }
    return { txs: collected, complete, horizonHeight, total };
}

// ----------------------------------------------------------------------------- msg decoding
// The LCD returns tx.body.messages[].msg ALREADY DECODED as JSON (proven in
// nft-inventory parseUnstakeTxs). For cw20 send-hooks the inner msg is base64.
function decodeMaybeB64(v) {
    if (v == null) return null;
    if (typeof v === 'object') return v;
    if (typeof v === 'string') { try { return JSON.parse(Buffer.from(v, 'base64').toString('utf8')); } catch { return null; } }
    return null;
}
function wasmActions(tr) {
    // distinct wasm `action` attributes emitted in this tx (event-level fallback classifier)
    const acts = new Set();
    for (const ev of tr?.events || []) {
        if (ev.type !== 'wasm') continue;
        for (const kv of ev.attributes || []) if (kv.key === 'action' && kv.value) acts.add(kv.value);
    }
    return acts;
}

// Pull a normalized votes array [[assetId, bps], ...] from a vote msg, tolerant
// of arg-shape variants. assetId is canonical "cw20:terra1.." / "native:uluna".
function normalizeAssetId(asset) {
    if (asset == null) return null;
    if (typeof asset === 'string') return asset;
    if (asset.cw20) return `cw20:${asset.cw20}`;
    if (asset.native) return `native:${asset.native}`;
    if (asset.token?.contract_addr) return `cw20:${asset.token.contract_addr}`;
    if (asset.native_token?.denom) return `native:${asset.native_token.denom}`;
    return JSON.stringify(asset);
}
function extractVotes(voteArgs) {
    const arr = voteArgs?.votes || voteArgs?.weights || voteArgs?.allocations || voteArgs?.gauge_votes;
    if (!Array.isArray(arr)) return null;
    const out = [];
    for (const v of arr) {
        if (Array.isArray(v) && v.length >= 2) out.push([normalizeAssetId(v[0]), Number(v[1])]);
        else if (v && typeof v === 'object') {
            const asset = v.asset ?? v.pool ?? v.gauge ?? v.id;
            const bps = v.bps ?? v.weight ?? v.amount ?? v.power;
            if (asset != null && bps != null) out.push([normalizeAssetId(asset), Number(bps)]);
        }
    }
    return out.length ? out : null;
}

// ----------------------------------------------------------------------------- classify
function classifyVoteTxs(txResponses, discovered) {
    const events = [];
    for (const tr of txResponses) {
        const meta = { height: Number(tr.height), timestamp: tr.timestamp, tx_hash: tr.txhash };
        for (const m of tr?.tx?.body?.messages || []) {
            const msg = m?.msg; if (!msg || typeof msg !== 'object') continue;
            const key = Object.keys(msg)[0]; if (!key) continue;
            discovered[`gauge:${key}`] = (discovered[`gauge:${key}`] || 0) + 1;
            const type = VOTE_ACTION_KEYS[key];
            if (type !== 'vote') continue; // only vote-class msgs become vote events; rest just counted
            events.push({ type: 'vote', wallet: m.sender, ...meta, votes: extractVotes(msg[key]), raw_msg: extractVotes(msg[key]) ? undefined : msg[key] });
        }
    }
    return events;
}
function classifyLockTxs(txResponses, discovered) {
    const events = [];
    for (const tr of txResponses) {
        const meta = { height: Number(tr.height), timestamp: tr.timestamp, tx_hash: tr.txhash };
        const acts = wasmActions(tr);
        let matchedThisTx = false;
        for (const m of tr?.tx?.body?.messages || []) {
            const msg = m?.msg; if (!msg || typeof msg !== 'object') continue;
            const key = Object.keys(msg)[0]; if (!key) continue;

            // CW20 send-hook (lock funded by an LST cw20 `send`): inner msg is base64.
            if (key === 'send' && msg.send?.contract === TLA_VOTING_ESCROW) {
                const inner = decodeMaybeB64(msg.send.msg);
                const innerKey = inner ? Object.keys(inner)[0] : null;
                discovered[`escrow_hook:${innerKey || 'undecodable'}`] = (discovered[`escrow_hook:${innerKey || 'undecodable'}`] || 0) + 1;
                const type = innerKey && LOCK_HOOK_KEYS[innerKey];
                if (type) { events.push({ type, wallet: m.sender, ...meta, asset: inner[innerKey]?.asset ? normalizeAssetId(inner[innerKey].asset) : null, amount: msg.send.amount != null ? Number(msg.send.amount) : null, args: inner[innerKey] }); matchedThisTx = true; }
                continue;
            }
            discovered[`escrow:${key}`] = (discovered[`escrow:${key}`] || 0) + 1;
            const type = LOCK_ACTION_KEYS[key];
            if (!type) continue;
            const a = msg[key] || {};
            events.push({ type, wallet: m.sender, ...meta,
                token_id: a.token_id != null ? String(a.token_id) : (a.lock_id != null ? String(a.lock_id) : null),
                asset: a.asset ? normalizeAssetId(a.asset) : null,
                amount: a.amount != null ? Number(a.amount) : null,
                end_period: a.time != null ? a.time : (a.unlock_period ?? a.period ?? null),
                args: a });
            matchedThisTx = true;
        }
        // Event-level fallback: escrow touched but no top-level/hook msg matched
        // (e.g. routed through a frontend multicall). Record a typed-but-thin event
        // so it isn't lost; args unknown → flagged for later enrichment.
        if (!matchedThisTx) {
            for (const act of acts) {
                if (/lock|deposit|withdraw|relock|merge|extend/i.test(act)) {
                    discovered[`escrow_event:${act}`] = (discovered[`escrow_event:${act}`] || 0) + 1;
                    events.push({ type: `event:${act}`, wallet: tr?.tx?.body?.messages?.[0]?.sender || null, ...meta, via: 'wasm_event', args_unknown: true });
                }
            }
        }
    }
    return events;
}

// ----------------------------------------------------------------------------- merge / dedup / guards
function mergeEvents(prior, fresh) {
    const byHash = new Map();
    for (const e of prior) byHash.set(`${e.tx_hash}|${e.type}|${e.wallet}`, e);
    let added = 0;
    for (const e of fresh) { const k = `${e.tx_hash}|${e.type}|${e.wallet}`; if (!byHash.has(k)) { byHash.set(k, e); added++; } }
    const merged = [...byHash.values()].sort((a, b) => (a.height - b.height) || String(a.tx_hash).localeCompare(String(b.tx_hash)));
    return { merged, added };
}

// ----------------------------------------------------------------------------- epoch mapping
function makeEpochResolver(epochDates) {
    if (!Array.isArray(epochDates) || !epochDates.length) return () => null;
    const rows = epochDates.map(r => ({ epoch: r.epoch, start: Date.parse(r.start_time), end: Date.parse(r.end_time) })).filter(r => Number.isFinite(r.start));
    return (iso) => { const t = Date.parse(iso); if (!Number.isFinite(t)) return null; for (const r of rows) if (t >= r.start && t < r.end) return r.epoch; const last = rows[rows.length - 1]; return (t >= last.end) ? last.epoch + Math.floor((t - last.end) / (7 * 864e5)) + 1 : null; };
}

// ----------------------------------------------------------------------------- rollups (consumer-facing derived)
function buildRollups(voteEvents, lockEvents, epochOf) {
    const wallets = {};
    const w = (addr) => (wallets[addr] ||= { wallet: addr, vote_count: 0, first_vote_epoch: null, last_vote_epoch: null, pools_voted: {}, vote_changes: 0, locks: [], first_lock_ts: null });
    let prevByWallet = {};
    for (const e of voteEvents) {
        if (!e.wallet) continue;
        const r = w(e.wallet); r.vote_count++;
        const ep = epochOf(e.timestamp);
        if (ep != null) { if (r.first_vote_epoch == null || ep < r.first_vote_epoch) r.first_vote_epoch = ep; if (r.last_vote_epoch == null || ep > r.last_vote_epoch) r.last_vote_epoch = ep; }
        const sig = JSON.stringify((e.votes || []).slice().sort());
        if (prevByWallet[e.wallet] != null && prevByWallet[e.wallet] !== sig) r.vote_changes++;
        prevByWallet[e.wallet] = sig;
        for (const [asset] of (e.votes || [])) if (asset) r.pools_voted[asset] = (r.pools_voted[asset] || 0) + 1;
    }
    for (const e of lockEvents) {
        if (!e.wallet) continue;
        const r = w(e.wallet);
        r.locks.push({ type: e.type, token_id: e.token_id ?? null, asset: e.asset ?? null, amount: e.amount ?? null, timestamp: e.timestamp, epoch: epochOf(e.timestamp), tx_hash: e.tx_hash });
        if (r.first_lock_ts == null || Date.parse(e.timestamp) < Date.parse(r.first_lock_ts)) r.first_lock_ts = e.timestamp;
    }
    // churn rate = changes / (votes-1), tidy pools list
    for (const r of Object.values(wallets)) {
        r.vote_churn_rate = r.vote_count > 1 ? +(r.vote_changes / (r.vote_count - 1)).toFixed(4) : 0;
        r.pools_voted = Object.entries(r.pools_voted).sort((a, b) => b[1] - a[1]).map(([asset, n]) => ({ asset, times: n }));
        r.locks.sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
    }
    return { schemaVersion: SCHEMA_VERSION, builtAt: new Date().toISOString(), wallet_count: Object.keys(wallets).length, wallets: Object.values(wallets).sort((a, b) => b.vote_count - a.vote_count) };
}

// ----------------------------------------------------------------------------- github
function githubApiRequest(method, apiPath, body) {
    return new Promise((resolve, reject) => {
        const opts = { hostname: 'api.github.com', path: apiPath, method, headers: { 'User-Agent': 'aDAO-tla-history-backfill', 'Authorization': `Bearer ${GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json' } };
        if (body) opts.headers['Content-Type'] = 'application/json';
        const req = https.request(opts, res => { let data = ''; res.on('data', c => data += c); res.on('end', () => { if (res.statusCode >= 200 && res.statusCode < 300) { try { resolve(JSON.parse(data)); } catch { resolve(data); } } else reject(new Error(`GitHub ${method} ${apiPath}: ${res.statusCode} ${data.slice(0, 200)}`)); }); });
        req.on('error', reject); if (body) req.write(JSON.stringify(body)); req.end();
    });
}
async function publishFile(filePath, contentObj, message) {
    const content = typeof contentObj === 'string' ? contentObj : JSON.stringify(contentObj, null, 2);
    const apiPath = `/repos/${GITHUB_REPO}/contents/${filePath}`;
    let sha = null;
    try { sha = (await githubApiRequest('GET', apiPath + `?ref=${GITHUB_BRANCH}`)).sha; } catch { /* new file */ }
    const body = { message, content: Buffer.from(content).toString('base64'), branch: GITHUB_BRANCH };
    if (sha) body.sha = sha;
    return githubApiRequest('PUT', apiPath, body);
}
const RAW = (file) => `https://raw.githubusercontent.com/${GITHUB_REPO}/${GITHUB_BRANCH}/${YEAR_DIR}/${file}`;

// ----------------------------------------------------------------------------- probe mode
async function runProbe() {
    console.log('🔬 PROBE_ONLY — scanning recent txs per contract; writing nothing.\n');
    for (const [name, addr] of [['gauge controller', TLA_GAUGE_CONTROLLER], ['voting escrow', TLA_VOTING_ESCROW]]) {
        console.log(`── ${name}  (${addr})`);
        const res = await txSearchPage(addr, 1);
        if (res === null) { console.log('   ✗ both LCDs failed\n'); continue; }
        console.log(`   total txs (LCD reports): ${res.total ?? 'n/a'};  sampling ${res.txs.length}`);
        const seen = {};
        for (const tr of res.txs) {
            for (const m of tr?.tx?.body?.messages || []) {
                const msg = m?.msg; if (!msg || typeof msg !== 'object') continue;
                const key = Object.keys(msg)[0]; if (!key) continue;
                if (!seen[key]) { seen[key] = 1; console.log(`   action "${key}"  e.g. ${JSON.stringify(msg[key]).slice(0, 220)}`); }
                else seen[key]++;
                if (key === 'send' && msg.send?.contract === addr) { const inner = decodeMaybeB64(msg.send.msg); console.log(`     ↳ send-hook inner: ${inner ? JSON.stringify(inner).slice(0,220) : 'undecodable'}`); }
            }
        }
        console.log(`   counts: ${JSON.stringify(seen)}\n`);
    }
    console.log('Map any unrecognized keys into VOTE_ACTION_KEYS / LOCK_ACTION_KEYS, then run the real seed.');
}

// ----------------------------------------------------------------------------- main
async function run() {
    const startedAt = new Date();
    const deadline = Date.now() + WATCHDOG_MS;
    const errors = new ErrorLog();
    const discovered = {};
    console.log(`\n📜 tla-history-backfill — ${startedAt.toISOString()}\n`);

    if (PROBE_ONLY) { await runProbe(); return; }
    if (!GITHUB_TOKEN) throw new Error('GITHUB_TOKEN missing — refusing to run (no publish target).');

    // load prior state (committed event logs) + epoch dates
    const [priorVotes, priorLocks, epochDates] = await Promise.all([
        tryFetchJson(RAW('data/vote-events.json'), 'prior vote-events'),
        tryFetchJson(RAW('data/lock-events.json'), 'prior lock-events'),
        tryFetchJson(EPOCH_DATES_URL, 'epoch dates'),
    ]);
    const epochOf = makeEpochResolver(epochDates);

    const voteState = { lastScannedHeight: priorVotes?.lastScannedHeight || 0, events: priorVotes?.events || [], horizonHeight: priorVotes?.horizonHeight ?? null };
    const lockState = { lastScannedHeight: priorLocks?.lastScannedHeight || 0, events: priorLocks?.events || [], horizonHeight: priorLocks?.horizonHeight ?? null };
    const runMode = (voteState.events.length === 0 && lockState.events.length === 0) ? 'seed' : 'forward';
    console.log(`   mode: ${runMode}  (prior votes=${voteState.events.length}, locks=${lockState.events.length})`);

    // scan both contracts
    const gauge = await scanContract(TLA_GAUGE_CONTROLLER, voteState.lastScannedHeight, deadline);
    const escrow = await scanContract(TLA_VOTING_ESCROW,   lockState.lastScannedHeight, deadline);
    console.log(`   gauge: +${gauge.txs.length} txs (complete=${gauge.complete})  |  escrow: +${escrow.txs.length} txs (complete=${escrow.complete})`);

    // classify
    const freshVotes = classifyVoteTxs(gauge.txs, discovered);
    const freshLocks = classifyLockTxs(escrow.txs, discovered);

    // merge + dedup
    const vm = mergeEvents(voteState.events, freshVotes);
    const lm = mergeEvents(lockState.events, freshLocks);
    console.log(`   votes: ${voteState.events.length} → ${vm.merged.length} (+${vm.added})  |  locks: ${lockState.events.length} → ${lm.merged.length} (+${lm.added})`);

    // F3 never-shrink guard
    if (vm.merged.length < voteState.events.length || lm.merged.length < lockState.events.length) {
        errors.add('shrink-guard', new Error('merged event count < committed — aborting publish'));
        await publishHeartbeat({ startedAt, runMode, status: 'error', errors, discovered, voteCount: voteState.events.length, lockCount: lockState.events.length, note: 'F3 shrink guard tripped; nothing published' });
        throw new Error('F3 shrink guard: refusing to overwrite history with fewer events.');
    }

    // advance scan height only on a COMPLETE scan (else leave a gap to re-cover next run — F1/F2)
    const newVoteHeight = gauge.complete ? Math.max(voteState.lastScannedHeight, ...(gauge.txs.length ? gauge.txs.map(t => Number(t.height)) : [voteState.lastScannedHeight])) : voteState.lastScannedHeight;
    const newLockHeight = escrow.complete ? Math.max(lockState.lastScannedHeight, ...(escrow.txs.length ? escrow.txs.map(t => Number(t.height)) : [lockState.lastScannedHeight])) : lockState.lastScannedHeight;
    const voteHorizon = minDefined(voteState.horizonHeight, gauge.horizonHeight);
    const lockHorizon = minDefined(lockState.horizonHeight, escrow.horizonHeight);
    const complete = gauge.complete && escrow.complete;

    const voteFile = { schemaVersion: SCHEMA_VERSION, builtAt: startedAt.toISOString(), contract: TLA_GAUGE_CONTROLLER, lastScannedHeight: newVoteHeight, horizonHeight: voteHorizon, scan_complete: gauge.complete, count: vm.merged.length, events: vm.merged };
    const lockFile = { schemaVersion: SCHEMA_VERSION, builtAt: startedAt.toISOString(), contract: TLA_VOTING_ESCROW, lastScannedHeight: newLockHeight, horizonHeight: lockHorizon, scan_complete: escrow.complete, count: lm.merged.length, events: lm.merged };
    const rollups = buildRollups(vm.merged, lm.merged, epochOf);

    await publishFile(`${YEAR_DIR}/data/vote-events.json`, voteFile, `vote-events ${runMode}: ${vm.merged.length} (+${vm.added})`);
    await publishFile(`${YEAR_DIR}/data/lock-events.json`, lockFile, `lock-events ${runMode}: ${lm.merged.length} (+${lm.added})`);
    await publishFile(`${YEAR_DIR}/data/rollups.json`, rollups, `rollups: ${rollups.wallet_count} wallets`);

    // per-day delta archive (append-only)
    if (vm.added || lm.added) {
        const day = startedAt.toISOString().slice(0, 10);
        await publishFile(`${YEAR_DIR}/daily/${day}.json`, { day, runMode, votes_added: vm.added, locks_added: lm.added, vote_total: vm.merged.length, lock_total: lm.merged.length, builtAt: startedAt.toISOString() }, `daily delta ${day}`).catch(e => errors.add('daily archive', e));
    }

    await publishHeartbeat({ startedAt, runMode, status: complete ? 'ok' : 'partial', errors, discovered, voteCount: vm.merged.length, lockCount: lm.merged.length, voteHorizon, lockHorizon, newVoteHeight, newLockHeight });

    console.log(`\n✅ done — votes ${vm.merged.length}, locks ${lm.merged.length}, wallets ${rollups.wallet_count}, status ${complete ? 'ok' : 'PARTIAL'}`);
    if (Object.keys(discovered).length) console.log(`   discovered_actions: ${JSON.stringify(discovered)}`);
}

function minDefined(a, b) { const xs = [a, b].filter(v => v != null); return xs.length ? Math.min(...xs) : null; }

async function publishHeartbeat({ startedAt, runMode, status, errors, discovered, voteCount, lockCount, voteHorizon, lockHorizon, newVoteHeight, newLockHeight, note }) {
    const hb = {
        schemaVersion: SCHEMA_VERSION, capturedAt: startedAt.toISOString(), runId: startedAt.getTime().toString(36),
        runMode, status, note: note || undefined,
        vote_event_count: voteCount, lock_event_count: lockCount,
        vote_last_height: newVoteHeight, lock_last_height: newLockHeight,
        vote_horizon_height: voteHorizon, lock_horizon_height: lockHorizon,
        discovered_actions: discovered,
        next_expected_run_at: new Date(startedAt.getTime() + FORWARD_CADENCE_HOURS * 3600 * 1000).toISOString(),
        error_count: errors.count(), recent_errors: errors.list(),
    };
    try { await publishFile(`${YEAR_DIR}/heartbeat.json`, hb, `heartbeat ${status}`); }
    catch (e) { console.warn(`  ⚠ heartbeat publish failed: ${e.message}`); }
}

if (require.main === module) {
    run().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
}

module.exports = { classifyVoteTxs, classifyLockTxs, mergeEvents, buildRollups, extractVotes, normalizeAssetId, makeEpochResolver };
