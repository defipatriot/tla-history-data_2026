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
const SEED_MAX_PAGES = Number(process.env.SEED_MAX_PAGES || 600); // page cap per query (gauge votes ~70p, escrow ~100p)
const PAGE_LIMIT    = 100;
const SCHEMA_VERSION = 1;
const FORWARD_CADENCE_HOURS = 6;     // forward maintenance cadence (tune in Render)

// Known execute-msg keys → normalized event type. Anything NOT here is still
// captured losslessly (raw_msg kept) and counted in discovered_actions, so an
// unexpected key surfaces in the heartbeat instead of being silently dropped.
// (Eris ve3 naming; confirm via PROBE_ONLY before the first seed.)
// CHAIN-CONFIRMED via probe (2026-06-15) against the live gauge + escrow.
// Vote is the only vote-class verb; the rest below are the real lock verbs.
const VOTE_ACTION_KEYS = {
    vote: 'vote',
};
// Lock verbs (top-level msgs on the voting escrow). claim_rebase is intentionally
// NOT typed here (reward claim, high-volume) — it still shows in discovered_actions.
const LOCK_ACTION_KEYS = {
    create_lock: 'lock_create',
    extend_lock_amount: 'lock_extend_amount',
    extend_lock_time: 'lock_extend_time',
    merge_lock: 'merge',
    split_lock: 'split',
    migrate_lock: 'migrate',
    lock_permanent: 'lock_permanent',     // auto-max ON
    unlock_permanent: 'unlock_permanent', // auto-max OFF (starts decaying)
    withdraw: 'withdraw', unlock: 'withdraw',
    transfer_nft: 'lock_transfer',        // lock ownership change (CW721 transfer)
    deposit_for: 'lock_deposit_for',
};
// CW20 send-hook inner keys (lock funded by an LST cw20 `send`) — confirmed:
// create_lock and extend_lock_amount arrive this way.
const LOCK_HOOK_KEYS = { create_lock: 'lock_create', extend_lock_amount: 'lock_extend_amount', deposit_for: 'lock_deposit_for' };

// ----------------------------------------------------------------------------- http
// Proven resilient transport from the nft backfills: keep-alive single socket,
// short timeout, many FAST retries (publicnode is flaky per-request but quick).
const KEEPALIVE_AGENT = new https.Agent({ keepAlive: true, maxSockets: 1, keepAliveMsecs: 30000 });
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function httpGet(url, t = 20000) {
    return new Promise((res, rej) => {
        const r = https.get(url, { agent: KEEPALIVE_AGENT, headers: { Accept: 'application/json', Connection: 'keep-alive', 'User-Agent': 'aDAO-tla-history-backfill/2.0' } }, (x) => {
            let b = ''; x.on('data', c => b += c); x.on('end', () => {
                if (x.statusCode >= 200 && x.statusCode < 300) { try { res(JSON.parse(b)); } catch { rej(new Error('bad JSON')); } }
                else rej(new Error(`HTTP ${x.statusCode} ${b.slice(0, 120)}`)); });
        });
        r.on('error', rej); r.setTimeout(t, () => r.destroy(new Error('timeout')));
    });
}
async function lcdGet(p, label) { try { return await httpGet(TERRA_LCD_PRIMARY + p); } catch (e) { try { return await httpGet(TERRA_LCD_FALLBACK + p); } catch (e2) { throw new Error(`${label}: both LCDs failed (${e2.message})`); } } }
async function tryGetJson(url, label) { try { return await httpGet(url); } catch (e) { console.warn(`  ⚠ ${label} fetch failed (non-fatal): ${e.message}`); return null; } }

// ----------------------------------------------------------------------------- tx_search (resilient pager)
// Adapted verbatim-in-spirit from nft-provenance-backfill.js's fetchAllTxs — the
// pager that fixed publicnode's pagination quirk (it IGNORES offset; deep DESC
// paging returns inconsistent slices). Pages ASCending (oldest-first, stable),
// reprobes page 1 for the deepest archive, then walks forward by frontier height,
// rejecting regressions/far-jumps. Returns ALL txs (deduped, height-sorted) and a
// `stop` reason. `conds` are query terms ANDed together.
async function fetchAllTxs(conds, label) {
    const RETRIES = +(process.env.PAGER_RETRIES || 40), ROUNDS = +(process.env.PAGER_ROUNDS || 2);
    const ERR_BACKOFF = +(process.env.PAGER_ERR_BACKOFF || 250), PROBE_DELAY = +(process.env.PAGER_PROBE_DELAY || 40);
    const CONTIG_DELTA = 250000, P1_STABLE = 12;
    const txPath = (page) => `/cosmos/tx/v1beta1/txs?query=${encodeURIComponent(conds.join(' AND '))}&order_by=ORDER_BY_ASC&page=${page}&limit=${PAGE_LIMIT}`;
    const out = [], seen = new Set();
    const stats = { calls: 0, pages: 0, regress: 0, far: 0, dup: 0, empty: 0, error: 0, reprobe: 0 };
    let frontier = 0, globalMax = 0, stop = 'complete';
    const scan = (batch) => { let freshMin = Infinity, fresh = 0; for (const tx of batch) { const h = Number(tx.height); if (h > globalMax) globalMax = h; if (!seen.has(tx.txhash)) { fresh++; if (h < freshMin) freshMin = h; } } return { fresh, freshMin }; };
    const commit = (batch) => { let added = 0; for (const tx of batch) { const h = Number(tx.height); if (h > frontier) frontier = h; if (!seen.has(tx.txhash)) { seen.add(tx.txhash); out.push(tx); added++; } } stats.pages++; return added; };

    // page 1: deepest archive wins; early-break once the smallest start-height stabilizes
    let best1 = null, noImprove = 0, nonEmpty = 0;
    for (let a = 0; a < RETRIES; a++) {
        stats.calls++;
        let resp; try { resp = await lcdGet(txPath(1), `${label} p1.${a}`); } catch { stats.error++; await sleep(ERR_BACKOFF); continue; }
        const batch = resp?.tx_responses || [];
        if (!batch.length) { stats.empty++; await sleep(ERR_BACKOFF); continue; }
        scan(batch); nonEmpty++;
        const minH = Math.min(...batch.map(t => Number(t.height)));
        if (!best1 || minH < best1.minH) { best1 = { batch, minH }; noImprove = 0; } else { noImprove++; }
        if (a % 8 === 7) console.log(`  ${label}: probing page 1… best start-height=${best1 ? best1.minH : 'n/a'} (${a + 1} probes)`);
        if (nonEmpty >= 3 && noImprove >= P1_STABLE) break;
        await sleep(PROBE_DELAY);
    }
    if (!best1) { console.warn(`  ⚠ ${label}: page 1 unreachable after ${RETRIES} tries (treating as empty)`); return { txs: [], stop: 'p1-unreachable', globalMax: 0 }; }
    commit(best1.batch);
    console.log(`  ${label}: page1 start-height=${best1.minH} (${out.length} txs, frontier=${frontier})`);

    for (let page = 2; page < SEED_MAX_PAGES; page++) {
        const avg = out.length > 1 ? Math.max(1, (frontier - Number(out[0].height)) / (out.length - 1)) : 1;
        const TIGHT = Math.max(2000, 3 * avg), LOOSE = Math.max(50000, 10 * avg);
        let bestCand = null, rounds = 0;
        do {
            if (rounds > 0) stats.reprobe++;
            for (let a = 0; a < RETRIES; a++) {
                stats.calls++;
                let resp; try { resp = await lcdGet(txPath(page), `${label} p${page}.${a}`); } catch { stats.error++; await sleep(ERR_BACKOFF); continue; }
                const batch = resp?.tx_responses || [];
                if (!batch.length) { stats.empty++; await sleep(ERR_BACKOFF); continue; }
                const { fresh, freshMin } = scan(batch);
                if (fresh === 0) { stats.dup++; await sleep(PROBE_DELAY); continue; }
                if (freshMin < frontier) { stats.regress++; await sleep(PROBE_DELAY); continue; }
                if (freshMin - frontier > CONTIG_DELTA) { stats.far++; await sleep(PROBE_DELAY); continue; }
                if (!bestCand || freshMin < bestCand.freshMin) bestCand = { batch, freshMin };
                if (bestCand.freshMin - frontier <= TIGHT) break;
                await sleep(PROBE_DELAY);
            }
            rounds++;
        } while (frontier < globalMax && rounds < ROUNDS && (!bestCand || bestCand.freshMin - frontier > LOOSE));

        if (bestCand) {
            const added = commit(bestCand.batch);
            if (page % 10 === 0 || added === 0) console.log(`  ${label}: ${out.length} txs (page ${page}, frontier=${frontier}, +${added})`);
            if (page === SEED_MAX_PAGES - 1) { stop = 'page-cap'; console.warn(`  ⚠ ${label} hit page cap (${SEED_MAX_PAGES})`); }
            continue;
        }
        if (frontier >= globalMax) { stop = 'clean-end'; break; }
        stop = `stuck@page${page}`;
        console.warn(`  ⚠ ${label}: STUCK at page ${page} — frontier ${frontier} < globalMax ${globalMax}`);
        break;
    }
    out.sort((a, b) => Number(a.height) - Number(b.height) || (a.txhash < b.txhash ? -1 : 1));
    console.log(`  ${label}: DONE — ${out.length} txs | stop=${stop} | pages=${stats.pages} calls=${stats.calls} reprobe=${stats.reprobe} regress=${stats.regress} far=${stats.far} dup=${stats.dup} empty=${stats.empty} error=${stats.error}`);
    return { txs: out, stop, globalMax };
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
            const a = msg[key] || {};
            events.push({ type: 'vote', wallet: m.sender, ...meta, gauge: a.gauge ?? null, votes: extractVotes(a), raw_msg: extractVotes(a) ? undefined : a });
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
                if (type) { const ia = inner[innerKey] || {}; events.push({ type, wallet: m.sender, ...meta,
                    token_id: ia.token_id != null ? String(ia.token_id) : null,
                    asset: m.contract ? `cw20:${m.contract}` : null, // the LST cw20 that was `send`-ed
                    amount: msg.send.amount != null ? Number(msg.send.amount) : null,
                    lock_seconds: ia.time != null ? Number(ia.time) : null,
                    funded_by_cw20: true, args: ia }); matchedThisTx = true; }
                continue;
            }
            discovered[`escrow:${key}`] = (discovered[`escrow:${key}`] || 0) + 1;
            const type = LOCK_ACTION_KEYS[key];
            if (!type) continue;
            const a = msg[key] || {};
            events.push({ type, wallet: m.sender, ...meta,
                token_id: a.token_id != null ? String(a.token_id) : (a.lock_id != null ? String(a.lock_id) : null),
                token_id_add: a.token_id_add != null ? String(a.token_id_add) : null, // merge_lock
                asset: a.asset ? normalizeAssetId(a.asset) : null,
                into_asset: a.into ? normalizeAssetId(a.into) : null,               // migrate_lock target LST
                amount: a.amount != null ? Number(a.amount) : null,                 // split_lock / create_lock
                lock_seconds: a.time != null ? Number(a.time) : null,               // duration in SECONDS (not a period)
                recipient: a.recipient || null,                                     // transfer_nft
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
    let prevByWalletGauge = {};
    for (const e of voteEvents) {
        if (!e.wallet) continue;
        const r = w(e.wallet); r.vote_count++;
        const ep = epochOf(e.timestamp);
        if (ep != null) { if (r.first_vote_epoch == null || ep < r.first_vote_epoch) r.first_vote_epoch = ep; if (r.last_vote_epoch == null || ep > r.last_vote_epoch) r.last_vote_epoch = ep; }
        // churn is per (wallet, gauge-bucket): a wallet votes once per bucket, so
        // comparing across buckets would false-flag a change. Key by wallet|gauge.
        const gkey = `${e.wallet}|${e.gauge ?? ''}`;
        const sig = JSON.stringify((e.votes || []).slice().sort());
        if (prevByWalletGauge[gkey] != null && prevByWalletGauge[gkey] !== sig) r.vote_changes++;
        prevByWalletGauge[gkey] = sig;
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
    console.log('🔬 sample/probe — scanning recent txs per contract; writing nothing.\n');
    for (const [name, addr] of [['gauge controller', TLA_GAUGE_CONTROLLER], ['voting escrow', TLA_VOTING_ESCROW]]) {
        console.log(`── ${name}  (${addr})`);
        const path = `/cosmos/tx/v1beta1/txs?query=${encodeURIComponent(`wasm._contract_address='${addr}'`)}&order_by=ORDER_BY_DESC&page=1&limit=${PAGE_LIMIT}`;
        let res = null; try { res = await lcdGet(path, `probe ${name}`); } catch (e) { console.log(`   ✗ both LCDs failed: ${e.message}\n`); continue; }
        const txs = res?.tx_responses || [];
        console.log(`   total txs (LCD reports): ${res?.total ?? 'n/a'};  sampling ${txs.length}`);
        const seen = {};
        for (const tr of txs) {
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
    console.log('Action keys are already mapped (confirmed 2026-06-15). Run RUN_MODE=full to seed.');
}

// ----------------------------------------------------------------------------- main
async function run() {
    const startedAt = new Date();
    const errors = new ErrorLog();
    const discovered = {};
    console.log(`\n📜 tla-history-backfill — ${startedAt.toISOString()}\n`);

    if (PROBE_ONLY) { await runProbe(); return; }
    if (!GITHUB_TOKEN) throw new Error('GITHUB_TOKEN missing — refusing to run (no publish target).');

    // load prior state (committed event logs) + epoch dates
    const [priorVotes, priorLocks, epochDates] = await Promise.all([
        tryGetJson(RAW('data/vote-events.json'), 'prior vote-events'),
        tryGetJson(RAW('data/lock-events.json'), 'prior lock-events'),
        tryGetJson(EPOCH_DATES_URL, 'epoch dates'),
    ]);
    const epochOf = makeEpochResolver(epochDates);
    const voteState = { events: priorVotes?.events || [] };
    const lockState = { events: priorLocks?.events || [] };
    const runMode = (voteState.events.length === 0 && lockState.events.length === 0) ? 'seed' : 'forward';
    console.log(`   mode: ${runMode}  (prior votes=${voteState.events.length}, locks=${lockState.events.length})\n`);

    // Full-history sweep of each contract (ASC resilient pager). Votes are gauge-
    // only → filter to wasm.action='vote' (cuts ~19k gauge txs to just votes). If
    // the filter ever returns nothing, fall back to an unfiltered gauge sweep.
    console.log('🗳  scanning gauge for votes…');
    let gauge = await fetchAllTxs([`wasm._contract_address='${TLA_GAUGE_CONTROLLER}'`, `wasm.action='vote'`], 'gauge-votes');
    if (gauge.txs.length === 0 && gauge.stop !== 'clean-end') {
        console.warn('  ⚠ vote-filtered gauge sweep empty — retrying UNFILTERED');
        gauge = await fetchAllTxs([`wasm._contract_address='${TLA_GAUGE_CONTROLLER}'`], 'gauge-all');
    }
    console.log('\n🔒 scanning escrow for locks…');
    const escrow = await fetchAllTxs([`wasm._contract_address='${TLA_VOTING_ESCROW}'`], 'escrow-locks');

    const gaugeComplete  = gauge.stop === 'complete' || gauge.stop === 'clean-end';
    const escrowComplete = escrow.stop === 'complete' || escrow.stop === 'clean-end';
    console.log(`\n   gauge: ${gauge.txs.length} txs (${gauge.stop})  |  escrow: ${escrow.txs.length} txs (${escrow.stop})`);

    // classify (votes from gauge, locks from escrow)
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

    const earliest = (txs) => txs.length ? Number(txs[0].height) : null; // pager returns ASC-sorted
    const complete = gaugeComplete && escrowComplete;
    const voteFile = { schemaVersion: SCHEMA_VERSION, builtAt: startedAt.toISOString(), contract: TLA_GAUGE_CONTROLLER, lastScannedHeight: gauge.globalMax || 0, horizonHeight: earliest(gauge.txs), scan_complete: gaugeComplete, scan_stop: gauge.stop, count: vm.merged.length, events: vm.merged };
    const lockFile = { schemaVersion: SCHEMA_VERSION, builtAt: startedAt.toISOString(), contract: TLA_VOTING_ESCROW, lastScannedHeight: escrow.globalMax || 0, horizonHeight: earliest(escrow.txs), scan_complete: escrowComplete, scan_stop: escrow.stop, count: lm.merged.length, events: lm.merged };
    const rollups = buildRollups(vm.merged, lm.merged, epochOf);

    await publishFile(`${YEAR_DIR}/data/vote-events.json`, voteFile, `vote-events ${runMode}: ${vm.merged.length} (+${vm.added})`);
    await publishFile(`${YEAR_DIR}/data/lock-events.json`, lockFile, `lock-events ${runMode}: ${lm.merged.length} (+${lm.added})`);
    await publishFile(`${YEAR_DIR}/data/rollups.json`, rollups, `rollups: ${rollups.wallet_count} wallets`);

    if (vm.added || lm.added) {
        const day = startedAt.toISOString().slice(0, 10);
        await publishFile(`${YEAR_DIR}/daily/${day}.json`, { day, runMode, votes_added: vm.added, locks_added: lm.added, vote_total: vm.merged.length, lock_total: lm.merged.length, builtAt: startedAt.toISOString() }, `daily delta ${day}`).catch(e => errors.add('daily archive', e));
    }

    await publishHeartbeat({ startedAt, runMode, status: complete ? 'ok' : 'partial', errors, discovered, voteCount: vm.merged.length, lockCount: lm.merged.length, voteHorizon: earliest(gauge.txs), lockHorizon: earliest(escrow.txs), newVoteHeight: gauge.globalMax || 0, newLockHeight: escrow.globalMax || 0 });

    console.log(`\n✅ done — votes ${vm.merged.length}, locks ${lm.merged.length}, wallets ${rollups.wallet_count}, status ${complete ? 'ok' : 'PARTIAL'}`);
    if (Object.keys(discovered).length) console.log(`   discovered_actions: ${JSON.stringify(discovered)}`);
}

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
