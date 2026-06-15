// =============================================================================
// tla-history-annotate.js  —  one-time: tag published lock events `canonical`
// =============================================================================
//
// Retro-fits the `canonical` flag onto the already-seeded lock-events.json +
// rollups.json WITHOUT a 45-min re-seed. Wrapper-layer events (votion-la/*,
// arb/*, launch-nft/*, ca/*) duplicate a canonical `ve/*` twin for the same
// deposit; this flag lets VP/lock-delta math sum canonical-only and not
// double-count. Idempotent — safe to run more than once. Bumps schema → 2.
//
// (The main backfill now writes `canonical` natively, so future runs need no
// annotation; this only fixes the existing seed.)
//
// Env: GITHUB_TOKEN (required), GITHUB_REPO (default tla-history-data_2026),
//      GITHUB_BRANCH (default main).
// =============================================================================

'use strict';
const https = require('https');

const GITHUB_TOKEN  = process.env.GITHUB_TOKEN;
const GITHUB_REPO   = process.env.GITHUB_REPO   || 'defipatriot/tla-history-data_2026';
const GITHUB_BRANCH = process.env.GITHUB_BRANCH || 'main';
const YEAR_DIR = '2026';

// SINGLE SOURCE OF TRUTH for the rule (mirrors isCanonicalLock in the backfill):
// canonical = direct messages / cw20 hooks / the escrow's own `event:ve/*`.
function isCanonicalLock(type) {
    if (typeof type !== 'string') return true;
    if (!type.startsWith('event:')) return true;
    return type.startsWith('event:ve/');
}

function gh(method, apiPath, body) {
    return new Promise((resolve, reject) => {
        const opts = { hostname: 'api.github.com', path: apiPath, method, headers: { 'User-Agent': 'aDAO-annotate', Authorization: `Bearer ${GITHUB_TOKEN}`, Accept: 'application/vnd.github+json' } };
        if (body) opts.headers['Content-Type'] = 'application/json';
        const req = https.request(opts, res => { let d = ''; res.on('data', c => d += c); res.on('end', () => { if (res.statusCode >= 200 && res.statusCode < 300) { try { resolve(JSON.parse(d)); } catch { resolve(d); } } else reject(new Error(`GitHub ${method} ${apiPath}: ${res.statusCode} ${d.slice(0,160)}`)); }); });
        req.on('error', reject); if (body) req.write(JSON.stringify(body)); req.end();
    });
}
async function getFile(path) {
    const r = await gh('GET', `/repos/${GITHUB_REPO}/contents/${path}?ref=${GITHUB_BRANCH}`);
    return { json: JSON.parse(Buffer.from(r.content, 'base64').toString('utf8')), sha: r.sha };
}
async function putFile(path, obj, sha, msg) {
    return gh('PUT', `/repos/${GITHUB_REPO}/contents/${path}`, { message: msg, branch: GITHUB_BRANCH, sha, content: Buffer.from(JSON.stringify(obj, null, 0)).toString('base64') });
}

async function run() {
    if (!GITHUB_TOKEN) throw new Error('GITHUB_TOKEN missing — refusing to run.');
    console.log(`\n🏷  tla-history-annotate — ${new Date().toISOString()}\n   repo: ${GITHUB_REPO}@${GITHUB_BRANCH}\n`);

    // ---- lock-events.json ----
    const lockPath = `${YEAR_DIR}/data/lock-events.json`;
    const { json: lock, sha: lockSha } = await getFile(lockPath);
    let canon = 0, wrap = 0;
    for (const e of lock.events || []) { const c = isCanonicalLock(e.type); e.canonical = c; c ? canon++ : wrap++; }
    lock.schemaVersion = 2;
    lock.annotated_at = new Date().toISOString();
    lock.canonical_count = canon;
    lock.wrapper_count = wrap;
    await putFile(lockPath, lock, lockSha, `annotate lock-events canonical flag (${canon} canonical / ${wrap} wrapper)`);
    console.log(`  ✓ lock-events.json — ${canon} canonical, ${wrap} wrapper (of ${(lock.events||[]).length})`);

    // ---- rollups.json (per-wallet lock lists) ----
    const rollPath = `${YEAR_DIR}/data/rollups.json`;
    try {
        const { json: roll, sha: rollSha } = await getFile(rollPath);
        let touched = 0;
        const container = roll.wallets || roll.by_wallet || roll;
        const walletList = Array.isArray(container) ? container : Object.values(container);
        for (const wEntry of walletList) {
            const locks = wEntry && wEntry.locks;
            if (Array.isArray(locks)) for (const lk of locks) { lk.canonical = isCanonicalLock(lk.type); touched++; }
        }
        roll.schemaVersion = 2;
        roll.annotated_at = new Date().toISOString();
        await putFile(rollPath, roll, rollSha, `annotate rollups canonical flag (${touched} lock entries)`);
        console.log(`  ✓ rollups.json — tagged ${touched} per-wallet lock entries`);
    } catch (e) { console.warn(`  ⚠ rollups.json not annotated (non-fatal): ${e.message}`); }

    console.log(`\n✅ done. Consumers: sum lock deltas with \`canonical === true\` only.`);
}
if (require.main === module) run().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
module.exports = { isCanonicalLock };
