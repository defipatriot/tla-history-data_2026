# tla-history-backfill

**Vote + lock EVENT history** for the Terra Liquidity Alliance — the behavioral,
ungameable history layer that feeds **Vote Intelligence** and member tenure.

Writes to `defipatriot/tla-history-data_2026`.

## Why this exists

Public Terra LCDs prune **state** (~100 blocks), so we can never reconstruct what
a position was *worth* on a past date. But the LCD's `tx_search` exposes the
permanent **transaction log** — and every vote, lock create, extend, relock,
merge, and withdraw is a transaction. So we backfill **events (what members did
and when)**, not valuations.

| Want to backfill | Possible? | Why |
|---|---|---|
| When a member changed their votes | ✅ | each vote is a tx on the gauge controller |
| Lock create / extend / relock / merge / withdraw | ✅ | each is a tx on the voting escrow |
| Position USD over time, past APR, past pending rewards | ❌ | derived state, pruned — never a tx (stays forward-only via `adao-positions`) |

This is the TLA analogue of the proven `nft-inventory` pending-claims backfill:
**seed the full history once, then forward-maintain from `lastScannedHeight`.**

## What it produces (`2026/`)

| File | Contents |
|---|---|
| `data/vote-events.json` | append-only `events[]` (`type:'vote'`, `wallet`, `height`, `timestamp`, `tx_hash`, `votes:[[assetId,bps]]`) + `lastScannedHeight`, `horizonHeight` |
| `data/lock-events.json` | append-only `events[]` (`lock_create` / `lock_extend_time` / `lock_extend_amount` / `relock` / `merge` / `withdraw` …) + scan state |
| `data/rollups.json` | per-wallet derived: `vote_count`, `vote_changes`, `vote_churn_rate`, `first/last_vote_epoch`, `pools_voted[]`, lock `locks[]` timeline, `first_lock_ts` |
| `heartbeat.json` | freshness, `status`, `discovered_actions` tally, scan horizon |
| `daily/YYYY-MM-DD.json` | per-day delta (votes/locks added) |

`assetId` is canonical `cw20:terra1…` / `native:uluna`. Epochs are resolved from
`website-adao-core/epoch_1-300_date.json` at runtime.

## Contracts

- Gauge controller (votes): `terra1hfksrhchkmsj4qdq33wkksrslnfles6y2l77fmmzeep0xmq24l2smsd3lj`
- Voting escrow (locks): `terra1uqhj8agyeaz8fu6mdggfuwr3lp32jlrx5hqag4jxexde92rzkamq3l62zg`

## Reliability (README → F-class checklist)

- **F1 pagination** — publicnode ignores `pagination.offset`; we page with `page=` + `ORDER_BY_DESC` and a hard page ceiling.
- **F2 null≠[]** — a failed page returns `null` (try fallback LCD, else partial), never silently "no data."
- **F3 never-shrink** — history is append-only; a merged count below the committed count **aborts the publish** and flips heartbeat to `error`.
- **F7 heartbeat honesty** — `status` is `partial` whenever a scan was incomplete (page ceiling, LCD failure, or watchdog), so the System Health page never green-lights a gap.
- **F8 horizon** — records the earliest reachable height (`horizonHeight`) so the data is honestly "history from height H," not a false "from genesis."
- **Scan-height discipline** — `lastScannedHeight` only advances on a **complete** scan; a partial scan leaves the gap to re-cover next run.

## Classification is lossless + self-probing

Every execute against each contract is bucketed by its decoded top-level msg key
(the LCD returns `tx.body.messages[].msg` already-decoded as JSON). cw20 `send`
hooks (locks funded by an LST cw20) are decoded too. Anything not in the known
action map is **still counted** in `heartbeat.discovered_actions` and, where it
touches the escrow, captured as a thin `event:<action>` so it is never dropped —
it just shows up for later enrichment instead of vanishing.

### Action keys — CHAIN-CONFIRMED (probe, 2026-06-15)

The execute-msg verbs are confirmed against the live contracts, so you can run
`full` directly. For the record, what the probe found:
- **Vote** (gauge controller): `vote` → `{gauge:"<bucket>", votes:[[assetId,bps]]}`. Captured with its bucket; a wallet votes once per bucket.
- **Locks** (voting escrow): `create_lock`, `extend_lock_amount`, `extend_lock_time`, `merge_lock`, `split_lock`, `migrate_lock` (swap LST), `lock_permanent`/`unlock_permanent` (auto-max on/off), `transfer_nft` (ownership), `withdraw`. cw20-funded `create_lock`/`extend_lock_amount` arrive via the `send` hook and are decoded. `time` values are **lock durations in seconds**, stored as `lock_seconds`. `claim_rebase` is intentionally counted-only (reward claim, not a structural lock event).

Anything new that appears later still lands in `heartbeat.discovered_actions` (lossless), so the map can be extended without losing data.

Running `RUN_MODE=sample` re-prints this discovery any time, writing nothing.

## Run modes

- **`sample`** (default) — dry-run/probe: scans recent txs, prints action keys, writes nothing.
- **`full`, no committed state** — SEED: replays history to the LCD horizon, publishes once. May take several minutes; `SEED_MAX_PAGES` (default 400) caps it — if hit, the run is `partial` and the next run continues from the horizon.
- **`full`, state exists** — FORWARD: scans only new blocks above `lastScannedHeight`, appends, dedups by `tx_hash`.

## Deploy (GitHub Actions — like the nft backfills)

This is a one-time job, so it runs as a **manual GitHub Action in this data repo**,
not a Render cron (matching `nft-inventory-data_2026`'s backfills). Commit three
files to `defipatriot/tla-history-data_2026`:

- `tla-history-backfill.js` (repo root)
- `package.json` (repo root)
- `.github/workflows/tla-history-backfill.yml`

Then: **Actions tab → "TLA History Backfill" → Run workflow.** Pick `mode = sample`
first (confirm action keys), then `mode = full` to seed. The workflow supplies
`GITHUB_TOKEN`/`GITHUB_REPO`/`GITHUB_BRANCH` automatically (`permissions: contents: write`)
— no secrets to set. Re-run any time to top up forward history (append-only +
shrink-guarded, so re-runs are safe).

**Optional forward maintenance (later):** if you want vote/lock events kept current
automatically, the same script can run on a Render cron (`0 */6 * * *`) with env
`GITHUB_REPO=defipatriot/tla-history-data_2026` (full owner prefix), `GITHUB_TOKEN`,
`RUN_MODE=full`. It seed-or-forwards based on existing state, so nothing changes.

## Consumers (next: the UI)

- **Vote Intelligence** — vote-change frequency, votes on later-inactive LPs (join `rollups.pools_voted` against the active-pool set from `tla-snapshot`).
- **Member tenure / behavior** — `first_vote_epoch` (also available natively via the gauge `user_first_participation` read) and lock timelines.
- **Portfolio Tracker** — lock event timeline per member.

## Recent changes

- **1.0.0** — initial build. Seed-once + forward-maintain; gauge-vote and escrow-
  lock classification (incl. cw20 send-hook locks); lossless `discovered_actions`;
  F1/F2/F3/F7/F8 guards; per-wallet rollups; `PROBE_ONLY` discovery mode. Pure
  classify/merge/rollup functions unit-tested against synthetic LCD fixtures.
  **Action-key map is inferred — run `PROBE_ONLY=1` once to confirm before seeding.**
