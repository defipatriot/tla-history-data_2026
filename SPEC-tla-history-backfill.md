# SPEC — `tla-history-backfill` cron (vote + lock event history)

**Status:** specified 2026-06-13. **BUILT + PROBED 2026-06-15.** Deployed as a
GitHub Action in `tla-history-data_2026` (sample/full like the nft backfills). The
`RUN_MODE=sample` probe confirmed the live Eris ve3 verbs: vote = `vote`
(`{gauge, votes:[[assetId,bps]]}`, one per bucket); locks = `create_lock`,
`extend_lock_amount`, `extend_lock_time`, `merge_lock`, `split_lock`,
`migrate_lock`, `lock_permanent`/`unlock_permanent`, `transfer_nft`, `withdraw`
(cw20-funded create/extend via the `send` hook; `time` is a duration in seconds).
Action map updated to match; classifiers unit-tested against the real probe
shapes. **Remaining: run `RUN_MODE=full` to seed** (gauge ~19,044 txs / ~191
pages; escrow ~9,591 / ~96 pages — within the Action timeout; partial runs
self-heal on re-run via dedup). This is the TLA equivalent of the NFT-inventory
provenance backfill — reconstruct *behavioral* history from the permanent
transaction log, the same way `nft-provenance-backfill.js` did.

---

## The core insight (why this is possible)

Public Terra LCDs prune **state** after ~100 blocks, so we cannot reconstruct
what a position was *worth* on a past date. BUT the LCD's `tx_search` exposes
the permanent **transaction log** — and every vote, lock creation, relock, and
merge is a transaction. So:

| Want to backfill | Possible? | Why |
|---|---|---|
| When a member adjusted their votes (history) | ✅ YES | Each vote is a `MsgExecuteContract` tx on the gauge controller |
| When/how locks were created, relocked, merged | ✅ YES | Each is a tx on the voting-escrow contract; lock `start` period also dates creation |
| Lock/unlock/claim events | ✅ YES | Transactions |
| **Position USD value over time** | ❌ NO | Derived state (reserves × prices at that moment), pruned — never a tx |
| **Past APR / past pending rewards** | ❌ NO | Same — derived snapshots, not transactions |

**So this cron backfills EVENTS (behavioral, ungameable), not VALUATIONS.**
The valuation time-series is forward-only (the daily `adao-positions` archives,
clock started 2026-06-13). This cron fills the *event* history backward to
genesis, then maintains it forward.

This directly feeds **Vote Intelligence** (vote-change frequency, voting on
inactive/abandoned LPs) and member tenure/behavior — the differentiated data.

---

## What it produces

New repo `tla-history-data_2026` (or a subpath in an existing TLA repo), e.g.:

```
data/vote-events.json      { schemaVersion, builtAt, lastScannedHeight,
                             events: [{ type:'vote', wallet, period, height,
                             timestamp, tx_hash, votes:[[asset, bps]] }] }
data/lock-events.json      { ..., events: [{ type:'lock_create'|'relock'|'merge'|
                             'withdraw', wallet, token_id, height, timestamp,
                             tx_hash, asset, amount, end_period }] }
```

Derived rollups the consumers actually use (can be computed at read-time or
emitted):
- per-wallet: vote-change count, last-vote epoch, vote churn rate, list of
  pools voted for over time, flag "voted for a pool that later went inactive."
- per-wallet: lock timeline (created → relocked → …), first-lock date.

---

## Build approach (mirror the NFT provenance backfill — proven pattern)

1. **`tx_search` by contract.** Query `/cosmos/tx/v1beta1/txs?query=...` filtering
   on `wasm._contract_address = <gauge controller>` (votes) and
   `= <voting escrow>` (locks). Page with `page` + `ORDER_BY_DESC` — **publicnode
   ignores `pagination.offset`** (F1 failure class; this bit the NFT cron for
   months). Use DESC paging if results exceed ~1000.
2. **Parse the execute msg** out of each tx to get the action + args (the vote
   weights, the lock params). The msg is in the tx body's
   `MsgExecuteContract.msg` (base64 JSON).
3. **Seed-once genesis replay**, then forward-maintain from `lastScannedHeight`.
   Commit the backfill output ONCE (it's expensive); forward runs only scan new
   blocks. Same as pending-claims seed.
4. **Never-shrink publish guard** (F3): history is append-only; a run producing
   fewer events than committed = incomplete → abort, don't overwrite.
5. **Distinguish null (query failed) from [] (genuine end)** (F2) on every page.
6. **Heartbeat honesty** (F7): partial/error status on any incomplete scan.

(All six are the reliability failure-classes from `cron-scripts/README.md` — run
the checklist against this cron.)

## Open questions to resolve at build time (probe first)
- Exact `wasm.action` value(s) for a vote vs a lock-create vs relock vs merge —
  capture one of each from a known recent tx via `tx_search` and read the
  attributes. (Same discovery step the NFT cron did for stake/unstake.)
- Volume estimate: gauge controller has ~189 epochs × N voters — could be a few
  thousand vote txs. Lock contract: 431 locks + historical churn. Both
  manageable but page carefully.
- Whether `tx_search` on publicnode reaches genesis or has a height floor — if
  floored, document the earliest reachable height as the backfill horizon (an
  honest "history from height H" rather than a false "from genesis").

## Sequencing
Independent of the member-expansion crons. Can be built any time. Highest value
*after* `tla-participants` exists (so the wallet universe to enrich is defined),
but doesn't depend on it. Its own cron, own repo, own schedule — like every
other capture.
