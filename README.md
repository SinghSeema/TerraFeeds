# Ownership Ledger Service

This service records who owns what. It reads a settlement feed that arrives out of order, may deliver the same message more than once, and can retract events it already sent. The ledger never pays out on a balance that a retraction or a duplicate could change.

## Run it

```
docker compose up -d --build   # Postgres 16 + API on :3000
npm install
npm test                       # deterministic tests + the concurrency test, including the must-fail runs
```

Postgres is published on host port `55432`, not 5432, so it does not clash with a Postgres already running locally. If a port is taken, set `PG_PORT` or `API_PORT`. The tests use their own `ledger_test` database, so the running API never touches test data.

## Endpoints

`POST /events` stands in for the queue consumer. It accepts one message and returns `202` only after the transaction has committed, so the 202 is the ack.

```
curl -XPOST localhost:3000/events -H 'content-type: application/json' \
  -d '{"eventId":"e1","type":"TRANSFER","height":1,"assetId":"FUND-A","from":null,"to":"alice","units":1000}'
# {"outcome":"accepted"}      possible outcomes: accepted | duplicate | replaced | conflict | applied | rejected

curl -XPOST localhost:3000/events -H 'content-type: application/json' \
  -d '{"eventId":"r1","type":"RETRACT","fromHeight":2}'
```

`from: null` is an issuance. Units are whole numbers and may be sent as a number or a string.

```
GET /assets/FUND-A/holders
{"assetId":"FUND-A","issuedSupply":"1000","appliedThrough":"4471","gapAt":"4472","waitingCount":1,
 "holders":[{"investorId":"alice","finalized":"0","provisional":"1000"}]}

GET /investors/alice/positions
{"investorId":"alice","appliedThrough":"4473","gapAt":null,"waitingCount":0,
 "positions":[{"assetId":"FUND-A","finalized":"0","provisional":"650"}]}
```

- `appliedThrough` is the frontier: the highest height applied with no gap below it.
- `gapAt` is the height the ledger is stuck on, if any.
- `waitingCount` is the number of events received but not yet applied. They appear in neither figure.
- Units are returned as strings so no client parses them into a lossy float.

`GET /metrics` returns JSON counters: duplicates ignored, alerts by type, drains, events applied, retractions applied and rejected, `frontier`, `highestSeen`, and `oldestWaitingAgeSeconds`. `GET /health` returns `{"ok":true}`.

To replay the gap demo from the plan, start the API with `GENESIS_FRONTIER=4470 docker compose up -d --build`. Then post 4471, then 4473 (`gapAt` is 4472), then 4472.

## Design

- **The event log is the source of truth.** `events` is append-only and never deleted from. Each event's status is `waiting`, `applied`, `retracted` or `rejected`. `holdings` is derived from it and has a `CHECK (units >= 0)` constraint.
- **Events apply strictly in height order**, one event per transaction, each at `frontier + 1`, under the frontier lock (`SELECT ... FOR UPDATE` on the single `ledger_state` row). A gap, an overspend or two events at one height stops the drain and raises an alert. Nothing is ever skipped automatically.
- **The database decides duplicates.** Ingest is `INSERT ... ON CONFLICT (event_id) DO NOTHING`. When a message arrives with an id already stored:

  | Stored row | Same content | Different content |
  |---|---|---|
  | `retracted` (a tombstone) | ignored | replaced as a new `waiting` event |
  | `waiting` or `applied` | ignored | left unchanged, `live_row_conflict` alert |
- **Retraction** takes the exclusive advisory lock, then the frontier lock. It undoes applied events newest-first, so no holder goes negative even briefly, and tombstones every event it sweeps. It then rewinds the frontier to `fromHeight - 1`. A retraction that reaches final heights is rejected with an alert.
- **The ingest barrier.** Transfer ingest holds the same advisory lock in shared mode. A retraction therefore waits for every ingest already in flight, and its sweep sees it. Ingests that start later wait until the rewind has committed.
- **Lock order is always the advisory lock first, then the `ledger_state` row.** The drain takes only the row lock, so the two can never wait on each other in a cycle.
- **Finalized and provisional are computed at read time.** An applied event is finalized when `height <= frontier - 12`. `provisional` is the net change from applied events above that line, and `finalized` is the balance minus provisional. Both reads run in one `REPEATABLE READ, READ ONLY` transaction, so the header and the figures come from one snapshot.
- **The drain runs after every ingest and on a 1s timer on every instance.** Two drains racing is safe: the second blocks on the frontier lock, then re-reads the frontier.
- **`reconcile()`** replays all applied events in memory and compares the result with `holdings` and `issued_supply`. The test calls it. In production it would run hourly and after every retraction; it is not scheduled here.

### Alerts

`alert(type, detail)` increments a counter and writes one JSON log line. Lag and stall alerts fire once when their condition starts, then again only after it has cleared.

| Type | When |
|---|---|
| `negative_balance_block` | the next event would overdraw its sender; the drain stops |
| `multiple_events_at_height` | two waiting events share the next height; the drain stops |
| `live_row_conflict` | an id already waiting or applied arrives with different content |
| `retraction_rejected_final` | a RETRACT reaches final heights |
| `lag_warn` / `lag_critical` | `highestSeen - frontier >= LAG_WARN` (4) / `>= LAG_CRITICAL` (10) |
| `frontier_stalled` | the frontier has not moved for `STALL_SECONDS` (300) while events wait |

## Assumptions

- One event per height, and no empty heights.
- Corrected events carry new ids and arrive after their RETRACT has been acked.
- The feed has no replay interface, so a gap is surfaced for a human to fix rather than repaired.
- The first height the service applies is `GENESIS_FRONTIER + 1` (env var, default `0`).
- A message whose first delivery has started before the RETRACT is processed counts as sent before it, so the retraction sweeps it.

## The test

`test/concurrency.test.ts` has 7 small deterministic tests and one concurrency scenario.

**Deterministic tests:**
- out-of-order apply (4472 before 4471);
- a gap holds, and the endpoints report it;
- a negative balance stops the drain and alerts once;
- a retraction is rejected at `frontier - 12` but accepted at `frontier - 11`;
- the same-id, different-content rules;
- lag and stall alerts;
- the finalized/provisional split at the `frontier - 12` boundary.

**Production vs test.** The test drives the same `Ledger` code the API runs. It delivers messages by calling it directly instead of over HTTP, and replaces the queue with its own workers. The final results are read through the real HTTP routes:

```
PRODUCTION                                      TEST (npm test)
──────────                                      ───────────────
queue / client                                  test process (vitest)
     │ HTTP POST /events                         ├─ builds the feed from a seed
     ▼                                           ├─ 8 workers deliver shuffled, duplicated jobs
server.ts  (Fastify, routes only)                │      │ direct calls: ingest(), drain()
     │ ledger.ingest(), ledger.holders() ...     │      ▼
     ▼                                           ├─ Ledger instance 1 ─┐  each with its own pool,
ledger.ts  (all the logic)  ◄──── same code ───► ├─ Ledger instance 2 ─┤  like two servers
     │  one pool per instance                    │                     ▼
     ▼                                           │      Postgres (ledger_test database)
Postgres (ledger database)                       │
                                                 ├─ reader loop: invariant checks during the run
                                                 └─ final check: GET both endpoints through the
                                                    real routes (app.inject), compared with the oracle
```

- **Why skip HTTP for delivery.** `server.ts` has no shared state, locks or logic: every route is a single call into `ledger.ts`. All the concurrency risk is in database transactions, and the test exercises those for real.
- **The endpoints are still tested.** `server.ts` exports `buildApp()`, and the final check sends real `GET` requests through it with Fastify's `inject()`. That runs the full routing and JSON serialisation, without a network socket.
- **What the direct calls allow.** The test can hold a transaction open at its commit point, switch protections off, and run two instances in one process.
- **Why two instances in one process is valid.** To Postgres, two `Ledger` objects with separate pools are indistinguishable from two servers: separate sessions competing for the same locks and rows.

**The concurrency scenario** races a retraction against shuffled, duplicated delivery by 8 workers split across two service instances, each with its own pool. It is the most dangerous case because it combines every rule at once, and it fails silently: a resurrected or double-applied event yields a balance that looks plausible but is wrong. The feed is generated from a seed:
- 40 heights across 3 assets and 6 investors;
- a RETRACT inside the provisional window;
- a corrected sequence that ends in one overspend.

Delivery runs in three phases:
1. Every original is delivered 1 to 3 times, shuffled. About 40% get a second copy at the same moment on the other instance.
2. The RETRACT is delivered from both instances, concurrently with redeliveries and with drains running. Two first deliveries are held open at their commit point while it arrives.
3. The corrected sequence is delivered, plus stale redeliveries of retracted originals. The ledger is drained until nothing more applies. Last comes a RETRACT that reaches final heights, which must be rejected.

A reader loop checks invariants throughout, through the endpoint handlers:
- no negative holding;
- each asset's holdings total equals its issued supply;
- `reconcile()` finds no mismatches.

The final state must also satisfy:
- `finalized + provisional` equals the stored balance for every holder;
- it matches a single-threaded oracle: holdings, issued supply, frontier, and every event's status;
- `GET /assets/{assetId}/holders` and `GET /investors/{investorId}/positions` return exactly what the oracle expects for every asset and investor. That covers every field: the finalized/provisional split, `appliedThrough`, `gapAt` and `waitingCount`.

**Must-fail proofs.** The scenario then reruns with each protection switched off, for up to `ROUNDS` (25) seeds, and asserts that at least one round breaks:

| Switch | What it removes | How the run breaks |
|---|---|---|
| `uniqueEventId` | atomic insert-or-ignore (non-atomic check-then-insert, primary key dropped) | duplicate rows → `multiple_events_at_height` stall → the frontier stays low, so the closing RETRACT is no longer final and wipes real history |
| `frontierLock` | `FOR UPDATE` in the drain | two drains apply the same height: `reconcile: holding A1/I1: replay 901, stored 1802` |
| `tombstones` | ignoring a resent retracted event | a retracted original comes back: `oracle holding ... expected 393, got 229` |
| `ingestBarrier` | the shared advisory lock on ingest | an in-flight first delivery escapes the sweep: `oracle status o37: expected retracted, got waiting` |
| `negativeCheck` | the drain's balance check (constraint dropped) | `negative holding A2/I3: -15` |
| `finalityGuard` | rejecting RETRACTs that reach final heights | final history is rewound: `expected applied, got retracted` |

Each switch is one `if` at the exact spot it guards, marked `test-only switch`, and all default to on. The only other test hook is `hooks.beforeCommit`, a yield the test uses to add jitter and to hold an ingest open.

**Reading the output.**
- A passing run prints `all protections on: seeds 1,2,3,4,5 match the oracle, no invariant broken`.
- Each switch prints a line like `frontierLock off -> failed in round 1 (seed 1001), 81 problem(s), e.g. ...`, followed by the first mismatches as evidence.
- If the passing run fails, the output prints `FAIL seed N` and the problems.
- To replay particular seeds, run `SEEDS=7,8 npm test`. To change the number of must-fail rounds, set `ROUNDS`.
- Every switch currently fails in round 1. In a 10-seed sample per switch, every switch failed every seed. The passing scenario has been run clean over 40 seeds.

## Open decisions

- **Message format.** Not specified anywhere, so I chose `{eventId, type: TRANSFER|RETRACT, height, assetId, from|null, to, units, fromHeight}`.
- **`highest_seen`** is refreshed by the drain, both when it applies an event and when it stops, and by retraction. Ingest stays insert-only. If it were refreshed only on apply, it would never move at a gap and the lag alert could not fire.
- **A retraction that finds nothing in range** still succeeds and rewinds nothing.
- **Units** are integers (`bigint`) end to end.
- **A retracted id that comes back with new content** is overwritten in place, and the log line records the reuse. History is kept by not deleting rows, not by keeping every version.
- **Alert counters are incremented inside the transaction.** A rollback after an alert would still leave it counted.
- **`uniqueEventId` failure mode.** With this switch off, the drain's `multiple_events_at_height` check turns duplicate rows into a stall rather than a double-apply. The run still breaks, through the cascade described in the table above.

## Deliberately not built

- No real queue: `POST /events` stands in for the consumer.
- No automatic gap repair or replay request.
- No scheduled reconciliation.
- No snapshot fold of old events, and no per-asset frontiers.
- No epoch or chain-reorganisation handling beyond RETRACT.
- No auth, no UI, no metrics backend.
