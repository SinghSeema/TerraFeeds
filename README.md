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
  -d '{"eventId":"e1","height":1,"assetId":"FUND-A","from":null,"to":"alice","units":1000}'
# {"outcome":"accepted"}      possible outcomes: accepted | duplicate | replaced | conflict | applied | rejected

curl -XPOST localhost:3000/events -H 'content-type: application/json' \
  -d '{"eventId":"r1","type":"RETRACT","fromHeight":2}'
```

`from: null` is an issuance. Units are whole numbers and may be sent as a number or a string.

```
GET /assets/FUND-A/holders
{"assetId":"FUND-A","issuedSupply":"1000","appliedThrough":"4471","finalizedThrough":"4459","gapAt":"4472","waitingCount":1,
 "holders":[{"investorId":"alice","finalized":"0","provisional":"1000"}]}

GET /investors/alice/positions
{"investorId":"alice","appliedThrough":"4473","finalizedThrough":"4461","gapAt":null,"waitingCount":0,
 "positions":[{"assetId":"FUND-A","finalized":"0","provisional":"650"}]}
```

- `appliedThrough` is the frontier: the highest height applied with no gap below it.
- `finalizedThrough` is the finality line: applied events at or below it are in `finalized`, the rest in `provisional`. It never goes down.
- `gapAt` is the height the ledger is stuck on, if any.
- `waitingCount` is the number of events received but not yet applied. They appear in neither figure.
- Units are returned as strings so no client parses them into a lossy float.

`GET /metrics` returns JSON counters: acks by outcome, alerts by type, drains, events applied, retractions applied and rejected, `frontier`, `finalizedThrough`, `highestSeen`, and `oldestWaitingAgeSeconds`. `GET /health` returns `{"ok":true}`.

To replay the gap demo from the plan, start from an empty database, because the starting frontier is set only when the database is first created:

```
docker compose down -v
GENESIS_FRONTIER=4470 docker compose up -d --build
```

Then post 4471, then 4473 (`gapAt` is 4472), then 4472.

## Design

- **The event log is the source of truth.** `events` is append-only and never deleted from. Each event's status is `waiting`, `applied`, `retracted` or `rejected`. `holdings` is derived from it and has a `CHECK (units >= 0)` constraint.
- **Events apply strictly in height order**, one event per transaction, each at `frontier + 1`, under the frontier lock (`SELECT ... FOR UPDATE` on the single `ledger_state` row). An overspend or two events at one height stops the drain and raises an alert at once. A gap stops it too: it shows as `gapAt` in the reads and raises the lag and stall alerts. Nothing is ever skipped automatically.
- **The database decides duplicates.** Ingest is `INSERT ... ON CONFLICT (event_id) DO NOTHING`. When a message arrives with an id already stored:

  | Stored row | Same content | Different content |
  |---|---|---|
  | `retracted` (a tombstone) | ignored | replaced as a new `waiting` event |
  | `waiting` or `applied` | ignored | left unchanged, `live_row_conflict` alert |
- **Retraction** takes the exclusive advisory lock, then the frontier lock. It undoes applied events newest-first, so no holder goes negative even briefly, and tombstones every event it sweeps. It then rewinds the frontier to `fromHeight - 1`, or leaves it where it is if it is already lower. A retraction that reaches final heights (`fromHeight <= finalized_through`) is rejected with an alert.
- **The ingest barrier.** Transfer ingest holds the same advisory lock in shared mode. A retraction therefore waits for every ingest already in flight, and its sweep sees it. Ingests that start later wait until the rewind has committed.
- **Lock order is always the advisory lock first, then the `ledger_state` row.** The drain takes only the row lock, so the two can never wait on each other in a cycle.
- **Finality never goes backwards.** `ledger_state.finalized_through` is the highest `frontier - 12` the ledger has ever reached; every frontier write raises it with `GREATEST`, nothing lowers it. An applied event is finalized when `height <= finalized_through`. The line is stored, not computed from the current frontier, because a retraction rewinds the frontier: a line computed from it would drop too. A client would then see a `finalized` figure go down, and a second RETRACT could reach heights already reported final. The retraction guard and the reads use the same stored line. The guard keeps `frontier >= finalized_through`, so every height up to the line is applied.
- **Finalized and provisional amounts are computed at read time.** `provisional` is the net change from applied events above the line, and `finalized` is the balance minus provisional. Both reads run in one `REPEATABLE READ, READ ONLY` transaction, so the header and the figures come from one snapshot.
- **The drain runs after every ingest and on a 1s timer on every instance.** Two drains racing is safe: the second blocks on the frontier lock, then re-reads the frontier.
- **`reconcile()`** replays all applied events in memory and compares the result with `holdings` and `issued_supply`. The test calls it. In production it would run hourly and after every retraction; it is not scheduled here.

### Alerts

Every log line is one JSON object: `{time, level, instance, msg, ...}`. `instance` is `INSTANCE_NAME` (default: the host name, `api-1` in Compose), so logs from several servers can be interleaved and still read. The service logs `ack` (after commit, with the outcome), `applied` (after commit), and a retraction's `waiting for in-flight ingests`, `holds the barrier` (with `waitedMs`) and `rewinding` steps. `alert(type, detail)` increments a counter and writes one line with `level: "alert"`. Lag and stall alerts fire once when their condition starts, then again only after it has cleared.

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

`test/concurrency.test.ts` has 8 small deterministic tests and one concurrency scenario.

**Deterministic tests:**
- out-of-order apply (4472 before 4471);
- a gap holds, and the endpoints report it;
- a negative balance stops the drain and alerts once;
- a retraction is rejected at `frontier - 12` but accepted at `frontier - 11`;
- after that rewind, no `finalized` figure goes down, and a second RETRACT reaching a height that was final before the rewind is rejected;
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
- `finalizedThrough` is never above the frontier (also checked mid-run). It equals the highest frontier reached minus 12. How far the drains got before the RETRACT took the frontier lock depends on timing, so the test checks it lies in its possible range (`final frontier - 12` to `max(40, final frontier) - 12`). Every endpoint figure must then be exact for that line;
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
- Each passing seed prints a summary, per instance, of what happened. The counts vary from run to run with timing; `ok` is what matters:
  ```
  seed 1: ok, frontier 34, finalized through 23
      node-1: acks {"accepted":24,"duplicate":49,"applied":1}, applied 17, retractions 1 applied / 0 rejected, alerts {"negative_balance_block":1}
      node-2: acks {"accepted":22,"duplicate":36,"rejected":1}, applied 23, retractions 0 applied / 1 rejected, alerts {"retraction_rejected_final":1}
  ```
  Then `all protections on: seeds 1,2,3,4,5 match the oracle, no invariant broken`.
- Each switch prints a line like `frontierLock off -> failed in round 1 (seed 1001), 81 problem(s), e.g. ...`, followed by the first mismatches as evidence and the same per-instance summary.
- If the passing run fails, the output prints `FAIL seed N` and the problems.
- To replay particular seeds, run `SEEDS=7,8 npm test`. To change the number of must-fail rounds, set `ROUNDS`.
- To watch one run as it happens, set `LOG=1`. The service log lines are printed as `time instance message key=value`, between the scenario's phase notes. One seed is about 200 lines:
  ```
  LOG=1 SEEDS=1 npx vitest run -t "all protections"        # everything working
  LOG=1 npx vitest run -t "ingestBarrier is switched off"   # one protection off, until it breaks
  ```
  ```
  ---- phase 2: RETRACT from 30 sent to both instances while redeliveries and drains run
  14:43:54.315 node-1 retraction waiting for in-flight ingests eventId=r-main fromHeight=30
  14:43:54.316 node-2 retraction waiting for in-flight ingests eventId=r-main fromHeight=30
  ---- releasing the held ingests
  14:43:54.366 node-1 ack eventId=o33 height=33 outcome=accepted
  14:43:54.367 node-1 ack eventId=o35 height=35 outcome=accepted
  14:43:54.367 node-1 retraction holds the barrier eventId=r-main waitedMs=52
  ...
  14:43:54.413 node-1 retraction rewinding eventId=r-main fromHeight=30 undone=7 swept=11 frontier=29
  14:43:54.417 node-1 ack eventId=r-main fromHeight=30 outcome=applied
  14:43:54.421 node-2 retraction holds the barrier eventId=r-main waitedMs=105
  14:43:54.424 node-2 ack eventId=r-main fromHeight=30 outcome=duplicate
  ```
- Every switch currently fails in round 1. In a 10-seed sample per switch, every switch failed every seed. The passing scenario has been run clean over 40 seeds.

**Test console (optional).** A browser page for testing by hand and for seeing what the tests check. It is not part of the service and is not needed to run `npm test`. With the stack up, run `npm run demo` and open http://localhost:4000.
- **A · Live API** sends prepared events to `:3000` in ten steps: an issuance, a gap and its fill, a duplicate, a same-id conflict, an overspend, a retraction, a resent tombstone, finality moving forward, and a retraction into final heights. Each step shows the outcome it expects and the one it got. Holders, positions and metrics refresh after every step.
- **B · Test results** runs the passing scenario for a chosen seed, or a must-fail proof with one protection off. It then reads what the run left in `ledger_test` through a second API container on `:3001`. After a full `npm test`, `ledger_test` holds the last must-fail run, which is broken on purpose; run the passing scenario first.
- Every request is logged with its body, its response and the equivalent `curl`. **Reset databases** runs `docker compose down -v` and starts the stack again.
- `demo/server.mjs` serves the page and forwards its requests to `:3000` and `:3001`, so the service needs no CORS settings. It runs only a fixed list of commands (`npx vitest`, `docker compose`), listens on 127.0.0.1 only, and has no dependencies.

## Open decisions

- **Message format.** Exactly the feed's two shapes. A transfer has no `type` field: `{eventId, height, assetId, from, to, units}`. A retraction is `{eventId, type: "RETRACT", fromHeight}`. Any other `type` is rejected with 400.
- **`highest_seen`** is refreshed by the drain, both when it applies an event and when it stops, and by retraction. Ingest stays insert-only. If it were refreshed only on apply, it would never move at a gap and the lag alert could not fire.
- **A retraction that finds nothing in range** still succeeds and rewinds nothing.
- **The finality line is stored.** The original rule was "finalized is computed at read time, never stored". The amounts still are, but the line (`finalized_through`) is stored, because a line computed as `frontier - 12` moves down when a retraction rewinds the frontier. It starts at 0 and only rises.
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
- No auth, no UI, no metrics backend. (`demo/` is a testing aid, described under "The test".)

## More detail

- [doc/FLOW.md](doc/FLOW.md): how a test round flows, from delivery through the two instances to the endpoints.
- [doc/PROTECTIONS.md](doc/PROTECTIONS.md): a walkthrough of the six protections and what breaks without each one.
