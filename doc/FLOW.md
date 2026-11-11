# How the project flows

This document traces the main paths through the code: one concurrency test round, how messages are delivered, what a `Ledger` instance does with them, and where the endpoints are called. It uses the real function names.

- **Production:** `src/server.ts` receives HTTP requests and calls into `src/ledger.ts`.
- **Test:** `test/concurrency.test.ts` plays the queue itself, with its own workers, and calls `src/ledger.ts` directly. Then it checks the result.

Everything runs in one Node process, but the work reaches Postgres through many separate connections, and that is where the real parallelism, locking and racing happen.

- Each test instance has its own pool with a limit of 20, so 40 at most. A third, smaller pool is used only for resetting the database and the final checks.
- Measured during a run: about 23 connections open at peak, with up to 13 running queries at the same instant. Postgres allows 100.
- In production each server has one pool with the `pg` default limit of 10.

## 1. The big picture: one test round

```
npx vitest run -t concurrency
│
└─ it('matches the oracle ...')  /  it.each('breaks when X is switched off')
   │
   └─ runScenario(seed, protections, pools) ── test/concurrency.test.ts
      │
      ├─ 1. resetDb()                       fresh tables in ledger_test
      ├─ 2. makeFeed(seed)                  build the input (nothing sent yet)
      │        originals (heights 1-40), fromHeight, corrected sequence + overspend,
      │        slow (2 held-back originals), retraction
      ├─ 3. inst = [new Ledger(pool1), new Ledger(pool2)]     the two "servers"
      ├─ 4. start background loops (run the whole round):
      │        drain loop on inst[0]  ─┐  ledger.drain() every 0-3 ms
      │        drain loop on inst[1]  ─┘  (like the 1s timer in production)
      │        reader loop ─────────────  invariants() → ledger.holders(), positions(), reconcile()
      │
      ├─ 5. PHASE 1  runJobs(deliveries(originals minus slow))
      ├─ 6. PHASE 2  slow ingests frozen ─► RETRACT ×2-3 + 12 redeliveries ─► release
      ├─ 7. PHASE 3  runJobs(deliveries(corrected) + stale originals)
      │              quiesce()  →  send RETRACT_FINAL to both  →  quiesce()
      ├─ 8. stop background loops
      │
      └─ 9. FINAL CHECKS
               invariants()                         ── direct calls
               oracle(f)  vs  database tables       ── holdings, supply, statuses, frontier
               buildApp(inst[0]) + app.inject(GET)  ── real HTTP routes vs oracle.endpoints
               → list of problems (empty = pass)
```

The **oracle** is a simple single-threaded function in the test. It applies the events that should survive, one at a time in height order, to work out the correct final state. If the ledger's result differs from it in any way, the round fails. Production has no oracle; the closest thing is `reconcile()`, which checks the ledger only against its own records.

## 2. Delivery: queue → workers → instances

```
deliveries(msgs)                         runJobs(jobs)
builds and shuffles the job list         starts 8 workers sharing one counter "next"

 jobs[] (the simulated queue)
 ┌───────────────────────────────┐
 │ [inst[1], o17]                │       W1 ┐
 │ [inst[0], o3] + [inst[1], o3] │◄──────W2 │  while (next < jobs.length)
 │ [inst[0], o29]                │       ...│     job = jobs[next++]
 │ [inst[1], o17]  (a duplicate) │       W8 ┘     for each [instance, message]: send()
 └───────────────────────────────┘
                                          │
                     send(instance, message)
                          │
                          ├─► instance.ingest(message)   save it (commit = ack)
                          └─► instance.drain()           apply whatever is ready
```

- **Messages** are the items in the queue; **workers** are the consumers that take them out.
- Each job's instance is chosen at random when the job list is built. Workers don't belong to an instance.
- About 40% of jobs send the same message to **both** instances at the same moment. This mimics a queue redelivering one message to two consumers.
- There is no real queue system. The queue is a shuffled array, and the workers are async functions created by `runJobs()` and awaited with `Promise.all`.

| Real-world behaviour | How the test simulates it |
|---|---|
| Messages arrive out of order | the job list is shuffled |
| Each message delivered at least once, sometimes more | each message gets 1–3 copies |
| Two consumers get the same message at once | 40% of jobs go to both instances |
| A late redelivery of an old message | phase 3 resends retracted originals |
| A slow message still being processed | two ingests are frozen before commit while the RETRACT arrives |

## 3. Inside one instance: `Ledger` in `src/ledger.ts`

```
ingest(message)
 ├─ parseMessage()  →  contentHash()
 │
 ├─ transfer (no type) ── tx ─────────────────────────────────────────┐
 │    1. pg_advisory_xact_lock_shared        (ingest barrier, shared)  │
 │    2. insertOnce()                        INSERT ... ON CONFLICT    │
 │         new?  → 'accepted' (status waiting)                         │
 │         seen? → resolveExisting()                                   │
 │                   same content     → 'duplicate'                    │
 │                   retracted + new  → 'replaced'                     │
 │                   live + different → alert live_row_conflict        │
 │    3. hooks.beforeCommit()      (test only: jitter / freeze)        │
 │    COMMIT  ◄── the ack ───────────────────────────────────────────┘
 │
 └─ type RETRACT ── retract() ── tx ───────────────────────────────────┐
      1. pg_advisory_xact_lock          (exclusive: waits for ingests) │
      2. insertOnce()                   duplicate RETRACT → stop       │
      3. readState(FOR UPDATE)          frontier lock                  │
      4. fromHeight <= frontier-12 ?  → 'rejected' + alert             │
      5. select events >= fromHeight, newest first                     │
         adjust() to undo each applied one; mark all 'retracted'       │
      6. saveState(frontier = fromHeight - 1)                          │
      COMMIT ──────────────────────────────────────────────────────────┘

drain()
 └─ loop: applyNext()  ── one tx per event ────────────────────────────┐
      1. readState(FOR UPDATE)          frontier lock                  │
      2. find a waiting event at frontier + 1                          │
           none      → saveState(), stop          (gap or done)        │
           two       → block() + alert multiple_events_at_height       │
           overspend → block() + alert negative_balance_block          │
      3. adjust(sender, -units) and adjust(receiver, +units)           │
      4. mark 'applied', saveState(frontier + 1)                       │
      COMMIT, then repeat ─────────────────────────────────────────────┘

holders(assetId)  /  positions(investorId)   ── REPEATABLE READ snapshot
 ├─ readHeader()      appliedThrough, gapAt, waitingCount
 └─ positionRows()    finalized / provisional per holding

reconcile()     replays applied events, compares with holdings and supply
monitor()       lag and stall alerts      (production timer only)
metrics()       counters                  (production only)
```

Lock order is always the advisory lock first, then the `ledger_state` row. `drain()` takes only the row lock, so a drain and a retraction can never wait on each other in a cycle.

## 4. Where the endpoints get hit

```
PRODUCTION (docker compose)                 TEST (npm test)
───────────────────────────                 ───────────────
client
 │
 ├─ POST /events ──────► ledger.ingest()     not used: workers call ingest() directly
 │                       then ledger.drain()
 │
 ├─ GET /assets/:id/holders ──┐              1) mid-run: reader loop → invariants()
 │                            ├► ledger.holders()      calls ledger.holders() / positions()
 ├─ GET /investors/:id/positions ┘  ledger.positions()  directly
 │                                          2) final: buildApp(inst[0]) + app.inject()
 ├─ GET /metrics ──────► ledger.metrics()      → real GET routes → ledger.holders() /
 └─ GET /health                                  positions() → compared with the oracle

timer every 1s ──► ledger.drain() → ledger.monitor()   ← background drain loops play this role
```

- **Delivery skips HTTP** because `server.ts` holds no state or logic: every route is one call into `ledger.ts`. All the concurrency risk is in database transactions, and the test exercises those for real.
- **The read endpoints are still tested over their real routes.** `buildApp()` builds the same Fastify app the server runs, and `app.inject()` sends a `GET` through its routing and JSON output without a network socket.
