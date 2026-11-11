# The six protections, one by one

A plain-language walkthrough of each protection in `ALL_PROTECTIONS` ([src/ledger.ts](../src/ledger.ts)):
why it exists, which failure it prevents, and how the code prevents it.
Questions asked along the way are recorded under each protection.

---

## 0. What a "protection" is here

In production every protection is always on. `ALL_PROTECTIONS` exists only so the test can switch
**one** of them off and show that something really breaks. That is the proof the protection is needed,
not decoration.

```
 production:  new Ledger(pool)                         -> all six = true
 test:        new Ledger(pool, { frontierLock: false }) -> that one off, other five on
```

Each switch is a single `if` placed at the exact line it guards, marked `// test-only switch`.

The feed we consume is hostile in four ways, and each protection answers one or more of them:

```
 feed property             what can go wrong                         protection(s)
 ------------------------  ----------------------------------------  ------------------------------
 at-least-once delivery    same event arrives 2+ times, even at      uniqueEventId, tombstones
                           the same instant on two servers
 out of order              height 7 arrives before height 6          frontierLock (apply in order)
 retractable (RETRACT)     "heights >= N never happened"             ingestBarrier, tombstones,
                                                                     finalityGuard
 bad data                  a sender spends units they don't have     negativeCheck
```

And we run **two instances** of the service against one database, so every protection must hold
across processes, not only inside one Node process. That is why they all live in Postgres
(constraints, row locks, advisory locks), never in a JS variable.

---

## 1. `uniqueEventId` — the same event is stored only once

### Why we need it

The feed is **at-least-once**: if the sender does not get our ack in time, it sends the message again.
So the same `eventId` can arrive twice, and in the worst case at the *same moment* on *both* instances.

If both copies get stored, the drain later finds two `waiting` rows for one event and either applies
the transfer twice (units created out of nothing) or gets stuck.

### The scenario it solves

```
 time   instance A                               instance B
 ----   ---------------------------------------  ---------------------------------------
 t1     receives e7 (A->B, 30 units)             receives e7 again (redelivery)
 t2     SELECT ... WHERE event_id='e7'  -> none  SELECT ... WHERE event_id='e7'  -> none
 t3     INSERT e7                                INSERT e7
 t4     COMMIT                                   COMMIT
        => two rows for e7.  Duplicate stored.
```

This is the classic **check-then-insert race**: both look, both see "absent", both insert. Checking
first in application code can never be made safe, because between the check and the insert the other
instance can do the same thing.

### How the code solves it

Let the database decide, in one atomic statement, on a unique key.

1. Schema ([src/schema.sql](../src/schema.sql)): `event_id text PRIMARY KEY` — Postgres guarantees at
   most one row per id.
2. `insertOnce()` uses insert-or-ignore:

   ```sql
   INSERT INTO events (...) VALUES (...) ON CONFLICT (event_id) DO NOTHING
   ```

   `rowCount === 1` means "I stored it, it is new"; `rowCount === 0` means "someone already has it".

What happens in the race now:

```
 time   instance A                               instance B
 ----   ---------------------------------------  ---------------------------------------
 t1     INSERT e7 ... ON CONFLICT DO NOTHING     INSERT e7 ... ON CONFLICT DO NOTHING
        -> takes the unique-key slot             -> sees A's uncommitted row on the key,
                                                    WAITS for A to finish
 t2     COMMIT  (rowCount = 1, 'accepted')
 t3                                              wakes up, conflict -> does nothing
                                                 (rowCount = 0)
                                                 -> resolveExisting(): same content_hash
                                                    -> 'duplicate', ack, nothing changed
```

The key point: B does not "check then act". Its single statement **blocks on A's row** until A
commits, then sees the conflict. There is no gap between check and insert for anyone to slip into.

Then `resolveExisting()` decides what the second copy means, by comparing `content_hash`:

```
 same id, same content hash             -> 'duplicate'  (normal redelivery, ignore)
 same id, different content, row was
   retracted, new msg is a transfer     -> 'replaced'   (corrected re-send after a RETRACT)
 same id, different content otherwise   -> 'conflict' + alert live_row_conflict (a human looks)
```

And we **ack only after commit** (`ingest()` resolves after `tx()` commits). If we crash before
commit, nothing was stored and the sender's redelivery is simply a first delivery.

### The test-only switch

```ts
if (!this.protections.uniqueEventId) { // test-only switch: check-then-insert
  const { rowCount } = await c.query('SELECT 1 FROM events WHERE event_id = $1', [e.eventId]);
  if (rowCount) return false;
  await c.query(insert, values);
  return true;
}
```

With the switch off, the code does exactly the naive check-then-insert shown in the first diagram.
The test also drops the `events_pkey` constraint (`resetDb()` in
[test/concurrency.test.ts](../test/concurrency.test.ts)) so there is no database backstop hiding the bug.

### How the test proves it

`runScenario()` sends roughly 40% of events as two copies at the same moment, one to each instance
(`deliveries()`), plus random later redeliveries. With the protection off, some event ends up with two
rows. The final check groups rows by `event_id` and reports e.g.
`oracle status e12: expected applied, got applied+waiting (duplicate rows)`, and holdings usually
disagree with the oracle as well. So the "breaks when uniqueEventId is switched off" test passes
only because the scenario really fails.

### One-line summary

> Duplicates are decided by a unique key inside one atomic `INSERT ... ON CONFLICT DO NOTHING`, never by
> a "look first" query, so two copies racing on two servers still produce one row.

### Questions and answers

**Q: Does making `event_id` the primary key and using `insertOnce()` solve every kind of duplication?**

No. It solves exactly one thing: **the same `event_id` is never stored twice**, even under a race.
Other kinds of "duplicate" need other pieces:

```
 kind of duplicate                                  handled by
 -------------------------------------------------  ------------------------------------------------
 1. same id, same content, sent again               PRIMARY KEY + ON CONFLICT DO NOTHING   (this one)
 2. same id arrives AFTER it was retracted          tombstones (#3): the row must still exist, or the
                                                    primary key has nothing to conflict with
 3. same id, different content                      resolveExisting(): content_hash differs ->
                                                    'replaced' (if retracted) or 'conflict' + alert
 4. stored once, but APPLIED twice                  frontierLock (#2): storing once != applying once
 5. same RETRACT delivered twice                    retract() also calls insertOnce(); the second copy
                                                    is a 'duplicate' and does not rewind again
 6. same real transfer under two DIFFERENT ids      not detectable by id. If both claim the same
                                                    height, applyNext() blocks with
                                                    multiple_events_at_height and alerts. If they
                                                    claim different heights, they are two events by
                                                    the feed's own contract, and we trust it.
```

Two details make case 1 airtight end to end:

- **Ack only after commit.** If we crash after insert but before commit, the row is rolled back and the
  redelivery is a first delivery. If we crash after commit but before the ack, the redelivery hits
  the primary key and becomes a `duplicate`. Either way: stored exactly once.
- **Never `DELETE` from `events`.** A unique key only remembers ids whose rows still exist. Deleting
  (for a retraction, or for cleanup or retention) would make the database "forget" the id, and a late
  redelivery would be inserted as new. That is case 2 and is what protection #3 proves.

---

## 2. `frontierLock` — each height is applied exactly once, in order

### Background: what the "frontier" is

`ledger_state` is a table with exactly one row. Its `frontier` column means
"every height up to here has been applied to `holdings`". The next event to apply is always
`frontier + 1`, never anything else.

```
 heights:   1   2   3   4   5   6   7   8   9
 status:    A   A   A   A   A   A   W   .   W        A = applied, W = waiting, . = not arrived
                                ^
                            frontier = 6     next = 7

 drain applies 7 -> frontier = 7 -> looks for 8 -> not arrived -> STOP (gap), 9 keeps waiting
```

This is how **out-of-order** arrival is handled: `ingest()` only stores an event as `waiting`.
`drain()` → `applyNext()` is the only code that moves units, and it only ever takes height
`frontier + 1`. Height 9 arriving early just waits until 8 arrives.

### Why we need a lock

`drain()` runs in many places at once: after every ingest, and on a timer, on **both instances**.
`applyNext()` reads the frontier, finds the event at `frontier + 1`, moves units, then writes
`frontier + 1`. That is read-modify-write. Without a lock, two drains can read the same frontier.

### The scenario it solves (double apply)

```
 time   drain on instance A                      drain on instance B
 ----   ---------------------------------------  ---------------------------------------
 t1     read frontier = 6                        read frontier = 6
 t2     SELECT waiting event at height 7 -> e7   SELECT waiting event at height 7 -> e7
 t3     holdings: Alice -30, Bob +30             holdings: Alice -30, Bob +30  (waits on A's
                                                   holdings row, then adds on top of it)
 t4     e7 -> applied, frontier = 7, COMMIT      e7 -> applied, frontier = 7, COMMIT

 result: e7 moved 60 units instead of 30. Bob holds units that were never transferred.
```

Postgres row locks on `holdings` do not save us: B's `UPDATE holdings` waits for A, then simply
applies its delta on top of A's committed value. Both transactions are individually "valid"; the bug is
that both decided to apply e7 based on a stale read of the frontier.

The balance check in `negativeCheck` (protection 5) has the same weakness: both drains can read
Alice's balance before either subtracts, so both pass the check.

### How the code solves it

`applyNext()` starts with `readState(c, true)`, which runs:

```sql
SELECT frontier, blocked_at FROM ledger_state WHERE id = 1 FOR UPDATE
```

`FOR UPDATE` locks that single row until the transaction commits. Since every writer of `holdings`
and `frontier` takes this lock first, they form a single queue across all instances:

```
 time   drain on instance A                      drain on instance B
 ----   ---------------------------------------  ---------------------------------------
 t1     SELECT ... FOR UPDATE -> frontier = 6    SELECT ... FOR UPDATE -> BLOCKED (A holds it)
 t2     apply e7, frontier = 7, COMMIT  (lock released)
 t3                                              unblocked, reads the NEW row: frontier = 7
                                                 looks for height 8 -> applies it, or stops
```

Why B sees `7`, not `6`: under `READ COMMITTED` (Postgres's default), a `SELECT ... FOR UPDATE` that
waited on a lock returns the latest committed version of the row, and every later statement in B's
transaction takes a fresh snapshot, so it also sees A's applied event and A's holdings.

Supporting choices around the lock:

- **One event per transaction** (`drain()` loops `applyNext()`). The lock is held only for one event,
  so ingests and reads are never stuck behind a long batch, and a failure loses only that one event.
- **Never skip.** If the next height is missing, it stops and waits (a gap). If it is bad
  (`negative_balance_block`, `multiple_events_at_height`), `block()` records `blocked_at`, alerts once,
  and stops. It never jumps to `frontier + 2`.
- **Same lock for retraction.** `retract()` also takes this row lock (after the advisory lock, see
  protection 4), so no drain can apply while holdings are being rewound.

### The test-only switch

```ts
const state = await this.readState(c, this.protections.frontierLock); // test-only switch
```

With the switch off, `readState` runs the plain `SELECT` without `FOR UPDATE`, so the race in the
first diagram is possible.

### How the test proves it

`runScenario()` runs a background `drain()` loop on **both** instances, and every `send()` triggers
one more drain after its ingest. The `beforeCommit` hook adds random 0–2 ms pauses just before commit,
which widens the window between "read frontier" and "commit". With the lock off, two drains apply the
same height. The single-threaded oracle disagrees on holdings and issued supply, the per-asset total stops
equalling supply, or `reconcile()` reports mismatches, so the "breaks when frontierLock is switched off"
test passes.

### One-line summary

> Every writer of holdings and frontier first locks the one `ledger_state` row with `FOR UPDATE`, which
> turns concurrent drains on any number of servers into a single queue that applies `frontier + 1`
> exactly once.

### Questions and answers

**Q: Is the frontier lock an exclusive lock or a shared lock?**

Exclusive. `SELECT ... FOR UPDATE` takes Postgres's strongest **row-level** lock on the one
`ledger_state` row. A second `FOR UPDATE` (or an `UPDATE`) on that row waits until the holder commits.
Plain `SELECT`s are **not** blocked: thanks to MVCC, readers see the last committed version, which is
why the read endpoints never wait on a drain.

There are two different kinds of lock in this codebase. Do not mix them up:

```
 lock                               kind                         taken by                mode
 ---------------------------------  ---------------------------  ----------------------  ---------
 frontier lock                      row lock on ledger_state     applyNext() (drain)     exclusive
   SELECT ... FOR UPDATE            (id = 1)                     retract()               exclusive
 ingest barrier (INGEST_BARRIER)    advisory lock: a lock on a   ingest() of a transfer  SHARED
   pg_advisory_xact_lock[_shared]   number (4202), not a row     retract()               EXCLUSIVE
```

Ingest never touches the frontier lock. Ingest only stores a `waiting` row; it never changes
`holdings` or `frontier`, so it has nothing to queue on there. This is why a slow drain never slows
down receiving messages.

**Q: Why does ingest use a shared lock? Is it because `uniqueEventId` already handles duplication?**

Partly, yes. A shared lock means "many holders at once": ingests do not block **each other**. That is
safe because two ingests only conflict when they carry the same `event_id`, and that case is already
serialized by the primary key (protection #1). Different ids insert different rows and do not
interfere. So there is no reason to make ingests wait for each other, and doing so would cap
throughput at one message at a time.

```
 shared + shared        -> both proceed       (ingest e7 and ingest e9 at the same time: fine)
 shared + exclusive     -> one waits          (ingest vs retraction)
 exclusive + exclusive  -> one waits          (retraction vs retraction)
```

**Q: Then why do we need a lock during ingest at all?**

Not for ingest vs ingest, but for **ingest vs retraction**. The shared lock exists only so a
retraction (which takes the same number **exclusively**) can shut the door. Without it:

```
 time   ingest of e22 (height 22)                retraction "from height 20"
 ----   ---------------------------------------  ---------------------------------------
 t1     INSERT e22 status 'waiting'
        (not committed yet)
 t2                                              sweep: SELECT events with height >= 20
                                                 -> e22 is NOT visible (uncommitted)
 t3                                              tombstones e20, e21; frontier = 19; COMMIT
 t4     COMMIT
 t5     e22 sits as 'waiting' and a later drain applies it, although the feed said
        "everything from height 20 never happened".
```

With the barrier, the retraction's exclusive lock waits until every in-flight ingest (each holding
the shared lock) commits, so its sweep sees them. Ingests that start later wait until the rewind has
committed, so they are stored after it. Every event is either swept or arrives afterwards; none
slips through the middle. Protection #4 (`ingestBarrier`) covers this in full.

**Q: Is the "barrier" a shared lock, or something else?**

"Barrier" is just our name for what the lock is used for. It is not a separate Postgres feature.
Underneath it is **one advisory lock**, on the number `INGEST_BARRIER = 4202`, and that one lock can be
taken in two modes:

```
 who          call                                  mode
 -----------  ------------------------------------  ----------
 ingest()     pg_advisory_xact_lock_shared(4202)    shared
 retract()    pg_advisory_xact_lock(4202)           exclusive
```

So the barrier is neither "a shared lock" nor "an exclusive lock". It is one lock that ingests take
shared and retractions take exclusive. This is the classic **reader-writer lock** pattern: many
"readers" (ingests) together, or one "writer" (a retraction) alone, never both.

```
 4202 held by:  ingest, ingest, ingest   (shared, all run together)
                    | retraction arrives, asks for exclusive -> waits for all three to commit
                    v
                retraction alone         (exclusive)
                    | a new ingest arrives, asks for shared -> waits for the retraction to commit
                    v
                ingest, ingest ...       (shared again)
```

Why it is called a barrier: from the retraction's point of view it is a line drawn across the stream
of ingests. Everything that started before the line has committed before the sweep runs; everything
after the line is stored after the rewind commits.

What "advisory" means: Postgres attaches no meaning to the number 4202. It does not lock a table or a
row. It only guarantees that callers asking for the same number in conflicting modes wait for each
other. The meaning comes entirely from our code agreeing to use it. Both instances use the same
constant, so it works across servers. The `_xact_` variants release automatically at commit or
rollback, so a crash can never leave it held.

**Q: So which locks does each operation take?**

There are exactly **two locks** in the system. For ingest, "the shared lock" and "the barrier" are the
same thing (the barrier taken in shared mode), not two separate locks.

```
                      LOCK 1: barrier                 LOCK 2: frontier lock
                      advisory lock on 4202           row lock on ledger_state (FOR UPDATE)
                      ----------------------------    -------------------------------------
 ingest()  (transfer) shared                          -  (not taken)
 drain()   applyNext  -  (not taken)                  exclusive
 retract()            exclusive          --then-->    exclusive
```

- **ingest**: one lock, the barrier in shared mode.
- **drain**: one lock, the frontier lock.
- **retract**: both locks, always in this order: barrier (exclusive) first, then frontier lock.

What each lock keeps apart:

```
 barrier        keeps   ingest  <-->  retraction     apart   (a retraction never misses an in-flight event)
 frontier lock  keeps   drain   <-->  drain          apart   (no height applied twice)
                        drain   <-->  retraction     apart   (no apply during a rewind)
```

Ingest and drain share no lock, so receiving and applying never wait for each other.

Why the fixed order matters: if one transaction took lock 1 then lock 2, and another took lock 2 then
lock 1, each could hold one and wait for the other forever (deadlock). Only `retract()` takes both,
and always barrier first, so no such cycle can form.

**Q: What is the number 4202? Can it be any random number?**

Yes, it is arbitrary. Postgres gives it no meaning; it is just the name of the lock. Any 64-bit integer
works. Three rules matter more than the value:

1. **Every instance must use the same number.** That is why it is a constant in the code and not
   generated at startup. Two servers using different numbers would never block each other, and the
   barrier would silently do nothing.
2. **It must not collide with another advisory lock in the same database.** Advisory locks are
   per database, and the number space is shared by anything connected to it: other services,
   migration tools, job queues. If something else also used 4202 for a different purpose, the two would
   block each other for no reason. In this project there is exactly one other advisory lock:
   `MIGRATION_LOCK = 4201` in [src/db.ts](../src/db.ts), which stops two instances starting at once from
   running the schema at the same time. That is why the barrier uses a different number. Any number other
   than 4201 is fine here. In a shared database you would pick one less likely to clash, for example derived from a name
   with `hashtext('ledger.ingest_barrier')`, or use the two-key form `pg_advisory_lock(app_id, lock_id)`.
3. **Don't change it during a rolling deploy.** If old and new instances run at the same time with
   different numbers, a retraction on one would not wait for ingests on the other. Change it only with
   all instances stopped.

## 3. `tombstones` — a retracted event stays dead

### Background: what a retraction does

A `RETRACT { fromHeight: 20 }` message means "every event at height 20 and above never happened; a
corrected sequence will follow". `retract()` must then:

1. undo the holdings of every applied event at height >= 20 (newest first),
2. mark those events as no longer valid,
3. move the frontier back to 19, so the corrected events at 20, 21, ... can be applied.

A **tombstone** is how step 2 is done: the row stays in `events` with `status = 'retracted'`. It is
never deleted.

```
 before RETRACT from 20              after RETRACT from 20
 event  height  status               event  height  status
 o19    19      applied              o19    19      applied
 o20    20      applied       ->     o20    20      retracted   <- tombstone
 o21    21      applied              o21    21      retracted   <- tombstone
 o22    22      waiting              o22    22      retracted   <- tombstone
 frontier = 21                       frontier = 19
```

### Why we need it

The feed is at-least-once, so a copy of `o21` that was sent **before** the retraction can still arrive
**after** it, late, as a redelivery. That old copy describes a transfer that the feed has already said
never happened. We must recognise it and ignore it.

The only way to recognise it is to remember that `o21` exists and is dead. A deleted row is
forgotten: the primary key (protection #1) only blocks ids whose rows still exist.

### The scenario it solves (resurrection)

If retraction deleted rows instead of tombstoning them:

```
 time   event
 ----   ----------------------------------------------------------------------------------
 t1     o21 (height 21, Alice -> Bob 30) applied
 t2     RETRACT from 20: undo o21, DELETE o21, frontier = 19
 t3     corrected events arrive with new ids: c20, c21 (height 21, Alice -> Carol 5)
 t4     a late redelivery of the OLD o21 arrives
        INSERT o21 ... ON CONFLICT DO NOTHING -> no row with that id -> inserted as 'waiting'
 t5     height 21 now has TWO waiting events: c21 and the resurrected o21
          -> drain blocks with multiple_events_at_height, ledger stuck
        or, if o21 is stored before c21 arrives, it is applied:
          -> Bob receives 30 units from a transfer the feed retracted
```

### How the code solves it

1. **Retraction tombstones, never deletes.** In `retract()`:

   ```sql
   UPDATE events SET status = 'retracted' WHERE event_id = ANY($1)
   ```

   Nothing in the codebase runs `DELETE FROM events`. The schema comment says the table is append-only.

2. **A redelivery hits the tombstone.** The late `o21` runs `insertOnce()`, the primary key conflicts
   with the tombstone row, and `resolveExisting()` decides:

   ```
   row status = 'retracted', same content_hash      -> 'duplicate'  (stale copy, ignore, stays dead)
   row status = 'retracted', different content,
     and it is a transfer                           -> 'replaced'   (the feed re-uses the id for its
                                                                     correction: becomes 'waiting')
   ```

   So the stale copy is acked and changes nothing; the event stays `retracted` forever.

3. **The drain ignores tombstones.** `applyNext()` only selects `status = 'waiting'`, and the
   read queries and `reconcile()` only count `status = 'applied'`. A tombstone takes part in nothing
   except duplicate detection.

The extra benefit is an audit trail: you can always see what was retracted, when it was received,
and that its holdings were undone. That matters for a system that is the source of truth for
ownership.

### The test-only switch

```ts
if (row.status === 'retracted' && !this.protections.tombstones) { // test-only switch: as if the row had been deleted
  await c.query(`UPDATE events SET status = 'waiting' WHERE event_id = $1`, [e.eventId]);
  return 'accepted';
}
```

With the switch off, a same-content redelivery of a retracted event brings it back to `waiting`,
which is exactly what would happen if the row had been deleted and re-inserted.

### How the test proves it

In `makeFeed()`, the corrected sequence uses **new** ids (`c20`, `c21`, ...) at the same heights as
the retracted originals (`o20`, `o21`, ...). In phase 3, `runScenario()` sends the corrected sequence
mixed with **stale redeliveries** of every retracted original. With tombstones off, a stale `o21` is
resurrected next to `c21`: either the drain blocks at that height or the old transfer is applied. The
oracle expects every `o >= fromHeight` to be `retracted`, so the status and holdings comparison
fails.

The deterministic test `applies the same-id-different-content rules` also checks the tombstone
directly: after a RETRACT, resending the same transfer returns `'duplicate'` and the status stays
`'retracted'`.

### One-line summary

> Retraction marks events `retracted` instead of deleting them, so the primary key still remembers
> the id and a late copy of a retracted event is recognised as a duplicate instead of coming back to
> life.

### Questions and answers

_(none yet)_

## 4. `ingestBarrier` — a retraction never misses an event that was in flight

_(Covered last. The basics of the lock itself, shared vs exclusive and what "advisory" means, are in
section 2's Q&A.)_

### Why we need it

A retraction works by **sweeping**: it selects every `waiting` or `applied` event at
`height >= fromHeight` and tombstones it. A sweep can only see rows that are **committed**. An ingest
that has inserted its row but not committed yet is invisible to it.

At the same time, ingests run constantly and in parallel, on both instances. So without coordination
there is always a chance that an event is half-way through being stored at the moment the retraction
sweeps.

### The scenario it solves (an event slips past the sweep)

```
 time   instance node-1: ingest o37 (height 37)   instance node-2: RETRACT from 30
 ----   ---------------------------------------  ---------------------------------------
 t1     INSERT o37 status 'waiting'
        (not committed yet)
 t2                                              sweep: height >= 30, waiting/applied
                                                 -> o37 is NOT visible (uncommitted)
 t3                                              tombstone o30..o36, frontier = 29, COMMIT
 t4     COMMIT  -> ack
 t5     o37 sits 'waiting' at height 37. When the corrected sequence reaches 36, the drain
        applies o37, a transfer the feed has already declared void.
        (Or the corrected c37 arrives too: two events at 37 -> multiple_events_at_height, stuck.)
```

Note that `uniqueEventId` and `tombstones` cannot help here. `o37` was never in the table when the
sweep ran, so there is no row to tombstone and no conflict to detect. The problem is **timing**, not
duplication.

### How the code solves it

One advisory lock on the number `INGEST_BARRIER` (4202), taken in two modes. It is the reader-writer
lock pattern:

```ts
// ingestTransfer(): every transfer ingest, first statement in its transaction
await c.query('SELECT pg_advisory_xact_lock_shared($1)', [INGEST_BARRIER]);   // shared

// retract(): first statement in its transaction, before the frontier lock
await c.query('SELECT pg_advisory_xact_lock($1)', [INGEST_BARRIER]);          // exclusive
```

Both are `_xact_` locks, so they are held until that transaction commits or rolls back. This gives
two guarantees:

```
 ingests that started BEFORE the retraction        ingests that start AFTER it
 ------------------------------------------        ------------------------------------------
 hold the shared lock -> the retraction's          ask for the shared lock -> wait until the
 exclusive request waits until ALL of them         retraction COMMITS -> then insert, after the
 have committed -> its sweep sees every one        rewind. They are new events, not missed ones.
```

So every event is either **in the sweep** or **after the rewind**. Nothing can be in between. The
same scenario with the barrier:

```
 time   node-1: ingest o37                       node-2: RETRACT from 30
 ----   ---------------------------------------  ---------------------------------------
 t1     shared lock 4202, INSERT o37 (uncommitted)
 t2                                              exclusive lock 4202 -> WAITS for node-1
 t3     COMMIT (shared lock released)
 t4                                              gets the lock; sweep sees o37 -> tombstoned
                                                 frontier = 29, COMMIT
```

Design points worth defending:

- **Shared for ingests:** they do not slow each other down. Only a retraction, which is rare,
  makes anyone wait.
- **Lock order.** The retraction takes the barrier first, then the frontier lock. Drains take only the
  frontier lock and ingests only the barrier, so no cycle and no deadlock is possible.
- **Drains are not stopped by the barrier, on purpose.** Between "retraction holds the barrier" and
  "retraction takes the frontier lock", a drain can still apply a few events. That is harmless. They are
  committed before the retraction gets the frontier lock, so the sweep sees them as `applied` and undoes
  them. You can see this in the `LOG=1` output (`applied o33..o36`, then `rewinding ... undone=7`).
- **Why not lock the `events` table instead?** `LOCK TABLE events IN SHARE MODE` in the retraction would
  also block in-flight inserts. But it would block the drain's `UPDATE events SET status = 'applied'`
  too, and a drain holding the frontier lock while waiting for `events` would deadlock with a
  retraction holding `events` while waiting for the frontier lock. The advisory lock targets exactly
  the operation that needs it, ingest, and nothing else.
- **Reads are never blocked.** Neither lock affects plain `SELECT`s.

### What it does not cover (an assumption)

The barrier orders deliveries that have **started**. It cannot recognise an original that the feed
sent before the RETRACT but that is first delivered **after** the retraction has committed (for
example, stuck in a queue for a long time). To the ledger that looks like a new event. The README
states the assumption: "a message whose first delivery has started before the RETRACT is processed
counts as sent before it", and corrected events arrive after the RETRACT has been acked. If that
assumption broke, the result is visible, not silent: `multiple_events_at_height` stops the drain and
alerts.

### The test-only switch

```ts
if (this.protections.ingestBarrier) { // test-only switch
  await c.query('SELECT pg_advisory_xact_lock_shared($1)', [INGEST_BARRIER]);
}
```

With the switch off, ingests do not take the shared lock, so the retraction's exclusive lock has
nothing to wait for.

### How the test proves it

This race needs exact timing, so phase 2 of `runScenario()` **forces** it instead of hoping for it:

1. `makeFeed()` picks two originals at `height >= fromHeight` as `slow`.
2. Their first delivery runs inside `held.run(...)`. The `beforeCommit` hook sees that and **pauses
   the ingest just before COMMIT**, with the row inserted but uncommitted, until a gate opens.
3. The RETRACT is then sent from both instances.
4. The gate opens when the RETRACT has committed **or** 50 ms have passed, whichever is first.

```
 barrier ON:   RETRACT waits for the held ingests -> cannot commit first -> 50 ms timeout releases
               them -> they commit -> RETRACT gets the lock, sweeps them -> correct
               log: "retraction holds the barrier eventId=r-main waitedMs=52"

 barrier OFF:  RETRACT does not wait -> sweeps without them -> commits -> gate opens -> held
               ingests commit afterwards -> left 'waiting' at retracted heights
               result: "oracle status o37: expected retracted, got waiting"
```

The `waitedMs=52` in the log is the barrier visibly doing its job: the retraction waited for the
held ingests, then the 50 ms gate released them.

To watch it break: `LOG=1 npx vitest run -t "ingestBarrier is switched off"`.

### One-line summary

> Transfer ingests hold advisory lock 4202 shared and a retraction takes it exclusive, so the
> retraction waits for every in-flight ingest to commit before it sweeps, and holds new ones off until
> its rewind commits: every event is either swept or arrives after the rewind.

### Questions and answers

_(none yet)_

## 5. `negativeCheck` — nobody can spend units they do not have

_(Covered before #4 at the user's request.)_

### Why we need it

`holdings` answers "who owns what". A negative holding means someone owns **less than nothing**, which
is meaningless for ownership. It also breaks the supply invariant: if Alice goes to -20 and Bob gets
+20, the per-asset total still adds up, but 20 units now exist that were never issued.

The feed can contain a transfer that overspends. Sometimes it is a bad event; more often it is
temporary: the feed is about to retract and correct that range. Either way, we must not apply it.

### The scenario it solves

```
 holdings before:  Alice = 50
 next event:       c27 (height 27)  Alice -> Bob 80

 without the check:   Alice = -30, Bob = +80     (30 units created out of thin air)
 with the check:      balance 50 < 80 -> STOP, alert negative_balance_block, c27 stays 'waiting'
```

### How the code solves it

Two layers: a clean stop in code, and a hard wall in the database.

**Layer 1: the check in `applyNext()`**, run before any holding is touched:

```ts
if (e.from !== null && this.protections.negativeCheck) { // test-only switch
  const { rows: [h] } = await c.query(
    'SELECT units FROM holdings WHERE asset_id = $1 AND investor_id = $2', [e.assetId, e.from]);
  const balance = h ? BigInt(h.units) : 0n;
  if (balance < e.units!) {
    return this.block(c, state, 'negative_balance_block', { eventId: e.eventId, investor: e.from, balance, units: e.units });
  }
}
```

- `e.from !== null`: issuance (`from = null`) is the issuer creating units. It has no balance to
  check; `adjust()` grows `issued_supply` instead.
- `block()` does **not** skip the event. It records `blocked_at = frontier + 1` and
  `blocked_reason` in `ledger_state`, logs one alert, and returns. The frontier stays put, so every
  later height waits too. Repeated drains see the same `blocked_at`, so the alert is not repeated
  every tick.
- The units stay `bigint` throughout (`BigInt(h.units)`), so the comparison is exact for any size.

**Why the check is race-free:** it runs inside `applyNext()`, after the frontier lock (#2) is taken.
Every writer of `holdings` holds that lock, so Alice's balance cannot change between the `SELECT` and
the `UPDATE`. Without the frontier lock, two drains could both read 50 and both pass. That is why
the two protections are listed separately: each one has its own failure.

**Layer 2: the database `CHECK`** in [src/schema.sql](../src/schema.sql):

```sql
units bigint NOT NULL CONSTRAINT holdings_units_nonnegative CHECK (units >= 0)
```

If any code path ever tried to write a negative holding, Postgres rejects the statement and the whole
transaction rolls back. That includes a future bug or a mistake in the undo order of a retraction.

Why keep both, if the `CHECK` already refuses negatives?

```
                       code check (layer 1)               CHECK constraint (layer 2)
 what happens          clean stop, state saved            error thrown, transaction rolled back
 alert                 one negative_balance_block alert   just an exception in the logs
                       with investor, balance, units
 ledger_state          blocked_at / blocked_reason set,   unchanged; every drain retries and
                       visible in the read endpoints      fails again
 role                  the normal, expected path          last line of defence against bugs
```

A related detail in `adjust()`: decreases are a plain `UPDATE ... SET units = units + $3`, not
`INSERT ... ON CONFLICT DO UPDATE`. Postgres checks the `CHECK` on the **proposed new row** of an
`INSERT` before it looks for a conflict, so inserting `-30` would fail even when the existing row
holds 50.

### Why stop instead of skipping the bad event

Skipping would change every balance after it, and later events may depend on the skipped one. The
system cannot know the right answer, so it stops and lets the feed fix it (usually a RETRACT and a
correction) or a human decide. This is the "never skip a height or a failing event automatically"
design rule. Retraction also clears the block: the rewind saves `blocked_at = null`.

### The test-only switch

With the switch off, the `if` is skipped and the event is applied. The test also drops
`holdings_units_nonnegative` in `resetDb()`, otherwise layer 2 would still catch it and we would be
testing the constraint, not the switch.

### How the test proves it

`makeFeed()` ends the corrected sequence with **one deliberate overspend**: a transfer of
`balance + (1..100)` units from someone who holds `balance`. The oracle stops there, so that event
stays `waiting`. With the check off it is applied, a holding goes negative, and `invariants()`
reports `negative holding ...`, along with an oracle mismatch on holdings and statuses.

The deterministic test `stops the drain at a negative balance and alerts once` checks the stop and
that the alert fires only once across repeated drains.

### One-line summary

> Before moving units, `applyNext()` checks the sender's balance under the frontier lock. If it is
> short, the ledger stops at that height and alerts once, and a `CHECK (units >= 0)` constraint
> backs this up in the database.

### Questions and answers

_(none yet)_

## 6. `finalityGuard` — a retraction can never undo final history

### Background: finalized vs provisional

Every applied event is either **finalized** or **provisional**, depending on how far it is below the
frontier. `FINALITY_DEPTH = 12n`:

```
 an applied event at height h is   finalized   if  h <= frontier - 12
                                   provisional if  h >  frontier - 12

 frontier = 40
 heights:  1 ........................ 28 | 29 ............ 40
           <------- finalized -------->  | <-- provisional -->
                                         ^ frontier - 12 = 28
```

The **line** is stored as `ledger_state.finalized_through`: the highest `frontier - 12` ever reached
(see "The fix" at the end of this section for why it is stored). The **amounts** are computed at read
time: `positionRows()` sums the net change from applied events with `height > finalized_through` as
`provisional`, and `finalized = balance - provisional`. Both read endpoints return the two numbers
separately, plus `finalizedThrough` itself.

The meaning for a client is a promise:

- **provisional**: "this is the current state, but the feed may still retract it."
- **finalized**: "this will not change." A client can settle on it, pay out, or report it to a
  regulator.

### Why we need it

A RETRACT from a low height would rewind events the ledger has already told clients are final. The
promise would be broken after people had acted on it. A retraction that deep is not a routine
correction; it means something is seriously wrong upstream, and a human must decide.

### The scenario it solves

```
 frontier = 40, so heights <= 28 are final
 a client reads: Bob finalized = 500  -> pays Bob out

 RETRACT { fromHeight: 5 } arrives

 without the guard:  undo heights 5..40, tombstone them, frontier = 4
                     Bob finalized = 0   (but he was already paid on 500)
 with the guard:     5 <= 40 - 12 -> reject: RETRACT stored as 'rejected',
                     alert retraction_rejected_final, nothing else changes
```

### How the code solves it

In `retract()`, after both locks are held and the RETRACT row is stored:

```ts
const state = await this.readState(c, true);           // frontier lock: frontier cannot move now
const fromHeight = e.fromHeight!;
if (this.protections.finalityGuard && fromHeight <= state.finalizedThrough) { // test-only switch
  await c.query(`UPDATE events SET status = 'rejected' WHERE event_id = $1`, [e.eventId]);
  this.counters.retractionsRejected++;
  this.alert('retraction_rejected_final', { eventId: e.eventId, fromHeight, finalizedThrough: state.finalizedThrough });
  return 'rejected';
}
```

Points worth defending:

- **Same boundary as the reads.** A retraction is rejected exactly when `fromHeight <= finalized_through`,
  i.e. when it touches at least one finalized height. The reads use the same stored line, so
  "allowed to retract" and "shown as provisional" always agree. At frontier 40 (line 28): from 28 is
  rejected, from 29 is allowed (29 is still provisional).
- **Checked under the frontier lock.** The frontier cannot move between the check and the decision. A
  drain cannot advance it, and another retraction cannot rewind it, because both need the same lock.
- **Rejected, not dropped.** The RETRACT is kept in `events` with status `rejected`. That is an audit
  record, and it makes a redelivery of the same RETRACT a `duplicate` (via `insertOnce()` and
  `resolveExisting()`), so the alert is not raised again and it is never applied later by accident.
- **Nothing is undone and the frontier stays.** The ledger keeps running. This is an alert for a
  human, not a stop.

### The test-only switch

With the switch off, the `if` is skipped and the retraction proceeds like any other: it undoes and
tombstones everything from `fromHeight` up and rewinds the frontier into final history.

### How the test proves it

The last step of `runScenario()` sends `RETRACT_FINAL = retract(5)` on both instances. By then the
frontier is far above 17, so height 5 is deep in final history. The oracle expects it to be
`rejected` and every original below `fromHeight` to stay `applied`. With the guard off, those events
become `retracted` and holdings collapse, so the oracle comparison fails with
`expected applied, got retracted`.

The deterministic test `rejects a retraction that reaches final heights` checks the exact boundary:
`frontier - 12` is rejected, `frontier - 11` is applied, and a repeat of the applied one is a
`duplicate`.

### The hole we found, and the fix (2026-11-11)

**The hole.** Originally the line was computed as `frontier - 12` from the **current** frontier. An
allowed retraction moves the frontier **down**, so the line moved down with it:

```
 frontier = 40   final line = 28     RETRACT from 29 -> allowed (provisional)
 frontier = 28   final line = 16     heights 17..28 were final a moment ago, now provisional again
 RETRACT from 20 -> 20 > 16 -> allowed, although height 20 was reported as final before
```

Two consequences:

1. After a rewind, a client can see an investor's `finalized` figure go **down** (part of it moves into
   `provisional`) even though no final event was undone.
2. A **second** retraction can then reach heights that an earlier read reported as final.

This needs a misbehaving feed (an honest one never retracts that deep), but catching a misbehaving
feed is exactly the guard's job. With the hole, the guard could be walked around in two steps.

**The fix: store the line and only ever raise it.**

```
 schema.sql   ledger_state.finalized_through bigint NOT NULL DEFAULT 0
 saveState()  finalized_through = GREATEST(finalized_through, frontier - 12)   -- every frontier write
 retract()    reject if fromHeight <= finalized_through
 reads        split finalized/provisional at finalized_through, return it as finalizedThrough

 frontier 40 -> line 28
 RETRACT from 29 -> allowed -> frontier 28 -> line STAYS 28
 RETRACT from 20 -> 20 <= 28 -> REJECTED
```

Why it is safe:

- **Invariant `frontier >= finalized_through`.** The guard only allows `fromHeight > finalized_through`,
  and the rewind sets `frontier = fromHeight - 1 >= finalized_through`. So every height up to the line
  is applied, and the finalized amounts are exact.
- **Still under the frontier lock.** `saveState()` is only called while holding it, so the
  read-and-raise of the line cannot race.
- **Design rule, updated.** "Finalized is computed at read time" still holds for the **amounts**. Only
  the **line** is stored, because a line computed from the current frontier moves backwards on a
  rewind.

**How it is tested.**

- New deterministic test `keeps final heights final after a rewind lowers the frontier`: frontier
  4490, RETRACT from 4479 is allowed, no `finalized` figure drops, then RETRACT from 4475 is rejected.
  With the old `frontier - 12` guard put back temporarily, it fails with
  `expected 'applied' to be 'rejected'`.
- The concurrency scenario checks `finalizedThrough <= appliedThrough` mid-run and at the end. The
  line itself depends on timing: drains may apply a few more heights after the RETRACT took the barrier
  but before it took the frontier lock. You can see this in the `LOG=1` output: `applied o33..o36`
  after `retraction holds the barrier`. So the test checks the line is within its possible range, then
  requires every endpoint figure to be exact for it.

**For the architecture note** (paste-ready):

> *Finality never moves backwards.* Finality is "height ≤ frontier − 12", but a retraction rewinds the
> frontier, so a line computed from the current frontier would drop. Clients would see a finalized
> figure fall, and a second retraction could reach heights already reported final. We store the line
> as a high-water mark (`finalized_through = GREATEST(finalized_through, frontier − 12)`, raised under the
> frontier lock on every frontier write). Both the retraction guard and the reads use it. Finalized
> and provisional amounts are still computed at read time. Only the line is stored.

### One-line summary

> Under the frontier lock, a RETRACT whose `fromHeight <= finalized_through` would touch finalized
> history, so it is stored as `rejected`, alerted once, and changes nothing else. The line only ever
> rises, so no sequence of retractions can walk back into final heights.

### Questions and answers

**Q: The two-step hole is serious. Why not handle it in code and call it out in the architecture
note? How would you fix it?**

Agreed, and it is now fixed in code (see "The hole we found, and the fix" above). The fix stores the
finality line as a high-water mark that only rises, and the guard and reads both use it. A new
deterministic test proves the hole is closed. It fails against the old guard. The paste-ready
paragraph for the architecture note is at the end of that subsection.
