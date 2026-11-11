import { AsyncLocalStorage } from 'node:async_hooks';
import { isDeepStrictEqual } from 'node:util';
import pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPool, migrate } from '../src/db.js';
import { ALL_PROTECTIONS, Ledger, type Hooks, type Protections } from '../src/ledger.js';
import { buildApp } from '../src/server.js';

const print = console.log.bind(console); // the service's own log lines are muted below; test evidence is not

// A separate database, so the API container's drain timer never touches test data.
const ADMIN_URL = process.env.TEST_DATABASE_URL ?? `postgres://ledger:ledger@localhost:${process.env.PG_PORT ?? '55432'}/ledger`;
const TEST_URL = ADMIN_URL.replace(/\/[^/]+$/, '/ledger_test');

let pool: pg.Pool;

async function resetDb(genesis = 0n, protections: Partial<Protections> = {}) {
  await pool.query('DROP SCHEMA public CASCADE; CREATE SCHEMA public');
  await migrate(pool, genesis);
  // The protections these switches remove also have a database backstop; drop it so the switch is really off.
  if (protections.uniqueEventId === false) await pool.query('ALTER TABLE events DROP CONSTRAINT events_pkey');
  if (protections.negativeCheck === false) await pool.query('ALTER TABLE holdings DROP CONSTRAINT holdings_units_nonnegative');
}

const transfer = (height: number, from: string | null, to: string, units: number, assetId = 'X', eventId = `e${height}`) =>
  ({ eventId, height, assetId, from, to, units });
const retract = (fromHeight: number, eventId = `r${fromHeight}`) => ({ eventId, type: 'RETRACT', fromHeight });

async function status(eventId: string) {
  const { rows } = await pool.query('SELECT status FROM events WHERE event_id = $1', [eventId]);
  return rows[0]?.status;
}

beforeAll(async () => {
  vi.spyOn(console, 'log').mockImplementation(() => {});
  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  const { rowCount } = await admin.query(`SELECT 1 FROM pg_database WHERE datname = 'ledger_test'`);
  if (!rowCount) await admin.query('CREATE DATABASE ledger_test');
  await admin.end();
  pool = createPool(TEST_URL);
});

afterAll(() => pool.end());

describe('deterministic', () => {
  let ledger: Ledger;
  const ingestAll = async (msgs: object[]) => { for (const m of msgs) await ledger.ingest(m); await ledger.drain(); };

  beforeEach(async () => {
    await resetDb(4470n);
    ledger = new Ledger(pool);
  });

  it('applies out of order arrivals in height order', async () => {
    await ledger.ingest(transfer(4472, 'A', 'B', 30));
    await ledger.drain();
    expect(await status('e4472')).toBe('waiting');
    await ingestAll([transfer(4471, null, 'A', 100)]);
    const h = await ledger.holders('X');
    expect(h.appliedThrough).toBe(4472n);
    expect(h.holders.map((x) => [x.investorId, x.finalized + x.provisional])).toEqual([['A', 70n], ['B', 30n]]);
    expect(await ledger.reconcile()).toEqual([]);
  });

  it('holds at a gap and reports it', async () => {
    await ingestAll([transfer(4471, null, 'A', 100), transfer(4473, 'A', 'B', 10)]);
    const h = await ledger.holders('X');
    expect([h.appliedThrough, h.gapAt, h.waitingCount]).toEqual([4471n, 4472n, 1]);
    expect(h.holders).toEqual([{ investorId: 'A', finalized: 0n, provisional: 100n }]);
    await ingestAll([transfer(4472, 'A', 'C', 5)]);
    const p = await ledger.positions('A');
    expect([p.appliedThrough, p.gapAt, p.waitingCount]).toEqual([4473n, null, 0]);
    expect(p.positions[0].provisional).toBe(85n);
  });

  it('stops the drain at a negative balance and alerts once', async () => {
    await ingestAll([transfer(4471, null, 'A', 100), transfer(4472, 'A', 'B', 150), transfer(4473, 'A', 'B', 10)]);
    await ledger.drain();
    const h = await ledger.holders('X');
    expect([h.appliedThrough, h.gapAt, h.waitingCount]).toEqual([4471n, 4472n, 2]);
    expect(ledger.counters.alerts.negative_balance_block).toBe(1);
    expect(await status('e4473')).toBe('waiting');
  });

  it('rejects a retraction that reaches final heights', async () => {
    const msgs = [transfer(4471, null, 'A', 100)];
    for (let h = 4472; h <= 4490; h++) msgs.push(transfer(h, 'A', 'B', 1));
    await ingestAll(msgs);
    expect(await ledger.ingest(retract(4490 - 12))).toBe('rejected');
    expect(ledger.counters.alerts.retraction_rejected_final).toBe(1);
    expect((await ledger.holders('X')).appliedThrough).toBe(4490n);
    expect(await ledger.ingest(retract(4490 - 11))).toBe('applied');
    expect(await ledger.ingest(retract(4490 - 11))).toBe('duplicate');
    const h = await ledger.holders('X');
    expect(h.appliedThrough).toBe(4478n);
    expect(h.holders.map((x) => [x.investorId, x.finalized + x.provisional])).toEqual([['A', 93n], ['B', 7n]]);
    expect(await status('e4479')).toBe('retracted');
    expect(await ledger.reconcile()).toEqual([]);
  });

  it('applies the same-id-different-content rules', async () => {
    await ingestAll([transfer(4471, null, 'A', 100), transfer(4472, 'A', 'B', 10)]);
    expect(await ledger.ingest(transfer(4472, 'A', 'B', 10))).toBe('duplicate');
    expect(await ledger.ingest(transfer(4472, 'A', 'B', 99))).toBe('conflict');
    expect(ledger.counters.alerts.live_row_conflict).toBe(1);
    await ledger.ingest(retract(4472));
    expect(await ledger.ingest(transfer(4472, 'A', 'B', 10))).toBe('duplicate'); // tombstone holds
    expect(await status('e4472')).toBe('retracted');
    expect(await ledger.ingest(transfer(4472, 'A', 'C', 20))).toBe('replaced');
    await ledger.drain();
    const h = await ledger.holders('X');
    expect(h.holders.map((x) => [x.investorId, x.finalized + x.provisional])).toEqual([['A', 80n], ['C', 20n]]);
  });

  it('alerts once on lag and on a stalled frontier, and clears when the gap closes', async () => {
    const t0 = Date.now();
    const issue = transfer(4471, null, 'A', 100);
    const behindGap = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => transfer(from + i, 'A', 'B', 1));
    await ingestAll([issue, ...behindGap(4473, 4477)]); // gap at 4472, lag 6
    await ledger.monitor(t0);
    await ledger.monitor(t0 + 1000);
    expect(ledger.counters.alerts).toEqual({ lag_warn: 1 });
    await ingestAll(behindGap(4478, 4482)); // lag 11
    await ledger.monitor(t0 + 2000);
    expect(ledger.counters.alerts).toEqual({ lag_warn: 1, lag_critical: 1 });
    await ledger.monitor(t0 + 300_000); // frontier unmoved for 5 minutes with events waiting
    await ledger.monitor(t0 + 301_000);
    expect(ledger.counters.alerts).toEqual({ lag_warn: 1, lag_critical: 1, frontier_stalled: 1 });
    await ingestAll([transfer(4472, 'A', 'C', 1)]);
    await ledger.monitor(t0 + 302_000);
    const m = await ledger.metrics();
    expect([m.frontier, m.highestSeen, m.oldestWaitingAgeSeconds]).toEqual([4482n, 4482n, null]);
    await ingestAll(behindGap(4484, 4489)); // a new gap at 4483, lag 7: the cleared warning fires again
    await ledger.monitor(t0 + 303_000);
    expect(ledger.counters.alerts).toEqual({ lag_warn: 2, lag_critical: 1, frontier_stalled: 1 });
  });

  it('splits finalized and provisional at frontier - 12', async () => {
    const msgs = [transfer(4471, null, 'A', 100)];
    for (let h = 4472; h <= 4484; h++) msgs.push(transfer(h, 'A', 'B', 1));
    await ingestAll(msgs);
    // frontier 4484: heights <= 4472 are final, 4473..4484 (12 transfers) are provisional.
    const h = await ledger.holders('X');
    expect(h.issuedSupply).toBe(100n);
    expect(h.holders).toEqual([
      { investorId: 'A', finalized: 99n, provisional: -12n },
      { investorId: 'B', finalized: 1n, provisional: 12n },
    ]);
  });
});

// ---------------------------------------------------------------------------------------------------------
// Concurrency: a retraction racing shuffled, duplicated delivery across workers on two service instances.
// The most dangerous case because it combines every rule at once and fails silently: a resurrected or
// double-applied event yields a plausible but wrong balance.
// ---------------------------------------------------------------------------------------------------------

type Msg = ReturnType<typeof transfer>;
type Job = Array<[Ledger, object]>; // messages one worker sends at the same moment (simultaneous redelivery)

const HEIGHTS = 40;
const WORKERS = 8;
const ASSETS = ['A1', 'A2', 'A3'];
const INVESTORS = ['I1', 'I2', 'I3', 'I4', 'I5', 'I6'];
const ROUNDS = Number(process.env.ROUNDS ?? '25');
const PASS_SEEDS = (process.env.SEEDS ?? '1,2,3,4,5').split(',').map(Number);
const RETRACT_FINAL = retract(5, 'r-final'); // reaches final heights: must always be rejected

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function mulberry32(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Applies one transfer to an in-memory ledger; false (and no change) if the sender cannot cover it.
function applyTo(holdings: Map<string, bigint>, supply: Map<string, bigint>, m: Msg): boolean {
  const units = BigInt(m.units);
  if (m.from === null) supply.set(m.assetId, (supply.get(m.assetId) ?? 0n) + units);
  else {
    const balance = holdings.get(`${m.assetId}/${m.from}`) ?? 0n;
    if (balance < units) return false;
    holdings.set(`${m.assetId}/${m.from}`, balance - units);
  }
  holdings.set(`${m.assetId}/${m.to}`, (holdings.get(`${m.assetId}/${m.to}`) ?? 0n) + units);
  return true;
}

// Deterministic inputs from the seed: originals, the retraction point, a corrected sequence, the delivery plan.
function makeFeed(seed: number) {
  const rand = mulberry32(seed);
  const int = (lo: number, hi: number) => lo + Math.floor(rand() * (hi - lo + 1));
  const pick = <T>(xs: T[]) => xs[int(0, xs.length - 1)];
  const shuffle = <T>(xs: T[]) => {
    for (let i = xs.length - 1; i > 0; i--) { const j = int(0, i); [xs[i], xs[j]] = [xs[j], xs[i]]; }
    return xs;
  };
  const holdings = new Map<string, bigint>();
  const supply = new Map<string, bigint>();
  const validTransfer = (height: number, prefix: string) => {
    const [key, balance] = pick([...holdings].filter(([, b]) => b > 0n));
    const [assetId, from] = key.split('/');
    const m = transfer(height, from, pick(INVESTORS.filter((i) => i !== from)), int(1, Number(balance)), assetId, `${prefix}${height}`);
    applyTo(holdings, supply, m);
    return m;
  };

  const originals: Msg[] = ASSETS.map((a, i) => transfer(i + 1, null, pick(INVESTORS), int(500, 2000), a, `o${i + 1}`));
  originals.forEach((m) => applyTo(holdings, supply, m));
  for (let h = ASSETS.length + 1; h <= HEIGHTS; h++) originals.push(validTransfer(h, 'o'));

  // Inside the provisional window even when every original is applied (HEIGHTS - 12 < fromHeight).
  const fromHeight = int(HEIGHTS - 10, HEIGHTS - 4);
  holdings.clear();
  supply.clear();
  originals.filter((m) => m.height < fromHeight).forEach((m) => applyTo(holdings, supply, m));
  const corrected: Msg[] = [];
  const valid = int(4, 8);
  for (let h = fromHeight; h < fromHeight + valid; h++) corrected.push(validTransfer(h, 'c'));
  // One overspend at the end: a correct ledger stops on it; without the balance check it goes negative.
  const [key, balance] = pick([...holdings]);
  const [assetId, from] = key.split('/');
  const top = fromHeight + valid;
  corrected.push(transfer(top, from, pick(INVESTORS.filter((i) => i !== from)), Number(balance) + int(1, 100), assetId, `c${top}`));

  // First deliveries still in flight when the RETRACT arrives: sent before it, so they must be swept.
  const slow = shuffle(originals.filter((m) => m.height >= fromHeight)).slice(0, 2);
  return { int, shuffle, originals, corrected, fromHeight, slow, retraction: retract(fromHeight, 'r-main') };
}
type Feed = ReturnType<typeof makeFeed>;

// Single-threaded truth: surviving events applied in height order, stopping at the first overspend.
function oracle(f: Feed) {
  const holdings = new Map<string, bigint>();
  const supply = new Map<string, bigint>();
  const statuses = new Map<string, string>([[f.retraction.eventId, 'applied'], [RETRACT_FINAL.eventId, 'rejected']]);
  for (const m of f.originals) statuses.set(m.eventId, m.height < f.fromHeight ? 'applied' : 'retracted');
  for (const m of f.corrected) statuses.set(m.eventId, 'waiting');
  let frontier = 0;
  const applied: Msg[] = [];
  for (const m of [...f.originals.filter((m) => m.height < f.fromHeight), ...f.corrected]) {
    if (!applyTo(holdings, supply, m)) break;
    frontier = m.height;
    applied.push(m);
    statuses.set(m.eventId, 'applied');
  }

  // Expected endpoint responses. Finalized = the effect of applied events at height <= frontier - 12.
  const finalized = new Map<string, bigint>();
  applied.filter((m) => m.height <= frontier - 12).forEach((m) => applyTo(finalized, new Map(), m));
  const waitingCount = [...statuses.values()].filter((s) => s === 'waiting').length;
  const header = { appliedThrough: String(frontier), gapAt: waitingCount ? String(frontier + 1) : null, waitingCount };
  const figures = (key: string) => {
    const fin = finalized.get(key) ?? 0n;
    const provisional = (holdings.get(key) ?? 0n) - fin;
    return fin === 0n && provisional === 0n ? null : { finalized: String(fin), provisional: String(provisional) };
  };
  const endpoints = new Map<string, object>();
  for (const assetId of ASSETS) {
    const holders = INVESTORS.flatMap((investorId) => { const x = figures(`${assetId}/${investorId}`); return x ? [{ investorId, ...x }] : []; });
    endpoints.set(`/assets/${assetId}/holders`, { assetId, issuedSupply: String(supply.get(assetId) ?? 0n), ...header, holders });
  }
  for (const investorId of INVESTORS) {
    const positions = ASSETS.flatMap((assetId) => { const x = figures(`${assetId}/${investorId}`); return x ? [{ assetId, ...x }] : []; });
    endpoints.set(`/investors/${investorId}/positions`, { investorId, ...header, positions });
  }
  return { holdings, supply, statuses, frontier, endpoints };
}

// Invariants read through the endpoint handlers, each a single consistent snapshot.
async function invariants(ledger: Ledger): Promise<string[]> {
  const out: string[] = [];
  for (const assetId of ASSETS) {
    const h = await ledger.holders(assetId);
    let total = 0n;
    for (const x of h.holders) {
      if (x.finalized + x.provisional < 0n) out.push(`negative holding ${assetId}/${x.investorId}: ${x.finalized + x.provisional}`);
      total += x.finalized + x.provisional;
    }
    if (total !== h.issuedSupply) out.push(`${assetId}: holdings total ${total} != issued supply ${h.issuedSupply}`);
  }
  for (const investorId of INVESTORS) {
    for (const p of (await ledger.positions(investorId)).positions) {
      if (p.finalized + p.provisional < 0n) out.push(`negative position ${p.assetId}/${investorId}: ${p.finalized + p.provisional}`);
    }
  }
  for (const m of await ledger.reconcile()) out.push(`reconcile: ${m}`);
  return out;
}

function diff(what: string, want: Map<string, unknown>, have: Map<string, unknown>, out: Set<string>) {
  for (const key of new Set([...want.keys(), ...have.keys()])) {
    if (String(want.get(key)) !== String(have.get(key))) out.add(`oracle ${what} ${key}: expected ${want.get(key)}, got ${have.get(key)}`);
  }
}

// Lets the test hold one ingest open at its commit point; everything else gets random jitter there.
const held = new AsyncLocalStorage<() => Promise<void>>();
const hooks: Hooks = {
  beforeCommit: async () => {
    const hold = held.getStore();
    if (hold) return hold();
    if (Math.random() < 0.5) await sleep(Math.random() * 2);
    else await new Promise(setImmediate);
  },
};

async function runScenario(seed: number, protections: Partial<Protections>, pools: pg.Pool[]): Promise<string[]> {
  await resetDb(0n, protections);
  const f = makeFeed(seed);
  const inst = pools.map((p) => new Ledger(p, protections, hooks));
  const problems = new Set<string>();
  const fail = (where: string) => (err: Error) => { problems.add(`${where} error: ${err.message}`); };
  const send = (l: Ledger, m: object) => l.ingest(m).then(() => l.drain()).then(() => {}, fail('ingest/drain'));
  const runJobs = (jobs: Job[]) => {
    let next = 0;
    return Promise.all(Array.from({ length: WORKERS }, async () => {
      while (next < jobs.length) await Promise.all(jobs[next++].map(([l, m]) => send(l, m)));
    }));
  };
  // Every event once or more, shuffled; sometimes a second copy at the same moment on the other instance.
  const deliveries = (msgs: Msg[]) => f.shuffle(msgs.flatMap((m) => {
    const i = f.int(0, 1);
    const first: Job = f.int(0, 9) < 4 ? [[inst[i], m], [inst[1 - i], m]] : [[inst[i], m]];
    return [first, ...Array.from({ length: f.int(0, 2) }, (): Job => [[inst[f.int(0, 1)], m]])];
  }));
  const quiesce = async () => {
    for (let i = 0; i < 100; i++) {
      const applied = (await inst[0].drain()) + (await inst[1].drain());
      if (applied === 0) return;
    }
  };

  // Background drains on both instances (the timer) and a reader sampling invariants mid-run.
  let stopped = false;
  const loops = [
    ...inst.map(async (l) => { while (!stopped) { await l.drain().catch(fail('timer drain')); await sleep(Math.random() * 3); } }),
    (async () => {
      while (!stopped) for (const v of await invariants(inst[Math.round(Math.random())]).catch((e) => [`reader error: ${e.message}`])) problems.add(`mid-run ${v}`);
    })(),
  ];

  try {
    // Phase 1: every original except the slow ones, delivered and committed.
    const slowIds = new Set(f.slow.map((m) => m.eventId));
    await runJobs(deliveries(f.originals.filter((m) => !slowIds.has(m.eventId))));

    // Phase 2: hold the slow first deliveries open at their commit point, then send the RETRACT (duplicated,
    // both instances) alongside redeliveries and running drains. Release the held ingests once the RETRACT has
    // committed or 50ms have passed (with the barrier it cannot commit until they do).
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const slow = f.slow.map((m) => {
      let reached!: () => void;
      const arrived = new Promise<void>((r) => { reached = r; });
      const done = held.run(async () => { reached(); await gate; }, () => inst[0].ingest(m)).then(() => inst[0].drain(), fail('slow ingest'));
      return { arrived: Promise.race([arrived, done]), done };
    });
    await Promise.all(slow.map((s) => s.arrived));
    const retracting = Promise.all(Array.from({ length: f.int(2, 3) }, (_, k) => send(inst[k % 2], f.retraction)));
    const redelivering = runJobs(f.shuffle(f.originals.map((m): Job => [[inst[f.int(0, 1)], m]])).slice(0, 12));
    await Promise.race([retracting, sleep(50)]);
    release();
    await Promise.all([retracting, redelivering, ...slow.map((s) => s.done)]);

    // Phase 3: the corrected sequence plus stale redeliveries of retracted originals, then quiesce. Last, a
    // RETRACT that reaches final heights, which must change nothing.
    const stale = f.originals.filter((m) => m.height >= f.fromHeight).map((m): Job => [[inst[f.int(0, 1)], m]]);
    await runJobs(f.shuffle([...deliveries(f.corrected), ...stale]));
    await quiesce();
    await Promise.all([send(inst[0], RETRACT_FINAL), send(inst[1], RETRACT_FINAL)]);
    await quiesce();
  } catch (err) {
    problems.add(`scenario error: ${(err as Error).message}`);
  } finally {
    stopped = true;
    await Promise.all(loops);
  }

  // Final state against invariants and the single-threaded oracle.
  for (const v of await invariants(inst[0]).catch((e) => [`reader error: ${e.message}`])) problems.add(`final ${v}`);
  const want = oracle(f);
  const { rows: holdings } = await pool.query('SELECT asset_id, investor_id, units FROM holdings WHERE units <> 0');
  const stored = new Map(holdings.map((h) => [`${h.asset_id}/${h.investor_id}`, BigInt(h.units)]));
  diff('holding', new Map([...want.holdings].filter(([, u]) => u !== 0n)), stored, problems);
  const { rows: assets } = await pool.query('SELECT asset_id, issued_supply FROM assets');
  diff('issued supply', want.supply, new Map(assets.map((a) => [a.asset_id, BigInt(a.issued_supply)])), problems);
  const { rows: events } = await pool.query('SELECT event_id, status FROM events ORDER BY event_id');
  const statuses = new Map<string, string>();
  for (const e of events) statuses.set(e.event_id, statuses.has(e.event_id) ? `${statuses.get(e.event_id)}+${e.status} (duplicate rows)` : e.status);
  diff('status', want.statuses, statuses, problems);
  const { rows: [state] } = await pool.query('SELECT frontier FROM ledger_state');
  diff('frontier', new Map([['', want.frontier]]), new Map([['', state.frontier]]), problems);
  for (const assetId of ASSETS) {
    for (const x of (await inst[0].holders(assetId)).holders) {
      const balance = stored.get(`${assetId}/${x.investorId}`) ?? 0n;
      if (x.finalized + x.provisional !== balance) problems.add(`finalized + provisional != balance for ${assetId}/${x.investorId}`);
    }
  }
  // Both read endpoints through the real HTTP routes (in-process, no socket): every field, including the
  // finalized/provisional split, must equal the oracle's.
  const app = buildApp(inst[0], false);
  for (const [url, expected] of want.endpoints) {
    const res = await app.inject({ method: 'GET', url });
    if (res.statusCode !== 200 || !isDeepStrictEqual(JSON.parse(res.body), expected)) {
      problems.add(`endpoint GET ${url}: expected ${JSON.stringify(expected)}, got ${res.statusCode} ${res.body}`);
    }
  }
  await app.close();
  return [...problems];
}

describe('concurrency: retraction racing shuffled duplicated delivery on two instances', () => {
  let pools: pg.Pool[];
  beforeAll(() => { pools = [new pg.Pool({ connectionString: TEST_URL, max: 20 }), new pg.Pool({ connectionString: TEST_URL, max: 20 })]; });
  afterAll(() => Promise.all(pools.map((p) => p.end())));

  it('matches the oracle and holds every invariant with all protections on', async () => {
    for (const seed of PASS_SEEDS) {
      const problems = await runScenario(seed, {}, pools);
      if (problems.length) print(`FAIL seed ${seed}:\n  ${problems.slice(0, 10).join('\n  ')}`);
      expect(problems, `seed ${seed}`).toEqual([]);
    }
    print(`all protections on: seeds ${PASS_SEEDS.join(',')} match the oracle, no invariant broken`);
  }, 600_000);

  it.each(Object.keys(ALL_PROTECTIONS))('breaks when %s is switched off', async (name) => {
    for (let round = 1; round <= ROUNDS; round++) {
      const seed = 1000 + round;
      const problems = await runScenario(seed, { [name]: false }, pools);
      if (problems.length) {
        print(`${name} off -> failed in round ${round} (seed ${seed}), ${problems.length} problem(s), e.g.\n  ${problems.slice(0, 3).join('\n  ')}`);
        return;
      }
    }
    expect.fail(`${name} off: no failure in ${ROUNDS} rounds, so the scenario does not prove this protection`);
  }, 600_000);
});
