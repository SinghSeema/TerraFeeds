import { createHash } from 'node:crypto';
import { hostname } from 'node:os';
import type pg from 'pg';
import { tx } from './db.js';

const FINALITY_DEPTH = 12n;
// One fixed key for every instance: transfer ingest holds it shared, retraction holds it exclusive.
const INGEST_BARRIER = 4202;
// Reads see one snapshot, so frontier, holdings and recent events always agree with each other.
const READ_SNAPSHOT = 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY';

export const ALL_PROTECTIONS = {
  uniqueEventId: true,
  frontierLock: true,
  tombstones: true,
  ingestBarrier: true,
  negativeCheck: true,
  finalityGuard: true,
};
export type Protections = typeof ALL_PROTECTIONS;
export interface Hooks { beforeCommit?: () => Promise<void> }

interface Event {
  eventId: string;
  kind: 'transfer' | 'retract';
  height: bigint | null;
  assetId: string | null;
  from: string | null;
  to: string | null;
  units: bigint | null;
  fromHeight: bigint | null;
}

export class BadMessage extends Error {}

function int(v: unknown, name: string): bigint {
  if ((typeof v === 'number' && Number.isSafeInteger(v) && v >= 0) || (typeof v === 'string' && /^\d+$/.test(v))) return BigInt(v);
  throw new BadMessage(`${name} must be a non-negative integer`);
}

function str(v: unknown, name: string): string {
  if (typeof v === 'string' && v !== '') return v;
  throw new BadMessage(`${name} must be a non-empty string`);
}

function parseMessage(m: any): Event {
  const eventId = str(m?.eventId, 'eventId');
  const none = { height: null, assetId: null, from: null, to: null, units: null, fromHeight: null };
  if (m.type === 'RETRACT') return { ...none, eventId, kind: 'retract', fromHeight: int(m.fromHeight, 'fromHeight') };
  // The feed's transfer shape has no type field; only a retraction carries one.
  if (m.type !== undefined) throw new BadMessage('type must be absent (transfer) or RETRACT');
  const units = int(m.units, 'units');
  if (units === 0n) throw new BadMessage('units must be positive');
  return {
    ...none, eventId, kind: 'transfer', units,
    height: int(m.height, 'height'),
    assetId: str(m.assetId, 'assetId'),
    from: m.from === null || m.from === undefined ? null : str(m.from, 'from'),
    to: str(m.to, 'to'),
  };
}

export const json = (v: unknown) => JSON.stringify(v, (_, x) => (typeof x === 'bigint' ? x.toString() : x));

const big = (v: string | null) => (v === null ? null : BigInt(v));
const param = (v: bigint | string | null) => (typeof v === 'bigint' ? v.toString() : v);

function contentHash(e: Event): string {
  const fields = [e.kind, e.height, e.assetId, e.from, e.to, e.units, e.fromHeight].map(param);
  return createHash('sha256').update(JSON.stringify(fields)).digest('hex');
}

function rowToEvent(r: any): Event {
  return {
    eventId: r.event_id, kind: r.kind, height: big(r.height), assetId: r.asset_id, from: r.from_investor,
    to: r.to_investor, units: big(r.units), fromHeight: big(r.from_height),
  };
}

export class Ledger {
  readonly counters = {
    ingests: {} as Record<string, number>, drains: 0, eventsApplied: 0, retractionsApplied: 0, retractionsRejected: 0,
    alerts: {} as Record<string, number>,
  };
  private readonly protections: Protections;
  private readonly activeAlerts = new Set<string>();
  private lastFrontier: bigint | null = null;
  private frontierMovedAt = Date.now();
  private readonly lagWarn = BigInt(process.env.LAG_WARN ?? '4');
  private readonly lagCritical = BigInt(process.env.LAG_CRITICAL ?? '10');
  private readonly stallMs = Number(process.env.STALL_SECONDS ?? '300') * 1000;

  constructor(
    private readonly pool: pg.Pool, protections: Partial<Protections> = {}, private readonly hooks: Hooks = {},
    readonly name = process.env.INSTANCE_NAME ?? hostname(),
  ) {
    this.protections = { ...ALL_PROTECTIONS, ...protections };
  }

  // One JSON line per entry, tagged with the instance so interleaved logs from several servers stay readable.
  private log(level: 'info' | 'alert', msg: string, detail: Record<string, unknown> = {}) {
    console.log(json({ time: new Date().toISOString(), level, instance: this.name, msg, ...detail }));
  }

  alert(type: string, detail: Record<string, unknown>) {
    this.counters.alerts[type] = (this.counters.alerts[type] ?? 0) + 1;
    this.log('alert', type, detail);
  }

  // The caller acks only after this resolves, i.e. after commit, and then triggers drain().
  async ingest(message: unknown): Promise<'accepted' | 'duplicate' | 'replaced' | 'conflict' | 'applied' | 'rejected'> {
    const e = parseMessage(message);
    const hash = contentHash(e);
    const outcome = e.kind === 'retract' ? await this.retract(e, hash) : await this.ingestTransfer(e, hash);
    this.counters.ingests[outcome] = (this.counters.ingests[outcome] ?? 0) + 1;
    // Logged after commit, so this line marks exactly when the message may be acked.
    this.log('info', 'ack', { eventId: e.eventId, height: e.height ?? undefined, fromHeight: e.fromHeight ?? undefined, outcome });
    return outcome;
  }

  private ingestTransfer(e: Event, hash: string) {
    return tx(this.pool, async (c) => {
      // Shared barrier: ingests run in parallel with each other, but a retraction waits for every in-flight
      // ingest to commit (so its sweep sees them) and holds new ones off until its rewind commits.
      if (this.protections.ingestBarrier) { // test-only switch
        await c.query('SELECT pg_advisory_xact_lock_shared($1)', [INGEST_BARRIER]);
      }
      const outcome = (await this.insertOnce(c, e, hash)) ? 'accepted' : await this.resolveExisting(c, e, hash);
      await this.hooks.beforeCommit?.();
      return outcome;
    });
  }

  private async insertOnce(c: pg.PoolClient, e: Event, hash: string): Promise<boolean> {
    const values = [e.eventId, e.kind, e.height, e.assetId, e.from, e.to, e.units, e.fromHeight, hash].map(param);
    const insert = `INSERT INTO events (event_id, kind, height, asset_id, from_investor, to_investor, units, from_height,
      content_hash, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'waiting')`;
    if (!this.protections.uniqueEventId) { // test-only switch: check-then-insert, two racing copies both see "absent"
      const { rowCount } = await c.query('SELECT 1 FROM events WHERE event_id = $1', [e.eventId]);
      if (rowCount) return false;
      await c.query(insert, values);
      return true;
    }
    // The database decides duplicates: a concurrent copy blocks on the unique key, then does nothing.
    const { rowCount } = await c.query(`${insert} ON CONFLICT (event_id) DO NOTHING`, values);
    return rowCount === 1;
  }

  private async resolveExisting(c: pg.PoolClient, e: Event, hash: string) {
    const { rows: [row] } = await c.query('SELECT status, content_hash FROM events WHERE event_id = $1 LIMIT 1', [e.eventId]);
    if (row.content_hash === hash) {
      if (row.status === 'retracted' && !this.protections.tombstones) { // test-only switch: as if the row had been deleted
        await c.query(`UPDATE events SET status = 'waiting' WHERE event_id = $1`, [e.eventId]);
        return 'accepted';
      }
      return 'duplicate';
    }
    if (row.status === 'retracted' && e.kind === 'transfer') {
      // A retracted id may come back with corrected content; it becomes a new waiting event under the same id.
      await c.query(
        `UPDATE events SET height = $2, asset_id = $3, from_investor = $4, to_investor = $5, units = $6,
           content_hash = $7, status = 'waiting', received_at = now(), applied_at = NULL
         WHERE event_id = $1 AND status = 'retracted'`,
        [e.eventId, e.height, e.assetId, e.from, e.to, e.units, hash].map(param),
      );
      this.log('info', 'retracted id reused with new content', { eventId: e.eventId });
      return 'replaced';
    }
    this.alert('live_row_conflict', { eventId: e.eventId, status: row.status });
    return 'conflict';
  }

  private async readState(c: pg.PoolClient, forUpdate: boolean) {
    const { rows: [s] } = await c.query(
      `SELECT frontier, finalized_through, blocked_at FROM ledger_state WHERE id = 1${forUpdate ? ' FOR UPDATE' : ''}`,
    );
    return { frontier: BigInt(s.frontier), finalizedThrough: BigInt(s.finalized_through), blockedAt: big(s.blocked_at) };
  }

  private async saveState(c: pg.PoolClient, frontier: bigint, blockedAt: bigint | null, reason: string | null) {
    // highest_seen is refreshed on every drain stop too, so the lag alert sees heights waiting behind a gap.
    // finalized_through only rises: a height once reported final stays final even when a retraction rewinds the frontier.
    await c.query(
      `UPDATE ledger_state SET frontier = $1, blocked_at = $2, blocked_reason = $3,
         finalized_through = GREATEST(finalized_through, $1::bigint - $4),
         highest_seen = GREATEST($1::bigint, (SELECT max(height) FROM events WHERE status IN ('waiting', 'applied')))
       WHERE id = 1`,
      [frontier, blockedAt, reason, FINALITY_DEPTH].map(param),
    );
  }

  // Signed change to one holding; a null investor is the issuer, whose side moves issued_supply instead.
  private async adjust(c: pg.PoolClient, assetId: string, investor: string | null, delta: bigint) {
    // Decreases are plain UPDATEs: INSERT ... ON CONFLICT checks constraints on the proposed row, so a negative one fails.
    if (investor === null) {
      const supplyDelta = (-delta).toString(); // the issuer giving units out grows supply
      if (delta > 0n) {
        await c.query('UPDATE assets SET issued_supply = issued_supply + $2 WHERE asset_id = $1', [assetId, supplyDelta]);
        return;
      }
      await c.query(
        `INSERT INTO assets (asset_id, issued_supply) VALUES ($1, $2)
         ON CONFLICT (asset_id) DO UPDATE SET issued_supply = assets.issued_supply + EXCLUDED.issued_supply`,
        [assetId, supplyDelta],
      );
      return;
    }
    const values = [assetId, investor, delta.toString()];
    if (delta < 0n) {
      const { rowCount } = await c.query(
        'UPDATE holdings SET units = units + $3 WHERE asset_id = $1 AND investor_id = $2', values,
      );
      // No row only if the balance check was skipped; the insert then records (or rejects) the negative balance.
      if (rowCount) return;
    }
    await c.query(
      `INSERT INTO holdings (asset_id, investor_id, units) VALUES ($1, $2, $3)
       ON CONFLICT (asset_id, investor_id) DO UPDATE SET units = holdings.units + EXCLUDED.units`,
      values,
    );
  }

  async drain(): Promise<number> {
    this.counters.drains++;
    for (let applied = 0; ; applied++) {
      const e = await this.applyNext();
      if (!e) return applied;
      this.counters.eventsApplied++;
      // After commit, so the log never shows an apply that was rolled back.
      this.log('info', 'applied', { eventId: e.eventId, height: e.height, assetId: e.assetId, from: e.from, to: e.to, units: e.units });
    }
  }

  // One event per transaction: a failure loses only the event in flight, and the lock is held briefly.
  private applyNext(): Promise<Event | null> {
    return tx(this.pool, async (c) => {
      // Frontier lock: serializes every writer of holdings and frontier across all instances, so two drains
      // cannot apply the same height twice. Under READ COMMITTED each later statement sees what the previous
      // lock holder committed.
      const state = await this.readState(c, this.protections.frontierLock); // test-only switch
      const next = state.frontier + 1n;
      const { rows } = await c.query(
        `SELECT * FROM events WHERE status = 'waiting' AND kind = 'transfer' AND height = $1`, [next.toString()],
      );
      if (rows.length === 0) {
        await this.saveState(c, state.frontier, null, null);
        return null;
      }
      if (rows.length > 1) return this.block(c, state, 'multiple_events_at_height', { eventIds: rows.map((r) => r.event_id) });
      const e = rowToEvent(rows[0]);
      if (e.from !== null && this.protections.negativeCheck) { // test-only switch
        const { rows: [h] } = await c.query(
          'SELECT units FROM holdings WHERE asset_id = $1 AND investor_id = $2', [e.assetId, e.from],
        );
        const balance = h ? BigInt(h.units) : 0n;
        if (balance < e.units!) {
          return this.block(c, state, 'negative_balance_block', { eventId: e.eventId, investor: e.from, balance, units: e.units });
        }
      }
      await this.adjust(c, e.assetId!, e.from, -e.units!);
      await this.adjust(c, e.assetId!, e.to, e.units!);
      await c.query(`UPDATE events SET status = 'applied', applied_at = now() WHERE event_id = $1`, [e.eventId]);
      await this.saveState(c, next, null, null);
      await this.hooks.beforeCommit?.();
      return e;
    });
  }

  // Stop without skipping: the event stays waiting and a human decides. Alert once per blocked height.
  private async block(c: pg.PoolClient, state: { frontier: bigint; blockedAt: bigint | null }, type: string, detail: object) {
    const next = state.frontier + 1n;
    if (state.blockedAt !== next) this.alert(type, { height: next, ...detail });
    await this.saveState(c, state.frontier, next, type);
    return null;
  }

  private retract(e: Event, hash: string) {
    return tx(this.pool, async (c) => {
      // Lock order is always advisory, then ledger_state row. The exclusive barrier waits for in-flight
      // ingests, so every event is either swept below or arrives after the rewind has committed.
      const waitStart = Date.now();
      this.log('info', 'retraction waiting for in-flight ingests', { eventId: e.eventId, fromHeight: e.fromHeight });
      await c.query('SELECT pg_advisory_xact_lock($1)', [INGEST_BARRIER]);
      this.log('info', 'retraction holds the barrier', { eventId: e.eventId, waitedMs: Date.now() - waitStart });
      if (!(await this.insertOnce(c, e, hash))) return this.resolveExisting(c, e, hash);
      // Frontier lock: no drain may apply while holdings are being rewound.
      const state = await this.readState(c, true);
      const fromHeight = e.fromHeight!;
      // Compared with the stored high-water mark, not frontier - 12: an earlier rewind lowers the frontier, and a
      // second retraction must still not reach heights that were already reported final.
      if (this.protections.finalityGuard && fromHeight <= state.finalizedThrough) { // test-only switch
        await c.query(`UPDATE events SET status = 'rejected' WHERE event_id = $1`, [e.eventId]);
        this.counters.retractionsRejected++;
        this.alert('retraction_rejected_final', { eventId: e.eventId, fromHeight, finalizedThrough: state.finalizedThrough });
        return 'rejected';
      }
      // No row lock needed: the barrier and the frontier lock already exclude every other writer of these rows.
      const { rows } = await c.query(
        `SELECT * FROM events WHERE kind = 'transfer' AND height >= $1 AND status IN ('waiting', 'applied')
         ORDER BY height DESC`,
        [fromHeight.toString()],
      );
      // Newest first, so every intermediate state is one the ledger really passed through: never negative.
      for (const r of rows.filter((r) => r.status === 'applied').map(rowToEvent)) {
        await this.adjust(c, r.assetId!, r.to, -r.units!);
        await this.adjust(c, r.assetId!, r.from, r.units!);
      }
      await c.query(`UPDATE events SET status = 'retracted' WHERE event_id = ANY($1)`, [rows.map((r) => r.event_id)]);
      await c.query(`UPDATE events SET status = 'applied', applied_at = now() WHERE event_id = $1`, [e.eventId]);
      const frontier = state.frontier < fromHeight - 1n ? state.frontier : fromHeight - 1n;
      await this.saveState(c, frontier, null, null);
      await this.hooks.beforeCommit?.();
      this.counters.retractionsApplied++;
      this.log('info', 'retraction rewinding', {
        eventId: e.eventId, fromHeight, undone: rows.filter((r) => r.status === 'applied').length, swept: rows.length, frontier,
      });
      return 'applied';
    });
  }

  // Header shared by both read endpoints; must run inside the caller's snapshot transaction.
  private async readHeader(c: pg.PoolClient) {
    const { rows: [s] } = await c.query(
      `SELECT s.frontier, s.finalized_through, s.blocked_at, count(e.event_id) AS waiting,
              count(e.event_id) FILTER (WHERE e.height > s.frontier) AS waiting_above
       FROM ledger_state s LEFT JOIN events e ON e.status = 'waiting'
       WHERE s.id = 1 GROUP BY s.frontier, s.finalized_through, s.blocked_at`,
    );
    const frontier = BigInt(s.frontier);
    const gapAt = big(s.blocked_at) ?? (Number(s.waiting_above) > 0 ? frontier + 1n : null);
    return { appliedThrough: frontier, finalizedThrough: BigInt(s.finalized_through), gapAt, waitingCount: Number(s.waiting) };
  }

  // Provisional = net change from applied events above finalized_through; finalized = balance - provisional.
  // `column` is one of two typed literals, never request input, so interpolating it is safe.
  private async positionRows(c: pg.PoolClient, finalizedThrough: bigint, column: 'asset_id' | 'investor_id', id: string) {
    const { rows } = await c.query(
      `SELECT h.asset_id, h.investor_id, h.units, coalesce(sum(d.delta), 0) AS provisional
       FROM holdings h LEFT JOIN (
         SELECT asset_id, to_investor AS investor_id, units AS delta FROM events
           WHERE status = 'applied' AND height > $1
         UNION ALL
         SELECT asset_id, from_investor, -units FROM events
           WHERE status = 'applied' AND height > $1 AND from_investor IS NOT NULL
       ) d USING (asset_id, investor_id)
       WHERE h.${column} = $2
       GROUP BY h.asset_id, h.investor_id, h.units
       ORDER BY h.asset_id, h.investor_id`,
      [finalizedThrough.toString(), id],
    );
    return rows
      .map((r) => ({ assetId: r.asset_id as string, investorId: r.investor_id as string, balance: BigInt(r.units), provisional: BigInt(r.provisional) }))
      .filter((r) => r.balance !== 0n || r.provisional !== 0n)
      .map((r) => ({ ...r, finalized: r.balance - r.provisional }));
  }

  holders(assetId: string) {
    return tx(this.pool, async (c) => {
      const header = await this.readHeader(c);
      const { rows: [a] } = await c.query('SELECT issued_supply FROM assets WHERE asset_id = $1', [assetId]);
      const rows = await this.positionRows(c, header.finalizedThrough, 'asset_id', assetId);
      return {
        assetId, issuedSupply: a ? BigInt(a.issued_supply) : 0n, ...header,
        holders: rows.map((r) => ({ investorId: r.investorId, finalized: r.finalized, provisional: r.provisional })),
      };
    }, READ_SNAPSHOT);
  }

  positions(investorId: string) {
    return tx(this.pool, async (c) => {
      const header = await this.readHeader(c);
      const rows = await this.positionRows(c, header.finalizedThrough, 'investor_id', investorId);
      return {
        investorId, ...header,
        positions: rows.map((r) => ({ assetId: r.assetId, finalized: r.finalized, provisional: r.provisional })),
      };
    }, READ_SNAPSHOT);
  }

  // Replays applied events into memory and compares with the derived tables. Empty list = consistent.
  reconcile(): Promise<string[]> {
    return tx(this.pool, async (c) => {
      const mismatches: string[] = [];
      const replay = new Map<string, bigint>();
      const supply = new Map<string, bigint>();
      const { rows: events } = await c.query(
        `SELECT * FROM events WHERE kind = 'transfer' AND status = 'applied' ORDER BY height`,
      );
      for (const e of events.map(rowToEvent)) {
        const move = (investor: string | null, delta: bigint) => {
          if (investor === null) return supply.set(e.assetId!, (supply.get(e.assetId!) ?? 0n) - delta);
          const key = json([e.assetId, investor]);
          const units = (replay.get(key) ?? 0n) + delta;
          if (units < 0n) mismatches.push(`replay negative at height ${e.height}: ${key} = ${units}`);
          replay.set(key, units);
        };
        move(e.from, -e.units!);
        move(e.to, e.units!);
      }
      const { rows: holdings } = await c.query('SELECT asset_id, investor_id, units FROM holdings');
      const stored = new Map(holdings.map((h) => [json([h.asset_id, h.investor_id]), BigInt(h.units)]));
      const totals = new Map<string, bigint>();
      for (const h of holdings) totals.set(h.asset_id, (totals.get(h.asset_id) ?? 0n) + BigInt(h.units));
      for (const key of new Set([...replay.keys(), ...stored.keys()])) {
        const want = replay.get(key) ?? 0n;
        const have = stored.get(key) ?? 0n;
        if (want !== have) mismatches.push(`holding ${key}: replay ${want}, stored ${have}`);
      }
      const { rows: assets } = await c.query('SELECT asset_id, issued_supply FROM assets');
      for (const a of assets) {
        const issued = BigInt(a.issued_supply);
        if (issued !== (supply.get(a.asset_id) ?? 0n)) mismatches.push(`supply ${a.asset_id}: replay ${supply.get(a.asset_id) ?? 0n}, stored ${issued}`);
        if (issued !== (totals.get(a.asset_id) ?? 0n)) mismatches.push(`supply ${a.asset_id}: holdings total ${totals.get(a.asset_id) ?? 0n}, issued ${issued}`);
      }
      return mismatches;
    }, READ_SNAPSHOT);
  }

  // Lag and stall checks, run on the drain timer. Each alert fires on entering its condition, not every tick.
  async monitor(now = Date.now()) {
    const { rows: [s] } = await this.pool.query(
      `SELECT frontier, highest_seen, (SELECT count(*) FROM events WHERE status = 'waiting') AS waiting
       FROM ledger_state WHERE id = 1`,
    );
    const frontier = BigInt(s.frontier);
    const lag = BigInt(s.highest_seen) - frontier;
    if (frontier !== this.lastFrontier) {
      this.lastFrontier = frontier;
      this.frontierMovedAt = now;
    }
    this.raise('lag_critical', lag >= this.lagCritical, { lag, frontier });
    this.raise('lag_warn', lag >= this.lagWarn && lag < this.lagCritical, { lag, frontier });
    this.raise('frontier_stalled', Number(s.waiting) > 0 && now - this.frontierMovedAt >= this.stallMs, { frontier });
  }

  private raise(type: string, active: boolean, detail: Record<string, unknown>) {
    if (active && !this.activeAlerts.has(type)) this.alert(type, detail);
    if (active) this.activeAlerts.add(type);
    else this.activeAlerts.delete(type);
  }

  async metrics() {
    const { rows: [s] } = await this.pool.query(
      `SELECT frontier, finalized_through, highest_seen,
         (SELECT extract(epoch FROM now() - min(received_at)) FROM events WHERE status = 'waiting') AS oldest_waiting
       FROM ledger_state WHERE id = 1`,
    );
    return {
      ...this.counters, frontier: BigInt(s.frontier), finalizedThrough: BigInt(s.finalized_through), highestSeen: BigInt(s.highest_seen),
      oldestWaitingAgeSeconds: s.oldest_waiting === null ? null : Number(s.oldest_waiting),
    };
  }
}
