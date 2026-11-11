import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

// Resolves from both src/ (tests) and dist/ (container), since src/ is shipped alongside dist/.
const SCHEMA = readFileSync(join(dirname(fileURLToPath(import.meta.url)), '../src/schema.sql'), 'utf8');
const MIGRATION_LOCK = 4201;

export function createPool(connectionString = process.env.DATABASE_URL): pg.Pool {
  return new pg.Pool({ connectionString });
}

export async function migrate(pool: pg.Pool, genesisFrontier = BigInt(process.env.GENESIS_FRONTIER ?? '0')) {
  await tx(pool, async (c) => {
    // Several instances may start at once; CREATE ... IF NOT EXISTS is not race-safe on its own.
    await c.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK]);
    await c.query(SCHEMA);
    await c.query(
      'INSERT INTO ledger_state (id, frontier, highest_seen) VALUES (1, $1, $1) ON CONFLICT (id) DO NOTHING',
      [genesisFrontier.toString()],
    );
  });
}

export async function tx<T>(pool: pg.Pool, fn: (c: pg.PoolClient) => Promise<T>, begin = 'BEGIN'): Promise<T> {
  const c = await pool.connect();
  try {
    await c.query(begin);
    const result = await fn(c);
    await c.query('COMMIT');
    return result;
  } catch (err) {
    await c.query('ROLLBACK');
    throw err;
  } finally {
    c.release();
  }
}
