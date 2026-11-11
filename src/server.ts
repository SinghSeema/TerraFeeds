import { fileURLToPath } from 'node:url';
import Fastify, { type FastifyReply } from 'fastify';
import { createPool, migrate } from './db.js';
import { BadMessage, json, Ledger } from './ledger.js';

// Routes only; every route is one call into the ledger. Exported so the test can send requests with inject().
export function buildApp(ledger: Ledger, logger = true) {
  const app = Fastify({ logger });
  // Units are bigint end to end; they go out as JSON strings so no client parses them into a lossy float.
  const send = (reply: FastifyReply, body: unknown) => reply.type('application/json').send(json(body));

  // Stand-in for the queue consumer: 202 is the ack, sent only after the ingest transaction has committed.
  app.post('/events', async (req, reply) => {
    try {
      const outcome = await ledger.ingest(req.body);
      send(reply.code(202), { outcome });
    } catch (err) {
      if (err instanceof BadMessage) return send(reply.code(400), { error: err.message });
      throw err;
    }
    void ledger.drain().catch((err) => app.log.error(err, 'drain failed'));
  });

  app.get<{ Params: { assetId: string } }>('/assets/:assetId/holders', async (req, reply) =>
    send(reply, await ledger.holders(req.params.assetId)));
  app.get<{ Params: { investorId: string } }>('/investors/:investorId/positions', async (req, reply) =>
    send(reply, await ledger.positions(req.params.investorId)));
  app.get('/metrics', async (_req, reply) => send(reply, await ledger.metrics()));
  app.get('/health', async () => ({ ok: true }));
  return app;
}

// Started as a process (node dist/server.js), not when imported by the test.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const pool = createPool();
  await migrate(pool);
  const ledger = new Ledger(pool);
  const app = buildApp(ledger);

  // The timer catches anything an ingest-triggered drain missed (a lost trigger, a peer instance's events).
  setInterval(() => {
    ledger.drain().then(() => ledger.monitor()).catch((err) => app.log.error(err, 'drain/monitor failed'));
  }, Number(process.env.DRAIN_INTERVAL_MS ?? '1000'));

  await app.listen({ host: '0.0.0.0', port: 3000 });
}
