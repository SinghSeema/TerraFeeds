// Test console for the Ownership Ledger Service: a local page for testing by hand and seeing what the tests check.
// Optional, and not part of the service: the build and the Docker image use src/ only.
//
//   npm run demo        then open http://localhost:4000
//
// It serves index.html and forwards /a/* to the API on :3000 and /b/* to the test viewer on :3001, so the page
// talks to one origin and the service needs no CORS change. /run/* runs a fixed set of commands in the repo;
// nothing from the page is ever passed to a shell. No dependencies: Node 20 only.
import { spawn } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, '..');
const PORT = Number(process.env.DEMO_PORT ?? 4000);
const TARGETS = { a: 'http://127.0.0.1:3000', b: 'http://127.0.0.1:3001' };
const VIEWER = 'ledger-test-view';
const SWITCHES = ['uniqueEventId', 'frontierLock', 'tombstones', 'ingestBarrier', 'negativeCheck', 'finalityGuard'];

function run(cmd, args, env = {}) {
  return new Promise((done) => {
    const started = Date.now();
    let output = '';
    let settled = false;
    const finish = (code) => {
      if (settled) return;
      settled = true;
      done({ code, ms: Date.now() - started, output: output.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, '') });
    };
    const child = spawn(cmd, args, { cwd: REPO, env: { ...process.env, NO_COLOR: '1', FORCE_COLOR: '0', ...env } });
    child.stdout.on('data', (d) => { output += d; });
    child.stderr.on('data', (d) => { output += d; });
    child.on('error', (err) => { output += `\n${err.message}`; finish(-1); });
    child.on('close', finish);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function healthy(base, waitMs) {
  const end = Date.now() + waitMs;
  do {
    try {
      if ((await fetch(`${base}/health`)).ok) return true;
    } catch { /* not up yet */ }
    await sleep(500);
  } while (Date.now() < end);
  return false;
}

const stopViewer = () => run('docker', ['rm', '-f', VIEWER]);

// A second API container that reads ledger_test, so the endpoints show what the last test run left behind.
async function startViewer() {
  await stopViewer();
  const r = await run('docker', ['compose', 'run', '-d', '--rm', '--name', VIEWER, '-p', '3001:3000',
    '-e', 'DATABASE_URL=postgres://ledger:ledger@db:5432/ledger_test', '-e', 'INSTANCE_NAME=test-view', 'api']);
  const ok = r.code === 0 && await healthy(TARGETS.b, 30_000);
  const note = ok ? 'Test viewer is up on localhost:3001 (reads ledger_test).'
    : 'Test viewer did not start. Run the passing scenario first so ledger_test exists.';
  return { ok, ms: r.ms, note, output: r.output };
}

const actions = {
  async 'viewer-start'() {
    const v = await startViewer();
    return { ok: v.ok, code: v.ok ? 0 : 1, ms: v.ms, output: `${v.output}\n${v.note}`, banner: v.note };
  },
  async 'viewer-stop'() {
    const r = await stopViewer();
    return { ok: true, code: 0, ms: r.ms, output: 'docker rm -f ledger-test-view', banner: 'Test viewer stopped.' };
  },
  // The viewer is stopped during a run: it is a live ledger instance and would drain ledger_test while the test resets it.
  async test({ seed }) {
    if (!/^\d{1,6}$/.test(String(seed))) throw new Error('Seed must be a whole number.');
    await stopViewer();
    const r = await run('npx', ['vitest', 'run', '-t', 'all protections'], { SEEDS: String(seed) });
    const v = await startViewer();
    return {
      ok: r.code === 0, code: r.code, ms: r.ms + v.ms, output: `${r.output}\n${v.note}`,
      banner: r.code === 0 ? `Seed ${seed} matches the oracle. No invariant broken.` : `Seed ${seed} failed. See the output below.`,
    };
  },
  async proof({ name }) {
    if (!SWITCHES.includes(name)) throw new Error('Unknown protection.');
    await stopViewer();
    const r = await run('npx', ['vitest', 'run', '-t', `${name} is switched off`]);
    const v = await startViewer();
    return {
      ok: r.code === 0, code: r.code, ms: r.ms + v.ms, output: `${r.output}\n${v.note}`,
      banner: r.code === 0 ? `Proof holds: with ${name} off the scenario broke, as it must.`
        : `With ${name} off the scenario did not break. The test does not prove this protection.`,
    };
  },
  async reset() {
    await stopViewer();
    const down = await run('docker', ['compose', 'down', '-v']);
    const up = await run('docker', ['compose', 'up', '-d', '--build', '--wait']);
    return {
      ok: up.code === 0, code: up.code, ms: down.ms + up.ms, output: `${down.output}\n${up.output}`,
      banner: up.code === 0 ? 'Both databases wiped. The API is back on :3000 with an empty ledger.' : 'The stack did not come back up. See the output.',
    };
  },
};

function readBody(req) {
  return new Promise((done, fail) => {
    let body = '';
    req.on('data', (d) => { body += d; });
    req.on('end', () => done(body));
    req.on('error', fail);
  });
}

function sendJson(res, status, value) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(JSON.stringify(value));
}

let running = false;

createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const body = await readBody(req);
    if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      return res.end(await readFile(join(HERE, 'index.html')));
    }
    const forward = url.pathname.match(/^\/(a|b)(\/.*)$/);
    if (forward) {
      const r = await fetch(`${TARGETS[forward[1]]}${forward[2]}${url.search}`, {
        method: req.method, headers: body ? { 'content-type': 'application/json' } : {}, body: body || undefined,
      });
      res.writeHead(r.status, { 'content-type': r.headers.get('content-type') ?? 'text/plain' });
      return res.end(await r.text());
    }
    const action = url.pathname.match(/^\/run\/([\w-]+)$/);
    if (action && req.method === 'POST' && Object.hasOwn(actions, action[1])) {
      if (running) return sendJson(res, 409, { ok: false, code: -1, ms: 0, output: '', banner: 'Another command is still running.' });
      running = true;
      try {
        return sendJson(res, 200, await actions[action[1]](body ? JSON.parse(body) : {}));
      } finally {
        running = false;
      }
    }
    sendJson(res, 404, { error: 'not found' });
  } catch (err) {
    sendJson(res, 502, { error: err?.cause?.code ?? err.message });
  }
}).listen(PORT, '127.0.0.1', () => {
  console.log(`Ledger test console:http://localhost:${PORT}   (repo: ${REPO})`);
});
