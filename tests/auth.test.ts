import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { TokenProvider, parseToken, runHelper } from '../src/auth.ts';
import { Fault } from '../src/core.ts';

const token = (exp = Math.floor(Date.now() / 1000) + 3600) =>
  `eyJhbGciOiJub25lIn0.${Buffer.from(JSON.stringify({ exp, scp: 'connect', clusterId: 'usw2' })).toString('base64url')}.signature`;

test('parses supported connect-token JSON envelopes without caching expired or malformed secrets', () => {
  const jwt = token();
  assert.equal(parseToken(JSON.stringify({ token: jwt })).value, jwt);
  assert.equal(parseToken(JSON.stringify(jwt)).value, jwt);
  assert.throws(() => parseToken(JSON.stringify({ token: token(1) })), /expired/i);
  assert.throws(() => parseToken(JSON.stringify({ accessToken: 'not-a-token' })));
  assert.throws(() => parseToken('raw-secret'));
});

test('a new token generation does not join an aborted flight', async () => {
  let calls = 0;
  const provider = new TokenProvider('/unused', async (_path, _args, signal) => {
    calls++;
    if (calls === 1) return new Promise<string>((_resolve, reject) => {
      signal!.addEventListener('abort', () => reject(new Fault('aborted')), { once: true });
    });
    return JSON.stringify({ token: token() });
  });
  const controller = new AbortController();
  const aborted = provider.get({ tunnelId: 'example' }, controller.signal);
  await new Promise(resolve => setImmediate(resolve));
  controller.abort();
  await assert.rejects(aborted);
  await provider.get({ tunnelId: 'example' });
  assert.equal(calls, 2);
});

test('token helper uses argv, coalesces acquisition, and refreshes only in memory', async () => {
  let calls = 0;
  const provider = new TokenProvider('/bin/devtunnel', async (_path, args) => {
    calls++;
    assert.deepEqual(args, ['token', 'example.usw2', '--scopes', 'connect', '--json']);
    return JSON.stringify({ token: token() });
  });
  const p = { tunnelId: 'example', clusterId: 'usw2' };
  const [one, two] = await Promise.all([provider.get(p), provider.get(p)]);
  assert.equal(one, two);
  assert.equal(calls, 1);
  await provider.get(p); assert.equal(calls, 1);
  await provider.get(p, undefined, true); assert.equal(calls, 2);
  provider.invalidate(p);
  await provider.get(p); assert.equal(calls, 3);
});

test('cancelling one coalesced token waiter does not abort the remaining waiter', async () => {
  let release!: (output: string) => void, aborted = false;
  const provider = new TokenProvider('/unused', async (_path, _args, signal) =>
    new Promise(resolve => {
      release = resolve;
      signal!.addEventListener('abort', () => { aborted = true; }, { once: true });
    }));
  const controller = new AbortController();
  const first = provider.get({ tunnelId: 'example' }, controller.signal);
  const second = provider.get({ tunnelId: 'example' });
  controller.abort();
  await assert.rejects(first);
  assert.equal(aborted, false);
  const value = token();
  release(JSON.stringify({ token: value }));
  assert.equal((await second).value, value);
});

test('helper bounds time/output and never includes raw stderr in errors', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-helper-'));
  const helper = join(dir, 'helper');
  try {
    await writeFile(helper, `#!${process.execPath}\nprocess.stderr.write('SECRET authorization token');process.exit(1);\n`, { mode: 0o700 });
    await assert.rejects(runHelper(helper, [], undefined, 100), error =>
      error instanceof Fault && !error.message.includes('SECRET'));
    await writeFile(helper, `#!${process.execPath}\nprocess.stdout.write('x'.repeat(100000));setInterval(()=>{},1000);\n`);
    await assert.rejects(runHelper(helper, [], undefined, 1000), /output|limit/i);
    await writeFile(helper, `#!${process.execPath}\nsetInterval(()=>{},1000);\n`);
    await assert.rejects(runHelper(helper, [], undefined, 20), /deadline/i);
    const c = new AbortController();
    const pending = runHelper(helper, [], c.signal, 1000);
    c.abort();
    await assert.rejects(pending, error => error instanceof Fault && error.code === 'aborted');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
