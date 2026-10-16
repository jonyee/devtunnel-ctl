import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, stat, rm, mkdir, writeFile } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import net from 'node:net';
import { once } from 'node:events';
import { RpcServer, request } from '../src/rpc.ts';
import { Log } from '../src/log.ts';
import { Store } from '../src/config.ts';
import { DaemonLock } from '../src/lock.ts';
import { Fault } from '../src/core.ts';

test('private bounded Unix RPC validates requests and refuses duplicate daemons', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-rpc-'));
  const store = new Store(dir); await store.init();
  const socket = join(dir, 'control.sock');
  const lock = new DaemonLock(dir);
  await lock.acquire();
  await assert.rejects(new DaemonLock(dir).acquire(), /daemon|lock/i);
  const rpc = new RpcServer(socket, async req => ({ command: req.command }), () => {});
  await rpc.listen();
  try {
    assert.equal((await stat(socket)).mode & 0o777, 0o600);
    assert.deepEqual(await request(socket, { v: 1, command: 'status' }), { command: 'status' });
    await assert.rejects(request(socket, { v: 1, command: 'rm-everything' }), /control|message/i);
    const bad = net.connect(socket);
    let reply = '';
    bad.on('data', b => { reply += b; });
    bad.write('{not-json}\n');
    await once(bad, 'close');
    assert.equal(JSON.parse(reply).error.code, 'protocol');
    const client = net.connect(socket);
    client.on('error', () => {});
    client.write('x'.repeat(65536));
    await new Promise<void>(resolve => client.once('close', () => resolve()));
    const duplicate = new RpcServer(socket, async () => ({}), () => {});
    await assert.rejects(duplicate.listen(), /daemon|socket/i);
    assert.deepEqual(await request(socket, { v: 1, command: 'status' }), { command: 'status' });
  } finally { await rpc.close(); await lock.release(); await rm(dir, { recursive: true, force: true }); }
  await assert.rejects(request(socket, { v: 1, command: 'status' }), /Daemon unavailable/i);
});

test('RPC preserves actionable error codes and bounds hung handlers with uncertainty warning', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-rpc-bound-'));
  const socket = join(dir, 'control.sock');
  const rpc = new RpcServer(socket, async req => {
    if (req.command === 'status') throw new Fault('access-denied');
    return new Promise(() => {});
  }, () => {});
  await rpc.listen();
  try {
    await assert.rejects(request(socket, { v: 1, command: 'status' }),
      error => error instanceof Fault && error.code === 'access-denied');
    await assert.rejects(request(socket, { v: 1, command: 'pause', name: 'all' }, 20),
      /may have completed/i);
  } finally { await rpc.close(); await rm(dir, { recursive: true, force: true }); }
});

test('sanitized logs rotate within a fixed disk bound and never expose tokens', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-log-'));
  const log = new Log(dir, 1024);
  try {
    await log.init();
    for (let i = 0; i < 50; i++) await log.write({ event: 'state', profile: 'test', message: `token=private-${i} Authorization: Bearer PRIVATE` });
    await log.flush();
    const text = await readFile(join(dir, 'service.log'), 'utf8');
    const old = await readFile(join(dir, 'service.log.1'), 'utf8');
    assert.equal((text + old).includes('PRIVATE'), false);
    assert.equal((text + old).includes('private-'), false);
    assert.ok(Buffer.byteLength(text) <= 1024);
    assert.ok(Buffer.byteLength(old) <= 1024);
    assert.equal((await stat(join(dir, 'service.log'))).mode & 0o777, 0o600);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('stale daemon locks are reclaimed atomically and racing starters never both own the lock', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-stale-'));
  const child = spawn(process.execPath, ['-e', ''], { stdio: 'ignore' });
  await once(child, 'close');
  assert.ok(child.pid);
  await mkdir(join(dir, 'daemon.lock'), { mode: 0o700 });
  await writeFile(join(dir, 'daemon.lock', 'owner-stale.json'), JSON.stringify({ pid: child.pid, nonce: 'stale' }), { mode: 0o600 });
  const first = new DaemonLock(dir), second = new DaemonLock(dir);
  try {
    const results = await Promise.allSettled([first.acquire(), second.acquire()]);
    assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
    const live = new DaemonLock(dir);
    await assert.rejects(live.acquire(), /daemon|lock/i);
  } finally {
    await first.release(); await second.release();
    await rm(dir, { recursive: true, force: true });
  }
});
