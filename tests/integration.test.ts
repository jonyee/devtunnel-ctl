import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm, readFile } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import net from 'node:net';
import { once } from 'node:events';
import { request } from '../src/rpc.ts';

const main = resolve('src/main.ts');
async function cli(dir: string, ...args: string[]): Promise<{ code: number | null; out: string; err: string }> {
  const child = spawn(process.execPath, [main, '--state-dir', dir, ...args], { stdio: ['ignore', 'pipe', 'pipe'] });
  let out = '', err = '';
  child.stdout.on('data', b => { out += b; }); child.stderr.on('data', b => { err += b; });
  const [code] = await once(child, 'close');
  return { code, out, err };
}
async function ready(dir: string, child: ReturnType<typeof spawn>): Promise<void> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) throw new Error('Verification daemon exited before readiness.');
    try { await request(join(dir, 'control.sock'), { v: 1, command: 'status' }); return; }
    catch { await new Promise(resolve => setTimeout(resolve, 25)); }
  }
  throw new Error('Verification daemon did not become responsive.');
}

test('actual CLI/daemon subprocesses preserve paused state, reject absent daemon and forward SSH using isolated fake transport', { timeout: 20000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dtc-integration-')), dir = join(root, 'state');
  const helper = join(root, 'devtunnel-helper');
  const jwt = `eyJhbGciOiJub25lIn0.${Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, clusterId: 'usw2' })).toString('base64url')}.signature`;
  await writeFile(helper, `#!${process.execPath}\nif(process.argv[2]!=='token')process.exit(1);process.stdout.write(JSON.stringify({token:${JSON.stringify(jwt)}}));\n`, { mode: 0o700 });
  let sshResponding = true;
  const remoteSockets = new Set<net.Socket>();
  const remote = net.createServer(s => {
    remoteSockets.add(s);
    s.once('close', () => remoteSockets.delete(s));
    s.resume();
    if (sshResponding) s.end('SSH-2.0-isolated-test\r\n');
  });
  remote.listen(0, '127.0.0.1'); await once(remote, 'listening');
  const address = remote.address(); assert.ok(address && typeof address !== 'string');
  let daemon: ReturnType<typeof spawn> | undefined;
  let stderr = '';
  const launch = async () => {
    daemon = spawn(process.execPath, [resolve('tests/fixtures/daemon.ts'), dir, String(address.port)], { stdio: ['ignore', 'pipe', 'pipe'] });
    daemon.stderr!.on('data', b => { stderr += b; });
    await ready(dir, daemon);
  };
  const stop = async () => {
    if (daemon && daemon.exitCode === null) { const closed = once(daemon, 'close'); daemon.kill('SIGTERM'); await closed; }
    daemon = undefined;
  };
  try {
    const invalidArgs = await cli(dir, '--unknown-option');
    assert.notEqual(invalidArgs.code, 0);
    assert.match(invalidArgs.err, /invalid-config.*help/i);
    const badHelper = await cli(dir, 'init', '--helper', join(root, 'not-installed'));
    assert.notEqual(badHelper.code, 0);
    assert.match(badHelper.err, /invalid-config.*executable/i);
    const init = await cli(dir, 'init', '--helper', helper);
    assert.equal(init.code, 0, init.err);
    if (process.platform === 'darwin' && process.getuid?.() !== 0) {
      const serviceStatus = await cli(dir, 'service', 'status');
      assert.equal(serviceStatus.code, 0, serviceStatus.err);
      assert.equal(JSON.parse(serviceStatus.out).installed, false);
      assert.equal(JSON.parse(serviceStatus.out).loaded, false);
    }
    const absent = await cli(dir, 'status', '--json');
    assert.notEqual(absent.code, 0);
    assert.match(absent.err, /Daemon unavailable/i);
    await launch();
    const duplicate = await cli(dir, 'daemon');
    assert.notEqual(duplicate.code, 0);
    const local = net.createServer(); local.listen(0, '127.0.0.1'); await once(local, 'listening');
    const la = local.address(); assert.ok(la && typeof la !== 'string');
    await new Promise<void>(r => local.close(() => r()));
    const added = await cli(dir, 'profile', 'add', 'test', '--tunnel', 'example.usw2', '--local', String(la.port), '--paused');
    assert.equal(added.code, 0, added.err);
    assert.equal((await cli(dir, 'reconnect', 'test')).code, 0);
    assert.equal(JSON.parse((await cli(dir, 'status', '--json')).out).profiles[0].state, 'paused');
    await stop(); await launch();
    assert.equal(JSON.parse((await cli(dir, 'status', '--json')).out).profiles[0].state, 'paused');
    assert.equal((await cli(dir, 'resume', 'test')).code, 0);
    const deadline = Date.now() + 5000;
    let status;
    do {
      status = JSON.parse((await cli(dir, 'status', '--json')).out);
      if (status.profiles[0].state === 'healthy') break;
      await new Promise(r => setTimeout(r, 25));
    } while (Date.now() < deadline);
    assert.equal(status.profiles[0].state, 'healthy', stderr);
    const ssh = net.connect(la.port, '127.0.0.1');
    const [data] = await once(ssh, 'data'); assert.match(data.toString(), /^SSH-2\.0/); ssh.destroy();
    sshResponding = false;
    assert.equal((await cli(dir, 'reconnect', 'test')).code, 0);
    const failedDeadline = Date.now() + 8000;
    do {
      status = JSON.parse((await cli(dir, 'status', '--json')).out);
      if (status.profiles[0].state === 'remote-unavailable') break;
      await new Promise(r => setTimeout(r, 25));
    } while (Date.now() < failedDeadline);
    const hint = 'Tunnel: connected; SSH: no response. Check remote SSH/WSL.';
    assert.equal(status.profiles[0].state, 'remote-unavailable', stderr);
    assert.equal(status.profiles[0].relayConnected, true);
    assert.equal(status.profiles[0].cause.message, hint);
    const humanStatus = await cli(dir, 'status', 'test');
    assert.equal(humanStatus.code, 0, humanStatus.err);
    assert.equal(humanStatus.out.includes(hint), true);
    const healthLogs = await cli(dir, 'logs');
    assert.equal(healthLogs.code, 0, healthLogs.err);
    assert.equal(healthLogs.out.includes(hint), true);
    assert.equal((await cli(dir, 'pause', 'test')).code, 0);
    assert.equal(JSON.parse(await readFile(join(dir, 'config.json'), 'utf8')).profiles[0].paused, true);
    const crashed = once(daemon!, 'close');
    daemon!.kill('SIGKILL');
    await crashed;
    daemon = undefined;
    await launch();
    assert.equal(JSON.parse((await cli(dir, 'status', '--json')).out).profiles[0].state, 'paused');
    const logs = await cli(dir, 'logs');
    assert.equal(logs.code, 0); assert.equal(logs.out.includes(jwt), false);
    const unknown = await cli(dir, 'pause', 'missing');
    assert.notEqual(unknown.code, 0);
    await stop();
    assert.equal(stderr, '');
  } finally {
    await stop();
    for (const socket of remoteSockets) socket.destroy();
    await new Promise<void>(r => remote.close(() => r()));
    await rm(root, { recursive: true, force: true });
  }
});
