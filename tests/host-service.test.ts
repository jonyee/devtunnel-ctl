import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, readFile, mkdir, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { hostLaunchAgent, hostJobPaths, serveHost, readHostState } from '../src/host-service.ts';

const options = { tunnelId: 'example', clusterId: 'usw2', helperPath: '/opt/dev & tunnel', probePorts: [22, 3000] };

test('host launchd jobs pin arguments and do not restart terminal successful exits', () => {
  const plist = hostLaunchAgent({ label: 'local.host.test', node: '/absolute/node', entry: '/app/main.js',
    state: '/private/state', options });
  assert.match(plist, /<key>SuccessfulExit<\/key><false\/>/);
  assert.doesNotMatch(plist, /<key>KeepAlive<\/key><true\/>/);
  assert.match(plist, /<string>host<\/string>/);
  assert.match(plist, /\/opt\/dev &amp; tunnel/);
  assert.equal((plist.match(/<string>--probe-port<\/string>/g) ?? []).length, 2);
  assert.doesNotMatch(plist, /\/bin\/sh|daemon<\/string>/);
  assert.throws(() => hostLaunchAgent({ label: 'x', node: 'node', entry: '/main.js', state: '/state', options }));
});

test('host jobs have stable separate identities without reusing the client daemon state', () => {
  const first = hostJobPaths('/state', options);
  assert.equal(first.dir.startsWith('/state/hosts/'), true);
  assert.notEqual(first.label, hostJobPaths('/state', { ...options, clusterId: 'use2' }).label);
  assert.notEqual(first.label, hostJobPaths('/other', options).label);
  assert.equal(first.label, hostJobPaths('/state', { ...options, probePorts: [] }).label);
});

test('host state is private and never claims a stopped job is responsive', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dtc-host-state-'));
  try {
    await serveHost(options, root, async (_options, report) => {
      report({ kind: 'status', state: 'connected', relayConnected: true, ports: [22, 3000],
        localUnavailable: [], verifiedPorts: [] });
    }, () => {});
    const dir = hostJobPaths(root, options).dir;
    assert.equal((await readHostState(dir, process.pid)).responsive, false);
    const snapshot = JSON.parse(await readFile(join(dir, 'status.json'), 'utf8'));
    assert.equal(snapshot.running, false);
    assert.equal(snapshot.message.state, 'connected');
    assert.equal(JSON.stringify(snapshot).includes('helperPath'), false);
    assert.equal((await readHostState(dir, process.pid + 1)).responsive, false);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('private log failure aborts hosting instead of leaving an unlogged supervisor running', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dtc-host-log-failure-'));
  const marker = join(root, 'do-not-write');
  try {
    const dir = hostJobPaths(root, options).dir;
    await mkdir(dir, { recursive: true, mode: 0o700 });
    await writeFile(marker, 'untouched');
    await symlink(marker, join(dir, 'service.log'));
    await assert.rejects(serveHost(options, root, async (_options, report, signal) => {
      assert.ok(signal);
      report({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 });
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
    }, () => {}), /private host/i);
    assert.equal(await readFile(marker, 'utf8'), 'untouched');
  } finally { await rm(root, { recursive: true, force: true }); }
});
