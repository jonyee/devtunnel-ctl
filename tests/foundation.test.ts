import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, stat, readFile, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { validateConfig, Store } from '../src/config.ts';
import { bounded, Fault, safeFault, redact } from '../src/core.ts';

const profile = { name: 'work', tunnelId: 'example-ssh.usw2', remotePort: 22, localPort: 23022, paused: false };
test('validates SSH profiles and normalizes qualified tunnel IDs', () => {
  const config = validateConfig({ version: 1, helperPath: '/bin/devtunnel', profiles: [profile] });
  assert.equal(config.profiles[0]?.tunnelId, 'example-ssh');
  assert.equal(config.profiles[0]?.clusterId, 'usw2');
  for (const value of [
    { ...profile, localPort: 0 }, { ...profile, tunnelId: '--help' },
    { ...profile, localPort: '22' }, { ...profile, paused: 'yes' },
    { ...profile, name: '../x' }, { ...profile, remotePort: 65536 },
  ]) assert.throws(() => validateConfig({ version: 1, helperPath: '/bin/devtunnel', profiles: [value] }));
  assert.throws(() => validateConfig({ version: 1, helperPath: 'devtunnel', profiles: [] }));
  assert.throws(() => validateConfig({ version: 1, helperPath: '/bin/devtunnel', profiles: [profile, profile] }));
});

test('stores desired pause state atomically with private permissions; rejects symlinks', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'dtc-'));
  try {
    const store = new Store(join(dir, 'state'));
    await store.init();
    await store.save({ version: 1, helperPath: '/bin/devtunnel', profiles: [profile] });
    const config = await store.load();
    config.profiles[0]!.paused = true;
    await store.save(config);
    assert.equal((await store.load()).profiles[0]?.paused, true);
    assert.equal((await stat(store.dir)).mode & 0o777, 0o700);
    assert.equal((await stat(store.configPath)).mode & 0o777, 0o600);
    assert.equal(JSON.parse(await readFile(store.configPath, 'utf8')).profiles[0].paused, true);
    await rm(store.configPath);
    await symlink(join(dir, 'outside'), store.configPath);
    await assert.rejects(store.load(), /private|symlink/i);
    await assert.rejects(store.save(config), /private|symlink/i);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('bounded operation aborts hung work and disposes late results', async () => {
  let release!: (v: string) => void;
  let disposed = '';
  const result = bounded(() => new Promise<string>(resolve => { release = resolve; }), 10,
    undefined, value => { disposed = value; });
  await assert.rejects(result, (e: unknown) => e instanceof Fault && e.code === 'timeout');
  release('late');
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(disposed, 'late');
  const c = new AbortController();
  const pending = bounded(() => new Promise(() => {}), 10000, c.signal);
  c.abort();
  await assert.rejects(pending, (e: unknown) => e instanceof Fault && e.code === 'aborted');
});

test('safe failures do not expose raw SDK/helper secrets and honor retry-after', () => {
  const secret = 'eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE5OTk5OTk5OTl9.signature';
  assert.equal(safeFault(new Error(`Authorization: Bearer ${secret}`)).message.includes(secret), false);
  assert.equal(redact(`token=${secret} Authorization: Bearer ${secret}`).includes(secret), false);
  assert.equal(redact('{"access_token":"opaque-secret","authorization": "Bearer opaque-secret"}').includes('opaque-secret'), false);
  assert.equal(safeFault({ response: { status: 403 } }).code, 'access-denied');
  assert.equal(safeFault({ errorContext: { statusCode: 401 } }).code, 'login-required');
  assert.equal(safeFault({ errorContext: { statusCode: 403 } }).code, 'access-denied');
  assert.equal(safeFault(new Error('No hosts are currently accepting connections for the tunnel.')).code, 'remote-unavailable');
  assert.equal(safeFault({ response: { status: 429, headers: { 'retry-after': '61' } } }).retryAfterMs, 61000);
});
