import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cli, help } from '../src/cli.ts';

test('help describes hosting without replacing existing client commands', () => {
  assert.match(help, /host TUNNEL/);
  assert.match(help, /--probe-port/);
  assert.match(help, /host-service/);
  assert.match(help, /profile add/);
});

test('invalid host arguments fail before initializing private POSIX state or connecting', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dtc-host-cli-'));
  const entry = join(root, 'main.js');
  try {
    for (const args of [
      ['host', 'bad/id'], ['host', 'example', '--probe-port', '0'],
      ['host', 'example', '--probe-port', '22', '--probe-port', '22'],
      ['host', 'example', 'extra'], ['host', 'example', '--local', '22'],
      ['host-service', 'explode', 'example'],
    ]) {
      await assert.rejects(cli([...args, '--helper', process.execPath, '--state-dir', join(root, 'state')], entry));
    }
    assert.deepEqual(await readdir(root), []);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('package installs on Windows and builds without POSIX chmod commands', async () => {
  const pkg = JSON.parse(await readFile('package.json', 'utf8'));
  assert.ok(pkg.os.includes('win32'));
  assert.doesNotMatch(pkg.scripts.build, /&& chmod/);
  const lock = JSON.parse(await readFile('npm-shrinkwrap.json', 'utf8'));
  assert.deepEqual(lock.packages[''].os, pkg.os);
});

test('native Windows rejects the client daemon explicitly before creating POSIX state', { skip: process.platform !== 'win32' }, async () => {
  await assert.rejects(cli(['daemon'], 'main.js'), /Windows.*host|host.*Windows/i);
});

test('a missing helper becomes a terminal host status instead of a launchd restart loop', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dtc-host-missing-helper-'));
  const log = console.log, error = console.error, output: string[] = [];
  console.log = (...args) => { output.push(args.join(' ')); };
  console.error = () => {};
  try {
    await cli(['host', 'example.usw2', '--helper', join(root, 'does-not-exist'),
      ...(process.platform === 'win32' ? [] : ['--state-dir', join(root, 'state')])], 'main.js');
    assert.ok(output.some(line => line.includes('"kind":"fault"') && line.includes('"code":"invalid-config"')));
    assert.equal(output.some(line => line.includes('"state":"verified"')), false);
  } finally {
    console.log = log; console.error = error;
    await rm(root, { recursive: true, force: true });
  }
});
