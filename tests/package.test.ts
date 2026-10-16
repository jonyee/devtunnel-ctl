import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { access, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { record, redact } from '../src/core.ts';
import { verificationEnvironment } from '../scripts/package-check.ts';

const execute = promisify(execFile);
const packageName = '@jonathanyee/devtunnel-ctl';
const exists = (path: string) => access(path).then(() => true, () => false);

test('artifact consumer environment excludes publishing, cloud and SSH credentials', () => {
  const env = verificationEnvironment({
    PATH: '/bin', HOME: '/real-home', NPM_TOKEN: 'synthetic-secret', GITHUB_TOKEN: 'synthetic-secret',
    AZURE_CLIENT_SECRET: 'synthetic-secret', AWS_SESSION_TOKEN: 'synthetic-secret',
    SSH_AUTH_SOCK: '/private-agent', NODE_OPTIONS: '--require /private-module', NODE_PATH: '/private-modules',
    NODE_EXTRA_CA_CERTS: '/trusted/ca.pem',
  }, { home: '/isolated/home', cache: '/package-cache', registry: 'https://registry.npmjs.org/', userconfig: '/isolated/npmrc', temporary: '/isolated/tmp' });
  assert.equal(env.HOME, '/isolated/home');
  assert.equal(env.NPM_CONFIG_USERCONFIG, '/isolated/npmrc');
  assert.equal(env.NPM_CONFIG_CACHE, '/package-cache');
  assert.equal(env.NODE_OPTIONS, '');
  assert.equal(env.NODE_PATH, '');
  assert.equal(env.NODE_EXTRA_CA_CERTS, '/trusted/ca.pem');
  for (const key of ['NPM_TOKEN', 'GITHUB_TOKEN', 'AZURE_CLIENT_SECRET', 'AWS_SESSION_TOKEN', 'SSH_AUTH_SOCK']) {
    assert.equal(env[key], undefined);
  }
});

test('publishable metadata keeps the command name and targets the owned public npm scope', async () => {
  const pkg = JSON.parse(await readFile('package.json', 'utf8'));
  assert.equal(pkg.name, packageName);
  assert.notEqual(pkg.private, true);
  assert.equal(pkg.license, 'MIT');
  assert.deepEqual(pkg.bin, { 'devtunnel-ctl': 'dist/main.js' });
  assert.deepEqual(pkg.files, ['dist', 'npm-shrinkwrap.json']);
  assert.equal(pkg.engines.node, '>=24');
  assert.deepEqual(pkg.os, ['darwin', 'linux']);
  assert.deepEqual(pkg.publishConfig, { access: 'public', registry: 'https://registry.npmjs.org/' });
  assert.equal(pkg.repository.url, 'git+https://github.com/jonyee/devtunnel-ctl.git');
  assert.equal(pkg.scripts.prepack, 'npm run build');
  assert.equal(pkg.scripts['package:check'], 'node scripts/package-check.ts');
});

test('MIT license and publishable shrinkwrap preserve public runtime dependency origins', async () => {
  assert.equal(await exists('LICENSE'), true, 'MIT license must be included');
  const license = await readFile('LICENSE', 'utf8');
  assert.match(license, /MIT License/);
  assert.match(license, /Permission is hereby granted, free of charge/);
  assert.equal(await exists('npm-shrinkwrap.json'), true, 'global CLI installs need the publishable lock');
  assert.equal(await exists('package-lock.json'), false, 'maintain one authoritative lockfile');
  const lock = JSON.parse(await readFile('npm-shrinkwrap.json', 'utf8'));
  assert.equal(lock.name, packageName);
  assert.equal(lock.packages[''].name, packageName);
  assert.equal(lock.packages[''].license, 'MIT');
  for (const value of Object.values(lock.packages)) {
    assert.ok(record(value));
    if (value.resolved !== undefined) {
      assert.equal(typeof value.resolved, 'string');
      assert.equal(new URL(String(value.resolved)).hostname, 'registry.npmjs.org');
      assert.equal(typeof value.integrity, 'string');
    }
  }
});

test('CI validates packages and publication is manual, main-only, versioned and tokenless', async () => {
  assert.equal(await exists('.github/workflows/ci.yml'), true);
  assert.equal(await exists('.github/workflows/publish.yml'), true);
  const ci = await readFile('.github/workflows/ci.yml', 'utf8');
  const publish = await readFile('.github/workflows/publish.yml', 'utf8');
  assert.match(ci, /npm ci --ignore-scripts/);
  assert.match(ci, /npm run typecheck/);
  assert.match(ci, /npm test/);
  assert.match(publish, /^\s+workflow_dispatch:/m);
  assert.doesNotMatch(publish, /^\s+(push|pull_request|release):/m);
  assert.match(publish, /refs\/heads\/main/);
  assert.match(publish, /EXPECTED_VERSION/);
  assert.match(publish, /id-token: write/);
  assert.match(publish, /environment: npm/);
  assert.match(publish, /--verify-only/);
  assert.match(publish, /npm publish/);
  assert.match(publish, /--ignore-scripts --access public --registry=https:\/\/registry\.npmjs\.org\//);
  for (const workflow of [ci, publish]) {
    assert.match(workflow, /uses: actions\/checkout@[a-f0-9]{40}/);
    assert.match(workflow, /uses: actions\/setup-node@[a-f0-9]{40}/);
    assert.doesNotMatch(workflow, /NPM_TOKEN|NODE_AUTH_TOKEN/);
  }
});

test('the actual packed CLI installs independently from an empty cache and detects subsequent artifact tampering', { timeout: 120000 }, async () => {
  assert.equal(await exists('scripts/package-check.ts'), true, 'package checker must be available');
  const root = await mkdtemp(join(tmpdir(), 'dtcp-test-'));
  const output = join(root, 'artifacts');
  try {
    const result = await execute(process.execPath, ['scripts/package-check.ts', '--output', output], {
      timeout: 100000, maxBuffer: 1048576,
      env: {
        ...process.env,
        NPM_CONFIG_CACHE: join(root, 'cache'), npm_config_cache: join(root, 'cache'),
        NPM_CONFIG_REGISTRY: 'https://registry.npmjs.org/', npm_config_registry: 'https://registry.npmjs.org/',
      },
    }).catch(error => {
      const detail = record(error) && typeof error.stderr === 'string' ? redact(error.stderr) : 'no sanitized stderr';
      assert.fail(`Packed installation check failed: ${detail}`);
    });
    assert.ok(result);
    const report = JSON.parse(await readFile(join(output, 'result.json'), 'utf8'));
    assert.equal(report.name, packageName);
    assert.match(report.integrity, /^sha512-/);
    assert.equal(report.checks.help, true);
    assert.equal(report.checks.daemon, true);
    assert.equal(report.checks.pausedProfile, true);
    assert.equal(report.checks.dependencyIsolation, true);
    assert.ok(report.files.includes('dist/main.js'));
    assert.ok(report.files.includes('dist/cli.js'));
    assert.ok(report.files.includes('LICENSE'));
    assert.ok(report.files.includes('npm-shrinkwrap.json'));
    assert.equal(report.files.some((path: string) => /^(src|tests|scripts|node_modules|\.github)\//.test(path)), false);
    await writeFile(join(output, 'result.json'), JSON.stringify({ ...report, files: [...report.files, 'src/private.env'] }));
    await assert.rejects(execute(process.execPath, [
      'scripts/package-check.ts', '--output', output, '--verify-only',
    ], { timeout: 10000, maxBuffer: 65536 }), error =>
      record(error) && typeof error.stderr === 'string' && /inventory|files/i.test(error.stderr));
    await writeFile(join(output, 'result.json'), JSON.stringify(report));
    const tarball = join(output, report.filename);
    const bytes = await readFile(tarball);
    bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 1;
    await writeFile(tarball, bytes);
    await assert.rejects(execute(process.execPath, [
      'scripts/package-check.ts', '--output', output, '--verify-only',
    ], { timeout: 10000, maxBuffer: 65536 }), error =>
      record(error) && typeof error.stderr === 'string' && /integrity/i.test(error.stderr));
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('an incorrect expected release version fails before packing', async () => {
  assert.equal(await exists('scripts/package-check.ts'), true);
  const root = await mkdtemp(join(tmpdir(), 'dtcp-version-'));
  try {
    await assert.rejects(execute(process.execPath, [
      'scripts/package-check.ts', '--output', root, '--expected-version', '999.0.0',
    ], { timeout: 10000, maxBuffer: 65536 }), error =>
      record(error) && typeof error.stderr === 'string' && /version/i.test(error.stderr));
    assert.deepEqual(await readdir(root), []);
  } finally { await rm(root, { recursive: true, force: true }); }
});
