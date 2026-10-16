import { execFile, spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { promisify, parseArgs } from 'node:util';
import { readFile, writeFile, mkdir, mkdtemp, readdir, realpath, rm } from 'node:fs/promises';
import { dirname, join, resolve, basename, parse } from 'node:path';
import { homedir, tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { bounded, record, redact } from '../src/core.ts';

const root = fileURLToPath(new URL('../', import.meta.url));
const packageName = '@jonathanyee/devtunnel-ctl';
const registry = 'https://registry.npmjs.org/';
const execute = promisify(execFile);
const roots = new Set(['package.json', 'npm-shrinkwrap.json', 'README.md', 'LICENSE']);

export function verificationEnvironment(parent: NodeJS.ProcessEnv,
  paths: { home: string; cache: string; registry: string; userconfig: string; temporary: string }): NodeJS.ProcessEnv {
  return {
    PATH: dirname(process.execPath) + ':' + (parent.PATH ?? ''),
    HOME: paths.home, TMPDIR: paths.temporary, LANG: parent.LANG ?? 'C', CI: 'true',
    NODE_PATH: '', NODE_OPTIONS: '',
    NPM_CONFIG_USERCONFIG: paths.userconfig, npm_config_userconfig: paths.userconfig,
    NPM_CONFIG_CACHE: paths.cache, npm_config_cache: paths.cache,
    NPM_CONFIG_REGISTRY: paths.registry, npm_config_registry: paths.registry,
  };
}

function invalid(message: string): never { throw new Error(message); }
async function json(path: string): Promise<unknown> { return JSON.parse(await readFile(path, 'utf8')); }

async function metadata(expected?: string): Promise<{ name: string; version: string }> {
  const pkg = await json(join(root, 'package.json'));
  if (!record(pkg) || pkg.name !== packageName || pkg.private === true || pkg.license !== 'MIT' ||
      !record(pkg.bin) || pkg.bin['devtunnel-ctl'] !== 'dist/main.js' ||
      !record(pkg.publishConfig) || pkg.publishConfig.registry !== registry || pkg.publishConfig.access !== 'public' ||
      !record(pkg.repository) || pkg.repository.url !== 'git+https://github.com/jonyee/devtunnel-ctl.git' ||
      !Array.isArray(pkg.files) || pkg.files.length !== 2 || pkg.files[0] !== 'dist' || pkg.files[1] !== 'npm-shrinkwrap.json') {
    invalid('Package metadata is not ready for the intended public npm package.');
  }
  if (typeof pkg.version !== 'string' || !/^\d+\.\d+\.\d+(?:-[a-zA-Z0-9.-]+)?$/.test(pkg.version)) {
    invalid('Package version must be a release or prerelease semver.');
  }
  if (expected !== undefined && expected !== pkg.version) invalid('Expected release version does not match package.json.');
  const lock = await json(join(root, 'npm-shrinkwrap.json'));
  if (!record(lock) || lock.name !== pkg.name || !record(lock.packages)) invalid('Publishable dependency lock is missing or stale.');
  for (const value of Object.values(lock.packages)) {
    if (!record(value)) invalid('Invalid dependency lock entry.');
    if (value.resolved !== undefined) {
      if (typeof value.resolved !== 'string' || !value.resolved.startsWith(registry) || typeof value.integrity !== 'string') {
        invalid('Dependency lock must contain only public npm tarball origins with integrity hashes.');
      }
    }
  }
  return { name: pkg.name, version: pkg.version };
}

interface Report {
  name: string;
  version: string;
  filename: string;
  integrity: string;
  files: string[];
  checks: { help: boolean; daemon: boolean; pausedProfile: boolean; dependencyIsolation: boolean };
}

function inventory(files: string[]): void {
  for (const path of files) {
    if (!(roots.has(path) || /^dist\/[a-zA-Z0-9/_-]+\.(?:js|d\.ts|js\.map)$/.test(path))) {
      invalid('Artifact file inventory contains files outside the publishable runtime and metadata.');
    }
  }
  for (const required of [...roots, 'dist/main.js', 'dist/cli.js']) {
    if (!files.includes(required)) invalid('Artifact file inventory is missing required file: ' + required);
  }
}

async function integrity(path: string): Promise<string> {
  return 'sha512-' + createHash('sha512').update(await readFile(path)).digest('base64');
}

async function verifiedArtifact(output: string, pkg: { name: string; version: string }): Promise<Report> {
  const report = await json(join(output, 'result.json'));
  if (!record(report) || report.name !== pkg.name || report.version !== pkg.version ||
      typeof report.filename !== 'string' || report.filename !== basename(report.filename) ||
      !/^[a-zA-Z0-9][a-zA-Z0-9._+-]*\.tgz$/.test(report.filename) ||
      typeof report.integrity !== 'string' || !Array.isArray(report.files) ||
      !report.files.every((path: unknown) => typeof path === 'string') || !record(report.checks) ||
      report.checks.help !== true || report.checks.daemon !== true ||
      report.checks.pausedProfile !== true || report.checks.dependencyIsolation !== true) {
    invalid('Artifact report is missing, incomplete, or belongs to a different package/version.');
  }
  const files = report.files.filter((path): path is string => typeof path === 'string');
  inventory(files);
  if (await integrity(join(output, report.filename)) !== report.integrity) invalid('Artifact integrity verification failed.');
  return {
    name: pkg.name, version: pkg.version, filename: report.filename, integrity: report.integrity,
    files,
    checks: { help: true, daemon: true, pausedProfile: true, dependencyIsolation: true },
  };
}

async function command(file: string, args: string[], cwd: string, env: NodeJS.ProcessEnv,
  signal?: AbortSignal, timeout = 15000): Promise<string> {
  try {
    return (await execute(file, args, { cwd, env, ...(signal ? { signal } : {}), timeout, maxBuffer: 1048576 })).stdout;
  } catch (error) {
    const detail = record(error) && typeof error.stderr === 'string' ? redact(error.stderr) : 'deadline or process failure';
    throw new Error(`${basename(file)} failed: ${detail}`);
  }
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>(resolve => child.once('close', () => resolve()));
  child.kill('SIGTERM');
  try { await bounded(() => closed, 5000); }
  catch (error) {
    child.kill('SIGKILL');
    await bounded(() => closed, 2000);
    throw error;
  }
}

async function consumer(prefix: string, temporary: string, env: NodeJS.ProcessEnv, signal: AbortSignal,
  pkg: { name: string; version: string }): Promise<void> {
  const canonicalPrefix = await realpath(prefix);
  const installed = join(canonicalPrefix, 'lib', 'node_modules', '@jonathanyee', 'devtunnel-ctl');
  const installedMetadata = await json(join(installed, 'package.json'));
  if (!record(installedMetadata) || installedMetadata.name !== pkg.name || installedMetadata.version !== pkg.version) {
    invalid('Installed package identity/version does not match the checked artifact.');
  }
  const bin = join(prefix, 'bin', 'devtunnel-ctl');
  if (!(await realpath(bin)).startsWith(installed + '/')) invalid('Installed command unexpectedly points outside its prefix.');
  const require = createRequire(join(installed, 'package.json'));
  for (const name of ['@microsoft/dev-tunnels-connections', '@microsoft/dev-tunnels-management']) {
    if (!require.resolve(name).startsWith(canonicalPrefix + '/')) invalid('Installed CLI depends on modules outside its isolated prefix.');
  }
  for (const name of ['src', 'tests', 'scripts', 'node_modules/typescript']) {
    try {
      await realpath(join(installed, name));
      invalid('Installed CLI unexpectedly contains development files or dependencies.');
    } catch (error) {
      if (!(record(error) && error.code === 'ENOENT')) throw error;
    }
  }
  const cli = (args: string[]) => command(bin, args, temporary, env, signal);
  if (!(await cli(['--help'])).includes('resilient SSH-only')) invalid('Installed command help did not execute.');
  const state = join(temporary, 's');
  await cli(['--state-dir', state, 'init', '--helper', process.execPath]);
  const child = spawn(bin, ['--state-dir', state, 'daemon'], { cwd: temporary, env, stdio: ['ignore', 'pipe', 'pipe'] });
  let errorOutput = '', launchError = false;
  child.on('error', () => { launchError = true; });
  child.stdout?.resume();
  child.stderr?.on('data', (chunk: Buffer) => { errorOutput = (errorOutput + chunk.toString()).slice(-4096); });
  try {
    const deadline = Date.now() + 5000;
    let ready = false, lastStatusFailure = '';
    while (Date.now() < deadline && !signal.aborted) {
      if (launchError || child.exitCode !== null || child.signalCode !== null) invalid('Installed daemon exited before readiness: ' + redact(errorOutput));
      const result = await command(bin, ['--state-dir', state, 'status', '--json'], temporary, env, signal,
        Math.max(1, deadline - Date.now())).then(text => ({ text }), error => {
          lastStatusFailure = redact(error instanceof Error ? error.message : 'status command failed');
          return undefined;
        });
      if (result) {
        const status: unknown = JSON.parse(result.text);
        if (!record(status) || !Array.isArray(status.profiles) || status.profiles.length !== 0) invalid('Unexpected installed daemon status.');
        ready = true; break;
      }
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    if (!ready) invalid('Installed daemon did not become responsive before the deadline: ' + lastStatusFailure);
    await cli(['--state-dir', state, 'profile', 'add', 'package-check', '--tunnel', 'example.usw2', '--local', '45454', '--paused']);
    await cli(['--state-dir', state, 'reconnect', 'package-check']);
    const status: unknown = JSON.parse(await cli(['--state-dir', state, 'status', '--json']));
    if (!record(status) || !Array.isArray(status.profiles) || !record(status.profiles[0]) ||
        status.profiles[0].state !== 'paused' || status.profiles[0].paused !== true) invalid('Installed paused-profile control flow failed.');
  } finally { await stop(child); }
}

async function check(output: string, expected: string | undefined, verifyOnly: boolean, signal: AbortSignal): Promise<Report> {
  const pkg = await metadata(expected);
  if (verifyOnly) return verifiedArtifact(output, pkg);
  if (output === root.replace(/\/$/, '') || output === homedir() || output === parse(output).root) invalid('Use a dedicated artifact output directory.');
  const npmPath = process.env.npm_execpath;
  const configured = (await (npmPath
    ? command(process.execPath, [npmPath, 'config', 'get', 'registry'], root, process.env, signal)
    : command('npm', ['config', 'get', 'registry'], root, process.env, signal))).trim();
  const cache = (await (npmPath
    ? command(process.execPath, [npmPath, 'config', 'get', 'cache'], root, process.env, signal)
    : command('npm', ['config', 'get', 'cache'], root, process.env, signal))).trim();
  const cacheRegistry = new URL(configured);
  if (cacheRegistry.protocol !== 'https:' || cacheRegistry.username || cacheRegistry.password || cacheRegistry.search || cacheRegistry.hash) {
    invalid('The offline install cache registry must use HTTPS without embedded credentials.');
  }
  const filename = pkg.name.slice(1).replace('/', '-') + '-' + pkg.version + '.tgz';
  await mkdir(output, { recursive: true, mode: 0o700 });
  if ((await readdir(output)).some(name => name !== filename && name !== 'result.json')) {
    invalid('Artifact output contains unrelated files or older versions; use an empty directory.');
  }
  const temporary = await mkdtemp(join(tmpdir(), 'dtcp-'));
  try {
    const userconfig = join(temporary, 'npmrc');
    const home = join(temporary, 'h');
    await mkdir(home, { mode: 0o700 });
    await writeFile(userconfig, '', { mode: 0o600 });
    const env = verificationEnvironment(process.env, { home, cache, registry: cacheRegistry.href, userconfig, temporary });
    const npm = (args: string[], timeout = 60000) => npmPath
      ? command(process.execPath, [npmPath, ...args], root, env, signal, timeout)
      : command('npm', args, root, env, signal, timeout);
    const raw: unknown = JSON.parse(await npm(['pack', root, '--json', '--foreground-scripts=false', '--pack-destination', output]));
    if (!Array.isArray(raw) || raw.length !== 1 || !record(raw[0]) || raw[0].filename !== filename ||
        raw[0].name !== pkg.name || raw[0].version !== pkg.version || !Array.isArray(raw[0].files)) invalid('Unexpected npm pack output.');
    const files: string[] = [];
    for (const item of raw[0].files) {
      if (!record(item) || typeof item.path !== 'string') invalid('Invalid npm file inventory.');
      files.push(item.path);
    }
    inventory(files);
    const tarball = join(output, filename);
    const hash = await integrity(tarball);
    if (hash !== raw[0].integrity) invalid('Packed tarball integrity does not match npm metadata.');
    const prefix = join(temporary, 'prefix');
    await npm(['install', '--global', '--prefix', prefix, '--offline', '--omit=dev', '--ignore-scripts',
      '--no-audit', '--no-fund', '--registry=' + cacheRegistry.href, tarball]);
    await consumer(prefix, temporary, env, signal, pkg);
    if (await integrity(tarball) !== hash) invalid('Artifact changed during verification.');
    const report: Report = { ...pkg, filename, integrity: hash, files,
      checks: { help: true, daemon: true, pausedProfile: true, dependencyIsolation: true } };
    await writeFile(join(output, 'result.json'), JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
    return report;
  } finally { await rm(temporary, { recursive: true, force: true }); }
}

async function main(): Promise<void> {
  const { values } = parseArgs({ strict: true, options: {
    output: { type: 'string', default: '.artifacts' },
    'expected-version': { type: 'string' },
    'verify-only': { type: 'boolean', default: false },
  } });
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.once('SIGINT', cancel); process.once('SIGTERM', cancel);
  try {
    const report = await check(resolve(values.output!), values['expected-version'], values['verify-only']!, controller.signal);
    console.log(JSON.stringify({ name: report.name, version: report.version, filename: report.filename, integrity: report.integrity, checks: report.checks }));
  } finally { process.off('SIGINT', cancel); process.off('SIGTERM', cancel); }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await main().catch(error => {
    console.error('Package check failed: ' + redact(error instanceof Error ? error.message : 'unknown failure'));
    process.exitCode = 1;
  });
}
