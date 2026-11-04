import { createHash, randomUUID } from 'node:crypto';
import { isAbsolute, join } from 'node:path';
import { homedir } from 'node:os';
import { open, rename, readFile, writeFile, mkdir, unlink, realpath, lstat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { Fault, record } from './core.ts';
import { privateDir, privatePath, executable } from './paths.ts';
import { DaemonLock } from './lock.ts';
import { Log } from './log.ts';
import { launchctl, xml } from './launchd.ts';
import { runHost } from './host-process.ts';
import { parseHostMessage } from './host.ts';
import type { HostMessage, HostOptions } from './host.ts';

export function hostJobPaths(root: string, options: HostOptions) {
  const hash = createHash('sha256').update(JSON.stringify([root, options.tunnelId, options.clusterId ?? ''])).digest('hex').slice(0, 20);
  return { label: `local.devtunnel-ctl.host.${hash}`, dir: join(root, 'hosts', hash) };
}
interface HostAgent { label: string; node: string; entry: string; state: string; options: HostOptions }
export function hostLaunchAgent(agent: HostAgent): string {
  if (![agent.node, agent.entry, agent.state, agent.options.helperPath].every(p => isAbsolute(p) && !/[\0\r\n]/.test(p))) {
    throw new Fault('invalid-config', 'Host service requires absolute runtime, entrypoint, helper and state paths.');
  }
  const args = [agent.node, agent.entry, '--state-dir', agent.state, 'host', agent.options.tunnelId,
    '--helper', agent.options.helperPath, ...(agent.options.clusterId ? ['--cluster', agent.options.clusterId] : []),
    ...agent.options.probePorts.flatMap(p => ['--probe-port', String(p)])];
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>${xml(agent.label)}</string>
<key>ProgramArguments</key><array>${args.map(a => `<string>${xml(a)}</string>`).join('')}</array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><dict><key>SuccessfulExit</key><false/></dict>
<key>ThrottleInterval</key><integer>15</integer>
<key>ProcessType</key><string>Background</string>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>/dev/null</string>
</dict></plist>
`;
}

async function hostDirectory(root: string, options: HostOptions): Promise<string> {
  await privateDir(root);
  await privateDir(join(root, 'hosts'));
  const { dir } = hostJobPaths(root, options);
  await privateDir(dir);
  return dir;
}
export async function readHostState(dir: string, expectedPid?: number): Promise<{ responsive: boolean; snapshot?: unknown }> {
  const path = join(dir, 'status.json');
  try {
    await privatePath(dir, true);
    await privatePath(path);
    if ((await lstat(path)).size > 65536) throw new Fault('protocol');
    const value: unknown = JSON.parse(await readFile(path, 'utf8'));
    if (!record(value) || !Number.isSafeInteger(value.pid) || typeof value.updatedAt !== 'number' ||
        typeof value.running !== 'boolean' || !parseHostMessage(value.message)) throw new Fault('protocol');
    return { responsive: value.running && expectedPid !== undefined && value.pid === expectedPid &&
      Date.now() >= value.updatedAt && Date.now() - value.updatedAt < 5000,
    snapshot: { pid: value.pid, running: value.running, updatedAt: value.updatedAt,
      observedAt: typeof value.observedAt === 'number' ? value.observedAt : value.updatedAt,
      message: parseHostMessage(value.message) } };
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return { responsive: false };
    throw error instanceof Fault ? error : new Fault('protocol', 'Cannot read private host status.');
  }
}

export async function serveHost(options: HostOptions, root: string,
  runner = runHost, output = (message: HostMessage) => console.log(JSON.stringify(message))): Promise<void> {
  const dir = await hostDirectory(root, options);
  const lock = new DaemonLock(dir); await lock.acquire();
  const log = new Log(dir);
  const life = new AbortController();
  const instance = randomUUID();
  let message: HostMessage = { kind: 'status', state: 'connecting', relayConnected: false,
    ports: [], localUnavailable: [], verifiedPorts: [] };
  let running = true, failure: unknown, pending = 0, observedAt = Date.now();
  const fail = (error: unknown) => { failure ??= error; life.abort(); };
  let tail: Promise<void> = Promise.resolve();
  const snapshot = () => {
    if (failure) return;
    if (pending >= 32) { fail(new Fault('internal', 'Host status backlog exceeded its limit.')); return; }
    pending++;
    const value = { instance, pid: process.pid, running, observedAt, updatedAt: Date.now(), message };
    tail = tail.then(async () => {
      const path = join(dir, 'status.json'), temp = join(dir, `status-${instance}.tmp`);
      try { await privatePath(path); } catch (error) {
        if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
      }
      const handle = await open(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      try { await handle.writeFile(JSON.stringify(value) + '\n'); await handle.sync(); }
      finally { await handle.close(); }
      await rename(temp, path);
    }).catch(fail).finally(() => { pending--; });
  };
  const report = (value: HostMessage) => {
    if (value.kind === 'heartbeat' || value.kind === 'endpoint') return;
    message = value;
    observedAt = Date.now();
    output(value);
    void log.write({ event: 'host', state: value.kind === 'status' ? value.state : value.kind,
      ...(value.kind === 'status' ? { relayConnected: value.relayConnected } : {}),
      ...(value.kind === 'fault' ? { message: new Fault(value.code).message } : {}),
      ...(value.kind === 'retry' ? { attempt: value.attempt, nextRetryAt: value.nextRetryAt } : {}),
    }).catch(fail);
    snapshot();
  };
  let tick: ReturnType<typeof setInterval> | undefined;
  try {
    await log.init(); snapshot(); await tail;
    if (failure) throw failure;
    tick = setInterval(snapshot, 2000);
    await runner(options, report, life.signal);
  } finally {
    clearInterval(tick); running = false; snapshot();
    try { await tail; await log.flush(); }
    catch (error) { fail(error); }
    finally { await lock.release(); }
  }
  if (failure) throw new Fault('internal', 'Cannot maintain private host status or logs.');
}

export async function hostService(action: string, options: HostOptions, root: string, entry: string): Promise<unknown> {
  if (process.platform !== 'darwin' || process.getuid?.() === 0) throw new Fault('invalid-config', 'Host services require a non-root macOS user. Use foreground host mode on Linux or Windows.');
  const { label, dir } = hostJobPaths(root, options);
  const plistDir = join(homedir(), 'Library', 'LaunchAgents'), path = join(plistDir, label + '.plist');
  const target = `gui/${process.getuid!()}`;
  if (action === 'install') {
    await hostDirectory(root, options);
    const node = await executable(process.execPath), main = await realpath(entry), helperPath = await executable(options.helperPath);
    await mkdir(plistDir, { recursive: true, mode: 0o700 });
    try {
      await writeFile(path, hostLaunchAgent({ label, node, entry: main, state: root, options: { ...options, helperPath } }), { mode: 0o600, flag: 'wx' });
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'EEXIST') throw new Fault('invalid-config', 'Host service is already installed. Stop and uninstall it before changing its arguments.');
      throw error;
    }
    return { installed: true, loaded: false, label, path };
  }
  if (action === 'status') {
    const registration = await launchctl(['print', `${target}/${label}`], true);
    const status = await readHostState(dir, registration.pid);
    return { label, ...registration, ...status };
  }
  await privatePath(path);
  if (action === 'start') { await launchctl(['bootstrap', target, path]); return { label, loaded: true }; }
  if (action === 'stop') { await launchctl(['bootout', `${target}/${label}`]); return { label, loaded: false }; }
  if (action === 'uninstall') {
    const registration = await launchctl(['print', `${target}/${label}`], true);
    if (registration.loaded) await launchctl(['bootout', `${target}/${label}`]);
    await unlink(path); return { label, installed: false };
  }
  throw new Fault('invalid-config', 'Unknown host service action.');
}
