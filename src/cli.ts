import { parseArgs } from 'node:util';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { Store, validateConfig } from './config.ts';
import { stateDir, executable } from './paths.ts';
import { bounded, Fault, record, safeFault } from './core.ts';
import { request } from './rpc.ts';
import { Daemon } from './daemon.ts';
import { DaemonLock } from './lock.ts';
import { service } from './launchd.ts';

export const help = `devtunnel-ctl - resilient SSH-only Dev Tunnels forwarding

  init [--helper /absolute/path/devtunnel]
  daemon                         Foreground service; Ctrl-C stops it
  profile add NAME --tunnel ID[.CLUSTER] --local PORT [--remote 22] [--cluster ID] [--paused]
  profile list | status [NAME] [--json]
  pause NAME|all | resume NAME|all | reconnect NAME|all
  logs [--follow]                 Recent sanitized logs from the running daemon
  login [--device-code] [--github] Explicit devtunnel sign-in only
  service install|start|stop|uninstall|status

Global: --state-dir /absolute/private/directory, --help
State defaults to ~/.devtunnel-ctl. Start a daemon before modifying profiles.
Reconnect never unpauses. Stable ports never shift. Fresh clients may break SSH sessions.
`;

function argumentsFor(args: string[]) {
  try { return parseArgs({ args, allowPositionals: true, strict: true, options: {
    'state-dir': { type: 'string' }, helper: { type: 'string' }, tunnel: { type: 'string' },
    local: { type: 'string' }, remote: { type: 'string' }, cluster: { type: 'string' },
    paused: { type: 'boolean' }, json: { type: 'boolean' }, follow: { type: 'boolean' },
    'device-code': { type: 'boolean' }, github: { type: 'boolean' }, help: { type: 'boolean', short: 'h' },
  } }); }
  catch { throw new Fault('invalid-config', 'Invalid CLI arguments. Run devtunnel-ctl --help.'); }
}

export async function cli(args: string[], entry: string): Promise<void> {
  const parsed = argumentsFor(args);
  const { values: flags, positionals: words } = parsed;
  if (flags.help || !words.length) { console.log(help); return; }
  const [command, sub, name] = words;
  const allowed: Record<string, string[]> = {
    init: ['helper'], daemon: [], profile: sub === 'add' ? ['tunnel', 'local', 'remote', 'cluster', 'paused'] : ['json'],
    status: ['json'], pause: [], resume: [], reconnect: [], logs: ['follow'], login: ['device-code', 'github'], service: [],
  };
  if (!command || !(command in allowed) || Object.keys(flags).some(k => k !== 'state-dir' && !allowed[command]!.includes(k))) {
    throw new Fault('invalid-config', 'Unknown command or misplaced option. Run devtunnel-ctl --help.');
  }
  const store = new Store(stateDir(flags['state-dir']));
  await store.init();
  const socket = join(store.dir, 'control.sock');
  const print = (value: unknown) => console.log(JSON.stringify(value, null, flags.json ? 2 : undefined));
  if (command === 'init') {
    if (words.length !== 1) throw new Fault('invalid-config', 'init accepts only --helper.');
    const lock = new DaemonLock(store.dir); await lock.acquire();
    try {
      const config = await store.load();
      const helperPath = await executable(flags.helper ?? config.helperPath);
      await store.save(validateConfig({ ...config, helperPath }));
      print({ initialized: true, config: store.configPath, helper: helperPath });
    } finally { await lock.release(); }
    return;
  }
  if (command === 'daemon') {
    if (words.length !== 1) throw new Fault('invalid-config', 'daemon accepts no positional arguments.');
    let shutdown = () => {};
    const daemon = new Daemon(store, { fatal: fault => { console.error(`${fault.code}: ${fault.message}`); process.exitCode = 1; shutdown(); } });
    await daemon.start();
    console.log('Daemon ready. Private control socket: ' + socket);
    await new Promise<void>((resolve, reject) => {
      shutdown = () => {
        process.off('SIGINT', shutdown); process.off('SIGTERM', shutdown);
        void daemon.stop().then(resolve, reject);
      };
      process.once('SIGINT', shutdown); process.once('SIGTERM', shutdown);
    });
    return;
  }
  if (command === 'login') {
    if (words.length !== 1) throw new Fault('invalid-config', 'login accepts only --device-code and --github.');
    const config = await store.load();
    await bounded(signal => new Promise<void>((resolve, reject) => {
      const child = spawn(config.helperPath, ['user', 'login', ...(flags['device-code'] ? ['-d'] : []),
        ...(flags.github ? ['-g'] : [])], { stdio: 'inherit', shell: false });
      const abort = () => child.kill('SIGKILL');
      signal.addEventListener('abort', abort, { once: true });
      child.on('error', () => { signal.removeEventListener('abort', abort); reject(new Fault('invalid-config', 'Cannot execute configured login helper.')); });
      child.on('close', code => {
        signal.removeEventListener('abort', abort);
        if (code === 0) resolve(); else reject(new Fault('login-required'));
      });
    }), 300000);
    console.log('Sign-in completed. Run devtunnel-ctl reconnect all to retry blocked profiles.');
    return;
  }
  if (command === 'service') {
    if (words.length !== 2 || !sub || !['install', 'start', 'stop', 'uninstall', 'status'].includes(sub)) {
      throw new Fault('invalid-config', 'Use service install, start, stop, uninstall or status.');
    }
    print(await service(sub, store, entry)); return;
  }
  if (command === 'profile' && sub === 'add') {
    if (words.length !== 3 || !name || !flags.tunnel || !flags.local) {
      throw new Fault('invalid-config', 'Use profile add NAME --tunnel ID --local PORT [--remote 22] [--cluster ID] [--paused].');
    }
    const config = validateConfig({ version: 1, helperPath: '/unused', profiles: [{
      name, tunnelId: flags.tunnel, clusterId: flags.cluster, localPort: Number(flags.local),
      remotePort: Number(flags.remote ?? 22), paused: flags.paused ?? false,
    }] });
    print(await request(socket, { v: 1, command: 'add', profile: config.profiles[0] })); return;
  }
  if (command === 'status' || (command === 'profile' && sub === 'list')) {
    if ((command === 'status' && words.length > 2) || (command === 'profile' && words.length !== 2)) {
      throw new Fault('invalid-config', 'Use status [NAME] or profile list.');
    }
    const response = await request(socket, { v: 1, command: 'status' });
    if (!record(response) || !Array.isArray(response.profiles)) throw new Fault('protocol');
    const profiles = response.profiles.filter(p => record(p) && (command !== 'status' || !sub || p.name === sub));
    if (sub && command === 'status' && !profiles.length) throw new Fault('invalid-config', 'No matching profile.');
    if (flags.json) print({ ...response, profiles });
    else {
      console.log('NAME             LOCAL   REMOTE  STATE                 RELAY  LAST SSH CHECK / NEXT RETRY');
      for (const p of profiles) {
        if (!record(p)) throw new Fault('protocol');
        const success = typeof p.lastHealthyAt === 'number' ? new Date(p.lastHealthyAt).toISOString() : 'never';
        const retry = typeof p.nextRetryAt === 'number' ? new Date(p.nextRetryAt).toISOString() : '-';
        console.log(`${String(p.name).padEnd(16)} ${String(p.localPort).padEnd(7)} ${String(p.remotePort).padEnd(7)} ${String(p.state).padEnd(21)} ${p.relayConnected ? 'yes' : 'no '}    ${success} / ${retry}`);
        if (record(p.cause) && typeof p.cause.message === 'string') console.log('  ' + p.cause.message);
      }
    }
    return;
  }
  if (command === 'pause' || command === 'resume' || command === 'reconnect') {
    if (words.length !== 2 || !sub) throw new Fault('invalid-config', `${command} requires NAME or all.`);
    print(await request(socket, { v: 1, command, name: sub })); return;
  }
  if (command === 'logs') {
    if (words.length !== 1) throw new Fault('invalid-config', 'Use logs [--follow].');
    let instance = '', sequence = 0, stopped = false;
    const stop = () => { stopped = true; };
    if (flags.follow) { process.once('SIGINT', stop); process.once('SIGTERM', stop); }
    try {
      do {
        const result = await request(socket, { v: 1, command: 'logs' });
        if (!record(result) || typeof result.instance !== 'string' || !Array.isArray(result.entries)) throw new Fault('protocol');
        if (result.instance !== instance) { instance = result.instance; sequence = 0; }
        for (const value of result.entries) {
          if (!record(value) || typeof value.sequence !== 'number' || typeof value.line !== 'string') throw new Fault('protocol');
          if (value.sequence > sequence) { console.log(value.line); sequence = value.sequence; }
        }
        if (flags.follow && !stopped) await new Promise(resolve => setTimeout(resolve, 1000));
      } while (flags.follow && !stopped);
    } finally { process.off('SIGINT', stop); process.off('SIGTERM', stop); }
    return;
  }
  throw new Fault('invalid-config', 'Unknown command. Run devtunnel-ctl --help.');
}

export function report(error: unknown): void {
  const fault = safeFault(error);
  console.error(`${fault.code}: ${fault.message}`);
  process.exitCode = 1;
}
