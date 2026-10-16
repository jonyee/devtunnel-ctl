import { isAbsolute, join } from 'node:path';
import { homedir } from 'node:os';
import { realpath, mkdir, writeFile, lstat, unlink } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import { bounded, Fault } from './core.ts';
import type { Store } from './config.ts';
import { DaemonLock } from './lock.ts';
import { request } from './rpc.ts';
import { executable } from './paths.ts';

interface AgentPaths { label: string; node: string; entry: string; state: string }
const xml = (value: string) => value.replace(/[<>&"']/g, ch =>
  ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' })[ch]!);
export function launchAgent(paths: AgentPaths): string {
  if (![paths.node, paths.entry, paths.state].every(p => isAbsolute(p) && !/[\0\r\n]/.test(p))) {
    throw new Fault('invalid-config', 'launchd requires absolute runtime, entrypoint and state paths.');
  }
  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>${xml(paths.label)}</string>
<key>ProgramArguments</key><array>${[paths.node, paths.entry, '--state-dir', paths.state, 'daemon'].map(arg => `<string>${xml(arg)}</string>`).join('')}</array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>ThrottleInterval</key><integer>15</integer>
<key>ProcessType</key><string>Background</string>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>/dev/null</string>
</dict></plist>
`;
}

async function launchctl(args: string[], allowAbsent = false): Promise<{ loaded: boolean }> {
  return bounded(signal => new Promise((resolve, reject) => {
    const child = spawn('/bin/launchctl', args, { shell: false, stdio: ['ignore', 'pipe', 'pipe'] });
    let bytes = 0;
    const abort = () => { child.kill('SIGKILL'); };
    signal.addEventListener('abort', abort, { once: true });
    child.stdout.on('data', b => { bytes += b.length; if (bytes > 65536) abort(); });
    child.stderr.on('data', b => { bytes += b.length; if (bytes > 65536) abort(); });
    child.on('error', () => { signal.removeEventListener('abort', abort); reject(new Fault('internal', 'Cannot execute launchctl.')); });
    child.on('close', code => {
      signal.removeEventListener('abort', abort);
      if (code === 0) resolve({ loaded: true });
      else if (allowAbsent && code === 113) resolve({ loaded: false });
      else reject(new Fault('internal', 'launchctl failed. Check the service registration with launchctl print gui/$(id -u)/LABEL; no persistent action is assumed successful.'));
    });
  }), 5000);
}

export async function service(action: string, store: Store, entry: string): Promise<unknown> {
  if (process.platform !== 'darwin' || process.getuid?.() === 0) throw new Fault('invalid-config', 'Service management requires a non-root macOS user.');
  const label = `local.devtunnel-ctl.${createHash('sha256').update(store.dir).digest('hex').slice(0, 12)}`;
  const directory = join(homedir(), 'Library', 'LaunchAgents');
  const path = join(directory, label + '.plist');
  const target = `gui/${process.getuid!()}`;
  if (action === 'install') {
    const lock = new DaemonLock(store.dir);
    await lock.acquire();
    try {
      const config = await store.load();
      const node = await executable(process.execPath), main = await realpath(entry), helper = await executable(config.helperPath);
      await mkdir(directory, { recursive: true, mode: 0o700 });
      try {
        await writeFile(path, launchAgent({ label, node, entry: main, state: store.dir }), { flag: 'wx', mode: 0o600 });
      } catch (error) {
        if (error instanceof Error && 'code' in error && error.code === 'EEXIST') {
          throw new Fault('invalid-config', 'Service is already installed. Stop and uninstall it before reinstalling paths.');
        }
        throw error;
      }
      try { await store.save({ ...config, helperPath: helper }); }
      catch (error) { await unlink(path); throw error; }
      return { installed: true, loaded: false, label, path, node, entry: main, helper };
    } finally { await lock.release(); }
  }
  if (action === 'status') {
    let installed = true;
    try { await lstat(path); } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') installed = false;
      else throw error;
    }
    const result = await launchctl(['print', `${target}/${label}`], true);
    if (!result.loaded) return { label, installed, loaded: false, responsive: false };
    try {
      const status = await request(join(store.dir, 'control.sock'), { v: 1, command: 'status' });
      return { label, installed, loaded: true, responsive: true, status };
    } catch {
      return { label, installed, loaded: true, responsive: false,
        detail: 'Registered service is unresponsive. Stop it, then run daemon in a terminal to see its sanitized startup error.' };
    }
  }
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink() || info.uid !== process.getuid?.() || (info.mode & 0o077)) {
    throw new Fault('invalid-config', 'Service plist is not a private owned file.');
  }
  if (action === 'start') { await launchctl(['bootstrap', target, path]); return { loaded: true, label }; }
  if (action === 'stop') { await launchctl(['bootout', `${target}/${label}`]); return { loaded: false, label }; }
  if (action === 'uninstall') {
    const result = await launchctl(['print', `${target}/${label}`], true);
    if (result.loaded) await launchctl(['bootout', `${target}/${label}`]);
    await unlink(path);
    return { installed: false, loaded: false, label };
  }
  throw new Fault('invalid-config', 'Use service install, start, stop, uninstall or status.');
}
