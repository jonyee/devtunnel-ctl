import net from 'node:net';
import { lstat, chmod, unlink } from 'node:fs/promises';
import { bounded, Fault, record, safeFault, isFaultCode, redact } from './core.ts';
import { validateConfig } from './config.ts';
import type { Profile } from './config.ts';

export type Request =
  | { v: 1; command: 'status' }
  | { v: 1; command: 'logs' }
  | { v: 1; command: 'add'; profile: Profile }
  | { v: 1; command: 'pause' | 'resume' | 'reconnect'; name: string };

function parseRequest(value: unknown): Request {
  if (!record(value) || value.v !== 1) throw new Fault('protocol');
  const allowed = new Set(['v', 'command', 'profile', 'name']);
  if (Object.keys(value).some(key => !allowed.has(key))) throw new Fault('protocol');
  if (value.command === 'status' || value.command === 'logs') return { v: 1, command: value.command };
  if (value.command === 'add') {
    const config = validateConfig({ version: 1, helperPath: '/unused', profiles: [value.profile] });
    return { v: 1, command: 'add', profile: config.profiles[0]! };
  }
  if (['pause', 'resume', 'reconnect'].includes(String(value.command)) &&
      typeof value.name === 'string' && /^(all|[a-z][a-z0-9-]{0,31})$/.test(value.name)) {
    if (value.command === 'pause' || value.command === 'resume' || value.command === 'reconnect') {
      return { v: 1, command: value.command, name: value.name };
    }
  }
  throw new Fault('protocol');
}

function parseFrame(text: string): unknown {
  try { return JSON.parse(text); } catch { throw new Fault('protocol'); }
}

export class RpcServer {
  private readonly path: string;
  private readonly handler: (request: Request) => Promise<unknown>;
  private readonly log: (fault: Fault) => void;
  private server: net.Server | undefined;
  private inode: number | undefined;
  private readonly sockets = new Set<net.Socket>();
  constructor(path: string, handler: (request: Request) => Promise<unknown>, log: (fault: Fault) => void) {
    this.path = path; this.handler = handler; this.log = log;
  }
  async listen(): Promise<void> {
    if (Buffer.byteLength(this.path) > 100) throw new Fault('invalid-config', 'State directory path is too long for a Unix socket (limit 100 bytes including control.sock).');
    try {
      const info = await lstat(this.path);
      if (!info.isSocket() || info.uid !== process.getuid?.()) throw new Fault('invalid-config', 'Control socket path is not an owned Unix socket.');
      const live = await bounded(signal => new Promise<boolean>((resolve, reject) => {
        const socket = net.connect(this.path);
        const abort = () => { socket.destroy(); reject(new Fault('daemon-unavailable', 'Existing control socket is unresponsive; refusing to unlink it.')); };
        signal.addEventListener('abort', abort, { once: true });
        const done = (result: boolean) => { signal.removeEventListener('abort', abort); socket.destroy(); resolve(result); };
        socket.once('connect', () => done(true));
        socket.once('error', error => {
          if ('code' in error && error.code === 'ECONNREFUSED') done(false);
          else { signal.removeEventListener('abort', abort); socket.destroy(); reject(new Fault('daemon-unavailable')); }
        });
      }), 500);
      if (live) throw new Fault('daemon-unavailable', 'A daemon already owns the control socket.');
      const current = await lstat(this.path);
      if (current.ino !== info.ino) throw new Fault('daemon-unavailable', 'Control socket ownership changed; refusing cleanup.');
      await unlink(this.path);
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    }
    const server = net.createServer(socket => this.accept(socket));
    server.maxConnections = 32;
    this.server = server;
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(this.path, () => { server.off('error', reject); resolve(); });
    });
    this.inode = (await lstat(this.path)).ino;
    await chmod(this.path, 0o600);
    server.on('error', () => this.log(new Fault('internal', 'Control listener failed.')));
  }
  private accept(socket: net.Socket): void {
    this.sockets.add(socket);
    const timeout = setTimeout(() => socket.destroy(), 5000);
    socket.on('error', () => socket.destroy());
    socket.once('close', () => { clearTimeout(timeout); this.sockets.delete(socket); });
    let bytes = 0, text = '', handled = false;
    socket.on('data', (chunk: Buffer) => {
      if (handled) { socket.destroy(); return; }
      bytes += chunk.length;
      if (bytes > 32768) { this.log(new Fault('protocol')); socket.destroy(); return; }
      text += chunk.toString('utf8');
      const end = text.indexOf('\n');
      if (end < 0) return;
      handled = true;
      if (text.slice(end + 1).trim()) { socket.destroy(); return; }
      const send = (value: unknown) => {
        const frame = JSON.stringify(value) + '\n';
        if (Buffer.byteLength(frame) > 131072) { this.log(new Fault('protocol')); socket.destroy(); }
        else socket.end(frame);
      };
      void Promise.resolve().then(() => this.handler(parseRequest(parseFrame(text.slice(0, end))))).then(
        result => send({ ok: true, result }),
        error => {
          const fault = safeFault(error);
          this.log(fault);
          send({ ok: false, error: { code: fault.code, message: fault.message } });
        },
      );
    });
  }
  async close(): Promise<void> {
    for (const socket of this.sockets) socket.destroy();
    if (this.server?.listening) {
      await bounded(() => new Promise<void>((resolve, reject) => this.server!.close(error => error ? reject(error) : resolve())), 1000);
    }
    if (this.inode !== undefined) {
      try {
        const info = await lstat(this.path);
        if (info.ino === this.inode && info.isSocket()) await unlink(this.path);
      } catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error; }
    }
  }
}

export function request(path: string, value: unknown, timeoutMs = 4500): Promise<unknown> {
  const frame = JSON.stringify(value) + '\n';
  if (Buffer.byteLength(frame) > 32768) return Promise.reject(new Fault('protocol'));
  return bounded(signal => new Promise((resolve, reject) => {
    const socket = net.connect(path);
    let bytes = 0, text = '', done = false;
    const finish = (error?: Fault, result?: unknown) => {
      if (done) return;
      done = true;
      signal.removeEventListener('abort', abort);
      socket.destroy();
      if (error) reject(error); else resolve(result);
    };
    const abort = () => finish(new Fault('daemon-unavailable', 'Daemon did not respond before the deadline. State changes may have completed; check status before retrying.'));
    signal.addEventListener('abort', abort, { once: true });
    socket.once('connect', () => socket.write(frame));
    socket.on('error', () => finish(new Fault('daemon-unavailable')));
    socket.once('close', () => { if (!done) finish(new Fault('daemon-unavailable')); });
    socket.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > 131072) { finish(new Fault('protocol')); return; }
      text += chunk.toString('utf8');
      const end = text.indexOf('\n'); if (end < 0) return;
      try {
        const response = parseFrame(text.slice(0, end));
        if (!record(response) || typeof response.ok !== 'boolean') throw new Fault('protocol');
        if (response.ok) finish(undefined, response.result);
        else if (record(response.error) && typeof response.error.message === 'string' && isFaultCode(response.error.code)) {
          finish(new Fault(response.error.code, redact(response.error.message)));
        } else throw new Fault('protocol');
      } catch { finish(new Fault('protocol')); }
    });
  }), timeoutMs).catch(error => {
    if (error instanceof Fault && error.code === 'timeout') {
      throw new Fault('daemon-unavailable', 'Daemon did not respond before the deadline. State changes may have completed; check status before retrying.');
    }
    throw error;
  });
}
