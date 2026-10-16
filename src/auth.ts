import { spawn } from 'node:child_process';
import { bounded, Fault, record } from './core.ts';

export interface Token { value: string; expiresAt: number; clusterId?: string }
export function parseToken(output: string): Token {
  let value: unknown;
  try { value = JSON.parse(output); } catch { throw new Fault('protocol', 'Token helper returned invalid JSON.'); }
  const token = typeof value === 'string' ? value : record(value) ? value.token : undefined;
  if (typeof token !== 'string' ||
      !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(token)) {
    throw new Fault('protocol', 'Token helper returned an unsupported token envelope.');
  }
  let claims: unknown;
  try { claims = JSON.parse(Buffer.from(token.split('.')[1]!, 'base64url').toString()); }
  catch { throw new Fault('protocol', 'Token helper returned an invalid token.'); }
  if (!record(claims) || typeof claims.exp !== 'number' || !Number.isSafeInteger(claims.exp) ||
      !Number.isFinite(claims.exp * 1000)) {
    throw new Fault('protocol', 'Token has no usable expiration.');
  }
  const expiresAt = claims.exp * 1000;
  if (expiresAt <= Date.now() + 30000) throw new Fault('login-required', 'Acquired token is expired or about to expire. Run devtunnel-ctl login.');
  const clusterId = typeof claims.clusterId === 'string' && /^[a-z0-9][a-z0-9-]{0,62}$/.test(claims.clusterId)
    ? claims.clusterId : undefined;
  return { value: token, expiresAt, ...(clusterId ? { clusterId } : {}) };
}

export function runHelper(path: string, args: string[], signal?: AbortSignal, timeoutMs = 12000): Promise<string> {
  return bounded(inner => new Promise<string>((resolve, reject) => {
    if (inner.aborted) { reject(new Fault('aborted')); return; }
    const child = spawn(path, args, { stdio: ['ignore', 'pipe', 'pipe'], shell: false,
      env: { ...process.env, CI: 'true' } });
    let stdout = '', stderr = '', bytes = 0, finished = false;
    const stop = () => {
      child.kill('SIGTERM');
      const kill = setTimeout(() => { if (child.exitCode === null) child.kill('SIGKILL'); }, 200);
      kill.unref();
      child.once('close', () => clearTimeout(kill));
    };
    const abort = () => { stop(); complete(new Fault('aborted')); };
    const complete = (error?: Fault) => {
      if (finished) return;
      finished = true;
      inner.removeEventListener('abort', abort);
      if (error) reject(error); else resolve(stdout);
    };
    inner.addEventListener('abort', abort, { once: true });
    const data = (chunk: Buffer, out: boolean) => {
      if (finished) return;
      bytes += chunk.length;
      if (bytes > 65536) {
        stop(); complete(new Fault('protocol', 'Token helper output exceeded the private capture limit.'));
      } else if (out) stdout += chunk.toString(); else stderr += chunk.toString();
    };
    child.stdout.on('data', chunk => data(chunk, true));
    child.stderr.on('data', chunk => data(chunk, false));
    child.on('error', () => complete(new Fault('invalid-config', 'Cannot execute configured devtunnel helper. Check its absolute path.')));
    child.on('close', code => {
      if (code === 0) complete();
      else if (/sign.?in|log.?in|not authenticated|unauthorized|401/i.test(stderr)) complete(new Fault('login-required'));
      else if (/forbidden|access.?denied|403/i.test(stderr)) complete(new Fault('access-denied'));
      else if (/429|throttl|too many requests/i.test(stderr)) complete(new Fault('throttled', undefined, 30000));
      else complete(new Fault('retrying', 'Token acquisition failed; recovery will retry. No automatic sign-in is attempted.'));
      stdout = ''; stderr = '';
    });
  }), timeoutMs, signal);
}

type Runner = (path: string, args: string[], signal?: AbortSignal) => Promise<string>;
type Reference = { tunnelId: string; clusterId?: string };
interface Flight { promise: Promise<Token>; controller: AbortController; users: number }
export class TokenProvider {
  private readonly cache = new Map<string, Token>();
  private readonly flights = new Map<string, Flight>();
  readonly path: string;
  private readonly runner: Runner;
  constructor(path: string, runner: Runner = runHelper) { this.path = path; this.runner = runner; }
  async get(ref: Reference, signal?: AbortSignal, refresh = false): Promise<Token> {
    const key = ref.tunnelId + (ref.clusterId ? `.${ref.clusterId}` : '');
    if (signal?.aborted) throw new Fault('aborted');
    if (refresh) this.cache.delete(key);
    const cached = this.cache.get(key);
    if (cached && cached.expiresAt > Date.now() + 60000) return cached;
    let flight = this.flights.get(key);
    if (flight?.controller.signal.aborted) { this.flights.delete(key); flight = undefined; }
    if (!flight) {
      const controller = new AbortController();
      const promise = this.runner(this.path, ['token', key, '--scopes', 'connect', '--json'], controller.signal)
        .then(output => {
          if (controller.signal.aborted) throw new Fault('aborted');
          const token = parseToken(output);
          this.cache.set(key, token);
          return token;
        });
      flight = { promise, controller, users: 0 };
      this.flights.set(key, flight);
      const current = flight;
      void promise.finally(() => { if (this.flights.get(key) === current) this.flights.delete(key); }).catch(() => {});
    }
    flight.users++;
    try { return await bounded(() => flight.promise, 13000, signal); }
    finally { if (--flight.users === 0) flight.controller.abort(); }
  }
  clear(): void {
    this.cache.clear();
    for (const flight of this.flights.values()) flight.controller.abort();
    this.flights.clear();
  }
  invalidate(ref: Reference): void {
    const key = ref.tunnelId + (ref.clusterId ? `.${ref.clusterId}` : '');
    this.cache.delete(key);
  }
}
