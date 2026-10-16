export type FaultCode = 'login-required' | 'access-denied' | 'port-conflict' |
  'remote-unavailable' | 'retrying' | 'throttled' | 'timeout' | 'aborted' |
  'invalid-config' | 'protocol' | 'daemon-unavailable' | 'internal';

const messages: Record<FaultCode, string> = {
  'login-required': 'Sign-in required. Run devtunnel-ctl login, then reconnect.',
  'access-denied': 'Tunnel access denied. Check the account and tunnel permissions.',
  'port-conflict': 'A configured loopback port is occupied. Release it explicitly or change the profile.',
  'remote-unavailable': 'SSH host or remote forwarding path unavailable; network location is uncertain.',
  retrying: 'Connection unavailable; recovery will retry.',
  throttled: 'The service is throttling requests; waiting before retry.',
  timeout: 'Operation exceeded its deadline.',
  aborted: 'Operation cancelled.',
  'invalid-config': 'Invalid configuration.',
  protocol: 'Invalid or oversized control message.',
  'daemon-unavailable': 'Daemon unavailable. Run devtunnel-ctl daemon or start the installed service.',
  internal: 'Operation failed. Check configuration and sanitized service logs.',
};

export class Fault extends Error {
  readonly code: FaultCode;
  readonly retryAfterMs: number;
  constructor(code: FaultCode, message = messages[code], retryAfterMs = 0) {
    super(message);
    this.name = 'Fault';
    this.code = code;
    this.retryAfterMs = retryAfterMs;
  }
}

export function isFaultCode(value: unknown): value is FaultCode {
  return typeof value === 'string' && Object.hasOwn(messages, value);
}

export function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function safeFault(error: unknown): Fault {
  if (error instanceof Fault) return error;
  const obj = record(error) ? error : {};
  const response = record(obj.response) ? obj.response : {};
  const context = record(obj.errorContext) ? obj.errorContext : {};
  const status = response.status ?? obj.statusCode ?? obj.status ?? context.statusCode;
  if (status === 401) return new Fault('login-required');
  if (status === 403) return new Fault('access-denied');
  if (status === 429 || status === 503) {
    const headers = record(response.headers) ? response.headers : {};
    const retry = headers['retry-after'];
    const seconds = typeof retry === 'string' ? Number(retry) : NaN;
    const ms = Number.isFinite(seconds) ? seconds * 1000 :
      typeof retry === 'string' ? Date.parse(retry) - Date.now() : 0;
    return new Fault('throttled', messages.throttled, Math.max(0, Math.min(ms || 0, 86400000)));
  }
  if (obj.code === 'EADDRINUSE') return new Fault('port-conflict');
  if (status === 404 || obj.code === 'ECONNREFUSED') return new Fault('remote-unavailable');
  if (error instanceof Error && [
    'No hosts are currently accepting connections for the tunnel.',
    'The specified host is not currently accepting connections to the tunnel.',
    'The host is not currently accepting Tunnel relay connections.',
  ].includes(error.message)) return new Fault('remote-unavailable');
  return new Fault('retrying');
}

export function redact(text: string): string {
  return text
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '[redacted]')
    .replace(/\b(Bearer|Tunnel)\s+[^\s",;]+/gi, '$1 [redacted]')
    .replace(/((?:["']?(?:access[_-]?token|token|authorization)["']?)\s*[:=]\s*["']?)[^"'\s,;}]+/gi, '$1[redacted]')
    .slice(0, 1024);
}

export interface Clock {
  now(): number;
  later(ms: number, callback: () => void): () => void;
}
export const clock: Clock = {
  now: () => Date.now(),
  later: (ms, callback) => { const timer = setTimeout(callback, ms); return () => clearTimeout(timer); },
};

export function bounded<T>(
  action: (signal: AbortSignal) => Promise<T>, ms: number, parent?: AbortSignal,
  late?: (value: T) => void | Promise<void>, timer: Clock = clock,
): Promise<T> {
  const controller = new AbortController();
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let cancelTimer = () => {};
    const finish = (error?: unknown, value?: T) => {
      if (settled) return;
      settled = true;
      cancelTimer();
      parent?.removeEventListener('abort', aborted);
      if (error !== undefined) {
        controller.abort();
        reject(error);
      } else resolve(value!);
    };
    const aborted = () => finish(new Fault('aborted'));
    if (parent?.aborted) { aborted(); return; }
    parent?.addEventListener('abort', aborted, { once: true });
    cancelTimer = timer.later(ms, () => finish(new Fault('timeout')));
    Promise.resolve().then(() => {
      if (controller.signal.aborted) throw new Fault('aborted');
      return action(controller.signal);
    }).then(value => {
      if (settled) {
        if (late) void Promise.resolve().then(() => late(value)).catch(() =>
          process.emitWarning('Cancelled-operation resource cleanup failed.', { code: 'DTC_CLEANUP' }));
      }
      else finish(undefined, value);
    }, error => finish(error));
  });
}
