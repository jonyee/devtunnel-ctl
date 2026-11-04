import { isAbsolute } from 'node:path';
import { bounded, clock, Fault, isFaultCode, record, safeFault } from './core.ts';
import type { Clock, FaultCode } from './core.ts';

export interface HostOptions {
  tunnelId: string;
  clusterId?: string;
  helperPath: string;
  probePorts: number[];
}
export interface HostStatus {
  kind: 'status';
  state: 'connecting' | 'connected' | 'verified' | 'degraded';
  relayConnected: boolean;
  ports: number[];
  localUnavailable: number[];
  verifiedPorts: number[];
  probeFault?: FaultCode;
}
export type HostMessage = HostStatus | { kind: 'heartbeat' } |
  { kind: 'endpoint'; id: string; tunnelId: string; clusterId: string } |
  { kind: 'fault'; code: FaultCode; retryAfterMs: number } |
  { kind: 'retry'; attempt: number; nextRetryAt: number };
const port = (v: unknown): v is number => Number.isInteger(v) && Number(v) > 0 && Number(v) <= 65535;
const id = /^[a-z0-9][a-z0-9-]{0,62}$/i;
const terminal = (code: FaultCode) => ['login-required', 'access-denied', 'invalid-config', 'host-superseded', 'host-cleanup-failed'].includes(code);

export function validateHost(value: unknown): HostOptions {
  if (!record(value) || typeof value.tunnelId !== 'string' ||
      typeof value.helperPath !== 'string' || !isAbsolute(value.helperPath) ||
      /[\0\r\n]/.test(value.helperPath) || !Array.isArray(value.probePorts) ||
      value.probePorts.length > 16 || !value.probePorts.every(port) ||
      new Set(value.probePorts).size !== value.probePorts.length) throw new Fault('invalid-config', 'Invalid host options or probe ports.');
  const [tunnelId, suffix, extra] = value.tunnelId.split('.');
  const clusterId = value.clusterId ?? suffix;
  if (!tunnelId || !id.test(tunnelId) || extra !== undefined ||
      (clusterId !== undefined && (typeof clusterId !== 'string' || !id.test(clusterId))) ||
      (suffix !== undefined && value.clusterId !== undefined && suffix !== value.clusterId)) {
    throw new Fault('invalid-config', 'Invalid host tunnel or cluster ID.');
  }
  return { tunnelId, ...(clusterId ? { clusterId: String(clusterId) } : {}),
    helperPath: value.helperPath, probePorts: [...value.probePorts] };
}

export function parseHostMessage(value: unknown): HostMessage | undefined {
  if (!record(value)) return;
  if (value.kind === 'heartbeat') return { kind: 'heartbeat' };
  if (value.kind === 'retry' && Number.isSafeInteger(value.attempt) && Number(value.attempt) > 0 &&
      typeof value.nextRetryAt === 'number' && Number.isFinite(value.nextRetryAt) && value.nextRetryAt >= 0) {
    return { kind: 'retry', attempt: Number(value.attempt), nextRetryAt: value.nextRetryAt };
  }
  if (value.kind === 'endpoint' && typeof value.id === 'string' && /^[a-f0-9-]{36}-relay$/.test(value.id) &&
      typeof value.tunnelId === 'string' && id.test(value.tunnelId) && typeof value.clusterId === 'string' && id.test(value.clusterId)) {
    return { kind: 'endpoint', id: value.id, tunnelId: value.tunnelId, clusterId: value.clusterId };
  }
  if (value.kind === 'fault' && isFaultCode(value.code) && typeof value.retryAfterMs === 'number' &&
      Number.isFinite(value.retryAfterMs) && value.retryAfterMs >= 0 && value.retryAfterMs <= 86400000) {
    return { kind: 'fault', code: value.code, retryAfterMs: value.retryAfterMs };
  }
  if (value.kind !== 'status' || !['connecting', 'connected', 'verified', 'degraded'].includes(String(value.state)) ||
      typeof value.relayConnected !== 'boolean' || (value.probeFault !== undefined && !isFaultCode(value.probeFault))) return;
  const arrays = [value.ports, value.localUnavailable, value.verifiedPorts];
  if (!arrays.every(a => Array.isArray(a) && a.length <= 1024 && a.every(port) && new Set(a).size === a.length)) return;
  const ports = value.ports as number[], localUnavailable = value.localUnavailable as number[], verifiedPorts = value.verifiedPorts as number[];
  if (localUnavailable.some(p => !ports.includes(p)) || verifiedPorts.some(p => !ports.includes(p) || localUnavailable.includes(p)) ||
      (verifiedPorts.length > 0 && !value.relayConnected) || (value.state === 'verified' && (!value.relayConnected || !verifiedPorts.length))) return;
  return { kind: 'status', state: value.state as HostStatus['state'], relayConnected: value.relayConnected,
    ports: [...ports], localUnavailable: [...localUnavailable], verifiedPorts: [...verifiedPorts],
    ...(isFaultCode(value.probeFault) ? { probeFault: value.probeFault } : {}) };
}

export interface HostDependencies {
  worker(options: HostOptions, emit: (message: HostMessage) => void, signal: AbortSignal): Promise<void>;
  report(message: HostMessage): void;
  clock?: Clock;
  random?: () => number;
  sleep?(ms: number, signal: AbortSignal): Promise<void>;
}
export function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) { reject(new Fault('aborted')); return; }
    const aborted = () => { clearTimeout(timer); reject(new Fault('aborted')); };
    const timer = setTimeout(() => { signal.removeEventListener('abort', aborted); resolve(); }, ms);
    signal.addEventListener('abort', aborted, { once: true });
  });
}

export async function superviseHost(options: HostOptions, deps: HostDependencies, signal: AbortSignal): Promise<void> {
  const timer = deps.clock ?? clock;
  let attempt = 0;
  while (!signal.aborted) {
    const life = new AbortController();
    const parentAbort = () => life.abort();
    signal.addEventListener('abort', parentAbort, { once: true });
    const started = timer.now();
    let lastHeartbeat = started, established = false, failure: Fault | undefined;
    let cancelWatch = () => {};
    const watch = () => {
      if (timer.now() - lastHeartbeat > 15000 || (!established && timer.now() - started > 45000)) {
        failure ??= new Fault('timeout'); life.abort(); return;
      }
      cancelWatch = timer.later(1000, watch);
    };
    watch();
    let cancelAbort = () => {};
    const aborted = new Promise<void>(resolve => {
      const abort = () => resolve();
      life.signal.addEventListener('abort', abort, { once: true });
      cancelAbort = () => life.signal.removeEventListener('abort', abort);
    });
    const work = Promise.resolve().then(() => {
      if (signal.aborted) life.abort();
      return deps.worker(options, message => {
        if (life.signal.aborted) return;
        if (message.kind === 'heartbeat') { lastHeartbeat = timer.now(); return; }
        if (message.kind === 'fault') {
          failure = new Fault(message.code, undefined, message.retryAfterMs);
          life.abort();
        } else if (message.kind === 'status') {
          established ||= message.relayConnected;
          if ((message.state === 'verified' && options.probePorts.every(p => message.verifiedPorts.includes(p))) ||
              (!options.probePorts.length && message.relayConnected)) attempt = 0;
          deps.report(message);
        }
      }, life.signal);
    });
    try { await Promise.race([work, aborted]); }
    catch (error) { failure ??= safeFault(error); }
    finally {
      life.abort(); cancelWatch(); cancelAbort();
      signal.removeEventListener('abort', parentAbort);
    }
    // A replacement cannot overlap a worker that has not confirmed its exit.
    try { await bounded(() => work.catch(error => {
      const fault = safeFault(error);
      if (!failure || terminal(fault.code)) failure = fault;
    }), 25000, undefined, undefined, timer); }
    catch { throw new Fault('internal', 'Host worker exit could not be confirmed; refusing overlapping hosts.'); }
    if (signal.aborted) {
      if (failure?.code === 'host-cleanup-failed') {
        deps.report({ kind: 'fault', code: failure.code, retryAfterMs: failure.retryAfterMs });
      }
      return;
    }
    failure ??= new Fault('retrying');
    deps.report({ kind: 'fault', code: failure.code, retryAfterMs: failure.retryAfterMs });
    if (terminal(failure.code)) return;
    const backoff = Math.min(15000, 1000 * 2 ** Math.min(attempt++, 5));
    const delay = Math.max(failure.retryAfterMs, Math.round(backoff * (0.5 + (deps.random ?? Math.random)() * 0.5)));
    deps.report({ kind: 'retry', attempt, nextRetryAt: timer.now() + delay });
    try { await (deps.sleep ?? sleep)(delay, signal); }
    catch (error) { if (!signal.aborted) throw error; }
  }
}
