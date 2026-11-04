import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { Fault, safeFault } from './core.ts';
import { parseHostMessage, superviseHost } from './host.ts';
import type { HostMessage, HostOptions } from './host.ts';
import { retireHostEndpoint } from './host-sdk.ts';

export async function runHostWorker(options: HostOptions, emit: (message: HostMessage) => void,
  signal: AbortSignal, entry = new URL(import.meta.url.endsWith('.ts') ? './host-worker.ts' : './host-worker.js', import.meta.url),
  retire = retireHostEndpoint): Promise<void> {
  if (signal.aborted) return;
  let endpoint: Extract<HostMessage, { kind: 'endpoint' }> | undefined;
  let failure: Fault | undefined;
  await new Promise<void>((resolve, reject) => {
    const child = fork(fileURLToPath(entry), [], {
      execArgv: [], stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
      env: { ...process.env, NODE_OPTIONS: '', NODE_PATH: '' },
    });
    let stopping = false, exited = false;
    let terminate: ReturnType<typeof setTimeout> | undefined;
    let kill: ReturnType<typeof setTimeout> | undefined;
    const stop = () => {
      if (stopping || exited) return;
      stopping = true;
      if (child.connected) child.send({ kind: 'stop' }, error => { if (error && !exited) child.kill('SIGTERM'); });
      terminate = setTimeout(() => { if (!exited) child.kill('SIGTERM'); }, 2500);
      kill = setTimeout(() => { if (!exited) child.kill('SIGKILL'); }, 3000);
    };
    signal.addEventListener('abort', stop, { once: true });
    child.once('spawn', () => {
      if (signal.aborted) { stop(); return; }
      child.send({ kind: 'start', options }, error => {
        if (error) { failure ??= new Fault('protocol'); stop(); }
      });
    });
    child.on('message', value => {
      if (stopping) return;
      const message = parseHostMessage(value);
      if (!message) { failure ??= new Fault('protocol'); stop(); return; }
      if (message.kind === 'endpoint') {
        if (endpoint && (endpoint.id !== message.id || endpoint.clusterId !== message.clusterId)) {
          failure ??= new Fault('protocol'); stop(); return;
        }
        endpoint = message;
        child.send({ kind: 'endpoint-recorded', id: message.id }, error => {
          if (error && !exited) { failure ??= new Fault('protocol'); stop(); }
        });
      } else {
        if (message.kind === 'fault') failure ??= new Fault(message.code, undefined, message.retryAfterMs);
        emit(message);
      }
    });
    child.once('error', () => { failure ??= new Fault('invalid-config', 'Cannot start the host worker. Check the Node runtime and installed package.'); });
    child.once('close', () => {
      exited = true; clearTimeout(terminate); clearTimeout(kill);
      signal.removeEventListener('abort', stop);
      if (!signal.aborted) reject(failure ?? new Fault('retrying'));
      else resolve();
    });
    if (signal.aborted) stop();
  }).catch(error => { failure ??= safeFault(error); });
  if (endpoint) await retire(options, endpoint);
  if (failure && !signal.aborted) throw failure;
}

export async function runHost(options: HostOptions, report: (message: HostMessage) => void, parent?: AbortSignal): Promise<void> {
  const stop = new AbortController();
  const abort = () => stop.abort();
  parent?.addEventListener('abort', abort, { once: true });
  if (parent?.aborted) stop.abort();
  const signals: NodeJS.Signals[] = process.platform === 'win32' ? ['SIGINT', 'SIGTERM', 'SIGBREAK'] : ['SIGINT', 'SIGTERM'];
  for (const signal of signals) process.once(signal, abort);
  try { await superviseHost(options, { worker: runHostWorker, report }, stop.signal); }
  finally {
    parent?.removeEventListener('abort', abort);
    for (const signal of signals) process.off(signal, abort);
  }
}
