import { record, safeFault, bounded, Fault } from './core.ts';
import { validateHost } from './host.ts';
import type { HostMessage } from './host.ts';
import { runHostSdk } from './host-sdk.ts';

const life = new AbortController();
let started = false, pending = 0;
let endpointAck: { id: string; resolve(): void } | undefined;
const send = (message: HostMessage) => {
  if (!process.connected || (pending > 0 && message.kind === 'heartbeat')) return;
  if (pending >= 32) { life.abort(); throw new Fault('protocol', 'Host worker IPC backlog exceeded its limit.'); }
  pending++;
  process.send?.(message, error => {
    pending--;
    if (error) life.abort();
  });
};
const report = (message: HostMessage): void | Promise<void> => {
  if (message.kind !== 'endpoint') { send(message); return; }
  return bounded(() => new Promise<void>((resolve, reject) => {
    if (endpointAck) { reject(new Fault('protocol')); return; }
    endpointAck = { id: message.id, resolve };
    send(message);
  }), 2000, life.signal).finally(() => { endpointAck = undefined; });
};
const disconnected = () => life.abort();
process.once('disconnect', disconnected);
process.once('SIGTERM', disconnected);
process.once('SIGINT', disconnected);
const initial = setTimeout(() => { life.abort(); process.disconnect?.(); }, 5000);
process.on('message', value => {
  if (!record(value)) { life.abort(); return; }
  if (value.kind === 'stop') { life.abort(); return; }
  if (value.kind === 'endpoint-recorded' && value.id === endpointAck?.id) { endpointAck?.resolve(); return; }
  if (value.kind !== 'start' || started) { life.abort(); return; }
  started = true; clearTimeout(initial);
  let options;
  try { options = validateHost(value.options); }
  catch { send({ kind: 'fault', code: 'invalid-config', retryAfterMs: 0 }); process.disconnect?.(); return; }
  const heartbeat = setInterval(() => send({ kind: 'heartbeat' }), 1000);
  void runHostSdk(options, report, life.signal).catch(error => {
    const fault = safeFault(error);
    send({ kind: 'fault', code: fault.code, retryAfterMs: fault.retryAfterMs });
  }).finally(() => {
    clearInterval(heartbeat);
    if (process.connected) process.disconnect();
  });
});
if (!process.send) throw new Fault('invalid-config', 'The host worker requires a supervisor IPC channel.');
