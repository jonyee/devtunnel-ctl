import { test } from 'node:test';
import assert from 'node:assert/strict';
import { validateHost, superviseHost, parseHostMessage } from '../src/host.ts';
import type { HostMessage } from '../src/host.ts';
import { Fault } from '../src/core.ts';

const options = { tunnelId: 'example.usw2', helperPath: '/bin/devtunnel', probePorts: [22] };

test('validates explicit probe ports without restricting the hosted port set', () => {
  assert.deepEqual(validateHost(options), {
    tunnelId: 'example', clusterId: 'usw2', helperPath: '/bin/devtunnel', probePorts: [22],
  });
  for (const probePorts of [[0], [65536], [22, 22], ['22'], Array(17).fill(22)]) {
    assert.throws(() => validateHost({ ...options, probePorts }));
  }
  assert.throws(() => validateHost({ ...options, helperPath: 'devtunnel' }));
  assert.throws(() => validateHost({ ...options, clusterId: 'other' }));
});

test('worker IPC accepts safe evidence and strips untrusted text and fields', () => {
  assert.equal(parseHostMessage({ kind: 'heartbeat' })?.kind, 'heartbeat');
  assert.equal(parseHostMessage({ kind: 'fault', code: 'made-up' }), undefined);
  assert.deepEqual(parseHostMessage({ kind: 'fault', code: 'access-denied', retryAfterMs: 3000, token: 'private' }),
    { kind: 'fault', code: 'access-denied', retryAfterMs: 3000 });
  assert.equal(parseHostMessage({ kind: 'status', state: 'verified', relayConnected: false,
    ports: [22], localUnavailable: [], verifiedPorts: [22] }), undefined);
  assert.deepEqual(parseHostMessage({ kind: 'retry', attempt: 2, nextRetryAt: 30000, token: 'private' }),
    { kind: 'retry', attempt: 2, nextRetryAt: 30000 });
});

test('crashed workers are retired before retry; terminal supersession never retries', async () => {
  const messages: HostMessage[] = [];
  let calls = 0, active = 0, maxActive = 0;
  await superviseHost(validateHost(options), {
    report: message => { messages.push(message); },
    random: () => 0,
    sleep: async ms => { assert.equal(ms, 500); },
    worker: async (_options, emit) => {
      active++; maxActive = Math.max(maxActive, active);
      try {
        if (++calls === 1) throw new Fault('retrying');
        emit({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 });
      } finally { active--; }
    },
  }, new AbortController().signal);
  assert.equal(calls, 2);
  assert.equal(maxActive, 1);
  assert.ok(messages.some(m => m.kind === 'fault' && m.code === 'host-superseded'));
});

test('throttling survives worker exit and is not reset by an unverified connection', async () => {
  let calls = 0;
  const delays: number[] = [];
  await superviseHost(validateHost(options), {
    report: () => {}, random: () => 1,
    sleep: async ms => { delays.push(ms); },
    worker: async (_options, emit) => {
      calls++;
      emit({ kind: 'status', state: 'connected', relayConnected: true, ports: [22, 3000],
        localUnavailable: [], verifiedPorts: [] });
      if (calls === 1) emit({ kind: 'fault', code: 'throttled', retryAfterMs: 60000 });
      else if (calls === 2) throw new Fault('retrying');
      else emit({ kind: 'fault', code: 'login-required', retryAfterMs: 0 });
    },
  }, new AbortController().signal);
  assert.deepEqual(delays, [60000, 2000]);
});

test('aborting the supervisor cancels its live worker and never starts a replacement', async () => {
  const stop = new AbortController();
  let calls = 0;
  const result = superviseHost(validateHost(options), {
    report: () => {},
    worker: async (_options, _emit, signal) => {
      calls++;
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  }, stop.signal);
  await new Promise(resolve => setImmediate(resolve));
  stop.abort();
  await result;
  assert.equal(calls, 1);
});

test('a failed owned-endpoint cleanup stops replacement instead of masking the cleanup error', async () => {
  let calls = 0;
  await superviseHost(validateHost(options), {
    report: () => {}, sleep: async () => { assert.fail('must not replace after uncertain endpoint cleanup'); },
    worker: async (_options, emit) => {
      calls++;
      emit({ kind: 'fault', code: 'retrying', retryAfterMs: 0 });
      throw new Fault('host-cleanup-failed');
    },
  }, new AbortController().signal);
  assert.equal(calls, 1);
});

test('heartbeat loss aborts a stuck worker before retry, even if its relay was connected', async () => {
  let now = 0, next: (() => void) | undefined, calls = 0;
  const stop = new AbortController();
  const result = superviseHost(validateHost(options), {
    clock: { now: () => now, later: (_ms, fn) => { next = fn; return () => { next = undefined; }; } },
    report: () => {}, sleep: async () => {},
    worker: async (_options, emit, signal) => {
      calls++;
      if (calls === 2) { emit({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 }); return; }
      emit({ kind: 'status', state: 'connected', relayConnected: true, ports: [22], localUnavailable: [], verifiedPorts: [] });
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  }, stop.signal);
  await new Promise(resolve => setImmediate(resolve));
  now = 16000; next?.();
  await result;
  assert.equal(calls, 2);
});

test('long outages cap backoff at 15 seconds and obsolete workers cannot report recovery', async () => {
  let calls = 0, late: ((message: HostMessage) => void) | undefined;
  const delays: number[] = [], reports: HostMessage[] = [];
  await superviseHost(validateHost(options), {
    report: message => { reports.push(message); }, random: () => 1,
    sleep: async ms => {
      delays.push(ms);
      late?.({ kind: 'status', state: 'verified', relayConnected: true, ports: [22],
        verifiedPorts: [22], localUnavailable: [] });
    },
    worker: async (_options, emit) => {
      calls++; late = emit;
      if (calls < 9) throw new Fault('retrying');
      emit({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 });
    },
  }, new AbortController().signal);
  assert.deepEqual(delays, [1000, 2000, 4000, 8000, 15000, 15000, 15000, 15000]);
  assert.equal(reports.some(m => m.kind === 'status' && m.state === 'verified'), false);
});

test('heartbeats cannot hide a host that exceeds its startup deadline', async () => {
  let now = 0, calls = 0, next: (() => void) | undefined;
  let heartbeat = () => {};
  const work = superviseHost(validateHost(options), {
    clock: { now: () => now, later: (_ms, fn) => { next = fn; return () => { next = undefined; }; } },
    report: () => {}, sleep: async () => {},
    worker: async (_options, emit, signal) => {
      if (++calls === 2) { emit({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 }); return; }
      heartbeat = () => emit({ kind: 'heartbeat' });
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  }, new AbortController().signal);
  await new Promise(resolve => setImmediate(resolve));
  for (now = 1000; now <= 45000; now += 1000) { heartbeat(); next?.(); }
  assert.equal(calls, 1);
  heartbeat(); next?.();
  await work;
  assert.equal(calls, 2);
});

test('established workers keep their reconnect window after the startup deadline', async () => {
  let now = 0, next: (() => void) | undefined, emit!: (message: HostMessage) => void;
  let workerSignal!: AbortSignal, calls = 0;
  const stop = new AbortController();
  const work = superviseHost(validateHost(options), {
    clock: { now: () => now, later: (_ms, fn) => { next = fn; return () => { next = undefined; }; } },
    report: () => {},
    worker: async (_options, report, signal) => {
      calls++; emit = report; workerSignal = signal;
      report({ kind: 'status', state: 'connected', relayConnected: true, ports: [22], localUnavailable: [], verifiedPorts: [] });
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  }, stop.signal);
  try {
    await new Promise(resolve => setImmediate(resolve));
    now = 60000;
    emit({ kind: 'status', state: 'connecting', relayConnected: false, ports: [22], localUnavailable: [], verifiedPorts: [] });
    for (; now < 68000; now += 1000) {
      emit({ kind: 'heartbeat' }); next?.();
      assert.equal(workerSignal.aborted, false);
    }
    emit({ kind: 'status', state: 'connected', relayConnected: true, ports: [22], localUnavailable: [], verifiedPorts: [] });
    assert.equal(calls, 1);
  } finally { stop.abort(); await work; }
});

test('explicit shutdown reports failed endpoint cleanup without replacing the worker', async () => {
  const stop = new AbortController(), reports: HostMessage[] = [];
  let calls = 0;
  const work = superviseHost(validateHost(options), {
    report: message => { reports.push(message); },
    worker: async (_options, _emit, signal) => {
      calls++;
      await new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }));
      throw new Fault('host-cleanup-failed');
    },
  }, stop.signal);
  await new Promise(resolve => setImmediate(resolve));
  stop.abort(); await work;
  assert.deepEqual(reports, [{ kind: 'fault', code: 'host-cleanup-failed', retryAfterMs: 0 }]);
  assert.equal(calls, 1);
});
