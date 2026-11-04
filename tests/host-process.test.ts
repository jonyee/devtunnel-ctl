import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runHostWorker } from '../src/host-process.ts';
import { superviseHost } from '../src/host.ts';
import type { HostMessage } from '../src/host.ts';

const fixture = new URL('./fixtures/host-worker.ts', import.meta.url);
const options = { tunnelId: 'crash', helperPath: process.execPath, probePorts: [] };

test('an actual crashed worker is detected and does not strand an IPC process', async () => {
  await assert.rejects(runHostWorker(options, () => {}, new AbortController().signal, fixture), /recovery|Connection/i);
});

test('terminal worker faults stop the supervisor without a replacement', async () => {
  const reports: HostMessage[] = [];
  await superviseHost({ ...options, tunnelId: 'superseded' }, {
    worker: (o, emit, signal) => runHostWorker(o, emit, signal, fixture),
    report: message => { reports.push(message); },
    sleep: async () => { assert.fail('supersession must not retry'); },
  }, new AbortController().signal);
  assert.ok(reports.some(m => m.kind === 'fault' && m.code === 'host-superseded'));
});

test('a worker ignoring graceful stop is terminated before shutdown completes', { timeout: 10000 }, async () => {
  const stop = new AbortController();
  const work = runHostWorker({ ...options, tunnelId: 'stuck' }, message => {
    if (message.kind === 'heartbeat') stop.abort();
  }, stop.signal, fixture);
  await work;
});

test('the parent acknowledges and retires only the endpoint reported by its worker', async () => {
  const stop = new AbortController();
  let retired = '';
  await runHostWorker({ ...options, tunnelId: 'endpoint' }, message => {
    if (message.kind === 'fault') stop.abort();
  }, stop.signal, fixture, async (_options, endpoint) => { retired = endpoint.id; });
  assert.equal(retired, '00000000-0000-4000-8000-000000000001-relay');
});
