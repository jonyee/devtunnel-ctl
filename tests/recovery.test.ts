import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { Recovery } from '../src/recovery.ts';
import type { Status } from '../src/recovery.ts';
import { Fault } from '../src/core.ts';
import type { Profile } from '../src/config.ts';
import type { Session, TransportEvent } from '../src/sdk-transport.ts';
import type { OpenStream } from '../src/forwarding.ts';

class FakeClock {
  time = 0;
  id = 0;
  timers = new Map<number, { due: number; fn: () => void }>();
  now = () => this.time;
  later = (ms: number, fn: () => void) => {
    const id = ++this.id; this.timers.set(id, { due: this.time + ms, fn });
    return () => this.timers.delete(id);
  };
  async flush() { for (let i = 0; i < 50; i++) await Promise.resolve(); }
  async advance(ms: number) {
    const end = this.time + ms;
    await this.flush();
    while (true) {
      const next = [...this.timers.entries()].filter(([, t]) => t.due <= end).sort((a, b) => a[1].due - b[1].due)[0];
      if (!next) break;
      this.time = Math.max(this.time, next[1].due);
      this.timers.delete(next[0]); next[1].fn();
      await this.flush();
    }
    this.time = end; await this.flush();
  }
  async sleepGap(ms: number) {
    this.time += ms;
    const due = [...this.timers.entries()].filter(([, t]) => t.due <= this.time);
    for (const [id, t] of due) if (this.timers.delete(id)) t.fn();
    await this.flush();
  }
}
const profile = () => ({ name: 'test', tunnelId: 'example', clusterId: 'usw2', remotePort: 22, localPort: 23022, paused: false });
function setup(options: {
  connect?: (profile: Profile, signal: AbortSignal, attempt: number) => Promise<Session>;
  probe?: (signal: AbortSignal, checks: number) => Promise<void>;
} = {}) {
  const clock = new FakeClock();
  let calls = 0, checks = 0, disposals = 0, binds = 0;
  const session: Session & { connected: boolean } = { connected: true, open: async () => new PassThrough(), dispose: async () => { disposals++; } };
  const forwarder = {
    listen: async () => { binds++; }, close: async () => {}, setTarget: (_target: OpenStream | undefined) => {},
  };
  let event: ((event: TransportEvent) => void) | undefined;
  const transitions: Status[] = [];
  const controller = new Recovery(profile(), {
    clock, random: () => 1, forwarder,
    transport: { connect: async (p, signal, handler) => { calls++; event = handler; return options.connect ? options.connect(p, signal, calls) : session; } },
    probe: async signal => { checks++; if (options.probe) await options.probe(signal, checks); },
    log: status => { transitions.push(status); },
  });
  return { clock, controller, session, transitions, get calls() { return calls; }, get checks() { return checks; },
    get disposals() { return disposals; }, get binds() { return binds; }, emit: (e: TransportEvent) => event?.(e) };
}

test('healthy requires a banner probe; scheduling gaps recheck without destroying a healthy client', async () => {
  const s = setup();
  s.controller.start(); await s.clock.advance(0);
  assert.equal(s.controller.status().state, 'healthy');
  assert.equal(s.calls, 1); assert.equal(s.checks, 1);
  await s.clock.sleepGap(8 * 3600000);
  assert.equal(s.calls, 1);
  assert.ok(s.checks >= 2);
  await s.controller.stop();
});

test('capped outage retries restore within 60s of return and reset only after end-to-end health', async () => {
  let online = false;
  const s = setup({ connect: async () => {
    if (!online) throw new Fault('retrying');
    return new Promise<Session>(resolve => s.clock.later(24000, () => resolve(s.session)));
  }, probe: () => new Promise<void>(resolve => s.clock.later(2000, resolve)) });
  s.controller.start();
  await s.clock.advance(3600000);
  assert.ok(s.calls < 300, 'no busy retry loop during long outage');
  const returnedAt = s.clock.now(); online = true;
  await s.clock.advance(60000);
  assert.equal(s.controller.status().state, 'healthy');
  const firstHealthyAt = s.transitions.find(status => status.state === 'healthy')?.lastHealthyAt;
  assert.ok(firstHealthyAt !== undefined);
  assert.ok(firstHealthyAt - returnedAt <= 60000);
  assert.equal(s.controller.status().attempt, 0);
  await s.controller.stop();
});

test('hung connect, probe and disposal cannot block pause or late-generation cleanup', async () => {
  let release!: (session: Session) => void;
  const s = setup({ connect: () => new Promise(resolve => { release = resolve; }) });
  s.controller.start(); await s.clock.advance(0);
  await s.controller.pause();
  assert.equal(s.controller.status().state, 'paused');
  release(s.session); await s.clock.advance(0);
  assert.equal(s.disposals, 1);
  await s.clock.advance(120000);
  assert.equal(s.calls, 1);
  s.controller.reconnect(); await s.clock.advance(0);
  assert.equal(s.controller.status().state, 'paused');
  await s.controller.stop();

  const p = setup({ probe: () => new Promise(() => {}) });
  p.session.dispose = () => new Promise(() => {});
  p.controller.start(); await p.clock.advance(10000);
  assert.notEqual(p.controller.status().state, 'healthy');
  const stop = p.controller.stop();
  await p.clock.advance(2000); await stop;
});

test('SDK gets bounded reconnect grace; one broken profile does not block another', async () => {
  const bad = setup({ connect: () => new Promise(() => {}) });
  const good = setup();
  bad.controller.start(); good.controller.start();
  await good.clock.advance(0); await bad.clock.advance(26000);
  assert.equal(good.controller.status().state, 'healthy');
  assert.ok(bad.controller.status().attempt >= 1);
  good.session.connected = false;
  good.emit({ kind: 'relay', connected: false });
  await good.clock.advance(7000); assert.equal(good.calls, 1);
  good.session.connected = true;
  good.emit({ kind: 'relay', connected: true });
  await good.clock.advance(1000);
  assert.equal(good.calls, 1);
  assert.equal(good.controller.status().state, 'healthy');
  await bad.controller.stop(); await good.controller.stop();
});

test('remote failures are reported without asserting internet-offline; throttling and auth require correct policy', async () => {
  const remote = setup({ probe: async () => { throw new Fault('remote-unavailable'); } });
  remote.controller.start(); await remote.clock.advance(0);
  assert.equal(remote.controller.status().state, 'remote-unavailable');
  assert.equal(remote.controller.status().relayConnected, true);
  assert.equal(remote.calls, 1);
  await remote.clock.advance(10000);
  assert.ok(remote.calls >= 2, 'persistent stale remote path eventually replaces client');
  await remote.controller.stop();
  const auth = setup({ connect: async () => { throw new Fault('login-required'); } });
  auth.controller.start(); await auth.clock.advance(600000);
  assert.equal(auth.controller.status().state, 'login-required');
  assert.ok(auth.calls <= 2);
  auth.controller.reconnect(); await auth.clock.advance(0);
  assert.ok(auth.calls >= 2);
  await auth.controller.stop();
  const rate = setup({ connect: async () => { throw new Fault('throttled', undefined, 61000); } });
  rate.controller.start(); await rate.clock.advance(60000);
  assert.equal(rate.calls, 1);
  await rate.clock.advance(1000); assert.equal(rate.calls, 2);
  await rate.controller.stop();
});

test('terminal SDK disconnect evidence is preserved rather than turned into generic retry storms', async () => {
  const s = setup();
  s.controller.start(); await s.clock.advance(0);
  s.session.connected = false;
  s.emit({ kind: 'relay', connected: false, fault: new Fault('access-denied') });
  await s.clock.advance(600000);
  assert.equal(s.controller.status().state, 'access-denied');
  assert.equal(s.calls, 1);
  await s.controller.stop();
});

test('connected relay with failed SSH probe reports a concise remote SSH/WSL hint in status and transitions', async () => {
  const s = setup({ probe: async () => { throw new Fault('timeout'); } });
  try {
    s.controller.start(); await s.clock.advance(0);
    const status = s.controller.status();
    const message = 'Tunnel: connected; SSH: no response. Check remote SSH/WSL.';
    assert.equal(status.state, 'remote-unavailable');
    assert.equal(status.relayConnected, true);
    assert.equal(status.cause?.code, 'remote-unavailable');
    assert.equal(status.cause?.message, message);
    assert.equal(s.transitions.find(event => event.state === 'remote-unavailable')?.cause?.message, message);
    await s.clock.advance(10000);
    assert.ok(s.calls >= 2, 'clearer wording must not stop automatic recovery');
  } finally { await s.controller.stop(); }
});

test('unavailable relay does not claim a connected tunnel or diagnose WSL', async () => {
  const s = setup({ connect: async () => { throw new Fault('remote-unavailable'); } });
  try {
    s.controller.start(); await s.clock.advance(0);
    const status = s.controller.status();
    assert.equal(status.relayConnected, false);
    assert.equal(status.cause?.message.includes('Tunnel: connected'), false);
    assert.equal(status.cause?.message.includes('WSL'), false);
  } finally { await s.controller.stop(); }
});
