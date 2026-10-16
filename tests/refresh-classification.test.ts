import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import {
  TunnelRelayTunnelClient, ConnectionStatus, RelayConnectionError, RelayErrorType,
} from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { NodeStream, SshDisconnectReason, CancellationTokenSource } from '@microsoft/dev-tunnels-ssh';
import type { Stream, CancellationToken } from '@microsoft/dev-tunnels-ssh';
import { TokenProvider } from '../src/auth.ts';
import { SdkTransport } from '../src/sdk-transport.ts';
import type { TransportEvent } from '../src/sdk-transport.ts';
import { bounded, Fault } from '../src/core.ts';
import { Recovery } from '../src/recovery.ts';

const profile = { name: 'test', tunnelId: 'example', clusterId: 'usw2', remotePort: 22, localPort: 23022, paused: false };
const jwt = `eyJhbGciOiJub25lIn0.${Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, clusterId: 'usw2' })).toString('base64url')}.signature`;

class MemoryRelay extends TunnelRelayTunnelClient {
  rejectToken = false;
  attempts = 0;
  private readonly transports = new Set<NodeStream>();
  constructor() {
    super();
    this.streamFactory = {
      createRelayStream: async () => {
        this.attempts++;
        if (this.rejectToken) throw new RelayConnectionError('error.relayClientUnauthorized',
          { errorType: RelayErrorType.Unauthorized, statusCode: 401 });
        const stream = new NodeStream(new PassThrough());
        this.transports.add(stream);
        return { stream, protocol: 'tunnel-relay-client-v2-dev' };
      },
    };
  }
  override async startSshSession(_stream: Stream): Promise<void> {}
  override async waitForForwardedPort(): Promise<void> {}
  disconnect(): void {
    this.maybeStartReconnecting(SshDisconnectReason.connectionLost, 'Controlled test disconnect.');
  }
  freshToken(cancellation: CancellationToken) { return this.getFreshTunnelAccessToken(cancellation); }
  reportAuthFailure(): void {
    this.disconnectError = new RelayConnectionError('error.relayClientUnauthorized',
      { errorType: RelayErrorType.Unauthorized, statusCode: 401 });
    this.connectionStatus = ConnectionStatus.Disconnected;
  }
  override async dispose(): Promise<void> {
    await super.dispose();
    await Promise.all([...this.transports].map(stream => stream.close()));
    this.transports.clear();
  }
}

function setup() {
  const state: {
    helperCalls: number;
    refreshFailure: unknown;
    refresh: ((signal?: AbortSignal) => Promise<string>) | undefined;
  } = { helperCalls: 0, refreshFailure: undefined, refresh: undefined };
  const tokens = new TokenProvider('/unused', async (_path, _args, signal) => {
    if (++state.helperCalls > 1) {
      if (state.refresh) return state.refresh(signal);
      if (state.refreshFailure) throw state.refreshFailure;
    }
    return JSON.stringify({ token: jwt });
  });
  const client = new MemoryRelay();
  const clients = [client];
  let used = false;
  const transport = new SdkTransport(tokens, () => new TunnelManagementHttpClient(
    'tests', ManagementApiVersions.Version20230927preview, undefined, undefined, undefined,
    async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{
        connectionMode: 'TunnelRelay', hostId: 'h', hostPublicKeys: ['key'], clientRelayUri: 'wss://example.invalid/',
      }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }),
  ), () => {
    if (!used) { used = true; return client; }
    const next = new MemoryRelay();
    clients.push(next);
    return next;
  });
  const events: TransportEvent[] = [];
  return { state, tokens, client, clients, transport, events };
}

test('actual SDK initial 401 does not mask a transient token refresh failure as sign-in required', async () => {
  const fixture = setup();
  fixture.client.rejectToken = true;
  fixture.state.refreshFailure = new Fault('retrying');
  try {
    await assert.rejects(fixture.transport.connect(profile, new AbortController().signal, event => fixture.events.push(event)),
      error => error instanceof Fault && error.code === 'retrying');
    assert.equal(fixture.state.helperCalls, 2);
    assert.ok(fixture.client.disconnectError instanceof RelayConnectionError);
    assert.equal(fixture.client.disconnectError.errorContext.statusCode, 401, 'SDK retains the original 401');
    assert.equal(fixture.events.findLast(event => event.kind === 'relay' && event.fault)?.fault?.code, 'retrying');
  } finally { fixture.tokens.clear(); await fixture.client.dispose(); }
});

test('actual SDK asynchronous reconnect preserves token refresh failure evidence in disconnect events', async () => {
  const fixture = setup();
  const session = await fixture.transport.connect(profile, new AbortController().signal, event => fixture.events.push(event));
  try {
    assert.equal(session.connected, true);
    fixture.state.refreshFailure = new Fault('retrying');
    fixture.client.rejectToken = true;
    const disconnected = new Promise<void>(resolve => {
      const subscription = fixture.client.connectionStatusChanged(event => {
        if (event.status === ConnectionStatus.Disconnected) { subscription.dispose(); resolve(); }
      });
    });
    fixture.client.disconnect();
    await bounded(() => disconnected, 1000);
    assert.equal(session.connected, false);
    assert.ok(fixture.client.disconnectError instanceof RelayConnectionError);
    assert.equal(fixture.client.disconnectError.errorContext.statusCode, 401);
    assert.equal(fixture.events.findLast(event => event.kind === 'relay' && event.fault)?.fault?.code, 'retrying');
  } finally { await session.dispose(); fixture.tokens.clear(); }
});

for (const code of ['timeout', 'throttled', 'access-denied', 'aborted'] as const) {
  test(`wrapped initial relay 401 preserves refresh ${code} classification`, async () => {
    const fixture = setup();
    fixture.client.rejectToken = true;
    fixture.state.refreshFailure = new Fault(code, undefined, code === 'throttled' ? 61000 : 0);
    try {
      await assert.rejects(fixture.transport.connect(profile, new AbortController().signal, () => {}),
        error => error instanceof Fault && error.code === code &&
          error.retryAfterMs === (code === 'throttled' ? 61000 : 0));
    } finally { fixture.tokens.clear(); await fixture.client.dispose(); }
  });
}

test('actual repeated relay 401 after successful refresh still requires sign-in and refreshes only once', async () => {
  const fixture = setup();
  fixture.client.rejectToken = true;
  try {
    await assert.rejects(fixture.transport.connect(profile, new AbortController().signal, () => {}),
      error => error instanceof Fault && error.code === 'login-required');
    assert.equal(fixture.state.helperCalls, 2);
    assert.equal(fixture.client.attempts, 2);
  } finally { fixture.tokens.clear(); await fixture.client.dispose(); }
});

async function disconnect(fixture: ReturnType<typeof setup>): Promise<void> {
  const disconnected = new Promise<void>(resolve => {
    const subscription = fixture.client.connectionStatusChanged(event => {
      if (event.status === ConnectionStatus.Disconnected) { subscription.dispose(); resolve(); }
    });
  });
  fixture.client.disconnect();
  await bounded(() => disconnected, 1000);
  await new Promise(resolve => setImmediate(resolve));
}

test('a later successful episode clears old transient refresh evidence before real credential rejection', async () => {
  const fixture = setup();
  const session = await fixture.transport.connect(profile, new AbortController().signal, event => fixture.events.push(event));
  try {
    fixture.client.rejectToken = true;
    fixture.state.refreshFailure = new Fault('retrying');
    await disconnect(fixture);
    assert.equal(fixture.events.findLast(event => event.fault)?.fault?.code, 'retrying');
    fixture.state.refreshFailure = undefined;
    fixture.client.rejectToken = false;
    await fixture.client.connectTunnelSession();
    assert.equal(session.connected, true);
    fixture.client.rejectToken = true;
    await disconnect(fixture);
    assert.equal(fixture.events.findLast(event => event.kind === 'relay' && event.fault)?.fault?.code, 'login-required');
    assert.equal(fixture.state.helperCalls, 3);
  } finally { await session.dispose(); fixture.tokens.clear(); }
});

class RecoveryClock {
  private time = 0;
  private readonly timers = new Set<{ due: number; callback: () => void }>();
  now = () => this.time;
  later = (ms: number, callback: () => void) => {
    const task = { due: this.time + ms, callback };
    this.timers.add(task);
    return () => { this.timers.delete(task); };
  };
  async advance(ms: number): Promise<void> {
    const end = this.time + ms;
    await new Promise(resolve => setImmediate(resolve));
    while (true) {
      const next = [...this.timers].filter(task => task.due <= end).sort((a, b) => a.due - b.due)[0];
      if (!next) break;
      this.time = next.due;
      this.timers.delete(next);
      next.callback();
      await new Promise(resolve => setImmediate(resolve));
    }
    this.time = end;
    await new Promise(resolve => setImmediate(resolve));
  }
}

for (const phase of ['initial', 'reconnect'] as const) {
  test(`recovery automatically retries ${phase} token-refresh outage and returns healthy without login`, async () => {
    const fixture = setup(), clock = new RecoveryClock();
    const recovery = new Recovery(profile, {
      clock, random: () => 1, transport: fixture.transport,
      forwarder: { listen: async () => {}, close: async () => {}, setTarget: () => {} },
      probe: async () => {}, log: () => {},
    });
    try {
      if (phase === 'initial') {
        fixture.client.rejectToken = true;
        fixture.state.refreshFailure = new Fault('retrying');
      }

      recovery.start();
      await clock.advance(0);
      if (phase === 'reconnect') {
        assert.equal(recovery.status().state, 'healthy');
        fixture.client.rejectToken = true;
        fixture.state.refreshFailure = new Fault('retrying');
        await disconnect(fixture);
      }
      assert.equal(recovery.status().state, phase === 'initial' ? 'retrying' : 'reconnecting');
      assert.notEqual(recovery.status().cause?.code, 'login-required');
      fixture.state.refreshFailure = undefined;
      await clock.advance(12000);
      assert.equal(recovery.status().state, 'healthy');
      assert.equal(fixture.clients.length, 2);
      assert.equal(recovery.status().attempt, 0);
    } finally { await recovery.stop(); fixture.tokens.clear(); }
  });
}

test('wrapped refresh errors retain only safe classification, never raw helper output', async () => {
  const fixture = setup();
  fixture.client.rejectToken = true;
  fixture.state.refreshFailure = new Error(`private-helper-output Authorization: Bearer ${jwt}`);
  try {
    await assert.rejects(fixture.transport.connect(profile, new AbortController().signal, event => fixture.events.push(event)),
      error => error instanceof Fault && error.code === 'retrying' && !error.message.includes(jwt));
    assert.equal(JSON.stringify(fixture.events).includes(jwt), false);
    assert.equal(fixture.client.disconnectError?.message.includes('private-helper-output'), false);
  } finally { fixture.tokens.clear(); await fixture.client.dispose(); }
});

test('cancelling initial relay refresh stays cancelled despite wrapped401 and ignores late helper failure', async () => {
  const fixture = setup(), controller = new AbortController();
  let started!: () => void, rejectRefresh!: (error: Error) => void;
  const begun = new Promise<void>(resolve => { started = resolve; });
  fixture.client.rejectToken = true;
  fixture.state.refresh = () => new Promise((_resolve, reject) => { rejectRefresh = reject; started(); });
  const connecting = fixture.transport.connect(profile, controller.signal, event => fixture.events.push(event));
  const rejected = assert.rejects(connecting, error => error instanceof Fault && error.code === 'aborted');
  try {
    await bounded(() => begun, 1000);
    controller.abort();
    await rejected;
    const count = fixture.events.length;
    rejectRefresh(new Error(`private-helper-output ${jwt}`));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(fixture.events.length, count, 'disposed generation must not emit late auth failures');
    assert.equal(fixture.events.some(event => event.fault?.code === 'login-required'), false);
  } finally { controller.abort(); fixture.tokens.clear(); await fixture.client.dispose(); }
});

for (const outcome of ['success', 'failure'] as const) {
  test(`late refresh ${outcome} cannot contaminate a newer connected episode`, async () => {
    const fixture = setup(), source = new CancellationTokenSource();
    const session = await fixture.transport.connect(profile, new AbortController().signal, event => fixture.events.push(event));
    let started!: () => void, resolveRefresh!: (output: string) => void, rejectRefresh!: (error: Error) => void;
    const begun = new Promise<void>(resolve => { started = resolve; });
    fixture.state.refresh = () => new Promise((resolve, reject) => {
      resolveRefresh = resolve; rejectRefresh = reject; started();
    });
    const refreshing = fixture.client.freshToken(source.token);
    void refreshing.catch(() => {});
    try {
      await bounded(() => begun, 1000);
      await fixture.client.connectTunnelSession();
      assert.equal(session.connected, true);
      if (outcome === 'success') resolveRefresh(JSON.stringify({ token: jwt }));
      else rejectRefresh(new Fault('retrying'));
      await assert.rejects(refreshing, error => error instanceof Fault && error.code === 'aborted');
      fixture.client.reportAuthFailure();
      assert.equal(fixture.events.findLast(event => event.kind === 'relay' && event.fault)?.fault?.code, 'login-required');
    } finally { source.dispose(); await session.dispose(); fixture.tokens.clear(); }
  });
}
