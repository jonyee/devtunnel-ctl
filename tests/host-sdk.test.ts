import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TunnelRelayTunnelHost, ConnectionStatus, RetryingTunnelConnectionEventArgs, RelayConnectionError, RelayErrorType } from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { SshDisconnectReason, CancellationTokenSource } from '@microsoft/dev-tunnels-ssh';
import { TunnelConnectionMode } from '@microsoft/dev-tunnels-contracts';
import { HostProbe, runHostSdk, retireHostEndpoint } from '../src/host-sdk.ts';
import { TokenProvider } from '../src/auth.ts';
import { Fault } from '../src/core.ts';
import type { HostMessage } from '../src/host.ts';

const options = { tunnelId: 'example', clusterId: 'usw2', helperPath: '/unused', probePorts: [] };
const jwt = ['eyJhbGciOiJub25lIn0', Buffer.from(JSON.stringify({
  exp: Math.floor(Date.now() / 1000) + 3600, clusterId: 'usw2',
})).toString('base64url'), 'signature'].join('.');

test('SDK hosting uses host authorization, preserves every configured port and disposes on stop', async () => {
  const stop = new AbortController(), messages: HostMessage[] = [];
  let disposed = 0, hosted = false;
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => {
      assert.equal(config.headers?.Authorization, `tunnel ${jwt}`);
      return { config, status: 200, statusText: 'OK', headers: {},
        data: { tunnelId: 'example', clusterId: 'usw2', ports: [{ portNumber: 22 }, { portNumber: 3000 }] } };
    });
  class Host extends TunnelRelayTunnelHost {
    override get connectionStatus() { return ConnectionStatus.Connected; }
    override async connect(...[tunnel]: Parameters<TunnelRelayTunnelHost['connect']>) {
      assert.equal(tunnel.accessTokens?.host, jwt);
      assert.deepEqual(tunnel.ports?.map(p => p.portNumber), [22, 3000]);
      assert.equal(this.enableE2EEncryption, true);
      hosted = true;
    }
    override async dispose() { disposed++; }
  }
  await runHostSdk(options, message => {
    messages.push(message);
    if (message.kind === 'status' && message.state === 'connected') stop.abort();
  }, stop.signal, {
    tokens: new TokenProvider('/unused', async (_path, args) => {
      assert.equal(args[3], 'host'); return JSON.stringify({ token: jwt });
    }),
    management: () => management, host: m => new Host(m),
  });
  assert.equal(hosted, true);
  assert.equal(disposed, 1);
  assert.equal(messages.some(m => m.kind === 'status' && m.state === 'verified'), false);
});

test('host supersession is terminal even during the initial SDK connection', async () => {
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({ config, status: 200, statusText: 'OK', headers: {},
      data: { tunnelId: 'example', clusterId: 'usw2', ports: [] } }));
  class Host extends TunnelRelayTunnelHost {
    override get disconnectReason() { return SshDisconnectReason.tooManyConnections; }
    override async connect() { throw new Error('private SDK detail'); }
    override async dispose() {}
  }
  await assert.rejects(runHostSdk(options, () => {}, new AbortController().signal, {
    tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })),
    management: () => management, host: m => new Host(m),
  }), error => error instanceof Fault && error.code === 'host-superseded');
});

test('local application downtime does not trigger replacement or count as forwarding health', async () => {
  let remoteCalls = 0;
  const probe = new HostProbe([22], {
    local: async () => { throw new Fault('remote-unavailable'); },
    remote: async () => { remoteCalls++; },
  });
  for (let i = 0; i < 5; i++) {
    const result = await probe.check([22, 3000], new AbortController().signal, 60000);
    assert.deepEqual(result.status.localUnavailable, [22]);
    assert.deepEqual(result.status.verifiedPorts, []);
    assert.equal(result.replace, false);
  }
  assert.equal(remoteCalls, 0);
});

test('forwarding failures require consecutive evidence and cooldown; probe auth never restarts host', async () => {
  let remoteError: Fault | undefined = new Fault('timeout');
  const probe = new HostProbe([22], { local: async () => {}, remote: async () => {
    if (remoteError) throw remoteError;
  } });
  const check = (age: number) => probe.check([22, 3000], new AbortController().signal, age);
  assert.equal((await check(0)).replace, false);
  assert.equal((await check(10000)).replace, false);
  assert.equal((await check(30000)).replace, true);
  remoteError = new Fault('access-denied');
  const denied = await check(60000);
  assert.equal(denied.replace, false);
  assert.equal(denied.status.probeFault, 'access-denied');
  remoteError = undefined;
  assert.deepEqual((await check(70000)).status.verifiedPorts, [22]);
});

test('a probe port missing from tunnel metadata is invalid configuration', async () => {
  const probe = new HostProbe([22], { local: async () => {}, remote: async () => {} });
  await assert.rejects(probe.check([3000], new AbortController().signal, 0),
    error => error instanceof Fault && error.code === 'invalid-config');
});

test('a successfully refreshed host token gets one initial SDK auth retry', async () => {
  const stop = new AbortController();
  let acquisitions = 0;
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({ config, status: 200, statusText: 'OK', headers: {},
      data: { tunnelId: 'example', clusterId: 'usw2', ports: [] } }));
  class Host extends TunnelRelayTunnelHost {
    override async connect() {
      const cancellation = new CancellationTokenSource();
      try { await this.getFreshTunnelAccessToken(cancellation.token); }
      finally { cancellation.dispose(); }
      const retry = new RetryingTunnelConnectionEventArgs(new RelayConnectionError('private', {
        errorType: RelayErrorType.Unauthorized, statusCode: 401,
      }), 0);
      this.onRetrying(retry);
      assert.equal(retry.retry, true);
      this.connectionStatus = ConnectionStatus.Connected;
    }
    override async dispose() {}
  }
  await runHostSdk(options, message => {
    if (message.kind === 'status' && message.relayConnected) stop.abort();
  }, stop.signal, {
    tokens: new TokenProvider('/unused', async () => { acquisitions++; return JSON.stringify({ token: jwt }); }),
    management: () => management, host: m => new Host(m),
  });
  assert.equal(acquisitions, 2);
});

test('cleanup uses the originally selected identity and deletes only its recorded endpoint', async () => {
  let requests = 0;
  const owned = '00000000-0000-4000-8000-000000000001-relay';
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => {
      requests++;
      assert.equal(config.method, 'delete');
      assert.match(config.url!, new RegExp(`/tunnels/canonical/endpoints/${owned}`));
      assert.equal(config.headers?.Authorization, `tunnel ${jwt}`);
      return { config, status: 200, statusText: 'OK', headers: {}, data: true };
    });
  await retireHostEndpoint({ ...options, tunnelId: 'friendly-name' },
    { kind: 'endpoint', id: owned, tunnelId: 'canonical', clusterId: 'usw2' }, {
      tokens: new TokenProvider('/unused', async (_path, args) => {
        assert.equal(args[1], 'friendly-name.usw2');
        assert.equal(args[3], 'host');
        return JSON.stringify({ token: jwt });
      }), management: () => management,
    });
  assert.equal(requests, 1);
});

test('cleanup failure is explicit and terminal rather than permitting a fresh host generation', async () => {
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async () => { throw new Fault('throttled', 'private detail'); });
  await assert.rejects(retireHostEndpoint(options, {
    kind: 'endpoint', id: '00000000-0000-4000-8000-000000000001-relay', tunnelId: 'example', clusterId: 'usw2',
  }, { tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })), management: () => management }),
  error => error instanceof Fault && error.code === 'host-cleanup-failed' && !error.message.includes('private'));
});

test('endpoint registration waits until its ownership report has been acknowledged', async () => {
  const stop = new AbortController();
  let registered = 0, acknowledge!: () => void, reported!: () => void;
  const endpointReported = new Promise<void>(resolve => { reported = resolve; });
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => {
      if (config.method === 'put') registered++;
      return { config, status: 200, statusText: 'OK', headers: {}, data: config.method === 'put' ?
        { id: '00000000-0000-4000-8000-000000000001-relay', hostRelayUri: 'wss://test.invalid' } :
        { tunnelId: 'example', clusterId: 'usw2', ports: [] } };
    });
  class Host extends TunnelRelayTunnelHost {
    override get connectionStatus() { return ConnectionStatus.Connected; }
    override async connect(...[tunnel]: Parameters<TunnelRelayTunnelHost['connect']>) {
      await management.updateTunnelEndpoint(tunnel, {
        id: '00000000-0000-4000-8000-000000000001-relay', hostId: 'test', connectionMode: TunnelConnectionMode.TunnelRelay,
      });
    }
    override async dispose() {}
  }
  const run = runHostSdk(options, message => {
    if (message.kind === 'endpoint') {
      reported();
      return new Promise<void>(resolve => { acknowledge = resolve; });
    }
    if (message.kind === 'status' && message.relayConnected) stop.abort();
  }, stop.signal, {
    tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })),
    management: () => management, host: m => new Host(m),
  });
  await endpointReported;
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(registered, 0);
  acknowledge();
  await run;
  assert.equal(registered, 1);
});

test('probe throttling respects Retry-After instead of polling a fresh client every check', async () => {
  let calls = 0;
  const probe = new HostProbe([22, 3000], { local: async () => {}, remote: async () => {
    calls++; throw new Fault('throttled', undefined, 60000);
  } });
  const signal = new AbortController().signal;
  assert.equal((await probe.check([22, 3000], signal, 0)).replace, false);
  await probe.check([22, 3000], signal, 10000);
  assert.equal(calls, 1);
  assert.equal((await probe.check([22, 3000], signal, 60000)).status.probeFault, 'throttled');
  assert.equal(calls, 2);
});

test('connected hosts periodically refresh the existing configured port set', async () => {
  const stop = new AbortController();
  let now = 0, refreshes = 0;
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({ config, status: 200, statusText: 'OK', headers: {},
      data: { tunnelId: 'example', clusterId: 'usw2', ports: [{ portNumber: 22 }, { portNumber: 3000 }] } }));
  class Host extends TunnelRelayTunnelHost {
    override get connectionStatus() { return ConnectionStatus.Connected; }
    override async connect() {}
    override async refreshPorts() { refreshes++; stop.abort(); }
    override async dispose() {}
  }
  await runHostSdk(options, () => {}, stop.signal, {
    tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })),
    management: () => management, host: m => new Host(m),
    now: () => now, wait: async () => { now += 61000; if (now > 122000) throw new Fault('invalid-config'); },
  });
  assert.equal(refreshes, 1);
});

test('relay readiness is reported before startup probes can delay the connection result', async () => {
  const stop = new AbortController(), messages: HostMessage[] = [];
  let readyDuringConnect: HostMessage | undefined;
  const management = new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({ config, status: 200, statusText: 'OK', headers: {},
      data: { tunnelId: 'example', clusterId: 'usw2', ports: [{ portNumber: 22 }] } }));
  class Host extends TunnelRelayTunnelHost {
    override async connect() {
      this.connectionStatus = ConnectionStatus.Connected;
      readyDuringConnect = messages.at(-1);
      stop.abort();
    }
    override async dispose() {}
  }
  await runHostSdk({ ...options, probePorts: [22] }, message => { messages.push(message); }, stop.signal, {
    tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })),
    management: () => management, host: m => new Host(m),
  });
  assert.deepEqual(readyDuringConnect, { kind: 'status', state: 'connected', relayConnected: true,
    ports: [22], localUnavailable: [], verifiedPorts: [] });
  assert.equal(messages.some(message => message.kind === 'status' && message.state === 'verified'), false);
});
