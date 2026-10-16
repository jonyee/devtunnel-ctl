import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import net from 'node:net';
import { once } from 'node:events';
import { TunnelRelayTunnelClient } from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { TokenProvider } from '../src/auth.ts';
import { SecureStream, CancellationTokenSource } from '@microsoft/dev-tunnels-ssh';
import type { CancellationToken } from '@microsoft/dev-tunnels-ssh';
import { SdkTransport, resolveTunnel } from '../src/sdk-transport.ts';

const profile = { name: 'test', tunnelId: 'example', clusterId: 'usw2', remotePort: 22, localPort: 23022, paused: false };
const jwt = `eyJhbGciOiJub25lIn0.${Buffer.from(JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 3600, clusterId: 'usw2' })).toString('base64url')}.signature`;
const tokens = () => new TokenProvider('/bin/not-used', async () => JSON.stringify({ token: jwt }));

test('actual management SDK uses scoped Tunnel authentication and cluster routing, no identity provider', async () => {
  let requests = 0;
  const management = new TunnelManagementHttpClient('devtunnel-ctl-tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => {
      requests++;
      assert.match(config.url!, /^https:\/\/usw2\./);
      assert.match(config.url!, /\/tunnels\/example/);
      assert.equal(config.headers?.Authorization, `tunnel ${jwt}`);
      assert.match(config.url!, /includePorts=true/);
      return { data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [] },
        status: 200, statusText: 'OK', headers: {}, config };
    });
  const tunnel = await resolveTunnel(profile, tokens(), management, new AbortController().signal);
  assert.equal(tunnel.accessTokens?.connect, jwt);
  assert.equal(requests, 1);
  await management.dispose();
});

test('adapter configures real SDK client safely and closes on cancellation', async () => {
  let disposed = false, connected = false;
  class Client extends TunnelRelayTunnelClient {
    override async connect(...[tunnel, options]: Parameters<TunnelRelayTunnelClient['connect']>) {
      connected = true;
      assert.ok(options);
      assert.equal(this.acceptLocalConnectionsForForwardedPorts, false);
      assert.equal(this.enableE2EEncryption, true);
      assert.equal(options.keepAliveIntervalInSeconds, 10);
      assert.equal(options.enableReconnect, true);
      assert.equal(tunnel.accessTokens?.connect, jwt);
    }
    override async waitForForwardedPort(port: number) { assert.equal(port, 22); }
    override async connectToForwardedPort(port: number) { assert.equal(port, 22); return new PassThrough(); }
    override async dispose() { disposed = true; }
  }
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{ connectionMode: 'TunnelRelay', hostId: 'host', hostPublicKeys: ['test-key'], clientRelayUri: 'wss://example.invalid/' }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  const transport = new SdkTransport(tokens(), () => management, () => new Client());
  const controller = new AbortController();
  const session = await transport.connect(profile, controller.signal, () => {});
  assert.equal(connected, true);
  const stream = await session.open(controller.signal);
  stream.destroy();
  controller.abort();
  await session.dispose();
  assert.equal(disposed, true);
});

test('missing endpoint host keys are rejected rather than relying on SDK permissive fallback', async () => {
  let connected = false;
  class Client extends TunnelRelayTunnelClient {
    override async connect() { connected = true; }
    override async dispose() {}
  }
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{ connectionMode: 'TunnelRelay', hostId: 'h', clientRelayUri: 'wss://example.invalid' }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  await assert.rejects(new SdkTransport(tokens(), () => management, () => new Client())
    .connect(profile, new AbortController().signal, () => {}), /host|public key/i);
  assert.equal(connected, false);
});

test('cancelling an actual SDK WebSocket handshake closes pending network sockets', { timeout: 5000 }, async () => {
  const sockets = new Set<net.Socket>();
  const server = net.createServer(socket => {
    sockets.add(socket); socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const addr = server.address(); assert.ok(addr && typeof addr !== 'string');
  const relayPort = addr.port;
  class Client extends TunnelRelayTunnelClient {
    override async connect(...[_tunnel, options]: Parameters<TunnelRelayTunnelClient['connect']>) {
      assert.ok(options);
      assert.ok(options.httpAgent);
      this.httpAgent = options.httpAgent;
      await this.streamFactory.createRelayStream(`wss://127.0.0.1:${relayPort}`, ['test'], undefined, { tlsOptions: { agent: this.httpAgent } });
    }
    override async dispose() {}
  }
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{ connectionMode: 'TunnelRelay', hostId: 'h', hostPublicKeys: ['test-key'], clientRelayUri: `wss://127.0.0.1:${addr.port}` }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  try {
    const controller = new AbortController();
    const connecting = new SdkTransport(tokens(), () => management, () => new Client())
      .connect(profile, controller.signal, () => {});
    const rejected = assert.rejects(connecting);
    await once(server, 'connection');
    controller.abort();
    await rejected;
    const deadline = Date.now() + 1000;
    while (sockets.size && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10));
    assert.equal(sockets.size, 0, 'obsolete handshake must not retain sockets');
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});

test('clusterless IDs use a validated token routing hint; absent hints require explicit cluster', async () => {
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2' }, status: 200, statusText: 'OK', headers: {}, config,
    }));
  const { clusterId: _, ...clusterless } = profile;
  assert.equal((await resolveTunnel(clusterless, tokens(), management, new AbortController().signal)).clusterId, 'usw2');
  await management.dispose();
});

test('management 401 refreshes exactly once and repeated rejection surfaces required sign-in', async () => {
  let acquisitions = 0, requests = 0;
  const provider = new TokenProvider('/unused', async () => {
    acquisitions++;
    return JSON.stringify({ token: jwt.replace(/signature$/, 'signature' + acquisitions) });
  });
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => {
      requests++;
      if (requests === 1 || requests >= 3) throw Object.assign(new Error('private failure'), { response: { status: 401, headers: {}, data: {} } });
      assert.equal(config.headers.Authorization, `tunnel ${jwt.replace(/signature$/, 'signature2')}`);
      return { data: { tunnelId: 'example', clusterId: 'usw2' }, status: 200, statusText: 'OK', headers: {}, config };
    });
  try {
    await resolveTunnel(profile, provider, management, new AbortController().signal);
    assert.equal(acquisitions, 2); assert.equal(requests, 2);
    await assert.rejects(resolveTunnel(profile, provider, management, new AbortController().signal),
      error => error instanceof Error && 'code' in error && error.code === 'login-required');
    assert.equal(requests, 4);
    assert.equal(acquisitions, 3);
  } finally { provider.clear(); await management.dispose(); }
});

test('SDK endpoint refresh hook retrieves new metadata and updates host keys', async () => {
  let requests = 0;
  class Client extends TunnelRelayTunnelClient {
    override async connect() {}
    override async waitForForwardedPort() {}
    override async dispose() {}
    refreshForTest() { return this.refreshTunnel(false); }
  }
  const client = new Client();
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{
        connectionMode: 'TunnelRelay', hostId: 'h', hostPublicKeys: ++requests < 3 ? ['key-' + requests] : undefined,
        clientRelayUri: 'wss://example.invalid/' + requests,
      }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  const session = await new SdkTransport(tokens(), () => management, () => client)
    .connect(profile, new AbortController().signal, () => {});
  try {
    await client.refreshForTest();
    assert.equal(requests, 2);
    assert.deepEqual(client.endpoints?.[0]?.hostPublicKeys, ['key-2']);
    await assert.rejects(client.refreshForTest(), /public keys|host identity/i);
    assert.deepEqual(client.endpoints?.[0]?.hostPublicKeys, ['key-2']);
  } finally { await session.dispose(); }
});

test('destroying a returned SDK SecureStream disposes its separate encrypted SSH session', async () => {
  const raw = new PassThrough(), secure = new SecureStream(raw, { username: 'tunnel' });
  class Client extends TunnelRelayTunnelClient {
    override async connect() {}
    override async waitForForwardedPort() {}
    override async connectToForwardedPort() { return secure; }
    override async dispose() {}
  }
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{ connectionMode: 'TunnelRelay', hostId: 'h', hostPublicKeys: ['key'], clientRelayUri: 'wss://example.invalid' }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  const session = await new SdkTransport(tokens(), () => management, () => new Client())
    .connect(profile, new AbortController().signal, () => {});
  try {
    const stream = await session.open(new AbortController().signal);
    stream.destroy();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(secure.isClosed, true);
    assert.equal(raw.destroyed, true);
  } finally { secure.dispose(); await session.dispose(); }
});

test('SDK refresh cancellation aborts the private token helper rather than waiting for its deadline', async () => {
  let acquisitions = 0, helperAborted = false, started!: () => void;
  const helperStarted = new Promise<void>(resolve => { started = resolve; });
  const provider = new TokenProvider('/unused', async (_path, _args, signal) => {
    if (++acquisitions === 1) return JSON.stringify({ token: jwt });
    return new Promise<string>((_resolve, reject) => {
      signal!.addEventListener('abort', () => { helperAborted = true; reject(new Error('cancelled')); }, { once: true });
      started();
    });
  });
  class Client extends TunnelRelayTunnelClient {
    override async connect() {}
    override async waitForForwardedPort() {}
    override async dispose() {}
    freshToken(cancellation: CancellationToken) { return this.getFreshTunnelAccessToken(cancellation); }
  }
  const client = new Client();
  const management = new TunnelManagementHttpClient('tests', ManagementApiVersions.Version20230927preview,
    undefined, undefined, undefined, async config => ({
      data: { tunnelId: 'example', clusterId: 'usw2', endpoints: [{ connectionMode: 'TunnelRelay', hostId: 'h', hostPublicKeys: ['key'], clientRelayUri: 'wss://example.invalid' }] },
      status: 200, statusText: 'OK', headers: {}, config,
    }));
  const session = await new SdkTransport(provider, () => management, () => client)
    .connect(profile, new AbortController().signal, () => {});
  const source = new CancellationTokenSource();
  try {
    const refreshing = client.freshToken(source.token);
    void refreshing.catch(() => {});
    await helperStarted;
    source.cancel();
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(helperAborted, true);
    await assert.rejects(refreshing);
  } finally { source.dispose(); await session.dispose(); provider.clear(); }
});
