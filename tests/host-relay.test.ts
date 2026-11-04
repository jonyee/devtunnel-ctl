import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import net from 'node:net';
import { Duplex } from 'node:stream';
import { EventEmitter, once } from 'node:events';
import { createRequire } from 'node:module';
import { TunnelRelayTunnelHost } from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { NodeStream, SshClientSession, SshDisconnectReason, SshServerSession, SshSessionConfiguration, SshStream } from '@microsoft/dev-tunnels-ssh';
import { PortForwardingService } from '@microsoft/dev-tunnels-ssh-tcp';
import { runHostSdk } from '../src/host-sdk.ts';
import { TokenProvider } from '../src/auth.ts';
import { bounded } from '../src/core.ts';
import type { HostMessage } from '../src/host.ts';

const { server: WebSocketServer } = createRequire(import.meta.url)('websocket');
interface Connection extends EventEmitter {
  sendBytes(data: Buffer, callback: (error?: Error) => void): void;
  close(): void;
}
interface Request { accept(protocol: string): Connection }

async function startHost(initialPorts: number[]) {
  const server = createServer();
  const websocket = new WebSocketServer({ httpServer: server, keepalive: false });
  const peer = new SshServerSession(new SshSessionConfiguration(false));
  const stop = new AbortController(), messages: HostMessage[] = [];
  let ports = initialPorts, ignorePings = false, pingCount = 0;
  let connected!: () => void, disconnected!: () => void, healthy!: () => void, failed!: (error: unknown) => void;
  const ready = new Promise<void>((resolve, reject) => { connected = resolve; failed = reject; });
  const lost = new Promise<void>(resolve => { disconnected = resolve; });
  const responsive = new Promise<void>(resolve => { healthy = resolve; });
  websocket.on('request', (request: Request) => {
    const connection = request.accept(TunnelRelayTunnelHost.webSocketSubProtocol);
    const bridge = new Duplex({
      read() {},
      write(chunk: Buffer, _encoding, callback) { connection.sendBytes(chunk, callback); },
      destroy(error, callback) { connection.close(); callback(error); },
    });
    connection.on('message', (message: { binaryData: Buffer }) => bridge.push(message.binaryData));
    connection.on('close', () => bridge.push(null));
    connection.on('ping', (cancel: () => void) => {
      if (ignorePings) cancel();
      else if (++pingCount >= 2) healthy();
    });
    void peer.connect(new NodeStream(bridge)).catch(error => { failed(error); stop.abort(); });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address() as net.AddressInfo;
  const jwt = ['eyJhbGciOiJub25lIn0', Buffer.from(JSON.stringify({
    exp: Math.floor(Date.now() / 1000) + 3600, clusterId: 'usw2',
  })).toString('base64url'), 'signature'].join('.');
  let host!: TunnelRelayTunnelHost, wasConnected = false;
  const done = runHostSdk({ tunnelId: 'example', clusterId: 'usw2', helperPath: '/unused', probePorts: [] }, message => {
    messages.push(message);
    if (message.kind !== 'status') return;
    if (message.relayConnected) { wasConnected = true; connected(); }
    else if (wasConnected) { disconnected(); stop.abort(); }
  }, stop.signal, {
    tokens: new TokenProvider('/unused', async () => JSON.stringify({ token: jwt })),
    management: () => new TunnelManagementHttpClient('test', ManagementApiVersions.Version20230927preview,
      undefined, undefined, undefined, async config => ({ config, status: 200, statusText: 'OK', headers: {},
        data: config.method === 'put' ? { hostRelayUri: 'wss://relay.invalid' } : config.method === 'delete' ? true :
          { tunnelId: 'example', clusterId: 'usw2', ports: ports.map(portNumber => ({ portNumber })) },
      })),
    host: management => {
      host = new TunnelRelayTunnelHost(management, () => {});
      const factory = host.streamFactory;
      host.streamFactory = { createRelayStream: async (_uri, protocols, token, config, headers) =>
        factory.createRelayStream(`ws://127.0.0.1:${address.port}`, protocols, token,
          { ...config, keepaliveInterval: 25, keepaliveGracePeriod: 100 }, headers) };
      return host;
    },
  });
  void done.catch(failed);
  return { host, peer, messages, ready, lost, responsive,
    setPorts(value: number[]) { ports = value; },
    stall() { ignorePings = true; },
    async close() {
      stop.abort(); peer.dispose(); websocket.shutDown();
      try { await done; }
      finally { await new Promise<void>(resolve => server.close(() => resolve())); }
    },
  };
}

test('relay keepalives detect a silent transport without application probes or client sessions', async () => {
  const relay = await startHost([]);
  try {
    await bounded(() => relay.ready, 5000);
    await bounded(() => relay.responsive, 5000);
    const healthy = relay.messages.at(-1);
    assert.ok(healthy?.kind === 'status' && healthy.relayConnected && healthy.state === 'connected');
    relay.stall();
    await bounded(() => relay.lost, 5000);
    const last = relay.messages.at(-1);
    assert.ok(last?.kind === 'status' && !last.relayConnected);
  } finally { await relay.close(); }
});

test('metadata removal revokes new V1 streams while unrelated forwarded ports remain usable', async () => {
  const applications = [net.createServer(socket => socket.end('first')), net.createServer(socket => socket.end('second'))];
  for (const application of applications) { application.listen(0, '127.0.0.1'); await once(application, 'listening'); }
  const ports = applications.map(application => (application.address() as net.AddressInfo).port);
  const relay = await startHost(ports);
  const config = new SshSessionConfiguration(); config.addService(PortForwardingService);
  const client = new SshClientSession(config);
  client.onAuthenticating(event => { event.authenticationPromise = Promise.resolve({}); });
  client.onRequest(event => { event.isAuthorized = true; });
  const forwarding = client.activateService(PortForwardingService);
  forwarding.acceptLocalConnectionsForForwardedPorts = false;
  const readPort = async (port: number) => {
    const stream = await forwarding.connectToForwardedPort(port);
    try { const [data] = await once(stream, 'data'); return (data as Buffer).toString(); }
    finally { stream.destroy(); }
  };
  try {
    await bounded(() => relay.ready, 5000);
    const channel = await relay.peer.openChannel(TunnelRelayTunnelHost.clientStreamChannelType);
    await client.connect(new NodeStream(new SshStream(channel)));
    assert.equal(await client.authenticate({ username: 'tunnel' }), true);
    for (const port of ports) await bounded(() => forwarding.waitForForwardedPort(port), 5000);
    assert.equal(await readPort(ports[0]!), 'first');
    assert.equal(await readPort(ports[1]!), 'second');
    relay.setPorts([ports[1]!]);
    await relay.host.refreshPorts();
    await assert.rejects(forwarding.connectToForwardedPort(ports[0]!));
    assert.equal(await readPort(ports[1]!), 'second');
  } finally {
    await client.close(SshDisconnectReason.byApplication); client.dispose();
    await relay.close();
    for (const application of applications) await new Promise<void>(resolve => application.close(() => resolve()));
  }
});
