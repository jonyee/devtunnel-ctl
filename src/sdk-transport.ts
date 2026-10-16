import { TunnelRelayTunnelClient, ConnectionStatus, RelayConnectionError } from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { CancellationTokenSource, SecureStream } from '@microsoft/dev-tunnels-ssh';
import type { CancellationToken } from '@microsoft/dev-tunnels-ssh';
import type { Tunnel } from '@microsoft/dev-tunnels-contracts';
import { TunnelConnectionMode } from '@microsoft/dev-tunnels-contracts';
import { Agent } from 'node:https';
import type { Duplex } from 'node:stream';
import type { Profile } from './config.ts';
import { TokenProvider } from './auth.ts';
import { bounded, Fault, safeFault } from './core.ts';

export interface TransportEvent {
  kind: 'relay' | 'keepalive-failed' | 'keepalive-succeeded' | 'retry';
  connected?: boolean;
  fault?: Fault;
}
export interface Session {
  readonly connected: boolean;
  open(signal: AbortSignal): Promise<Duplex>;
  dispose(): Promise<void>;
}
export interface Transport {
  connect(profile: Profile, signal: AbortSignal, event: (event: TransportEvent) => void): Promise<Session>;
}

async function sdkCall<T>(signal: AbortSignal, action: (cancellation: CancellationToken) => Promise<T>): Promise<T> {
  const source = new CancellationTokenSource();
  const abort = () => source.cancel();
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) source.cancel();
  try { return await action(source.token); }
  finally { signal.removeEventListener('abort', abort); source.dispose(); }
}

async function sdkRefresh<T>(signal: AbortSignal, cancellation: CancellationToken | undefined,
  timeoutMs: number, action: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController();
  const subscription = cancellation?.onCancellationRequested(() => controller.abort());
  if (cancellation?.isCancellationRequested) controller.abort();
  try { return await bounded(action, timeoutMs, AbortSignal.any([signal, controller.signal])); }
  finally { subscription?.dispose(); }
}

export async function resolveTunnel(
  profile: Profile, tokens: TokenProvider, management: TunnelManagementHttpClient, signal: AbortSignal,
): Promise<Tunnel> {
  for (let attempt = 0; attempt < 2; attempt++) {
    const token = await tokens.get(profile, signal, attempt > 0);
    const clusterId = profile.clusterId ?? token.clusterId;
    if (!clusterId) throw new Fault('invalid-config', 'Specify --cluster: the token has no SDK routing hint.');
    try {
      const tunnel = await bounded(inner => sdkCall(inner, cancellation => management.getTunnel(
        { tunnelId: profile.tunnelId, clusterId },
        { accessToken: token.value, includePorts: true, followRedirects: true }, cancellation,
      )), 8000, signal);
      if (!tunnel) throw new Fault('remote-unavailable');
      return { ...tunnel, accessTokens: { connect: token.value } };
    } catch (error) {
      const fault = safeFault(error);
      if (fault.code === 'login-required' && attempt === 0) continue;
      throw fault;
    }
  }
  throw new Fault('login-required');
}

export class SdkTransport implements Transport {
  private readonly tokens: TokenProvider;
  private readonly managementFactory: (agent: Agent) => TunnelManagementHttpClient;
  private readonly clientFactory: () => TunnelRelayTunnelClient;
  constructor(tokens: TokenProvider,
    managementFactory = (agent: Agent) => new TunnelManagementHttpClient(
      { name: 'devtunnel-ctl', version: '0.1.0' }, ManagementApiVersions.Version20230927preview,
      undefined, undefined, agent),
    clientFactory = () => new TunnelRelayTunnelClient(undefined, () => {}),
  ) {
    this.tokens = tokens;
    this.managementFactory = managementFactory;
    this.clientFactory = clientFactory;
  }
  async connect(profile: Profile, signal: AbortSignal, event: (e: TransportEvent) => void): Promise<Session> {
    const agent = new Agent({ keepAlive: false, maxSockets: 8, timeout: 10000 });
    const management = this.managementFactory(agent);
    management.trace = () => {};
    management.enableEventsReporting = false;
    const client = this.clientFactory();
    client.trace = () => {};
    client.acceptLocalConnectionsForForwardedPorts = false;
    const lifetime = new AbortController();
    const streams = new Set<Duplex>();
    const destroyStream = (stream: Duplex): void => {
      if (stream instanceof SecureStream) stream.dispose();
      stream.destroy();
    };
    const metadata = async (signal: AbortSignal): Promise<Tunnel> => {
      const tunnel = await resolveTunnel(profile, this.tokens, management, signal);
      const endpoints = tunnel.endpoints?.filter(e => e.connectionMode === TunnelConnectionMode.TunnelRelay);
      if (!endpoints?.length) throw new Fault('remote-unavailable');
      if (endpoints.some(e => !e.hostPublicKeys?.length)) {
        throw new Fault('remote-unavailable', 'Tunnel host has not published public keys; refusing unverified host identity.');
      }
      return tunnel;
    };
    const subscriptions: { dispose(): void }[] = [];
    let disposed = false, established = false, refreshes = 0, authRetried = false, graceUntil = 0;
    let refreshEpoch = 0;
    let refreshFailure: Fault | undefined;
    const clearRefreshEvidence = () => { refreshEpoch++; refreshFailure = undefined; };
    const connectionFault = (error: unknown): Fault => {
      // The SDK can rethrow the original relay 401 after its refresh handler fails.
      if (error instanceof RelayConnectionError && error.errorContext.statusCode === 401 && refreshFailure) {
        return refreshFailure;
      }
      return safeFault(error);
    };
    let disposal: Promise<void> | undefined;
    const emit = (e: TransportEvent) => { if (!disposed && !lifetime.signal.aborted) event(e); };
    const dispose = (): Promise<void> => {
      if (disposal) return disposal;
      disposed = true;
      clearRefreshEvidence();
      lifetime.abort();
      agent.destroy();
      for (const stream of streams) destroyStream(stream);
      streams.clear();
      signal.removeEventListener('abort', abort);
      for (const subscription of subscriptions) subscription.dispose();
      disposal = bounded(() => Promise.all([client.dispose(), management.dispose()]).then(() => {}), 2000);
      return disposal;
    };
    const abort = () => { void dispose().catch(error => event({ kind: 'retry', fault: safeFault(error) })); };
    const relayFactory = client.streamFactory;
    client.streamFactory = {
      createRelayStream: async (...args: Parameters<typeof relayFactory.createRelayStream>) => {
        if (!args[0].startsWith('wss://')) throw new Fault('remote-unavailable', 'Relay endpoint must use authenticated TLS.');
        try {
          return await bounded(() => relayFactory.createRelayStream(args[0], args[1], args[2],
            { ...args[3], tlsOptions: { ...args[3]?.tlsOptions, agent } }, args[4]),
          10000, lifetime.signal, late => bounded(() => late.stream.close(), 1000));
        } catch (error) { agent.destroy(); throw error; }
      },
    };
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) { await dispose(); throw new Fault('aborted'); }
    subscriptions.push(client.portForwarding(e => { e.cancel = e.portNumber !== profile.remotePort; }));
    subscriptions.push(client.connectionStatusChanged(e => {
      const connected = e.status === ConnectionStatus.Connected;
      const fault = e.disconnectError ? connectionFault(e.disconnectError) : undefined;
      if (connected) {
        established = true; graceUntil = 0; refreshes = 0; authRetried = false;
        clearRefreshEvidence();
      } else {
        if (e.status === ConnectionStatus.Connecting &&
            (e.previousStatus === ConnectionStatus.Connected || e.previousStatus === ConnectionStatus.Disconnected)) {
          refreshes = 0; authRetried = false;
          clearRefreshEvidence();
        }
        if (established && !graceUntil) graceUntil = Date.now() + 8000;
      }
      emit({ kind: 'relay', connected, ...(fault ? { fault } : {}) });
    }));
    subscriptions.push(client.keepAliveFailed(() => emit({ kind: 'keepalive-failed' })));
    subscriptions.push(client.keepAliveSucceeded(() => emit({ kind: 'keepalive-succeeded' })));
    subscriptions.push(client.retryingTunnelConnection(e => {
      const fault = connectionFault(e.error);
      const initialAuthRetry = !established && refreshes === 1 && !authRetried;
      if (initialAuthRetry) authRetried = true;
      // The outer controller owns retry scheduling; the SDK only owns a short resumable grace.
      e.retry = (initialAuthRetry || (established && Date.now() < graceUntil)) &&
        fault.code !== 'access-denied' && fault.code !== 'throttled' && !refreshFailure && !disposed;
      emit({ kind: 'retry', fault });
    }));
    subscriptions.push(client.refreshingTunnelAccessToken(e => {
      clearRefreshEvidence();
      const epoch = refreshEpoch;
      const current = () => epoch === refreshEpoch && !disposed && !lifetime.signal.aborted;
      e.tunnelAccessToken = sdkRefresh(lifetime.signal, e.cancellation, 12000, inner => {
        if (!current()) throw new Fault('aborted');
        if (++refreshes > 1) throw new Fault('login-required');
        return this.tokens.get(profile, inner, true).then(token => token.value);
      }).then(token => {
        if (!current()) throw new Fault('aborted');
        return token;
      }).catch(error => {
        if (!current()) throw new Fault('aborted');
        const fault = safeFault(error);
        const sanitized = new Fault(fault.code, undefined, fault.retryAfterMs);
        refreshFailure = sanitized;
        throw sanitized;
      });
    }));
    subscriptions.push(client.refreshingTunnel(e => {
      e.tunnelPromise = sdkRefresh(lifetime.signal, e.cancellation, 20000, metadata);
    }));
    try {
      const tunnel = await metadata(lifetime.signal);
      await bounded(inner => sdkCall(inner, cancellation => client.connect(tunnel, {
        enableRetry: true, enableReconnect: true, keepAliveIntervalInSeconds: 10, httpAgent: agent,
      }, cancellation)), 15000, lifetime.signal);
      await bounded(inner => sdkCall(inner, cancellation =>
        client.waitForForwardedPort(profile.remotePort, cancellation)), 5000, lifetime.signal);
      if (disposed || signal.aborted) throw new Fault('aborted');
      return {
        get connected() { return !disposed && client.connectionStatus === ConnectionStatus.Connected; },
        open: async openSignal => {
          if (disposed) throw new Fault('aborted');
          const stream = await bounded(inner => sdkCall(inner, cancellation =>
            client.connectToForwardedPort(profile.remotePort, cancellation)), 3000,
          AbortSignal.any([openSignal, lifetime.signal]), destroyStream);
          if (disposed || openSignal.aborted || stream.destroyed) {
            destroyStream(stream);
            throw new Fault('aborted');
          }
          streams.add(stream);
          stream.on('error', () => destroyStream(stream));
          stream.once('close', () => {
            streams.delete(stream);
            // SecureStream.destroy() alone does not close its separate SDK SSH session.
            if (stream instanceof SecureStream) stream.dispose();
          });
          return stream;
        },
        dispose,
      };
    } catch (error) {
      const fault = signal.aborted || lifetime.signal.aborted ? new Fault('aborted') : connectionFault(error);
      await dispose().catch(disposeError => event({ kind: 'retry', fault: safeFault(disposeError) }));
      throw fault;
    }
  }
}
