import net from 'node:net';
import { Agent } from 'node:https';
import { TunnelRelayTunnelHost, ConnectionStatus, RelayConnectionError } from '@microsoft/dev-tunnels-connections';
import { TunnelManagementHttpClient, ManagementApiVersions } from '@microsoft/dev-tunnels-management';
import { SshDisconnectReason } from '@microsoft/dev-tunnels-ssh';
import { TokenProvider } from './auth.ts';
import { bounded, Fault, safeFault } from './core.ts';
import type { FaultCode } from './core.ts';
import { resolveTunnel, sdkCall, sdkRefresh, SdkTransport } from './sdk-transport.ts';
import type { HostMessage, HostOptions, HostStatus } from './host.ts';
import { sleep } from './host.ts';

type HostManagement = TunnelManagementHttpClient & { readonly httpsAgent: Agent };
export function hostManagement(agent: Agent): HostManagement {
  const management = Object.assign(new TunnelManagementHttpClient({ name: 'devtunnel-ctl', version: '0.1.0' },
    ManagementApiVersions.Version20230927preview, undefined, undefined, agent), { httpsAgent: agent });
  management.trace = () => {};
  management.enableEventsReporting = false;
  return management;
}
export function localPort(port: number, signal: AbortSignal): Promise<void> {
  return bounded(inner => new Promise<void>((resolve, reject) => {
    const socket = net.connect({ host: '127.0.0.1', port });
    const abort = () => { socket.destroy(); reject(new Fault('aborted')); };
    inner.addEventListener('abort', abort, { once: true });
    const finish = (error?: Fault) => {
      inner.removeEventListener('abort', abort); socket.destroy();
      if (error) reject(error); else resolve();
    };
    socket.once('connect', () => finish());
    socket.once('error', () => finish(new Fault('remote-unavailable')));
  }), 3000, signal);
}

interface ProbeDependencies {
  local(port: number, signal: AbortSignal): Promise<void>;
  remote(port: number, signal: AbortSignal): Promise<void>;
}
export class HostProbe {
  private readonly ports: number[];
  private readonly deps: ProbeDependencies;
  private readonly failures = new Map<number, number>();
  private retryAt = 0;
  constructor(ports: number[], deps: ProbeDependencies) { this.ports = ports; this.deps = deps; }
  async check(ports: number[], signal: AbortSignal, age: number): Promise<{ status: HostStatus; replace: boolean }> {
    const status: HostStatus = { kind: 'status', state: 'connected', relayConnected: true,
      ports, localUnavailable: [], verifiedPorts: [] };
    let replace = false;
    for (const port of this.ports) {
      if (!ports.includes(port)) throw new Fault('invalid-config', 'A probe port is not configured on this tunnel.');
      if (age < this.retryAt) { status.probeFault = 'throttled'; continue; }
      try { await this.deps.local(port, signal); }
      catch (error) {
        if (signal.aborted) throw new Fault('aborted');
        if (!['remote-unavailable', 'timeout'].includes(safeFault(error).code)) throw error;
        status.localUnavailable.push(port); this.failures.delete(port); continue;
      }
      try {
        await this.deps.remote(port, signal);
        status.verifiedPorts.push(port); this.failures.delete(port);
      } catch (error) {
        if (signal.aborted) throw new Fault('aborted');
        const fault = safeFault(error);
        status.probeFault = fault.code;
        if (['login-required', 'access-denied', 'invalid-config', 'throttled'].includes(fault.code)) {
          this.failures.delete(port);
          if (fault.code === 'throttled') this.retryAt = age + Math.max(1000, fault.retryAfterMs);
        } else {
          // Recheck the application after a failed tunnel attempt to avoid a downtime race.
          try { await this.deps.local(port, signal); }
          catch { status.localUnavailable.push(port); this.failures.delete(port); continue; }
          const count = (this.failures.get(port) ?? 0) + 1;
          this.failures.set(port, count);
          if (count >= 2 && age >= 30000) replace = true;
        }
      }
    }
    status.state = this.ports.length && status.verifiedPorts.length === this.ports.length ? 'verified' :
      status.localUnavailable.length || status.probeFault ? 'degraded' : 'connected';
    return { status, replace };
  }
}

interface HostSdkDependencies {
  tokens?: TokenProvider;
  management?(agent: Agent): TunnelManagementHttpClient;
  host?(management: HostManagement): TunnelRelayTunnelHost;
  now?(): number;
  wait?: typeof sleep;
}
export async function runHostSdk(options: HostOptions, report: (message: HostMessage) => void | Promise<void>,
  parent: AbortSignal, deps: HostSdkDependencies = {}): Promise<void> {
  const tokens = deps.tokens ?? new TokenProvider(options.helperPath);
  const now = deps.now ?? Date.now;
  const agent = new Agent({ keepAlive: false, maxSockets: 16, timeout: 10000 });
  const management = Object.assign((deps.management ?? hostManagement)(agent), { httpsAgent: agent });
  const host = deps.host?.(management) ?? new TunnelRelayTunnelHost(management, () => {});
  host.trace = () => {};
  const life = new AbortController(), signal = AbortSignal.any([parent, life.signal]);
  const subscriptions: { dispose(): void }[] = [];
  let fault: Fault | undefined, metadata: Awaited<ReturnType<typeof resolveTunnel>> | undefined;
  let refreshFailure: Fault | undefined, refreshEpoch = 0, refreshes = 0, graceUntil = 0;
  let established = false, authRetried = false;
  const fail = (error: unknown) => {
    fault ??= classify(error); life.abort();
  };
  const classify = (error: unknown): Fault => {
    if (host.disconnectReason === SshDisconnectReason.tooManyConnections ||
        (error instanceof Error && 'reason' in error && error.reason === SshDisconnectReason.tooManyConnections)) {
      return new Fault('host-superseded');
    }
    if (error instanceof RelayConnectionError && error.errorContext.statusCode === 401 && refreshFailure) return refreshFailure;
    return safeFault(error);
  };
  const readMetadata = async (inner: AbortSignal) => {
    const tunnel = await resolveTunnel(options, tokens, management, inner, 'host');
    const ports = tunnel.ports?.map(p => p.portNumber) ?? [];
    if (options.probePorts.some(p => !ports.includes(p))) throw new Fault('invalid-config', 'A probe port is not configured on this tunnel.');
    // SDK 1.3.61 refreshPorts() iterates its Map with Object.entries(), missing removals.
    for (const [key, forwarder] of host.remoteForwarders) {
      if (!ports.includes(forwarder.localPort)) {
        host.remoteForwarders.delete(key);
        forwarder.dispose();
      }
    }
    metadata = tunnel;
    return tunnel;
  };
  const originalUpdate = management.updateTunnelEndpoint.bind(management);
  management.updateTunnelEndpoint = async (tunnel, endpoint, requestOptions, cancellation) => {
    if (signal.aborted) throw new Fault('aborted');
    if (!endpoint.id || !tunnel.tunnelId || !tunnel.clusterId) throw new Fault('invalid-config');
    // The supervisor records the exact owned ID before a request can be interrupted.
    await report({ kind: 'endpoint', id: endpoint.id, tunnelId: tunnel.tunnelId, clusterId: tunnel.clusterId });
    return sdkRefresh(signal, cancellation, 8000, inner => sdkCall(inner, cancel =>
      originalUpdate(tunnel, endpoint, requestOptions, cancel)));
  };
  const relayFactory = host.streamFactory;
  host.streamFactory = {
    createRelayStream: async (...args: Parameters<typeof relayFactory.createRelayStream>) => {
      if (!args[0].startsWith('wss://')) throw new Fault('invalid-config', 'Host relay must use authenticated TLS.');
      const config = { ...args[3], keepalive: true, useNativeKeepalive: false, keepaliveInterval: 10000,
        dropConnectionOnKeepaliveTimeout: true, keepaliveGracePeriod: 10000,
        tlsOptions: { ...args[3]?.tlsOptions, agent } };
      return bounded(() => relayFactory.createRelayStream(args[0], args[1], args[2], config, args[4]),
        10000, signal, late => bounded(() => late.stream.close(), 1000));
    },
  };
  subscriptions.push(host.connectionStatusChanged(e => {
    if (signal.aborted) return;
    if (e.status === ConnectionStatus.Connected) {
      established = true; authRetried = false;
      graceUntil = 0; refreshFailure = undefined; refreshes = 0; refreshEpoch++;
      report({ kind: 'status', state: 'connected', relayConnected: true,
        ports: (host.tunnel ?? metadata)?.ports?.map(p => p.portNumber) ?? [], localUnavailable: [], verifiedPorts: [] });
    } else {
      if (e.status === ConnectionStatus.Connecting && e.previousStatus === ConnectionStatus.Connected) {
        refreshEpoch++; refreshFailure = undefined; refreshes = 0; authRetried = false;
      }
      if (host.disconnectReason === SshDisconnectReason.tooManyConnections) { fail(new Fault('host-superseded')); return; }
      if (e.disconnectError) {
        const error = classify(e.disconnectError);
        if (['host-superseded', 'login-required', 'access-denied', 'invalid-config', 'throttled'].includes(error.code)) { fail(error); return; }
      }
      graceUntil ||= now() + 8000;
      report({ kind: 'status', state: 'connecting', relayConnected: false,
        ports: metadata?.ports?.map(p => p.portNumber) ?? [], localUnavailable: [], verifiedPorts: [] });
    }
  }));
  subscriptions.push(host.retryingTunnelConnection(e => {
    const error = classify(e.error);
    const initialAuthRetry = !established && refreshes === 1 && !authRetried && !refreshFailure;
    if (initialAuthRetry) authRetried = true;
    e.retry = !signal.aborted && !refreshFailure && (initialAuthRetry || (established && now() < graceUntil)) &&
      !['host-superseded', 'access-denied', 'invalid-config', 'throttled'].includes(error.code);
    if (!e.retry) fail(error);
  }));
  subscriptions.push(host.refreshingTunnelAccessToken(e => {
    const epoch = ++refreshEpoch;
    refreshFailure = undefined;
    e.tunnelAccessToken = sdkRefresh(signal, e.cancellation, 12000, async inner => {
      if (++refreshes > 1) throw new Fault('login-required');
      return (await tokens.get(options, inner, true, 'host')).value;
    }).then(token => {
      if (epoch !== refreshEpoch || signal.aborted) throw new Fault('aborted');
      return token;
    }).catch(error => {
      if (epoch !== refreshEpoch || signal.aborted) throw new Fault('aborted');
      const failure = safeFault(error);
      refreshFailure = new Fault(failure.code, undefined, failure.retryAfterMs);
      throw refreshFailure;
    });
  }));
  subscriptions.push(host.refreshingTunnel(e => { e.tunnelPromise = sdkRefresh(signal, e.cancellation, 20000, readMetadata); }));
  const transport = new SdkTransport(tokens);
  const probes = new HostProbe(options.probePorts, {
    local: localPort,
    remote: async (port, inner) => {
      const profile = { ...options, name: 'host-probe', remotePort: port, localPort: 1, paused: false };
      const session = await bounded(s => transport.connect(profile, s, () => {}), 25000, inner, late => late.dispose());
      try {
        const stream = await session.open(inner);
        stream.destroy();
      } finally { await bounded(() => session.dispose(), 2000); }
    },
  });
  const destroy = () => agent.destroy();
  signal.addEventListener('abort', destroy, { once: true });
  try {
    report({ kind: 'status', state: 'connecting', relayConnected: false, ports: [], localUnavailable: [], verifiedPorts: [] });
    const tunnel = await bounded(readMetadata, 25000, signal);
    await bounded(inner => sdkCall(inner, cancellation => host.connect(tunnel, {
      enableRetry: true, enableReconnect: true, httpAgent: agent,
    }, cancellation)), 15000, signal);
    const connectedAt = now();
    let nextProbe = 0, nextRefresh = connectedAt + 60000;
    while (!signal.aborted) {
      if (host.disconnectReason === SshDisconnectReason.tooManyConnections) throw new Fault('host-superseded');
      if (host.connectionStatus !== ConnectionStatus.Connected) {
        graceUntil ||= now() + 8000;
        if (now() >= graceUntil) throw new Fault('retrying');
      } else if (now() >= nextProbe) {
        const ports = (host.tunnel ?? metadata)?.ports?.map(p => p.portNumber) ?? [];
        const result = await probes.check(ports, signal, now() - connectedAt);
        report(result.status);
        if (result.replace) throw new Fault('remote-unavailable');
        nextProbe = now() + 10000;
      }
      if (now() >= nextRefresh && host.connectionStatus === ConnectionStatus.Connected) {
        await bounded(inner => sdkCall(inner, cancellation => host.refreshPorts(cancellation)), 12000, signal);
        nextRefresh = now() + 60000;
      }
      await (deps.wait ?? sleep)(1000, signal);
    }
  } catch (error) {
    if (!parent.aborted) fault ??= classify(error);
  } finally {
    refreshEpoch++;
    for (const subscription of subscriptions) subscription.dispose();
    signal.removeEventListener('abort', destroy);
    // Delete only the SDK instance's own endpoint; never bulk-delete by host/tunnel.
    try { await bounded(() => host.dispose(), 2000); }
    catch { if (!parent.aborted) fault ??= new Fault('retrying'); }
    agent.destroy(); life.abort(); tokens.clear();
    await bounded(() => management.dispose(), 1000);
  }
  if (fault && !parent.aborted) throw fault;
}

export async function retireHostEndpoint(options: HostOptions, endpoint: Extract<HostMessage, { kind: 'endpoint' }>,
  deps: Pick<HostSdkDependencies, 'tokens' | 'management'> = {}): Promise<void> {
  if (options.clusterId && endpoint.clusterId !== options.clusterId) throw new Fault('host-cleanup-failed');
  const tokens = deps.tokens ?? new TokenProvider(options.helperPath);
  const agent = new Agent({ keepAlive: false, timeout: 10000 });
  const management = (deps.management ?? hostManagement)(agent);
  try {
    await bounded(async signal => {
      const token = await tokens.get(options, signal, false, 'host');
      await sdkCall(signal, cancellation => management.deleteTunnelEndpoints({
        tunnelId: endpoint.tunnelId, clusterId: endpoint.clusterId, accessTokens: { host: token.value },
      }, endpoint.id, undefined, cancellation));
    }, 20000);
  } catch { throw new Fault('host-cleanup-failed'); }
  finally {
    agent.destroy(); tokens.clear();
    try { await bounded(() => management.dispose(), 1000); }
    catch { throw new Fault('host-cleanup-failed'); }
  }
}
