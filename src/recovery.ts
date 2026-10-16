import type { Profile } from './config.ts';
import type { Transport, Session, TransportEvent } from './sdk-transport.ts';
import type { OpenStream } from './forwarding.ts';
import { clock as realClock, bounded, Fault, safeFault } from './core.ts';
import type { Clock, FaultCode } from './core.ts';

export type State = 'paused' | 'connecting' | 'healthy' | 'reconnecting' | FaultCode;
export interface Status {
  name: string;
  state: State;
  localPort: number;
  remotePort: number;
  tunnelId: string;
  clusterId?: string;
  paused: boolean;
  relayConnected: boolean;
  attempt: number;
  lastHealthyAt?: number;
  nextRetryAt?: number;
  cause?: { code: FaultCode; message: string };
}
interface Listener {
  listen(): Promise<void>;
  close(): Promise<void>;
  setTarget(target: OpenStream | undefined): void;
}
interface Dependencies {
  transport: Transport;
  forwarder: Listener;
  probe(signal: AbortSignal): Promise<void>;
  log(status: Status): void;
  clock?: Clock;
  random?: () => number;
}

export class Recovery {
  readonly profile: Profile;
  private readonly deps: Dependencies;
  private readonly timer: Clock;
  private readonly random: () => number;
  private session: Session | undefined;
  private sessionLife: AbortController | undefined;
  private active: { id: number; controller: AbortController } | undefined;
  private serial = 0;
  private generation = 0;
  private cancelTick: (() => void) | undefined;
  private running = false;
  private listening = false;
  private state: State = 'connecting';
  private attempts = 0;
  private remoteFailures = 0;
  private lastHealthyAt: number | undefined;
  private nextCheck = 0;
  private nextRetry = 0;
  private lastTick = 0;
  private graceUntil = 0;
  private fault: Fault | undefined;

  constructor(profile: Profile, deps: Dependencies) {
    this.profile = { ...profile };
    this.deps = deps;
    this.timer = deps.clock ?? realClock;
    this.random = deps.random ?? Math.random;
    this.state = profile.paused ? 'paused' : 'connecting';
  }
  status(): Status {
    return { name: this.profile.name, tunnelId: this.profile.tunnelId,
      ...(this.profile.clusterId ? { clusterId: this.profile.clusterId } : {}),
      localPort: this.profile.localPort, remotePort: this.profile.remotePort, state: this.state,
      paused: this.profile.paused, relayConnected: this.session?.connected ?? false, attempt: this.attempts,
      ...(this.lastHealthyAt !== undefined ? { lastHealthyAt: this.lastHealthyAt } : {}),
      ...(this.nextRetry > 0 && Number.isFinite(this.nextRetry) ? { nextRetryAt: this.nextRetry } : {}),
      ...(this.fault ? { cause: { code: this.fault.code, message: this.fault.message } } : {}) };
  }
  private transition(state: State, fault?: Fault): void {
    const changed = state !== this.state || fault?.code !== this.fault?.code;
    this.state = state;
    this.fault = fault;
    if (changed) this.deps.log(this.status());
  }
  start(): void {
    if (this.running) return;
    this.running = true;
    this.lastTick = this.timer.now();
    this.tick();
  }
  private tick = (): void => {
    if (!this.running) return;
    const now = this.timer.now(), gap = now - this.lastTick;
    this.lastTick = now;
    if (gap > 15000 && !this.profile.paused) {
      this.nextCheck = now;
      if (this.state !== 'login-required' && this.state !== 'access-denied' && this.state !== 'invalid-config' &&
          this.state !== 'throttled') this.nextRetry = Math.min(this.nextRetry, now);
    }
    if (!this.profile.paused && !this.active) {
      if (this.session) {
        if (this.graceUntil && now >= this.graceUntil && !this.session.connected) {
          this.fail(new Fault('retrying'));
        } else if (now >= this.nextCheck && (!this.graceUntil || this.session.connected)) this.check();
      } else if (now >= this.nextRetry) this.connect();
    }
    this.cancelTick = this.timer.later(1000, this.tick);
  };
  private begin(action: (signal: AbortSignal, valid: () => boolean) => Promise<void>): void {
    this.active?.controller.abort();
    const controller = new AbortController(), id = ++this.serial;
    this.active = { id, controller };
    const valid = () => this.active?.id === id && !controller.signal.aborted &&
      this.running && !this.profile.paused;
    void action(controller.signal, valid).catch(error => {
      if (valid()) this.fail(safeFault(error));
    }).finally(() => { if (this.active?.id === id) this.active = undefined; });
  }
  private connect(): void {
    this.begin(async (signal, valid) => {
      await this.retire();
      if (!valid()) return;
      if (!this.listening) {
        await bounded(() => this.deps.forwarder.listen(), 1000, signal, () => this.deps.forwarder.close(), this.timer);
        this.listening = true;
      }
      this.transition('connecting');
      const life = new AbortController();
      this.sessionLife = life;
      const abort = () => life.abort();
      signal.addEventListener('abort', abort, { once: true });
      const generation = ++this.generation;
      let session: Session;
      try {
        session = await bounded(inner => this.deps.transport.connect(this.profile,
          AbortSignal.any([inner, life.signal]),
          event => { if (generation === this.generation && !life.signal.aborted) this.onEvent(event); }),
        25000, signal, late => this.dispose(late), this.timer);
      } finally { signal.removeEventListener('abort', abort); }
      if (!valid()) { life.abort(); await this.dispose(session); return; }
      this.session = session;
      this.remoteFailures = 0;
      this.deps.forwarder.setTarget(openSignal => {
        if (!session.connected || generation !== this.generation || life.signal.aborted) return Promise.reject(new Fault('retrying'));
        return session.open(openSignal);
      });
      await this.probe(signal);
      if (valid()) this.healthy();
    });
  }
  private check(): void {
    this.begin(async (signal, valid) => {
      await this.probe(signal);
      if (valid()) this.healthy();
    });
  }
  private probe(signal: AbortSignal): Promise<void> {
    return bounded(inner => this.deps.probe(inner), 3000, signal, undefined, this.timer);
  }
  private healthy(): void {
    this.lastHealthyAt = this.timer.now();
    this.nextCheck = this.timer.now() + 10000;
    this.nextRetry = 0;
    this.attempts = 0;
    this.remoteFailures = 0;
    this.graceUntil = 0;
    this.transition('healthy');
  }
  private fail(fault: Fault): void {
    if (!this.running || this.profile.paused) return;
    const now = this.timer.now();
    if (this.session && this.session.connected && ++this.remoteFailures < 2 &&
        ['remote-unavailable', 'timeout', 'retrying'].includes(fault.code)) {
      this.nextCheck = now + 5000;
      this.transition('remote-unavailable', new Fault('remote-unavailable',
        'Tunnel: connected; SSH: no response. Check remote SSH/WSL.'));
      return;
    }
    this.attempts++;
    const backoff = Math.min(15000, 1000 * 2 ** Math.min(this.attempts - 1, 5));
    this.nextRetry = now + Math.max(fault.retryAfterMs, Math.round(backoff * (0.5 + this.random() * 0.5)));
    if (fault.code === 'login-required' || fault.code === 'access-denied' || fault.code === 'invalid-config') {
      this.nextRetry = Infinity;
    }
    const state = fault.code === 'timeout' ? 'retrying' : fault.code;
    this.transition(state, fault);
    this.graceUntil = 0;
    // Disposal is bounded and detached from the next generation's authority.
    const old = this.session;
    this.session = undefined;
    this.generation++;
    this.sessionLife?.abort();
    this.sessionLife = undefined;
    this.deps.forwarder.setTarget(undefined);
    if (old) void this.dispose(old).catch(error => this.deps.log({ ...this.status(),
      cause: { code: safeFault(error).code, message: 'Client teardown exceeded its bound; its generation is cancelled.' } }));
  }
  private onEvent(event: TransportEvent): void {
    if (!this.running || this.profile.paused || !this.session) return;
    if (event.fault && (
      (event.kind === 'relay' && !event.connected &&
        ['access-denied', 'login-required', 'invalid-config', 'throttled'].includes(event.fault.code)) ||
      (event.kind === 'retry' && event.fault.code === 'throttled'))) {
      this.active?.controller.abort();
      this.active = undefined;
      this.fail(event.fault);
      return;
    }
    if (event.kind === 'relay') {
      if (event.connected) { this.nextCheck = this.timer.now(); this.graceUntil = 0; }
      else {
        if (!this.graceUntil) this.graceUntil = this.timer.now() + 8000;
        this.transition('reconnecting', event.fault);
      }
    } else if (event.kind === 'keepalive-failed') this.nextCheck = this.timer.now();
  }
  private dispose(session: Session): Promise<void> {
    return bounded(() => session.dispose(), 1000, undefined, undefined, this.timer);
  }
  private async retire(): Promise<void> {
    this.generation++;
    this.sessionLife?.abort();
    this.sessionLife = undefined;
    const old = this.session;
    this.session = undefined;
    this.deps.forwarder.setTarget(undefined);
    if (old) {
      try { await this.dispose(old); }
      catch (error) { this.deps.log({ ...this.status(), cause: { code: safeFault(error).code,
        message: 'Client teardown exceeded its bound; its generation is cancelled.' } }); }
    }
  }
  reconnect(): void {
    if (this.profile.paused || !this.running) return;
    if (this.state === 'throttled' && this.nextRetry > this.timer.now()) return;
    this.active?.controller.abort();
    this.active = undefined;
    this.attempts = 0; this.nextRetry = 0; this.graceUntil = 0;
    this.connect();
  }
  async pause(): Promise<void> {
    this.profile.paused = true;
    this.active?.controller.abort();
    this.active = undefined;
    this.transition('paused');
    await this.retire();
    await bounded(() => this.deps.forwarder.close(), 1000, undefined, undefined, this.timer);
    this.listening = false;
  }
  resume(): void {
    if (!this.profile.paused) return;
    this.profile.paused = false;
    this.nextRetry = 0; this.nextCheck = 0; this.graceUntil = 0; this.attempts = 0;
    if (!this.running) this.start(); else this.connect();
  }
  async stop(): Promise<void> {
    this.running = false;
    this.cancelTick?.();
    this.cancelTick = undefined;
    await this.pause();
  }
}
