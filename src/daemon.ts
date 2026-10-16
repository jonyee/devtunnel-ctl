import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Store, validateConfig } from './config.ts';
import type { Config, Profile } from './config.ts';
import { TokenProvider } from './auth.ts';
import { SdkTransport } from './sdk-transport.ts';
import type { Transport } from './sdk-transport.ts';
import { Forwarder } from './forwarding.ts';
import { probeSsh } from './probe.ts';
import { Recovery } from './recovery.ts';
import type { Status } from './recovery.ts';
import { RpcServer } from './rpc.ts';
import type { Request } from './rpc.ts';
import { Log } from './log.ts';
import type { LogEntry } from './log.ts';
import { DaemonLock } from './lock.ts';
import { bounded, Fault, safeFault } from './core.ts';

export interface DaemonOptions {
  transport?: (tokens: TokenProvider) => Transport;
  fatal?: (fault: Fault) => void;
}
export class Daemon {
  private readonly store: Store;
  private readonly options: DaemonOptions;
  private readonly log: Log;
  private readonly lock: DaemonLock;
  private readonly instance = randomUUID();
  private config: Config | undefined;
  private tokens: TokenProvider | undefined;
  private transport: Transport | undefined;
  private rpc: RpcServer | undefined;
  private readonly controllers = new Map<string, Recovery>();
  private mutations: Promise<unknown> = Promise.resolve();
  private queued = 0;
  private stopping = false;
  private stopPromise: Promise<void> | undefined;

  constructor(store: Store, options: DaemonOptions = {}) {
    this.store = store; this.options = options;
    this.log = new Log(store.dir); this.lock = new DaemonLock(store.dir);
  }
  async start(): Promise<void> {
    await this.store.init();
    await this.lock.acquire();
    try {
      await this.log.init();
      this.config = await this.store.load();
      this.tokens = new TokenProvider(this.config.helperPath);
      this.transport = this.options.transport?.(this.tokens) ?? new SdkTransport(this.tokens);
      this.rpc = new RpcServer(join(this.store.dir, 'control.sock'), req => this.handle(req),
        fault => this.writeLog({ event: 'control-error', message: fault.message }));
      await this.rpc.listen();
      for (const profile of this.config.profiles) this.create(profile);
      await this.log.write({ event: 'daemon-ready', message: 'Private control socket ready.' });
    } catch (error) { await this.stop(); throw error; }
  }
  private writeLog(entry: LogEntry): void {
    void this.log.write(entry).catch(() => {
      const fault = new Fault('internal', 'Cannot maintain bounded private service logs; stopping daemon.');
      this.options.fatal?.(fault);
      void this.stop().catch(stopError => this.options.fatal?.(safeFault(stopError)));
    });
  }
  private create(profile: Profile): void {
    const forwarder = new Forwarder(profile.localPort);
    const controller = new Recovery(profile, {
      transport: this.transport!, forwarder, probe: signal => probeSsh(profile.localPort, signal),
      log: status => this.logStatus(status),
    });
    this.controllers.set(profile.name, controller);
    controller.start();
  }
  private logStatus(status: Status): void {
    this.writeLog({ event: 'state', profile: status.name, state: status.state, attempt: status.attempt,
      relayConnected: status.relayConnected,
      ...(status.cause ? { message: status.cause.message } : {}),
      ...(status.lastHealthyAt !== undefined ? { lastHealthyAt: status.lastHealthyAt } : {}),
      ...(status.nextRetryAt !== undefined ? { nextRetryAt: status.nextRetryAt } : {}),
    });
  }
  private handle(req: Request): Promise<unknown> {
    if (this.stopping) return Promise.reject(new Fault('daemon-unavailable', 'Daemon is shutting down.'));
    if (req.command === 'status') return Promise.resolve({ pid: process.pid, profiles: [...this.controllers.values()].map(c => c.status()) });
    if (req.command === 'logs') return Promise.resolve({ instance: this.instance, entries: this.log.snapshot() });
    if (this.queued >= 32) return Promise.reject(new Fault('throttled', 'Too many pending configuration changes.'));
    this.queued++;
    const mutation = this.mutations.then(async () => {
      if (this.stopping) throw new Fault('daemon-unavailable', 'Daemon is shutting down.');
      const config = this.config!;
      if (req.command === 'add') {
        const next = validateConfig({ ...config, profiles: [...config.profiles, req.profile] });
        await this.store.save(next);
        this.config = next;
        if (!this.stopping) this.create(req.profile);
        return { saved: true, name: req.profile.name };
      }
      const profiles = config.profiles.filter(p => req.name === 'all' || p.name === req.name);
      if (!profiles.length) throw new Fault('invalid-config', 'No matching profile. Use profile list to see configured names.');
      if (req.command !== 'reconnect') {
        const paused = req.command === 'pause';
        const names = new Set(profiles.map(p => p.name));
        const next = validateConfig({ ...config, profiles: config.profiles.map(p => names.has(p.name) ? { ...p, paused } : p) });
        await this.store.save(next);
        this.config = next;
      }
      if (!this.stopping) {
        await Promise.all(profiles.map(async profile => {
          const controller = this.controllers.get(profile.name)!;
          if (req.command === 'pause') await controller.pause();
          else if (req.command === 'resume') controller.resume();
          else { this.tokens!.invalidate(profile); controller.reconnect(); }
        }));
      }
      return { profiles: profiles.map(p => this.controllers.get(p.name)!.status()) };
    });
    this.mutations = mutation.catch(() => {});
    return mutation.finally(() => { this.queued--; });
  }
  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.stopping = true;
    this.stopPromise = (async () => {
      const failures: Fault[] = [];
      const attempt = async (action: () => Promise<unknown>) => {
        try { await action(); } catch (error) { failures.push(safeFault(error)); }
      };
      await attempt(() => this.rpc?.close() ?? Promise.resolve());
      await attempt(() => bounded(() => this.mutations, 2500));
      await Promise.all([...this.controllers.values()].map(controller =>
        attempt(() => bounded(() => controller.stop(), 3000))));
      this.tokens?.clear();
      await attempt(() => bounded(() => this.log.flush(), 1000));
      await attempt(() => this.lock.release());
      if (failures.length) throw new Fault('internal', `Daemon shutdown had ${failures.length} bounded cleanup failure(s).`);
    })();
    return this.stopPromise;
  }
}
