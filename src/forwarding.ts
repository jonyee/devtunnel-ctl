import net from 'node:net';
import type { Duplex } from 'node:stream';
import { bounded, Fault, safeFault } from './core.ts';

export type OpenStream = (signal: AbortSignal) => Promise<Duplex>;

export class Forwarder {
  readonly port: number;
  private readonly openTimeoutMs: number;
  private servers: net.Server[] = [];
  private target: OpenStream | undefined;
  private generation = 0;
  private readonly sockets = new Set<net.Socket>();
  constructor(port: number, openTimeoutMs = 3000) { this.port = port; this.openTimeoutMs = openTimeoutMs; }
  setTarget(target: OpenStream | undefined): void {
    this.generation++;
    this.target = target;
    for (const socket of this.sockets) socket.destroy();
  }
  async listen(): Promise<void> {
    if (this.servers.length) return;
    try {
      for (const host of ['127.0.0.1', '::1']) {
        const server = net.createServer({ allowHalfOpen: true }, socket => this.accept(socket));
        this.servers.push(server);
        await new Promise<void>((resolve, reject) => {
          const error = (e: Error) => { server.off('listening', listening); reject(e); };
          const listening = () => { server.off('error', error); resolve(); };
          server.once('error', error);
          server.once('listening', listening);
          server.listen({ port: this.port, host, ipv6Only: true, exclusive: true });
        });
        server.on('error', () => { this.setTarget(undefined); });
      }
    } catch (error) { await this.close(); throw safeFault(error); }
  }
  private accept(socket: net.Socket): void {
    socket.on('error', () => { socket.destroy(); });
    if (!this.target || this.sockets.size >= 256) { socket.destroy(); return; }
    this.sockets.add(socket);
    socket.setNoDelay(true);
    const controller = new AbortController();
    let stream: Duplex | undefined;
    socket.once('close', () => {
      this.sockets.delete(socket);
      controller.abort();
      stream?.destroy();
    });
    const target = this.target, generation = this.generation;
    void bounded(target, this.openTimeoutMs, controller.signal, late => { late.destroy(); }).then(result => {
      stream = result;
      if (socket.destroyed || generation !== this.generation) { result.destroy(); socket.destroy(); return; }
      let remoteEnded = false;
      result.once('end', () => { remoteEnded = true; });
      result.on('error', () => { socket.destroy(); result.destroy(); });
      result.once('close', () => {
        // pipe() drains queued writes on EOF; destroying the socket here would truncate them.
        if (!remoteEnded) socket.destroy();
      });
      socket.pipe(result);
      result.pipe(socket);
    }, () => socket.destroy());
  }
  async close(): Promise<void> {
    this.setTarget(undefined);
    const servers = this.servers.splice(0);
    await bounded(() => Promise.all(servers.map(server => new Promise<void>((resolve, reject) => {
      server.close(error => {
        if (error && 'code' in error && error.code !== 'ERR_SERVER_NOT_RUNNING') reject(error);
        else resolve();
      });
    }))).then(() => {}), 1000);
  }
}
