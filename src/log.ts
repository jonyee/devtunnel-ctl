import { open, rename, lstat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { join } from 'node:path';
import { Fault, redact } from './core.ts';
import { privateDir, privatePath } from './paths.ts';

export interface LogEntry {
  event: string;
  profile?: string;
  state?: string;
  message?: string;
  attempt?: number;
  relayConnected?: boolean;
  lastHealthyAt?: number;
  nextRetryAt?: number;
}
export class Log {
  private readonly dir: string;
  private readonly maxBytes: number;
  private pending = 0;
  private tail: Promise<void> = Promise.resolve();
  private sequence = 0;
  private readonly recent: { sequence: number; line: string }[] = [];
  constructor(dir: string, maxBytes = 262144) { this.dir = dir; this.maxBytes = maxBytes; }
  async init(): Promise<void> { await privateDir(this.dir); }
  write(entry: LogEntry): Promise<void> {
    if (this.pending >= 256) return Promise.reject(new Fault('internal', 'Log backlog limit exceeded.'));
    const line = JSON.stringify({ time: new Date().toISOString(), event: redact(entry.event),
      ...(entry.profile ? { profile: redact(entry.profile) } : {}),
      ...(entry.state ? { state: redact(entry.state) } : {}),
      ...(entry.message ? { message: redact(entry.message).slice(0, 256) } : {}),
      ...(entry.attempt !== undefined ? { attempt: entry.attempt } : {}),
      ...(entry.relayConnected !== undefined ? { relayConnected: entry.relayConnected } : {}),
      ...(entry.lastHealthyAt !== undefined ? { lastHealthyAt: entry.lastHealthyAt } : {}),
      ...(entry.nextRetryAt !== undefined ? { nextRetryAt: entry.nextRetryAt } : {}),
    }) + '\n';
    if (Buffer.byteLength(line) > this.maxBytes) return Promise.reject(new Fault('internal', 'Log entry exceeds disk bound.'));
    this.pending++;
    this.tail = this.tail.then(async () => {
      const path = join(this.dir, 'service.log'), previous = path + '.1';
      let size = 0;
      try { await privatePath(path); size = (await lstat(path)).size; }
      catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error; }
      if (size + Buffer.byteLength(line) > this.maxBytes) {
        try { await privatePath(previous); }
        catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error; }
        await rename(path, previous);
      }
      const file = await open(path, constants.O_APPEND | constants.O_CREAT | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
      try { await file.writeFile(line); } finally { await file.close(); }
      this.recent.push({ sequence: ++this.sequence, line: line.trimEnd() });
      if (this.recent.length > 128) this.recent.shift();
    }).finally(() => { this.pending--; });
    return this.tail;
  }
  flush(): Promise<void> { return this.tail; }
  snapshot(): { sequence: number; line: string }[] { return this.recent.map(entry => ({ ...entry })); }
}
