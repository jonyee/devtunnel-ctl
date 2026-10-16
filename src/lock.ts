import { mkdir, writeFile, readFile, lstat, unlink, rmdir, rename, readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Fault, record } from './core.ts';
import { privatePath } from './paths.ts';

export class DaemonLock {
  private readonly path: string;
  private readonly candidate: string;
  private readonly nonce = randomUUID();
  private owned = false;
  constructor(dir: string) {
    this.path = join(dir, 'daemon.lock');
    this.candidate = join(dir, `.daemon-lock-${this.nonce}`);
  }
  async acquire(): Promise<void> {
    await mkdir(this.candidate, { mode: 0o700 });
    const marker = `owner-${this.nonce}.json`;
    try {
      await writeFile(join(this.candidate, marker), JSON.stringify({ pid: process.pid, nonce: this.nonce }), { flag: 'wx', mode: 0o600 });
      for (let attempt = 0; attempt < 5; attempt++) {
        try {
          // A populated candidate atomically replaces only an absent or empty directory.
          await rename(this.candidate, this.path);
          this.owned = true;
          return;
        } catch (error) {
          if (!(error instanceof Error && 'code' in error &&
              (error.code === 'ENOTEMPTY' || error.code === 'EEXIST'))) throw error;
        }
        if (!await this.reclaim()) throw new Fault('daemon-unavailable', 'A live daemon owns the state lock.');
      }
      throw new Fault('daemon-unavailable', 'Daemon lock is changing; retry after the other starter finishes.');
    } finally {
      if (!this.owned) {
        await unlink(join(this.candidate, marker));
        await rmdir(this.candidate);
      }
    }
  }
  private async reclaim(): Promise<boolean> {
    try {
      await privatePath(this.path, true);
      const names = await readdir(this.path);
      if (!names.length) return true;
      if (names.length !== 1 || !/^owner-[a-z0-9-]{1,64}\.json$/.test(names[0]!)) {
        throw new Fault('daemon-unavailable', 'Unrecognized daemon lock contents; refusing automatic cleanup.');
      }
      const marker = join(this.path, names[0]!);
      await privatePath(marker);
      if ((await lstat(marker)).size > 1024) throw new Fault('daemon-unavailable', 'Invalid daemon lock record.');
      const owner: unknown = JSON.parse(await readFile(marker, 'utf8'));
      if (!record(owner) || !Number.isSafeInteger(owner.pid) || Number(owner.pid) <= 0 ||
          typeof owner.nonce !== 'string' || names[0] !== `owner-${owner.nonce}.json`) {
        throw new Fault('daemon-unavailable', 'Invalid daemon lock record.');
      }
      try { process.kill(Number(owner.pid), 0); return false; }
      catch (error) {
        if (!(error instanceof Error && 'code' in error && error.code === 'ESRCH')) return false;
      }
      // Unique marker names keep a racing reclaimer from deleting a new owner's record.
      await unlink(marker);
      try { await rmdir(this.path); }
      catch (error) {
        if (!(error instanceof Error && 'code' in error &&
            (error.code === 'ENOENT' || error.code === 'ENOTEMPTY' || error.code === 'EEXIST'))) throw error;
      }
      return true;
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return true;
      throw error;
    }
  }
  async release(): Promise<void> {
    if (!this.owned) return;
    const owner = join(this.path, `owner-${this.nonce}.json`);
    const info = await lstat(owner);
    if (info.isSymbolicLink() || !info.isFile()) throw new Fault('internal', 'Daemon lock ownership changed.');
    const value = JSON.parse(await readFile(owner, 'utf8'));
    if (value.nonce !== this.nonce) throw new Fault('internal', 'Daemon lock ownership changed.');
    await unlink(owner);
    try { await rmdir(this.path); }
    catch (error) {
      if (!(error instanceof Error && 'code' in error &&
          (error.code === 'ENOENT' || error.code === 'ENOTEMPTY' || error.code === 'EEXIST'))) throw error;
    }
    this.owned = false;
  }
}
