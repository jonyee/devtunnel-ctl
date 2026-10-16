import { open, rename, lstat, unlink } from 'node:fs/promises';
import { join, isAbsolute } from 'node:path';
import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { Fault, record } from './core.ts';
import { privateDir, privatePath } from './paths.ts';

export interface Profile {
  name: string;
  tunnelId: string;
  clusterId?: string;
  remotePort: number;
  localPort: number;
  paused: boolean;
}
export interface Config { version: 1; helperPath: string; profiles: Profile[] }
function invalid(detail: string): never { throw new Fault('invalid-config', detail); }
const port = (v: unknown): v is number => Number.isInteger(v) && Number(v) >= 1 && Number(v) <= 65535;
const idPattern = /^[a-z0-9][a-z0-9-]{0,62}$/i;

export function validateConfig(value: unknown): Config {
  if (!record(value) || value.version !== 1 || typeof value.helperPath !== 'string' ||
      !isAbsolute(value.helperPath) || /[\0\r\n]/.test(value.helperPath) ||
      !Array.isArray(value.profiles) || value.profiles.length > 64) invalid('Invalid configuration schema.');
  const names = new Set<string>(), ports = new Set<number>();
  const profiles = value.profiles.map((p: unknown): Profile => {
    if (!record(p) || typeof p.name !== 'string' || !/^[a-z][a-z0-9-]{0,31}$/.test(p.name) ||
        typeof p.tunnelId !== 'string' || !port(p.localPort) || !port(p.remotePort) ||
        typeof p.paused !== 'boolean') invalid('Invalid SSH profile.');
    const [tunnelId, suffix, extra] = p.tunnelId.split('.');
    const cluster = p.clusterId ?? suffix;
    if (!tunnelId || !idPattern.test(tunnelId) || extra !== undefined ||
        (cluster !== undefined && (typeof cluster !== 'string' || !idPattern.test(cluster))) ||
        (suffix !== undefined && p.clusterId !== undefined && suffix !== p.clusterId)) invalid('Invalid tunnel or cluster ID.');
    if (names.has(p.name) || ports.has(p.localPort)) invalid('Profile names and local ports must be unique.');
    names.add(p.name); ports.add(p.localPort);
    return { name: p.name, tunnelId, ...(cluster ? { clusterId: String(cluster) } : {}),
      localPort: p.localPort, remotePort: p.remotePort, paused: p.paused };
  });
  return { version: 1, helperPath: value.helperPath, profiles };
}

export class Store {
  readonly dir: string;
  readonly configPath: string;
  constructor(dir: string) { this.dir = dir; this.configPath = join(dir, 'config.json'); }
  async init(): Promise<void> { await privateDir(this.dir); }
  async load(): Promise<Config> {
    try {
      await privatePath(this.configPath);
      const handle = await open(this.configPath, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        if ((await handle.stat()).size > 65536) invalid('Configuration exceeds 64 KiB.');
        return validateConfig(JSON.parse(await handle.readFile('utf8')));
      } finally { await handle.close(); }
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
        return { version: 1, helperPath: '/opt/homebrew/bin/devtunnel', profiles: [] };
      }
      throw error instanceof Fault ? error : new Fault('invalid-config', 'Cannot read private configuration.');
    }
  }
  async save(value: Config): Promise<void> {
    const config = validateConfig(value);
    await this.init();
    try { await lstat(this.configPath); await privatePath(this.configPath); }
    catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error; }
    const temp = `${this.configPath}.${randomUUID()}.tmp`;
    const handle = await open(temp, 'wx', 0o600);
    try {
      await handle.writeFile(JSON.stringify(config, null, 2) + '\n');
      await handle.sync();
      await handle.close();
      await rename(temp, this.configPath);
      const dir = await open(this.dir, 'r');
      try { await dir.sync(); } finally { await dir.close(); }
    } catch (error) {
      await handle.close().catch(() => {});
      await unlink(temp).catch(() => {});
      throw error;
    }
  }
}
