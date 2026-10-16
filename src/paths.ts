import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { lstat, mkdir, chmod, realpath, access, stat } from 'node:fs/promises';
import { constants } from 'node:fs';
import { Fault } from './core.ts';

export function stateDir(input?: string): string {
  return input ? resolve(input) : join(homedir(), '.devtunnel-ctl');
}

export async function executable(path: string): Promise<string> {
  try {
    const resolved = await realpath(path);
    await access(resolved, constants.X_OK);
    if (!(await stat(resolved)).isFile()) throw new Fault('invalid-config');
    return resolved;
  } catch { throw new Fault('invalid-config', 'Configured executable is missing or not executable. Check its absolute path.'); }
}

export async function privatePath(path: string, directory = false): Promise<void> {
  const info = await lstat(path);
  if (info.isSymbolicLink() || (directory ? !info.isDirectory() : !info.isFile()) ||
      info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0) {
    throw new Fault('invalid-config', 'State must be private, owned by this user, and not a symlink.');
  }
}

export async function privateDir(path: string): Promise<void> {
  try {
    await mkdir(path, { mode: 0o700 });
    await chmod(path, 0o700);
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'EEXIST')) throw error;
  }
  await privatePath(path, true);
}
