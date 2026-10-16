import net from 'node:net';
import { bounded, Fault } from './core.ts';

export async function probeSsh(port: number, signal?: AbortSignal, timeoutMs = 3000): Promise<void> {
  return bounded(inner => new Promise<void>((resolve, reject) => {
    const socket = net.connect({ port, host: '127.0.0.1' });
    let text = '', done = false;
    const abort = () => finish(new Fault('aborted'));
    const finish = (error?: Fault) => {
      if (done) return;
      done = true;
      inner.removeEventListener('abort', abort);
      socket.destroy();
      if (error) reject(error); else resolve();
    };
    inner.addEventListener('abort', abort, { once: true });
    if (inner.aborted) { abort(); return; }
    socket.on('error', () => finish(new Fault('remote-unavailable')));
    socket.on('close', () => { if (!done) finish(new Fault('remote-unavailable')); });
    socket.on('data', (chunk: Buffer) => {
      text += chunk.toString('ascii');
      if (text.length > 4096) { finish(new Fault('remote-unavailable')); return; }
      const lines = text.split('\n');
      for (const line of lines.slice(0, -1)) {
        if (/^SSH-(2\.0|1\.99)-[!-~]+(?: .*)?\r?$/.test(line) && line.length <= 255) { finish(); return; }
      }
      if (lines.length > 8) finish(new Fault('remote-unavailable'));
    });
  }), timeoutMs, signal);
}
