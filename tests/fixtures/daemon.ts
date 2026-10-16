import net from 'node:net';
import { once } from 'node:events';
import { Daemon } from '../../src/daemon.ts';
import { Store } from '../../src/config.ts';
import type { Transport } from '../../src/sdk-transport.ts';
const dir = process.argv[2]!, port = Number(process.argv[3]);
const daemon = new Daemon(new Store(dir), {
  transport: tokens => ({
    connect: async (profile, signal) => {
      await tokens.get(profile, signal);
      return {
        connected: true,
        open: async () => { const socket = net.connect(port, '127.0.0.1'); await once(socket, 'connect'); return socket; },
        dispose: async () => {},
      };
    },
  } satisfies Transport),
});
await daemon.start();
process.once('SIGTERM', () => { void daemon.stop().then(() => { process.exitCode = 0; }, error => { console.error(error); process.exitCode = 1; }); });
