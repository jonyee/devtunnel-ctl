import { test } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { once } from 'node:events';
import { Duplex, PassThrough } from 'node:stream';
import { Forwarder } from '../src/forwarding.ts';
import { probeSsh } from '../src/probe.ts';
import { bounded } from '../src/core.ts';

async function port(): Promise<number> {
  const server = net.createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  await new Promise<void>(r => server.close(() => r()));
  return address.port;
}

test('owns fixed IPv4 and IPv6 listeners across transport replacement with bounded unavailable sockets', async () => {
  const p = await port();
  const forward = new Forwarder(p, 40);
  await forward.listen();
  try {
    const unavailable = net.connect(p, '127.0.0.1');
    await once(unavailable, 'close');
    let streams = 0;
    forward.setTarget(async () => {
      streams++;
      return new Duplex({
        read() { this.push('SSH-2.0-test\r\n'); this.push(null); },
        write(_chunk, _encoding, callback) { callback(); },
      });
    });
    await probeSsh(p, undefined, 200);
    const ipv6 = net.connect(p, '::1');
    ipv6.resume();
    await once(ipv6, 'close');
    assert.equal(streams, 2);
    forward.setTarget(undefined);
    await assert.rejects(probeSsh(p, undefined, 200));
    forward.setTarget(async () => new Duplex({
      read() { this.push('SSH-2.0-replaced\r\n'); this.push(null); },
      write(_c, _e, callback) { callback(); },
    }));
    await probeSsh(p, undefined, 200);
  } finally { await forward.close(); }
});

test('IPv4 and IPv6 conflicts fail explicitly and partial binds are released', async () => {
  for (const host of ['127.0.0.1', '::1']) {
    const occupied = net.createServer();
    occupied.listen({ host, port: 0, ipv6Only: true });
    await once(occupied, 'listening');
    const addr = occupied.address(); assert.ok(addr && typeof addr !== 'string');
    const forward = new Forwarder(addr.port);
    await assert.rejects(forward.listen(), /occupied/i);
    await forward.close();
    await new Promise<void>(r => occupied.close(() => r()));
  }
});

test('late stream completions after cancellation are destroyed', async () => {
  const forward = new Forwarder(await port(), 20);
  let release!: (value: Duplex) => void;
  forward.setTarget(() => new Promise(resolve => { release = resolve; }));
  await forward.listen();
  const socket = net.connect(forward.port, '127.0.0.1');
  await once(socket, 'close');
  const late = new PassThrough();
  release(late);
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(late.destroyed, true);
  await forward.close();
});

test('SSH probe bounds silent/non-SSH endpoints and permits pre-banner lines', async () => {
  const sockets = new Set<net.Socket>();
  const server = net.createServer(s => { sockets.add(s); s.on('close', () => sockets.delete(s)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const addr = server.address(); assert.ok(addr && typeof addr !== 'string');
  try {
    await assert.rejects(probeSsh(addr.port, undefined, 20), /deadline/i);
    server.removeAllListeners('connection');
    server.on('connection', s => s.end('welcome\r\nSSH-2.0-OpenSSH_test\r\n'));
    await probeSsh(addr.port, undefined, 200);
    server.removeAllListeners('connection');
    server.on('connection', s => s.end('HTTP/1.1 200 OK\r\n'));
    await assert.rejects(probeSsh(addr.port, undefined, 200));
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(r => server.close(() => r()));
  }
});

test('backpressure and half-close preserve a large response after the caller finishes sending', { timeout: 5000 }, async () => {
  const payload = Buffer.alloc(2 * 1024 * 1024, 's');
  let request = '';
  const remote = net.createServer({ allowHalfOpen: true }, socket => {
    socket.on('error', () => {});
    socket.on('data', b => { request += b.toString(); });
    socket.on('end', () => socket.end(payload));
  });
  remote.listen(0, '127.0.0.1'); await once(remote, 'listening');
  const address = remote.address(); assert.ok(address && typeof address !== 'string');
  const forward = new Forwarder(await port());
  forward.setTarget(async () => {
    const stream = net.connect({ port: address.port, host: '127.0.0.1', allowHalfOpen: true });
    await once(stream, 'connect'); return stream;
  });
  await forward.listen();
  try {
    const client = net.connect(forward.port, '127.0.0.1');
    const chunks: Buffer[] = [];
    client.on('data', b => {
      chunks.push(b); client.pause(); setTimeout(() => client.resume(), 2);
    });
    client.end('request');
    await once(client, 'close');
    assert.equal(request, 'request');
    const response = Buffer.concat(chunks);
    assert.equal(response.length, payload.length, 'response must not be truncated');
    assert.equal(response.equals(payload), true, 'response bytes must be preserved');
  } finally { await forward.close(); await new Promise<void>(r => remote.close(() => r())); }
});

test('remote close after EOF drains queued local writes before closing the client socket', { timeout: 5000 }, async () => {
  const payload = Buffer.alloc(8 * 1024 * 1024, 's');
  const forward = new Forwarder(await port());
  forward.setTarget(async () => new Duplex({
    read() { this.push(payload); this.push(null); },
    write(_chunk, _encoding, callback) { callback(); },
  }));
  await forward.listen();
  const client = net.connect(forward.port, '127.0.0.1');
  let received = 0;
  const closed = once(client, 'close');
  client.on('data', chunk => { received += chunk.length; });
  client.pause();
  client.end('request');
  const resume = setTimeout(() => client.resume(), 100);
  try {
    await closed;
    assert.equal(received, payload.length, 'orderly remote EOF must not discard buffered output');
  } finally {
    clearTimeout(resume);
    client.destroy();
    await forward.close();
  }
});

test('abrupt remote close without EOF does not strand the local half-open socket', { timeout: 2000 }, async () => {
  const forward = new Forwarder(await port());
  let remote!: PassThrough, connected!: () => void;
  const opened = new Promise<void>(resolve => { connected = resolve; });
  forward.setTarget(async () => { remote = new PassThrough(); connected(); return remote; });
  await forward.listen();
  const client = net.connect({ port: forward.port, host: '127.0.0.1' });
  client.resume();
  const closed = once(client, 'close');
  try {
    await opened;
    await new Promise(resolve => setImmediate(resolve));
    remote.destroy();
    await bounded(() => closed, 200);
    assert.equal(client.destroyed, true);
  } finally { client.destroy(); await forward.close(); }
});
