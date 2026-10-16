import { test } from 'node:test';
import assert from 'node:assert/strict';
import { launchAgent } from '../src/launchd.ts';
test('launchd configuration pins absolute runtime/entry/state paths and escapes XML without a shell', () => {
  const plist = launchAgent({ label: 'local.devtunnel-ctl.test', node: '/nix/store/node', entry: '/Users/test/a & b/dist/main.js', state: '/Users/test/.devtunnel-ctl' });
  assert.match(plist, /<string>\/nix\/store\/node<\/string>/);
  assert.match(plist, /a &amp; b/);
  assert.match(plist, /<string>daemon<\/string>/);
  assert.doesNotMatch(plist, /\/bin\/(?:ba)?sh|devtunnel connect/);
  assert.throws(() => launchAgent({ label: 'x', node: 'node', entry: '/app/main.js', state: '/state' }), /absolute/i);
});
