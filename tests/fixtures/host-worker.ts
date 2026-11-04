process.once('message', value => {
  const options = (value as { options: { tunnelId: string } }).options;
  if (options.tunnelId === 'crash') { process.exit(1); }
  if (options.tunnelId === 'superseded') {
    process.send?.({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 });
    return;
  }
  if (options.tunnelId === 'endpoint') {
    process.send?.({ kind: 'endpoint', id: '00000000-0000-4000-8000-000000000001-relay',
      tunnelId: 'endpoint', clusterId: 'usw2' });
    process.once('message', message => {
      if ((message as { kind: string }).kind !== 'endpoint-recorded') throw new Error('Missing endpoint acknowledgement.');
      process.send?.({ kind: 'fault', code: 'host-superseded', retryAfterMs: 0 });
    });
    return;
  }
  process.send?.({ kind: 'heartbeat' });
  process.on('message', message => {
    if ((message as { kind: string }).kind === 'stop' && options.tunnelId !== 'stuck') process.disconnect();
  });
});
