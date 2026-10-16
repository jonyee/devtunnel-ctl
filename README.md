# devtunnel-ctl

A per-user background service and CLI for **SSH-only** Microsoft Dev Tunnels forwarding. It uses the Microsoft TypeScript SDK directly, owns stable loopback ports, checks real SSH banners, and replaces stale SDK connections when bounded resumable recovery fails.

This is a first version, not a transparent SSH-session migration tool. A fresh SDK client can terminate existing SSH sessions. Use `tmux` on the remote machine when session continuity matters. No remote settings or SSH configuration are modified.

## Build and run

Requires Node.js 24 or later, npm, and an installed Microsoft `devtunnel` CLI. On macOS/Homebrew the default helper is `/opt/homebrew/bin/devtunnel`.

```sh
npm ci --ignore-scripts
npm run build
node dist/main.js --help
node dist/main.js init --helper /opt/homebrew/bin/devtunnel
node dist/main.js daemon
```

The last command stays in the foreground. In another terminal:

```sh
node dist/main.js profile add work \
  --tunnel example-ssh.usw2 --local 23022 --remote 22
node dist/main.js status
ssh -p 23022 remote-user@127.0.0.1
```

The names, tunnel ID and port above are fictitious examples. Choose an existing tunnel and an **unused** local port for initial verification. Do not stop or replace working forwards until you deliberately migrate them.

Optionally run `npm link --ignore-scripts` after building to make `devtunnel-ctl` available as a command. Otherwise use `node /absolute/path/to/devtunnel-ctl/dist/main.js` throughout.

## Profiles and commands

Each profile maps one remote SSH port to one fixed local port. Both `127.0.0.1:PORT` and `[::1]:PORT` are bound exclusively; there is no LAN listener and no automatic port shifting. A conflict on either address fails that profile explicitly. An IPv6-disabled environment must enable IPv6 loopback before using this version.

```sh
devtunnel-ctl profile add work --tunnel example-ssh \
  --cluster usw2 --local 23022 --remote 22 --paused
devtunnel-ctl profile list
devtunnel-ctl status work
devtunnel-ctl status --json
devtunnel-ctl resume work
devtunnel-ctl pause work
devtunnel-ctl reconnect work
devtunnel-ctl reconnect all
devtunnel-ctl logs
devtunnel-ctl logs --follow
```

Pause is persisted before acknowledging the command and survives daemon restart. Pause releases that profile's listeners and connections. **Reconnect never unpauses a profile.** Resume and reconnect initiate work asynchronously; consult status for the actual result, rather than treating the command acknowledgement as proof of connectivity.

Profile names and local ports must be unique; at most 64 profiles are supported. `--remote` defaults to 22 but may identify another SSH server port. Non-SSH services and other ports exposed by the tunnel are not forwarded. To change or remove an existing profile, stop the daemon, edit its private `config.json`, and restart it. Do not edit configuration while the daemon runs.

### Existing SSH aliases

Existing aliases remain unchanged when the same local port is assigned to a profile. Migration is explicit: first verify an isolated spare port, then stop the old forward yourself and configure the desired stable port. An occupied port is reported as `port-conflict`; the service never kills the current owner or silently chooses a different port.

## Authentication

The SDK performs connection and forwarding work. The Microsoft CLI is used **only** for explicit sign-in and acquiring short-lived, connect-scoped tunnel tokens:

```sh
devtunnel-ctl login
# Optional explicit alternatives:
devtunnel-ctl login --device-code
devtunnel-ctl login --github
devtunnel-ctl reconnect all
```

The service never invokes login, reads a credential cache, or uses a copied application ID. It runs the absolute helper executable with an argument array equivalent to `devtunnel token ID --scopes connect --json`, capturing stdout/stderr privately with a 12-second deadline and 64 KiB combined limit. Raw helper output and tokens are never printed or logged. There is no shell interpolation.

Tokens are coalesced and cached **in memory only**, with expiration checks and early renewal. On management 401, one fresh token is tried before surfacing sign-in required; SDK relay token refresh is similarly bounded. If relay authentication triggers token refresh but acquisition fails transiently, the sanitized refresh failure remains retryable even when the SDK wraps it in the original 401, including asynchronous reconnect events. Genuine rejection after a successful refresh still requires user action. Refresh evidence is scoped to its connection episode, and obsolete completions cannot overwrite a newer episode's classification. Reconnect invalidates that profile's cached token. Tunnel tokens currently normally expire after 24 hours; identity sign-in can also expire or be revoked. This does not provide unattended identity renewal beyond what the signed-in CLI can acquire.

The parser accepts a JSON string containing a JWT or an object with a `token` JWT field; unsupported output fails safely without exposing it. The installed CLI's real envelope was privately verified, including its `token` field and unrelated metadata. Token-only SDK metadata reads and a complete live SSH forwarding connection were also exercised.

Specify the cluster explicitly (`ID.CLUSTER` or `--cluster`) for predictable routing. For an unqualified ID, the currently issued token's validated-format `clusterId` claim is used only as a routing hint, not as proof of authorization. If that hint is absent, status requests an explicit cluster rather than guessing. TLS and SDK host-key checks remain enabled; endpoint metadata without published host public keys is rejected, including during refresh. The SDK's default E2E encryption request is preserved.

## Recovery policy

| Signal or condition | Behavior |
| --- | --- |
| Relay connected | Not sufficient for `healthy`; a local forwarding connection must receive a valid SSH banner. |
| Normal operation | SSH banner check every 10 seconds, with a 3-second check deadline; SDK keepalive interval is 10 seconds. |
| SDK disconnection | Allow up to 8 seconds for resumable SDK reconnect, then cancel/dispose and replace the generation. |
| Failed SSH check with apparently connected relay | Report `Tunnel: connected; SSH: no response. Check remote SSH/WSL.` in status and logs, recheck after 5 seconds, then replace a persistently stale client. WSL is a troubleshooting hint, not a detected cause; a broken forwarding path and an unavailable remote SSH server cannot always be distinguished. |
| Sleep or long scheduling gap | A gap over 15 seconds prompts re-evaluation. A passing SSH check keeps the healthy client rather than blindly replacing it. |
| Failed fresh connection | Independent per-profile exponential backoff, 50–100% jitter, capped at 15 seconds. Respect exposed Retry-After up to 24 hours; no automatic bypass on reconnect. |
| Successful end-to-end check | Update `lastHealthyAt` and reset retry backoff. |
| Sign-in required, access denied, invalid configuration | Stop automated attempts for that profile and report the required action. Explicit reconnect retries after the user addresses it. |
| Port conflict, absent host, unavailable path, network errors | Continue bounded unattended retry. One failed target does not establish that Wi-Fi or the internet is offline. |

The outer controller owns long-term scheduling. SDK retry events allow only the bounded resumable window and a single initial token-refresh retry; there are no competing unbounded retry loops.

A fresh connection generation has a 25-second overall deadline. SDK connect, metadata refresh, forwarded-port wait, stream opening, helper execution and disposal have their own deadlines and cancellation. Obsolete results cannot become the current client; late streams are destroyed and late clients disposed. Pending SDK WebSocket sockets are owned by a per-generation HTTPS agent and destroyed when cancelled. Failed or slow teardown does not indefinitely block explicit reconnect, pause, or another profile.

Listeners stay bound across SDK replacements. New connections fail immediately while no connected SDK client is available, or within 3 seconds when opening a forwarded stream stalls; they are not queued indefinitely. Existing streams may survive the short SDK resumable window, but a fresh-client replacement breaks them. There is a 256-connection-per-profile cap.

**Acceptance target:** restore forwarding within 60 seconds after connectivity returns when the host and authentication are available. Deterministic tests cover a sustained outage with maximum jitter/backoff, a 24-second fresh connection and a 2-second SSH probe. This is not a live SLA: provider throttling, expired identity, unavailable hosts, slow authentication, and upstream SDK behavior may exceed it. Actual post-sleep/offline recovery has not yet been verified against a live tunnel.

## Optional macOS login service

Installation is explicit and per user; never use `sudo`. Stop a foreground daemon before installing:

```sh
devtunnel-ctl service install
devtunnel-ctl service start
devtunnel-ctl service status
devtunnel-ctl status
devtunnel-ctl service stop
devtunnel-ctl service uninstall
```

Install writes a private plist under `~/Library/LaunchAgents/` with a state-directory-specific label. **Install does not start the service.** Start bootstraps it into the current user's GUI domain; stop bootouts it. launchd restarts an unexpectedly exited daemon, with a 15-second launch throttle. No agent has been installed or started as part of development.

Installation resolves and pins absolute Node, built entrypoint, helper, and state paths. It does not depend on login-shell PATH. Keep the project and pinned runtime available; stop/uninstall/build/reinstall after moving the project or changing/removing the runtime or helper. A stopped service retains profiles and logs. Status distinguishes launchd registration from an actually responsive daemon.

## Private state and control

Default state is `~/.devtunnel-ctl/`. Use the same `--state-dir /absolute/path` on every command to select a separate instance. The full control-socket path must be at most 100 bytes.

```json
{
  "version": 1,
  "helperPath": "/opt/homebrew/bin/devtunnel",
  "profiles": [
    {
      "name": "work",
      "tunnelId": "example-ssh",
      "clusterId": "usw2",
      "remotePort": 22,
      "localPort": 23022,
      "paused": true
    }
  ]
}
```

Directories are private (0700); config, lock records, logs and socket are private (0600). Symlinks and foreign/insecure state ownership are refused. Configuration uses a unique temporary file, fsync and atomic rename. There are no tokens in configuration.

The management interface is a Unix-domain socket, not HTTP. Requests are validated, limited to 32 KiB and timed out; responses are capped at 128 KiB. An absent daemon is an error. A timed-out mutation reports uncertainty: check status before retrying because its atomic configuration write may already have completed.

Daemon exclusion uses atomically renamed populated lock directories with unique PID records. A demonstrably dead owner's record can be reclaimed without removing a new owner's record. A live owner is never replaced. Stale sockets are removed only under exclusive state ownership and after connection refusal; live or unresponsive sockets are never unlinked. A reused live PID or malformed lock record is conservatively refused, not guessed away.

Logs use safe fields and redaction instead of raw SDK traces. Two rotating files (`service.log` and `service.log.1`) are limited to 256 KiB each; the current daemon retains at most 128 recent entries for `logs --follow`. Log queue growth is bounded; failure to maintain private logs stops the service with an explicit error. Logs from previous runs remain on disk; the CLI shows the current daemon's recent in-memory entries.

## Troubleshooting and live verification

| Symptom | Action |
| --- | --- |
| `daemon-unavailable` | Start `daemon` in a terminal, or `service start` if installed. Use the same state directory. |
| `port-conflict` | Inspect/release the occupied port yourself, or stop the daemon and edit the profile to use an unused port. Both IPv4 and IPv6 must be available. |
| `login-required` | Explicitly run `login`, then `reconnect NAME` or `reconnect all`. |
| `access-denied` | Check account and tunnel ACLs; signing in repeatedly is not assumed to solve permission errors. |
| `remote-unavailable` with relay connected | Confirm remote SSH service/port and host health, including WSL if used. A connected relay or open local port is not end-to-end SSH success. |
| Service loaded but unresponsive | Stop it and run `daemon` in a terminal to see sanitized startup errors. Check absolute pinned paths and private state permissions. |
| Lock refused after crash | PID reuse or malformed/foreign lock contents require manual inspection. Do not delete a lock until you have established there is no live daemon. |
| Unexpected SSH disconnect | Necessary fresh-client replacement may drop old sessions; reconnect and reattach `tmux`. |

For an isolated live check, coordinate first so no existing tunnel session is migrated or disrupted:

1. Build, create a separate private state directory, and initialize it with the installed helper.
2. Start a foreground daemon using that state directory. Add one existing tunnel's SSH endpoint on a separate **unused** loopback port.
3. Observe `status --json` until `healthy`; verify the real SSH banner or make an SSH connection on the spare port. This verifies fresh-token acquisition, management metadata, relay transport, host-key checks, and the SSH forwarding path together.
4. Exercise explicit reconnect and confirm the same local port recovers. Do not toggle networking or put a shared Mac to sleep for testing without approval. Observe a naturally occurring interruption separately and measure from connectivity return to `lastHealthyAt`.
5. Stop that verification daemon and remove only its isolated state after inspecting it. Do not change the original working forwards or SSH aliases.

The implementation has been checked against installed SDK 1.3.61 declarations/source, a real management SDK with isolated HTTP adapters, and the actual SDK WebSocket handshake cancellation path against a local stalled TCP peer. Automated daemon/CLI end-to-end tests use a deliberately injected fake transport and local SSH-banner server, not a production fallback.

Additional coordinated live checks used private in-memory CLI token acquisition, an unqualified tunnel ID resolved through SDK metadata, and a separate unused loopback port. They verified real SSH banners, the real SDK endpoint-refresh hook, and controlled CLI reconnect to a second client with the original client disposed (about 2 seconds). Disposing only the isolated test SDK client also exercised **automatic** grace/backoff/replacement recovery to a fresh SSH-banner-verified channel in 10.858 seconds, without issuing reconnect.

These are controlled checks, **not** proof of recovery after Mac sleep or of the real-world 60-second target. One additional live run reached its 45-second whole-check deadline without a definitive cause captured; subsequent staged checks and the controlled automatic recovery check passed. Unattended field reliability is not yet established. An initially unavailable target remained unhealthy despite a connected relay, matching the original forward's lack of an SSH banner. Temporary verification state and daemons were removed; existing forwards were untouched. No login or launchd registration was performed.

## Development

```sh
npm run typecheck
npm run build
npm test
```

Tests use Node's built-in runner and type stripping. They cover private configuration and helper handling, actual dual-loopback binds/conflicts, forwarding backpressure and half-close, queued-write draining after remote EOF, abrupt remote closure, SSH banners, fake-clock recovery/deadlines/long gaps, stale async completions, authentication/endpoint refresh, redaction/rotation, malformed and oversized IPC, concurrent daemon locking, and real CLI/daemon subprocess restart flows. Large-response assertions compare byte counts and equality without rendering multi-megabyte buffer diffs. All verification daemons are isolated and shut down by their tests.
