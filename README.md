<p align="center">
  <img src="https://raw.githubusercontent.com/h5vx/botmanager/main/docs/logo.svg" alt="botmanager logo" width="128" height="128">
</p>

<h1 align="center">botmanager</h1>

<p align="center">
  <a href="https://github.com/h5vx/botmanager/actions/workflows/docker.yml"><img src="https://github.com/h5vx/botmanager/actions/workflows/docker.yml/badge.svg?event=push" alt="Docker image build"></a>
  <a href="https://hub.docker.com/r/h5vx/botmanager"><img src="https://img.shields.io/docker/v/h5vx/botmanager?sort=semver&label=docker%20hub" alt="Docker Hub"></a>
</p>

A distributed gateway to the Telegram Bot API. botmanager holds bot
tokens, runs long polling, sends messages with idempotency guarantees and
streams incoming events over gRPC, so the rest of your system never talks
to Telegram directly — and keeps working when a node goes down.

botmanager knows nothing about any business domain: only bots, chats and
messages. All domain logic lives in the services that call it.

## Features

- **Raft cluster** (`hashicorp/raft` + BoltDB). Bots, message history,
  delivery statuses and the event journal are replicated. Only the leader
  talks to Telegram; when it fails, another node takes over within seconds
  and continues exactly where it stopped.
- **Leadership follows Telegram.** If the leader loses its connection to
  Telegram (network, proxy) while another node still has one, leadership
  moves to that node. A graceful shutdown hands leadership over instantly
  instead of waiting for a timeout.
- **Talk to any node.** Writes sent to a follower are forwarded to the
  leader transparently; reads and the event stream are served by every
  node.
- **No lost or duplicated incoming updates.** Updates are committed
  through Raft before they are acknowledged to Telegram, so a leader crash
  loses nothing, and every event carries a cluster-wide sequence number.
- **Resumable event stream** `Messaging.Subscribe`: incoming messages,
  inline button presses, chat membership changes, delivery status changes
  and bot state changes. Reconnect with the last sequence you saw and get
  exactly what you missed — from any node.
- **Reliable sending**: mandatory `idempotency_key`, a priority queue
  (`critical` goes ahead of `normal`), retries with backoff, delivery
  statuses `PENDING` / `RETRYING` / `SENT` / `FAILED` / `CANCELLED`, and
  cancellation of messages not yet sent.
- **Secure by default**: mutual TLS for the gRPC API and between nodes,
  bot tokens encrypted with AES-256-GCM before they enter the Raft log.
  The service refuses to start without certificates unless you explicitly
  enable development mode.
- **Bot lifecycle**: `disabled` → `enabled` → `broken` → `deleted`. A bot
  whose token was revoked or that was kicked from a chat moves to `broken`
  on its own, without affecting other bots.
- **Failure classification**: node problem (network, proxy), bot problem
  (token, permissions), rate limit (429), recipient problem (the user
  blocked the bot) — each class gets its own reaction.
- **SOCKS5 proxy** with name resolution on the proxy side: a node-wide
  default that each bot can override or explicitly disable.
- **Mini App `initData` signature verification**: the bot token never
  leaves botmanager, so the check is done here.
- **Go client library** with automatic failover, retries and a
  self-resuming `Subscribe`.
- **Optional web interface** with login: cluster members and their memory,
  leader assignment, adding and removing nodes, bots, delivery statistics
  and Telegram checks from any node.
- **Operations**: cluster status with real peer health checks, adding and
  removing nodes, leadership transfer, Telegram reachability check
  (including through the proxy), streaming state export for backups.

## Quick start with Docker

Only Docker is needed. Images for `linux/amd64` and `linux/arm64` are on
Docker Hub as [`h5vx/botmanager`](https://hub.docker.com/r/h5vx/botmanager).

### Try it in one command

Development mode: no TLS, no authentication, tokens in plaintext, data
lost with the container. For experiments only.

```bash
docker run --rm -p 9090:9090 -p 9091:9091 -e BOTMANAGER_SECURITY__INSECURE=true h5vx/botmanager:1
```

Then call it with `grpcurl -plaintext localhost:9090 ...` (see below).

### Run it properly

**1. Create certificates and the token key** with the tool shipped in the
image. Everything lands in `./certs`; keep `ca-key.pem` and `token.key`
secret.

```bash
mkdir -p certs data
```

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/certs:/certs" --entrypoint /app/botmanager-certs h5vx/botmanager:1 init -out /certs
```

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/certs:/certs" --entrypoint /app/botmanager-certs h5vx/botmanager:1 issue -out /certs -name node -hosts localhost,127.0.0.1
```

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/certs:/certs" --entrypoint /app/botmanager-certs h5vx/botmanager:1 issue -out /certs -name client
```

The node certificate is named `node` because that is what the image's
config expects (`/etc/botmanager/certs/node.pem`); `-hosts` lists the
names clients use to reach the node. `client.pem` is for your service.

**2. Start the node.** `--user` lets the container read the key files
and write `./data`, both owned by you.

```bash
docker run -d --name botmanager --user "$(id -u):$(id -g)" -p 9090:9090 -p 9091:9091 -v "$PWD/certs:/etc/botmanager/certs:ro" -v "$PWD/data:/var/lib/botmanager" h5vx/botmanager:1
```

**3. Check it.**

```bash
curl localhost:9091/readyz
```

```bash
grpcurl -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem localhost:9090 botmanager.v1.Maintenance/GetClusterStatus
```

### First bot

Use the same `grpcurl` flags (or `-plaintext` in development mode).
Create a bot (it starts `disabled`), enable it, send a message, watch
events:

```bash
grpcurl -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem -d '{"display_name": "My bot", "token": "123456:ABC..."}' localhost:9090 botmanager.v1.BotAdmin/CreateBot
```

```bash
grpcurl -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem -d '{"id": "<bot_id>", "state": "BOT_STATE_ENABLED"}' localhost:9090 botmanager.v1.BotAdmin/SetBotState
```

```bash
grpcurl -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem -d '{"idempotency_key": "hello-1", "bot_id": "<bot_id>", "chat_id": -1001234567890, "text": "Hello!"}' localhost:9090 botmanager.v1.Messaging/Send
```

```bash
grpcurl -cacert certs/ca.pem -cert certs/client.pem -key certs/client-key.pem -d '{}' localhost:9090 botmanager.v1.Messaging/Subscribe
```

Repeating `Send` with the same `idempotency_key` does not create a
duplicate.

### From source

```bash
make run
```

Starts one development-mode node with [config/dev.yaml](config/dev.yaml)
(data in `./data`, gRPC on `127.0.0.1:9090`).

## Running securely

Every node and every client presents a certificate signed by one private
CA, and bot tokens are encrypted with a key shared by all nodes. If you
have no PKI of your own, `botmanager-certs` creates everything:

```bash
go run ./cmd/botmanager-certs init -out certs
```

```bash
go run ./cmd/botmanager-certs issue -out certs -name node-1 -hosts 10.0.0.1,bm-1.internal
```

```bash
go run ./cmd/botmanager-certs issue -out certs -name my-service
```

`init` writes `ca.pem`, `ca-key.pem` and `token.key`; `issue` writes
`<name>.pem` and `<name>-key.pem`. A node certificate must cover the hosts
other nodes and clients use to reach it. Keep `ca-key.pem` and `token.key`
secret; every node needs `ca.pem`, its own certificate and the same
`token.key`.

Point the node at them (these are the defaults in
[config/config.yaml](config/config.yaml)):

```yaml
security:
  ca_file: /etc/botmanager/certs/ca.pem
  cert_file: /etc/botmanager/certs/node.pem
  key_file: /etc/botmanager/certs/node-key.pem
  token_key_file: /etc/botmanager/certs/token.key
```

Any certificate signed by the CA is fully trusted, so issue client
certificates only to services that should control your bots.

## Running a cluster

Three nodes tolerate the loss of one. Give each node its own `node.id`,
its advertise address and the list of the others:

```yaml
node:
  id: node-1
  raft_bind_addr: "0.0.0.0:9092"
  raft_advertise_addr: "10.0.0.1:9092"
raft:
  bootstrap: true
  peers:
    - {id: node-2, raft_addr: "10.0.0.2:9092", grpc_addr: "10.0.0.2:9090"}
    - {id: node-3, raft_addr: "10.0.0.3:9092", grpc_addr: "10.0.0.3:9090"}
```

The same as an environment variable:

```
BOTMANAGER_RAFT__PEERS=node-2/10.0.0.2:9092/10.0.0.2:9090,node-3/10.0.0.3:9092/10.0.0.3:9090
```

The list may also include the node itself; then every node can use the
same list, and the node takes its own advertise addresses from its entry.

Start all nodes; they elect a leader on their own. To grow a running
cluster, start the new node with `raft.bootstrap: false` and call
`Maintenance.AddNode` on any node; `Maintenance.RemoveNode` takes one out.

### When leadership changes

- **The leader process dies or loses the other nodes** — Raft elects a
  new leader after the heartbeat timeout (about 1–2 s with the defaults;
  tune `raft.heartbeat_timeout_ms`, `election_timeout_ms` and
  `leader_lease_timeout_ms`).
- **The leader is stopped gracefully** (SIGTERM) — it hands leadership to
  another node first, so writes continue almost without a pause.
- **The leader cannot reach Telegram, but a peer can** — when at least
  `failover.min_failing_bots` bots (and at least half of the running ones)
  have had no answer from Telegram for `failover.failure_window_seconds`,
  the leader asks its peers to check Telegram for those same bots (with
  their tokens and proxies) and hands leadership to one that succeeds.
  If nobody reaches Telegram — an outage on Telegram's side or a broken
  proxy of one bot — leadership stays where it is.
- **Manually** — `Maintenance.TransferLeadership`, e.g. before
  maintenance of a node.

Ports:

| Port | What |
|------|------|
| `9090` | gRPC: `BotAdmin`, `Messaging`, `Maintenance`, `grpc.health.v1`, reflection |
| `9091` | HTTP: `/healthz`, `/readyz`, `/metrics` |
| `9092` | Raft transport between nodes |
| `9093` | Web interface (only with `web.enabled`) |

### Docker image

Release tags publish `1.2.3`, `1.2`, `1` and `latest`. The image is
distroless and runs as an unprivileged user; to build it yourself run
`docker build -t botmanager .`.

## Web interface

An optional administration interface is built into the binary (no
separate frontend to deploy). It shows:

- cluster members with their role, addresses, memory, data size, Raft
  position and version; make any node the leader, add or remove nodes;
- bots with their state and failure reason; create, enable, disable and
  delete bots, view their recent messages;
- message counts by delivery status and other statistics;
- Telegram reachability checks run on any node you choose, for the node's
  own route or for a specific bot's token and proxy.

Every node can serve it: changes go to the leader like any other write,
and each node's statistics are fetched from that node.

**1. Create a password hash:**

```bash
docker run --rm -i --entrypoint /app/botmanager h5vx/botmanager:1 -hash-password
```

Type the password and press Enter (or `echo 'password' | botmanager
-hash-password` with the binary).

**2. Enable it** on the nodes you want to open it on:

```yaml
web:
  enabled: true
  listen_addr: ":9093"
  users:
    - username: admin
      password_hash: "$2a$10$..."
```

or, with Docker, add `-p 9093:9093 -e BOTMANAGER_WEB__ENABLED=true -e
BOTMANAGER_WEB__USERS='admin:$2a$10$...'` to the `docker run` command.

**3. Open** `https://<node>:9093`. The page is served over HTTPS with the
node certificate, so the browser has to trust the cluster CA (import
`certs/ca.pem`) or will show a warning. Behind a reverse proxy that
terminates TLS itself, set `web.tls: false`.

Sessions last `web.session_ttl_minutes` (12 hours) and are valid on every
node. Failed logins are limited to five per five minutes per client
address; every change is written to the log with the user's name.

## Using from Go

The API contract and the client library are a separate, lightweight
module (Go 1.23+, depends only on gRPC and protobuf):

```bash
go get github.com/h5vx/botmanager/api@latest
```

```go
import (
	"github.com/h5vx/botmanager/api/botmanagerpb"
	"github.com/h5vx/botmanager/api/client"
)

tlsCfg, err := client.LoadTLS("ca.pem", "my-service.pem", "my-service-key.pem")
if err != nil {
	return err
}
c, err := client.New(client.Config{
	Endpoints: []string{"10.0.0.1:9090", "10.0.0.2:9090", "10.0.0.3:9090"},
	TLS:       tlsCfg,
})
if err != nil {
	return err
}
defer c.Close()

_, err = c.Messaging.Send(ctx, &botmanagerpb.SendRequest{
	IdempotencyKey: "order-42-paid",
	BotId:          botID,
	ChatId:         chatID,
	Text:           "Payment received",
})
```

The client spreads calls over all endpoints, skips nodes that are down
and retries calls that are safe to repeat while the cluster elects a new
leader (`Config.RetryTimeout`, 30 s by default). `CreateBot` is never
retried automatically, since a repeat could create a second bot.

The event stream reconnects by itself and resumes after the last delivered
event; duplicates are skipped:

```go
err = c.Subscribe(ctx, client.SubscribeOptions{BotIDs: []string{botID}},
	func(u *botmanagerpb.Update) error {
		if m := u.GetIncomingMessage(); m != nil {
			// handle the message; u.GetSequence() identifies the event
		}
		return nil
	})
```

To survive restarts of your own service, store `u.GetSequence()` and pass
it back as `SubscribeOptions.AfterSequence`. If the cluster no longer
holds events that old (see `raft.journal_retention`), `Subscribe` returns
`client.ErrEventsLost`.

botmanager itself runs as a separate service; it is not an embeddable
library.

## API

The full contract with comments is in
[api/proto/botmanager.proto](api/proto/botmanager.proto).

**`BotAdmin`** — bot management: `CreateBot`, `UpdateBot`, `SetBotState`,
`DeleteBot`, `ListBots`, `GetBot`, `GetChat`, `ListChats`,
`VerifyInitData`, `GetUserProfilePhoto`. The bot token is accepted on
create and update but is never returned in any response.

**`Messaging`** — messages: `Send`, `SendBatch`, `EditMessage`,
`DeleteMessage`, `PinMessage`, `UnpinMessage`, `CancelPending`,
`AnswerCallback`, `GetMessage`, `GetMessages`, `ListMessages`,
`Subscribe`.

**`Maintenance`** — operations: `GetClusterStatus`, `GetNodeStats`,
`ExportState`, `TransferLeadership`, `PingTelegram`, `AddNode`,
`RemoveNode`.

Any node accepts any call. While no leader is known (during an election)
writes fail with `UNAVAILABLE`; the Go client retries them.

## Configuration

The config file is [config/config.yaml](config/config.yaml); its path is
set with the `-config` flag. Every setting can be overridden by an
environment variable with the `BOTMANAGER_` prefix, with nesting separated
by `__`:

```
BOTMANAGER_NODE__ID=node-1
BOTMANAGER_NODE__DATA_DIR=/var/lib/botmanager
BOTMANAGER_NODE__RAFT_ADVERTISE_ADDR=10.0.0.1:9092
BOTMANAGER_RAFT__PEERS=node-2/10.0.0.2:9092/10.0.0.2:9090
BOTMANAGER_GRPC__LISTEN_ADDR=:9090
BOTMANAGER_PROXY__ENABLED=true
BOTMANAGER_PROXY__ADDRESS=socks5h://proxy.internal:1080
BOTMANAGER_SECURITY__CERT_FILE=/etc/botmanager/certs/node-1.pem
BOTMANAGER_OBSERVABILITY__LOG_LEVEL=DEBUG
```

Bot tokens are not stored in the config: they arrive via
`BotAdmin.CreateBot` / `UpdateBot` and live, encrypted, in the Raft state.

All state is kept in memory on every node and bounded by record counts:
`raft.message_retention_per_bot` (500 messages per bot) and
`raft.journal_retention` (10 000 events available for resuming
`Subscribe`).

## Observability

- Logs are JSON (`log/slog`) on stdout, with no message contents.
- `GET /healthz` — the process is alive, always 200.
- `GET /readyz` — 200 once the Raft node knows a leader, 503 otherwise.
- `GET /metrics` — Prometheus (currently only the standard Go runtime
  metrics).

## Development

```bash
make test
```

```bash
make lint
```

`make test` and `make lint` cover both modules. `make lint` needs a recent
staticcheck (versions from system package managers don't understand
`go 1.25.0` in `go.mod`):

```bash
go install honnef.co/go/tools/cmd/staticcheck@latest
```

Tests don't need a real Telegram: the Bot API is emulated with
`httptest.Server`; clusters run on Raft's in-memory transport, and one test
runs a real two-node cluster over mutual TLS. To check the client against
the real Telegram Bot API, provide a bot and a chat it may write to:

```bash
BOTMANAGER_LIVE_TOKEN=123456:ABC... BOTMANAGER_LIVE_CHAT_ID=-1001234567890 go test ./internal/telegram -run Live -v
```

Generated protobuf code is committed in `api/botmanagerpb/`. After
changing the `.proto` file, regenerate it with `make proto` (uses
[buf](https://buf.build), no system `protoc` needed; see
[api/buf.gen.yaml](api/buf.gen.yaml) for the plugins).

### Layout

```
api/                     separate module: API contract and Go client
  proto/                 botmanager.proto
  botmanagerpb/          generated code
  client/                client library
cmd/botmanager/          service entry point
cmd/botmanager-certs/    CA, certificate and token key generator
internal/config/         YAML + environment overrides
internal/observability/  logging, health endpoints, metrics
internal/raftcluster/    Raft cluster, FSM, event journal, snapshots, mTLS transport, token encryption
internal/botlifecycle/   which bots should be running on this node
internal/failover/       hands leadership to a node that reaches Telegram
internal/telegram/       Bot API client, long polling, sending, proxy
internal/rpcserver/      gRPC services, forwarding writes to the leader
internal/security/       mTLS configuration, token key loading
internal/webui/          web interface: JSON API, login, embedded pages
internal/devcerts/       certificate generation
```

Each package's doc comment describes its design decisions.

## Limitations

- **Outgoing messages are delivered at least once.** If the leader
  crashes after Telegram accepted a message but before the `SENT` status
  was committed, the new leader sends it again.
- **One trust level.** Every certificate signed by the CA has full access
  to the API; there are no per-client permissions.
- **`GetClusterStatus.proxy_healthy`** is not probed; use `PingTelegram`
  on the node in question.
- **Custom metrics** (`botmanager_is_leader`, `botmanager_bots_running`)
  are not added yet.

## License

[MIT](LICENSE)
