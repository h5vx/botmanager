<p align="center">
  <img src="docs/logo.svg" alt="botmanager logo" width="128" height="128">
</p>

<h1 align="center">botmanager</h1>

A service for managing Telegram bots behind a gRPC API. It holds bot
tokens, runs long polling, sends messages with idempotency guarantees and
streams incoming events, so the rest of the system never has to talk to
Telegram directly.

botmanager knows nothing about any business domain: only bots, chats and
messages. All domain logic lives in the calling service.

## Features

- **Replicated state on Raft** (`hashicorp/raft` + BoltDB): bots, the
  message log and delivery statuses survive restarts and leader changes.
  Only the cluster leader talks to Telegram on behalf of bots.
- **Bot lifecycle**: `disabled` → `enabled` → `broken` → `deleted`. A bot
  whose token was revoked or that was kicked from a chat moves to `broken`
  on its own, without affecting other bots.
- **Reliable sending**: mandatory `idempotency_key`, a priority queue
  (`critical` goes ahead of `normal`), retries with backoff, delivery
  statuses `PENDING` / `RETRYING` / `SENT` / `FAILED` / `CANCELLED`, and
  cancellation of messages not yet sent.
- **Failure classification**: node problem (network, proxy), bot problem
  (token, permissions), rate limit (429), recipient problem (the user
  blocked the bot) — each class gets its own reaction.
- **SOCKS5 proxy** with name resolution on the proxy side: a node-wide
  default that each bot can override or explicitly disable.
- **Event stream** `Messaging.Subscribe`: incoming messages, inline button
  presses, chat membership changes, delivery status changes and bot state
  changes.
- **Mini App `initData` signature verification**: the bot token never
  leaves botmanager, so the check is done here.
- **Maintenance**: cluster status, leadership transfer, Telegram
  reachability check (including through the proxy), streaming state export
  for backups.

## Quick start

Requires Go 1.25+.

```bash
make build
```

```bash
make run
```

By default the node stores Raft data in `/var/lib/botmanager`. For a local
run, point it at a writable directory:

```bash
BOTMANAGER_NODE__DATA_DIR=./data make run
```

Once started:

| Port | What |
|------|------|
| `:9090` | gRPC: `BotAdmin`, `Messaging`, `Maintenance`, `grpc.health.v1`, reflection |
| `:9091` | HTTP: `/healthz`, `/readyz`, `/metrics` |
| `127.0.0.1:9092` | Raft transport between nodes |

### Docker

```bash
docker build -t botmanager .
```

```bash
docker run --rm -p 9090:9090 -p 9091:9091 -v botmanager-data:/var/lib/botmanager botmanager
```

The image is distroless and runs as an unprivileged user.

## Trying it with grpcurl

The server has reflection enabled, so no generated client is needed.

Create a bot (it starts in the `disabled` state):

```bash
grpcurl -plaintext -d '{"display_name": "My bot", "token": "123456:ABC..."}' localhost:9090 botmanager.v1.BotAdmin/CreateBot
```

Enable it:

```bash
grpcurl -plaintext -d '{"id": "<bot_id>", "state": "BOT_STATE_ENABLED"}' localhost:9090 botmanager.v1.BotAdmin/SetBotState
```

Send a message:

```bash
grpcurl -plaintext -d '{"idempotency_key": "hello-1", "bot_id": "<bot_id>", "chat_id": -1001234567890, "text": "Hello!", "priority": "PRIORITY_NORMAL"}' localhost:9090 botmanager.v1.Messaging/Send
```

Subscribe to events:

```bash
grpcurl -plaintext -d '{}' localhost:9090 botmanager.v1.Messaging/Subscribe
```

Repeating `Send` with the same `idempotency_key` does not create a
duplicate; it returns the existing message.

## Using from Go

The generated client lives in this module, so a Go service only needs:

```bash
go get github.com/h5vx/botmanager@latest
```

```go
import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	botmanagerpb "github.com/h5vx/botmanager/proto/gen"
)

conn, err := grpc.NewClient("localhost:9090",
	grpc.WithTransportCredentials(insecure.NewCredentials()))
if err != nil {
	return err
}
messaging := botmanagerpb.NewMessagingClient(conn)

_, err = messaging.Send(ctx, &botmanagerpb.SendRequest{
	IdempotencyKey: "hello-1",
	BotId:          botID,
	ChatId:         chatID,
	Text:           "Hello!",
	Priority:       botmanagerpb.Priority_PRIORITY_NORMAL,
})
```

botmanager runs as a separate process; it is not an embeddable library.

## API

The full contract with comments is in
[proto/botmanager.proto](proto/botmanager.proto).

**`BotAdmin`** — bot management: `CreateBot`, `UpdateBot`, `SetBotState`,
`DeleteBot`, `ListBots`, `GetBot`, `GetChat`, `ListChats`,
`VerifyInitData`, `GetUserProfilePhoto`. The bot token is accepted on
create and update but is never returned in any response.

**`Messaging`** — messages: `Send`, `SendBatch`, `EditMessage`,
`DeleteMessage`, `PinMessage`, `UnpinMessage`, `CancelPending`,
`AnswerCallback`, `GetMessage`, `GetMessages`, `ListMessages`,
`Subscribe`.

**`Maintenance`** — operations: `GetClusterStatus`, `ExportState`,
`TransferLeadership`, `PingTelegram`.

Write requests (anything that changes state) are accepted only by the
leader. A non-leader responds with `FAILED_PRECONDITION` and the current
leader's address.

## Configuration

The config file is [config/config.yaml](config/config.yaml); its path is
set with the `-config` flag. Every setting can be overridden by an
environment variable with the `BOTMANAGER_` prefix, with nesting separated
by `__`:

```
BOTMANAGER_NODE__ID=node-1
BOTMANAGER_NODE__DATA_DIR=/var/lib/botmanager
BOTMANAGER_GRPC__LISTEN_ADDR=:9090
BOTMANAGER_HTTP__LISTEN_ADDR=:9091
BOTMANAGER_PROXY__ENABLED=true
BOTMANAGER_PROXY__ADDRESS=socks5h://proxy.internal:1080
BOTMANAGER_RAFT__MESSAGE_RETENTION_PER_BOT=500
BOTMANAGER_TELEGRAM__API_BASE_URL_OVERRIDE=
BOTMANAGER_OBSERVABILITY__LOG_LEVEL=DEBUG
```

Bot tokens are not stored in the config: they arrive via
`BotAdmin.CreateBot` / `UpdateBot` and live in the Raft state.

Message history is capped by record count per bot
(`raft.message_retention_per_bot`, 500 by default), not by time.

## Observability

- Logs are JSON (`log/slog`) on stdout, with no personal data.
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

`make lint` runs `go vet` and `staticcheck`. Install a recent staticcheck
like this (versions from system package managers don't understand
`go 1.25.0` in `go.mod`):

```bash
go install honnef.co/go/tools/cmd/staticcheck@latest
```

Tests don't need a real Telegram: the Bot API is emulated with
`httptest.Server`, and the cluster runs on Raft's in-memory transport.

Generated protobuf code is committed in `proto/gen/`. After changing the
`.proto` file, regenerate it with `make proto` (requires a working
`protoc`) or with the container-based recipe described in
[CLAUDE.md](CLAUDE.md).

### Layout

```
cmd/botmanager/         entry point
internal/config/        YAML + environment overrides
internal/observability/ logging, health endpoints, metrics
internal/raftcluster/   Raft cluster, FSM, snapshots
internal/botlifecycle/  which bots should be running on this node
internal/telegram/      Bot API client, long polling, sending, proxy
internal/rpcserver/     gRPC service implementations
proto/                  API contract and generated code
```

Design details and decisions for each package are in
[CLAUDE.md](CLAUDE.md) and in each package's package doc.

## Current limitations

- The cluster is deployed as a single node. Multi-node operation is
  supported by the code and covered by tests, but adding and removing
  nodes via the API is not implemented yet.
- A non-leader does not forward write requests to the leader; the client
  has to reach the leader itself.
- No automatic leadership handoff when several bots fail on the same node
  at once.
- gRPC and the Raft transport have no TLS and no authentication. Run
  botmanager only inside a trusted network.
- Custom metrics (`botmanager_is_leader`, `botmanager_bots_running`) are
  not added yet.
