// Package rpcserver implements botmanagerpb.BotAdmin/Messaging/Maintenance
// as thin adapters over internal/raftcluster.Node and internal/telegram
// (see CLAUDE.md).
//
// # Boundary
//
// internal/raftcluster and internal/telegram do not import
// proto/gen (botmanagerpb) — a deliberate boundary. Every conversion between
// internal types (raftcluster.Bot/Message/DeliveryInfo/..., telegram.Update)
// and protobuf messages lives in this package (convert.go), in one
// direction at the RPC boundary.
//
// # Servers
//
//   - BotAdminServer (botadmin.go): CreateBot/UpdateBot/SetBotState/
//     DeleteBot/ListBots/GetBot go through Node.Apply/ListBots/GetBot.
//     GetBot and ListBots never return Bot.Token (the token never leaves
//     botmanager) — see botToProto. GetChat is the one live
//     Telegram call in this server: it builds a one-off telegram.Client
//     for the target bot (telegramClientFor) rather than reusing the
//     bot's long-lived Runner, since Runner has no synchronous
//     request/response entry point and GetChat's load is negligible.
//   - MessagingServer (messaging.go): Send/SendBatch/CancelPending apply
//     raftcluster commands (idempotency and the PENDING/RETRYING-only
//     cancel check are enforced by the FSM, not here — see
//     raftcluster.CommandPutMessage/CommandCancelPending).
//     EditMessage/DeleteMessage/PinMessage/UnpinMessage/AnswerCallback make
//     a live Telegram call the same way GetChat does, keyed off the
//     message's/request's bot_id — see CLAUDE.md for the note on
//     AnswerCallbackRequest.bot_id. GetMessage/GetMessages/ListMessages are plain reads.
//     Subscribe streams the replicated journal (raftcluster.JournalEntry)
//     — Telegram updates and status changes alike — filtered by
//     SubscribeRequest.bot_ids and resumable with after_sequence; every
//     node can serve it.
//   - MaintenanceServer (maintenance.go): GetClusterStatus reads Raft's
//     own configuration/leader (Node.Configuration/LeaderID) rather than
//     hardcoding a single node, so the same code keeps working once a
//     real multi-node deployment exists, with addresses from the node
//     registry and a real grpc.health.v1 probe of every peer. ExportState
//     streams Node.ExportSnapshot's JSON output in bounded chunks.
//     AddNode/RemoveNode change cluster membership.
//
// # Leadership — writes and live Telegram calls are forwarded to the leader
//
// Forwarder (forward.go) is a unary server interceptor: a write RPC
// (writeMethods) received by a follower is sent on to the current leader's
// gRPC address, taken from the replicated node registry, over a connection
// authenticated with this node's own certificate, and the leader's answer
// is returned as-is. A request already forwarded once is never forwarded
// again (metadata marker) — if leadership moved in between, it fails with
// UNAVAILABLE and the client retries. Without a known leader the answer is
// UNAVAILABLE as well. Every RPC that calls Node.Apply additionally checks
// leadership itself (requireLeader in leader.go), so a server built
// without the Forwarder answers FAILED_PRECONDITION naming the leader.
// Live Telegram calls (liveTelegramMethods: EditMessage, AnswerCallback,
// GetChat, ...) are forwarded the same way, because the leader is the node
// that reaches Telegram; needing no leadership, they fall back to the
// receiving node when no leader is known or leadership moved in transit.
// PingTelegram, reads, VerifyInitData and Subscribe are served by
// whichever node receives them.
package rpcserver
