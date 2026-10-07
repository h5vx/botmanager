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
//     Subscribe merges internal/telegram.UpdateBus (incoming_message/
//     callback_query/chat_member_changed) with raftcluster.Node.SubscribeEvents
//     (message_status_changed/bot_state_changed) into one stream, filtered
//     by SubscribeRequest.bot_ids.
//   - MaintenanceServer (maintenance.go): GetClusterStatus reads Raft's
//     own configuration/leader (Node.Configuration/LeaderID) rather than
//     hardcoding a single node, so the same code keeps working once a
//     real multi-node deployment exists. ExportState streams
//     Node.ExportSnapshot's JSON output in bounded chunks.
//
// # Leadership — deliberately not retransmitted
//
// The cluster is single-node for now (no configuration carries peer
// gRPC addresses to retransmit a write to). Every RPC that calls
// Node.Apply checks Node.IsLeader() first (requireLeader in leader.go) and
// returns codes.FailedPrecondition naming the current leader address
// (Node.LeaderAddr/LeaderID), or codes.Unavailable if no leader is known
// yet, instead of silently failing inside Apply or forwarding the call
// itself. Read-only RPCs (GetBot, ListBots, GetMessage, ListMessages,
// GetClusterStatus, and every RPC that only makes a live Telegram call
// without touching raft — GetChat, EditMessage, DeleteMessage, PinMessage,
// UnpinMessage, AnswerCallback) have no such restriction: they work on any
// node, replica included, since they never call Apply. Building real
// retransmission (dialing the current leader's own gRPC BotAdmin/Messaging
// as a client and forwarding the request) is future work that needs a
// peer address list this configuration does not have yet — see
// CLAUDE.md.
package rpcserver
