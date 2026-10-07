// Package telegram implements botlifecycle.BotRunner on top of the real
// Telegram Bot API: long polling of updates, sending/editing/deleting/pinning
// messages, socks5h proxy support, and — the core of this package — turning
// whatever Telegram's HTTP responses say into one of four failure
// classes and reacting accordingly.
//
// # Layout
//
//   - client.go, wire.go — a minimal HTTP client for the handful of Bot API
//     methods the gRPC API needs (getMe, getUpdates, sendMessage,
//     editMessageText, deleteMessage, pinChatMessage, unpinChatMessage,
//     answerCallbackQuery, getChat, getChatMember). No SDK dependency, in
//     keeping with the project's "few, heavy dependencies" stance —
//     hashicorp/raft and BoltDB remain the
//     only "real" ones; golang.org/x/net/proxy (already an indirect
//     dependency of this module before this package existed) is the only
//     addition.
//   - proxy.go — the three-level proxy resolution (EffectiveProxy) and
//     building an *http.Client whose Transport.DialContext goes through a
//     SOCKS5 dialer that never resolves the destination host locally
//     (socks5h semantics).
//   - failure.go — ClassifyFailure, the failure classification. See its doc
//     comment for the exact HTTP-status/description rules and
//     failure_test.go for the table of real Telegram response shapes it is
//     tested against.
//   - bus.go — Update/UpdateBus/InMemoryBus: the in-process fan-out this
//     package publishes incoming updates to. gRPC Messaging.Subscribe
//     attaches to it — see "Extension point" below.
//   - cluster.go — ClusterView, the narrow slice of *raftcluster.Node this
//     package needs (Apply/ListMessages/SubscribeApplied), mirroring
//     botlifecycle.ClusterView's pattern so Runner is testable without a
//     real Raft node.
//   - runner.go, pollloop.go, sendloop.go, backoff.go — Runner, the
//     botlifecycle.BotRunner implementation: one goroutine long-polling
//     updates (pollLoop) and one goroutine draining bot.ID's
//     PENDING/RETRYING outgoing messages in priority order (sendLoop),
//     both stopped together by Stop()/ctx cancellation.
//   - initdata.go — VerifyInitData: a pure function checking a Telegram
//     Mini App initData string's HMAC-SHA256 signature —
//     unrelated to Bot API HTTP calls, but it lives here rather than a
//     separate package because it is conceptually still "Telegram API
//     work" and internal/rpcserver.BotAdminServer already depends on this
//     package for its live-call RPCs (GetChat, ...); a one-function
//     package would not earn its own boundary. See
//     CLAUDE.md, "VerifyInitData" for the algorithm summary and
//     internal/rpcserver/botadmin.go for the RPC that calls it.
//
// # Reaction table
//
//	Класс       | Что делает Runner
//	------------|----------------------------------------------------------
//	Node        | Локальный backoff и повтор (poll: переспросить getUpdates;
//	            | send: статус RETRYING, next_retry_at растёт экспоненциально,
//	            | FAILED после исчерпания cfg.MaxSendRetries —
//	            | "исчерпаны попытки").
//	Bot         | Runner сам подаёт SetBotState(broken, class, reason) в
//	            | raftcluster через Apply и останавливает себя (обе горутины) —
//	            | ровно то, что описывает докстрока botlifecycle.BotRunner.Start:
//	            | "не через возврат ошибки Start, а через SetBotState после
//	            | того, как Start уже вернулся".
//	RateLimit   | НЕ пишется в raftcluster — чисто локальная пауза отправок
//	            | этого бота на retry_after (или fallback-задержку, если
//	            | retry_after не указан). Сообщение остаётся в прежнем
//	            | статусе, будет подобрано снова после паузы.
//	Recipient   | UpdateDelivery(FAILED, last_error) — терминально, повторов
//	            | нет (иначе сообщение вечно висело бы в RETRYING).
//
// # Осознанно не реализовано: "проблема узла ⇒ сложить полномочия"
//
// Возможен второй уровень реакции на FailureClassNode: если сбои
// происходят по НЕСКОЛЬКИМ ботам одновременно на одном узле, это признак
// проблемы самого узла (недоступен прокси, нет сети), и правильная реакция —
// узлу сложить лидерские полномочия, чтобы новый лидер (на другой машине,
// предположительно с рабочей сетью) подхватил ботов. Этот пакет
// классифицирует отдельные сбои как FailureClassNode и делает локальный
// backoff/retry для ОДНОГО бота (см. таблицу выше), но не реализует
// корреляцию "несколько ботов сразу" и не даёт Runner способа сложить
// лидерство узла — это требует состояния на уровне процесса (счётчик
// сбоев по всем ботам сразу, окно времени) вне отдельного BotRunner, и
// физически негде проверить на одной машине разработки без нескольких
// узлов кластера (по аналогии с одноузловым bootstrap в
// internal/raftcluster). Сознательное упрощение — см. CLAUDE.md,
// "Что сознательно пусто".
//
// # Extension point: UpdateBus → Messaging.Subscribe
//
// pollLoop publishes every converted update (incoming_message,
// callback_query, chat_member_changed — the Update kinds that
// originate from Telegram itself, as opposed to message_status_changed and
// bot_state_changed, which originate from raftcluster's own
// UpdateDelivery/SetBotState commands and are therefore not this package's
// job to produce) to r.bus, an UpdateBus. NewRunnerFactory takes an
// UpdateBus so callers (cmd/botmanager/main.go) can share one
// InMemoryBus across every bot's Runner and attach the Messaging.Subscribe
// gRPC stream as one more Subscribe() consumer — no changes to this
// package required.
package telegram
