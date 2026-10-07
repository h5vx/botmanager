// Package botlifecycle drives the bot lifecycle inside the process that
// currently holds Raft leadership:
//
//	disabled  — бот записан, процесс не запущен
//	enabled   — мастер держит горутину: long polling обновлений, отправка исходящих
//	broken    — автоматически при неустранимой ошибке; процесс остановлен, нужно вмешательство
//	deleted   — процесс остановлен, история сообщений сохраняется
//
// The state itself — which bot is in which state — lives in
// internal/raftcluster's replicated FSM, not here (only the leader
// runs bot processes, but every node, leader or not, keeps the same
// replicated view of who is enabled). botlifecycle only decides, given
// that view and this node's own leadership status, which goroutines
// should be running *on this node right now*.
//
// # Manager
//
// Manager (manager.go) subscribes to ClusterView.Subscribe() (leadership
// changes, including the initial election) and ClusterView.SubscribeApplied()
// (any committed command — the signal that a bot's state may have changed)
// and reconciles: while leader, exactly one BotRunner runs per
// BotStateEnabled bot; while not leader (or before any leader is known),
// none. A single reconcile() function handles both "we became/stopped
// being leader" and "a bot's state changed while we are leader" — both are
// just "re-read desired state, start/stop the difference" — rather than
// two special-cased code paths, since the invariant is the same either
// way ("running runners == leader && enabled bots").
//
// ClusterView is satisfied directly by *raftcluster.Node (see the
// compile-time assertion in cluster.go); tests substitute a fake
// implementation (see manager_test.go) so Manager's reconciliation logic
// is tested without a real Raft cluster.
//
// # BotRunner
//
// BotRunner (runner.go) is the minimal interface a bot process must
// implement — Start(ctx, bot) error / Stop() — so Manager is written and
// tested without depending on Telegram at all; internal/telegram provides
// the real implementation. Manager only ever holds a BotRunner
// through this interface.
//
// # Что сознательно не входит в этот пакет
//
// Классификация сбоев (проблема узла / проблема бота / rate-limit /
// проблема адресата) и итоговое решение "перевести бота в broken" —
// реализуется в internal/telegram, которое сообщает об
// этом обратно через обычную SetBotState-команду в raftcluster; Manager
// узнаёт об этом так же, как о любом другом изменении состояния — через
// SubscribeApplied, без специального механизма.
package botlifecycle
