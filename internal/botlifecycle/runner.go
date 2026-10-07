package botlifecycle

import (
	"context"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// BotRunner is the contract Manager drives one running bot process
// through. internal/telegram implements it on top of Telegram long
// polling and outgoing sends (through a socks5h proxy where configured).
//
// Contract:
//   - Start begins whatever goroutine(s) the runner needs and returns once
//     startup has been attempted. A non-nil error means the bot could not
//     be started at all (e.g. an obviously malformed token) — Manager logs
//     it and leaves the bot un-started, retrying on the next reconcile
//     (bot state or leadership change). A recoverable failure discovered
//     *after* Start succeeded (bad token rejected by Telegram, bot blocked
//     from a chat, …) is not reported through this return value:
//     that classification and the resulting disabled→broken transition is
//     the runner's own job, driven back into raftcluster via a
//     SetBotState command — Start already returned by then.
//   - ctx is cancelled by Manager when the bot should stop (state changed
//     away from enabled, or this node lost leadership); Stop is always
//     also called, so implementations may rely on either signal, or both.
//   - Stop must not block indefinitely and must tolerate being called at
//     most once per Start (Manager never calls it twice for the same
//     runner instance).
type BotRunner interface {
	Start(ctx context.Context, bot raftcluster.Bot) error
	Stop()
}

// RunnerFactory creates a fresh BotRunner instance for one bot. Manager
// calls it once per bot each time that bot transitions into "should be
// running"; the returned runner is discarded after Stop() and a fresh one
// is created if the bot starts again later.
type RunnerFactory func() BotRunner
