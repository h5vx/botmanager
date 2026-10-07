package telegram

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/h5vx/botmanager/internal/botlifecycle"
	"github.com/h5vx/botmanager/internal/raftcluster"
)

// tokenPattern is a minimal sanity check for "obviously malformed token" —
// botlifecycle.BotRunner.Start's docstring names this as the one kind of
// failure that IS reported via Start's return value (as opposed to a
// failure Telegram itself reports, which is the runner's own job via
// SetBotState — see reportBroken). Real Telegram bot tokens look like
// "123456789:AAExxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; this is a structural
// check only, not a call to Telegram.
var tokenPattern = regexp.MustCompile(`^\d+:[A-Za-z0-9_-]{10,}$`)

// Runner implements botlifecycle.BotRunner for one bot: long polling
// (pollLoop, pollloop.go) and the outgoing-message queue (sendLoop,
// sendloop.go) run as two goroutines started by Start and stopped together
// by Stop()/ctx cancellation.
type Runner struct {
	cluster   ClusterView
	bus       UpdateBus
	cfg       Config
	nodeProxy raftcluster.ProxyConfig
	logger    *slog.Logger

	mu       sync.Mutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	failOnce sync.Once
}

// NewRunnerFactory returns a botlifecycle.RunnerFactory that builds fresh
// Runner instances sharing cluster/bus/cfg/nodeProxy/logger — Manager calls
// it once per bot each time that bot starts (see botlifecycle.RunnerFactory
// doc). cluster and bus are typically shared across every bot on this node;
// nodeProxy is this node's proxy.* config (config.yaml), the default half
// of the per-bot proxy override.
func NewRunnerFactory(cluster ClusterView, bus UpdateBus, cfg Config, nodeProxy raftcluster.ProxyConfig, logger *slog.Logger) botlifecycle.RunnerFactory {
	if logger == nil {
		logger = slog.Default()
	}
	return func() botlifecycle.BotRunner {
		return &Runner{cluster: cluster, bus: bus, cfg: cfg, nodeProxy: nodeProxy, logger: logger}
	}
}

// Start implements botlifecycle.BotRunner. Per its docstring: this only
// spawns the two goroutines and returns — no Telegram call happens before
// Start returns, so any failure Telegram itself reports (bad token, bot
// blocked from a chat, …) is necessarily "discovered after Start
// succeeded" and handled by reportBroken from inside a loop, never by
// returning an error here. The only thing Start itself can reject is a
// structurally malformed token.
func (r *Runner) Start(ctx context.Context, bot raftcluster.Bot) error {
	if !tokenPattern.MatchString(bot.Token) {
		return fmt.Errorf("telegram: bot %s: malformed token", bot.ID)
	}

	effProxy := EffectiveProxy(r.nodeProxy, bot.Proxy)
	httpClient, err := newHTTPClient(effProxy)
	if err != nil {
		return fmt.Errorf("telegram: bot %s: %w", bot.ID, err)
	}
	api := NewClient(bot.Token, httpClient, r.cfg.apiBaseURL())

	runCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.mu.Unlock()

	r.logger.Info("bot runner starting",
		"event", "telegram.runner_start", "bot_id", bot.ID, "proxy_enabled", effProxy.Enabled)

	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		r.pollLoop(runCtx, bot, api)
	}()
	go func() {
		defer r.wg.Done()
		r.sendLoop(runCtx, bot, api)
	}()

	return nil
}

// Stop implements botlifecycle.BotRunner: cancels the internal context
// (idempotent — safe even if reportBroken already cancelled it) and waits
// for both loops to exit. Manager guarantees at most one call per Start
// (botlifecycle.BotRunner doc); reportBroken's own cancel makes an
// independent second cancel from Stop harmless either way.
func (r *Runner) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
}

// reportBroken classifies a failure discovered by either loop and, exactly
// once per Runner instance, drives it back into raftcluster as the
// enabled→broken transition, then stops both loops by cancelling the
// internal context. This is exactly the mechanism botlifecycle.BotRunner's
// Start docstring describes: "не через возврат ошибки Start, а через
// SetBotState после того, как Start уже вернулся" — Manager learns about it
// the same way it learns about any other state change, via
// ClusterView.SubscribeApplied (see botlifecycle/manager.go), no special
// signalling needed from this package.
func (r *Runner) reportBroken(bot raftcluster.Bot, class raftcluster.FailureClass, reason string) {
	r.failOnce.Do(func() {
		r.logger.Error("bot marked broken",
			"event", "telegram.bot_broken", "bot_id", bot.ID, "failure_class", class.String())

		cmd := raftcluster.Command{
			Type: raftcluster.CommandSetBotState,
			SetBotState: &raftcluster.SetBotStateCommand{
				ID:            bot.ID,
				State:         raftcluster.BotStateBroken,
				FailureClass:  class,
				FailureReason: reason,
				UpdatedAt:     time.Now().UTC(),
			},
		}
		if _, err := r.cluster.Apply(cmd, r.cfg.requestTimeout()); err != nil {
			// Например, узел только что потерял лидерство — не критично:
			// Manager на новом лидере сам остановит раннер здесь и, если
			// нужно, запустит новый там; либо это гонка с другим источником
			// изменения состояния бота, и конечный результат тот же.
			r.logger.Warn("failed to report broken bot state",
				"event", "telegram.report_broken_failed", "bot_id", bot.ID, "error", err.Error())
		}

		r.mu.Lock()
		cancel := r.cancel
		r.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})
}

var _ botlifecycle.BotRunner = (*Runner)(nil)
