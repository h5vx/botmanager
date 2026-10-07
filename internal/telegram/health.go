package telegram

// HealthReporter receives, per bot, whether this node currently reaches
// Telegram — the input for deciding that the node itself (its network or
// proxy) is broken rather than a single bot (internal/failover). Any
// response from Telegram, including an error response such as 429 or 403,
// counts as reachable; only FailureClassNode (no response at all) counts
// as unreachable.
type HealthReporter interface {
	TelegramReachable(botID string)
	TelegramUnreachable(botID string, err error)
	// BotStopped is called when the bot's runner stops on this node, so a
	// bot that is no longer running here stops counting either way.
	BotStopped(botID string)
}

// observe reports the outcome of one Telegram call to cfg.Health, if set.
func (r *Runner) observe(botID string, err error) {
	h := r.cfg.Health
	if h == nil {
		return
	}
	if err == nil {
		h.TelegramReachable(botID)
		return
	}
	if _, ok := asAPIError(err); ok {
		h.TelegramReachable(botID)
		return
	}
	h.TelegramUnreachable(botID, err)
}
