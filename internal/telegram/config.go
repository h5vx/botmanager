package telegram

import "time"

// DefaultAPIBaseURL is Telegram Bot API's production base URL.
// httptest-based tests set Config.APIBaseURL to a fake server's URL
// instead.
const DefaultAPIBaseURL = "https://api.telegram.org"

// DefaultMaxSendRetries is used when Config.MaxSendRetries <= 0. The
// FAILED delivery status explicitly includes "retries exhausted", so a
// FailureClassNode/Unspecified send failure must not retry forever.
const DefaultMaxSendRetries = 10

// DefaultSendPollInterval is used when Config.SendPollInterval <= 0 — the
// periodic fallback tick that catches RETRYING messages whose NextRetryAt
// has elapsed, on top of the SubscribeApplied-driven wakeups that catch new
// PENDING messages immediately.
const DefaultSendPollInterval = 2 * time.Second

// Config holds the tunables one Runner needs. Deliberately independent of
// internal/config's Config: this package must stay testable/reusable
// without that dependency, and the conversion (a handful of field copies
// from config.Config.Telegram/config.Config.Proxy) is main.go's job, not
// worth a coupling — see CLAUDE.md.
type Config struct {
	// RequestTimeout bounds every Bot API call except getUpdates (which
	// uses LongPollTimeout instead — see pollLoop).
	RequestTimeout time.Duration
	// LongPollTimeout is the `timeout` parameter passed to getUpdates
	// (seconds, per Telegram's API) — how long Telegram may hold the
	// connection open waiting for a new update before responding empty.
	LongPollTimeout time.Duration
	// APIBaseURL overrides Telegram's host; "" = DefaultAPIBaseURL.
	APIBaseURL string
	// MaxSendRetries caps FailureClassNode/Unspecified send retries before
	// a message is given up as FAILED; <= 0 = DefaultMaxSendRetries.
	MaxSendRetries int
	// SendPollInterval is sendLoop's periodic fallback tick; <= 0 =
	// DefaultSendPollInterval.
	SendPollInterval time.Duration
}

func (c Config) apiBaseURL() string {
	if c.APIBaseURL == "" {
		return DefaultAPIBaseURL
	}
	return c.APIBaseURL
}

func (c Config) maxSendRetries() int {
	if c.MaxSendRetries <= 0 {
		return DefaultMaxSendRetries
	}
	return c.MaxSendRetries
}

func (c Config) sendPollInterval() time.Duration {
	if c.SendPollInterval <= 0 {
		return DefaultSendPollInterval
	}
	return c.SendPollInterval
}

func (c Config) requestTimeout() time.Duration {
	if c.RequestTimeout <= 0 {
		return 10 * time.Second
	}
	return c.RequestTimeout
}

func (c Config) longPollTimeout() time.Duration {
	if c.LongPollTimeout <= 0 {
		return 30 * time.Second
	}
	return c.LongPollTimeout
}
