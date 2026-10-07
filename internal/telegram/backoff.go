package telegram

import (
	"context"
	"errors"
	"time"
)

// backoff is a small exponential backoff with a cap, used by pollLoop for
// FailureClassNode/Unspecified retries (a node-level failure is handled by
// one runner with a local backoff and retry). Not jittered: this is a single bot's own
// retry loop against one upstream, not a fleet stampeding a shared
// resource, so the thundering-herd concern jitter usually addresses does
// not apply here.
type backoff struct {
	attempt int
	base    time.Duration
	max     time.Duration
}

func newBackoff() *backoff {
	return &backoff{base: time.Second, max: time.Minute}
}

func (b *backoff) next() time.Duration {
	d := b.base << uint(b.attempt)
	if d <= 0 || d > b.max { // shifted past the cap, or overflowed to negative
		d = b.max
	}
	b.attempt++
	return d
}

func (b *backoff) reset() {
	b.attempt = 0
}

// sleepCtx sleeps for d or until ctx is cancelled, whichever comes first.
// Returns false when ctx was cancelled (caller should stop), true when the
// full duration elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// asAPIError extracts an *APIError from err, if it is (or wraps) one.
func asAPIError(err error) (*APIError, bool) {
	var apiErr *APIError
	ok := errors.As(err, &apiErr)
	return apiErr, ok
}
