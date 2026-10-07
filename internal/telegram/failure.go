package telegram

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// APIError represents a Telegram Bot API response that was received but
// signals failure — either envelope.ok == false or a non-2xx HTTP status
// (the two mostly coincide for Telegram, but nothing here assumes it).
// This is distinct from a transport error (no response ever arrived: DNS
// failure, connection refused, proxy unreachable, context deadline during
// the round trip itself) — see ClassifyFailure, which treats "not an
// *APIError" as the Node class precisely because that is what "no response
// arrived" means per the classification table in doc.go.
type APIError struct {
	HTTPStatus  int
	ErrorCode   int
	Description string
	// RetryAfter is Telegram's requested cooldown before retrying, taken
	// from parameters.retry_after in the body or the Retry-After HTTP
	// header (whichever is present — see retryAfterFrom in client.go). 0
	// means neither was present.
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram: api error %d (http %d): %s", e.ErrorCode, e.HTTPStatus, e.Description)
}

// ClassifyFailure implements the four-way failure classification for the outcome
// of one Telegram Bot API call. Call it only when the Client method
// returned a non-nil error.
//
//   - err is not an *APIError at all (net/http never received a response —
//     DNS failure, connection refused, proxy unreachable, context deadline
//     exceeded mid-request) → FailureClassNode. This is exactly the
//     "проблема узла" row: "сетевые ошибки, таймауты, DNS не резолвится".
//   - HTTP 429, or RetryAfter > 0 for any other status (Telegram sometimes
//     attaches retry_after to statuses other than 429) → FailureClassRateLimit.
//   - HTTP 401 → FailureClassBot ("неверный токен").
//   - HTTP 403 whose description matches Telegram's own wording for a
//     specific recipient being unreachable ("bot was blocked by the user",
//     "user is deactivated") → FailureClassRecipient.
//   - HTTP 403 for any other reason (Telegram's wording varies: "bot was
//     kicked from the group chat", "have no rights to send a message",
//     "not enough rights", "CHAT_ADMIN_REQUIRED", …) → FailureClassBot: the
//     bot itself cannot operate in that chat — that is the bot's problem to
//     fix (re-invite, grant rights), not one recipient's.
//   - Any other *APIError (400 Bad Request and similar — e.g. an invalid
//     chat_id, message text too long) → FailureClassUnspecified, since it
//     is none of the four documented buckets. Runner treats Unspecified the
//     same as Node (local backoff retry) as the safe default rather than
//     guessing at a class that is not defined.
func ClassifyFailure(err error) raftcluster.FailureClass {
	if err == nil {
		return raftcluster.FailureClassUnspecified
	}

	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return raftcluster.FailureClassNode
	}

	if apiErr.HTTPStatus == http.StatusTooManyRequests || apiErr.RetryAfter > 0 {
		return raftcluster.FailureClassRateLimit
	}

	switch apiErr.HTTPStatus {
	case http.StatusUnauthorized:
		return raftcluster.FailureClassBot
	case http.StatusForbidden:
		if isRecipientUnreachable(apiErr.Description) {
			return raftcluster.FailureClassRecipient
		}
		return raftcluster.FailureClassBot
	default:
		return raftcluster.FailureClassUnspecified
	}
}

// isRecipientUnreachable matches Telegram's documented wording for a
// per-recipient (not per-bot) delivery problem. Matching is
// case-insensitive substring search — Telegram's exact casing/punctuation
// has drifted across API versions, and description is prose, not a stable
// error code.
func isRecipientUnreachable(description string) bool {
	d := strings.ToLower(description)
	return strings.Contains(d, "blocked by the user") ||
		strings.Contains(d, "user is deactivated")
}
