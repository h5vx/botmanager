package telegram

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// assertNodeClass — сетевые/транспортные ошибки (никакого ответа от
// Telegram вообще не пришло) классифицируются как FailureClassNode
// (проблема узла: сетевые ошибки, таймауты, DNS не резолвится).
func assertNodeClass(t *testing.T, err error) {
	t.Helper()
	got := ClassifyFailure(err)
	if got != raftcluster.FailureClassNode {
		t.Errorf("ClassifyFailure(%v) = %v, want Node", err, got)
	}
}

func TestClassifyFailure_Node_TransportErrors(t *testing.T) {
	cases := []error{
		errors.New("dial tcp: lookup api.telegram.org: no such host"),
		errors.New("dial tcp 127.0.0.1:1080: connect: connection refused"),
		context.DeadlineExceeded,
		errors.New("proxyconnect tcp: EOF"),
	}
	for _, err := range cases {
		assertNodeClass(t, err)
	}
}

func TestClassifyFailure_Bot_InvalidToken401(t *testing.T) {
	err := &APIError{HTTPStatus: http.StatusUnauthorized, ErrorCode: 401, Description: "Unauthorized"}
	if got := ClassifyFailure(err); got != raftcluster.FailureClassBot {
		t.Errorf("ClassifyFailure(401) = %v, want Bot", got)
	}
}

func TestClassifyFailure_Bot_Forbidden_KickedOrNoRights(t *testing.T) {
	cases := []string{
		"Forbidden: bot was kicked from the group chat",
		"Forbidden: bot is not a member of the supergroup chat",
		"Forbidden: have no rights to send a message",
		"Forbidden: CHAT_ADMIN_REQUIRED",
	}
	for _, desc := range cases {
		err := &APIError{HTTPStatus: http.StatusForbidden, ErrorCode: 403, Description: desc}
		if got := ClassifyFailure(err); got != raftcluster.FailureClassBot {
			t.Errorf("ClassifyFailure(403 %q) = %v, want Bot", desc, got)
		}
	}
}

func TestClassifyFailure_Recipient_BlockedByUser(t *testing.T) {
	cases := []string{
		"Forbidden: bot was blocked by the user",
		"Forbidden: user is deactivated",
	}
	for _, desc := range cases {
		err := &APIError{HTTPStatus: http.StatusForbidden, ErrorCode: 403, Description: desc}
		if got := ClassifyFailure(err); got != raftcluster.FailureClassRecipient {
			t.Errorf("ClassifyFailure(403 %q) = %v, want Recipient", desc, got)
		}
	}
}

func TestClassifyFailure_RateLimit_429WithBodyRetryAfter(t *testing.T) {
	err := &APIError{
		HTTPStatus:  http.StatusTooManyRequests,
		ErrorCode:   429,
		Description: "Too Many Requests: retry after 5",
		RetryAfter:  5 * time.Second,
	}
	if got := ClassifyFailure(err); got != raftcluster.FailureClassRateLimit {
		t.Errorf("ClassifyFailure(429) = %v, want RateLimit", got)
	}
}

func TestClassifyFailure_RateLimit_RetryAfterWithoutStatus429(t *testing.T) {
	// Not every deployment necessarily reports status 429 for a rate-limit
	// situation (e.g. behind a reverse proxy that folds it into 200 with the
	// Telegram envelope's own error_code) — RetryAfter alone must still
	// classify as RateLimit regardless of HTTPStatus.
	err := &APIError{HTTPStatus: http.StatusOK, ErrorCode: 429, Description: "retry_after present", RetryAfter: 2 * time.Second}
	if got := ClassifyFailure(err); got != raftcluster.FailureClassRateLimit {
		t.Errorf("ClassifyFailure(retry_after, no 429 status) = %v, want RateLimit", got)
	}
}

func TestClassifyFailure_Unspecified_GenericBadRequest(t *testing.T) {
	err := &APIError{HTTPStatus: http.StatusBadRequest, ErrorCode: 400, Description: "Bad Request: chat not found"}
	if got := ClassifyFailure(err); got != raftcluster.FailureClassUnspecified {
		t.Errorf("ClassifyFailure(400) = %v, want Unspecified", got)
	}
}

func TestClassifyFailure_WrapsAPIError(t *testing.T) {
	// ClassifyFailure must use errors.As, not a direct type assertion, so
	// that an *APIError wrapped by client.go's own error formatting (which
	// none of the current client.go paths do, but a future caller's fmt.Errorf
	// wrapping might) is still recognized.
	inner := &APIError{HTTPStatus: http.StatusUnauthorized, ErrorCode: 401, Description: "Unauthorized"}
	wrapped := &wrappedErr{inner: inner}
	if got := ClassifyFailure(wrapped); got != raftcluster.FailureClassBot {
		t.Errorf("ClassifyFailure(wrapped 401) = %v, want Bot", got)
	}
}

type wrappedErr struct{ inner error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }
