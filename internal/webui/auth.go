package webui

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookie = "bm_session"
	// csrfHeader must be present on every state-changing request. A
	// cross-site form or link cannot set custom headers, and cross-origin
	// fetch with a custom header needs a CORS preflight this server never
	// approves — together with SameSite=Strict cookies this blocks CSRF.
	csrfHeader = "X-Botmanager-Request"
)

// HashPassword returns the bcrypt hash to put into web.users.
func HashPassword(password string) (string, error) {
	if len(password) < 8 {
		return "", errors.New("password must be at least 8 characters")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(h), nil
}

// dummyHash is compared against when the username is unknown, so a login
// attempt takes the same time whether or not the user exists.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("botmanager-dummy-password"), bcrypt.DefaultCost)

// checkPassword reports whether password matches username's hash.
func (s *Server) checkPassword(username, password string) bool {
	hash, ok := s.cfg.Users[username]
	if !ok {
		_ = bcrypt.CompareHashAndPassword(dummyHash, []byte(password))
		return false
	}
	return bcrypt.CompareHashAndPassword(hash, []byte(password)) == nil
}

// Sessions are stateless signed cookies: "base64(username|expiry).base64(mac)".
// Every node derives the same key from the shared token key, so a session
// opened on one node is valid on the others (e.g. behind a load balancer).

func (s *Server) sign(payload string) string {
	mac := hmac.New(sha256.New, s.cfg.SessionKey)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (s *Server) newSession(username string) (string, time.Time) {
	exp := s.now().Add(s.cfg.SessionTTL)
	payload := username + "|" + strconv.FormatInt(exp.Unix(), 10)
	enc := base64.RawURLEncoding.EncodeToString([]byte(payload))
	return enc + "." + s.sign(payload), exp
}

// sessionUser returns the user of a valid, unexpired session cookie whose
// user still exists in the configuration.
func (s *Server) sessionUser(r *http.Request) (string, bool) {
	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return "", false
	}
	enc, sig, ok := strings.Cut(c.Value, ".")
	if !ok {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		return "", false
	}
	payload := string(raw)
	if !hmac.Equal([]byte(sig), []byte(s.sign(payload))) {
		return "", false
	}
	user, expRaw, ok := strings.Cut(payload, "|")
	if !ok {
		return "", false
	}
	exp, err := strconv.ParseInt(expRaw, 10, 64)
	if err != nil || s.now().Unix() >= exp {
		return "", false
	}
	if _, exists := s.cfg.Users[user]; !exists {
		return "", false
	}
	return user, true
}

func (s *Server) setSessionCookie(w http.ResponseWriter, value string, exp time.Time) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    value,
		Path:     "/",
		Expires:  exp,
		HttpOnly: true,
		Secure:   s.cfg.SecureCookies,
		SameSite: http.SameSiteStrictMode,
	})
}

func (s *Server) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.cfg.SecureCookies,
		SameSite: http.SameSiteStrictMode,
	})
}

// loginLimiter slows down password guessing: after maxFailures failed
// logins from one client address within window, further attempts are
// refused until the window passes.
type loginLimiter struct {
	mu       sync.Mutex
	failures map[string][]time.Time
	max      int
	window   time.Duration
}

func newLoginLimiter(max int, window time.Duration) *loginLimiter {
	return &loginLimiter{failures: make(map[string][]time.Time), max: max, window: window}
}

func (l *loginLimiter) recent(key string, now time.Time) []time.Time {
	kept := l.failures[key][:0]
	for _, t := range l.failures[key] {
		if now.Sub(t) < l.window {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.failures, key)
		return nil
	}
	l.failures[key] = kept
	return kept
}

func (l *loginLimiter) blocked(key string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.recent(key, now)) >= l.max
}

func (l *loginLimiter) fail(key string, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failures[key] = append(l.recent(key, now), now)
}

func (l *loginLimiter) reset(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, key)
}

// clientKey identifies the client for rate limiting: the remote IP. A
// reverse proxy in front makes every client look the same, which only makes
// the limiter stricter.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
