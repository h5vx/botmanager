// Package webui is botmanager's optional administration web interface: a
// small JSON API plus static pages embedded into the binary.
//
// The API is a thin layer over botmanager's own gRPC services. Cluster-wide
// calls go to this node's gRPC endpoint (Config.Local), so writes reach the
// leader through the usual forwarding; per-node calls (statistics, Telegram
// checks) go to the node in question (Config.Dial with the address from the
// node registry).
//
// Access requires a login (users with bcrypt password hashes in the
// configuration). Sessions are signed cookies (HttpOnly, SameSite=Strict,
// Secure over HTTPS); state-changing requests additionally need the
// X-Botmanager-Request header, and failed logins are rate limited per
// client address.
package webui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

//go:embed static
var staticFiles embed.FS

// Config configures the web interface.
type Config struct {
	// Users maps usernames to bcrypt password hashes.
	Users map[string][]byte
	// SessionKey signs session cookies; the same key on every node makes a
	// session valid cluster-wide.
	SessionKey []byte
	// SessionTTL is the session lifetime (default 12h).
	SessionTTL time.Duration
	// SecureCookies marks cookies Secure; set it when serving HTTPS.
	SecureCookies bool
	// Local is a connection to this node's gRPC API.
	Local grpc.ClientConnInterface
	// LocalNodeID is this node's ID: calls for it use Local.
	LocalNodeID string
	// Dial returns a connection to another node's gRPC API.
	Dial func(addr string) (grpc.ClientConnInterface, error)
	// CallTimeout bounds one backend call (default 10s).
	CallTimeout time.Duration
	Logger      *slog.Logger
}

// Server serves the web interface.
type Server struct {
	cfg     Config
	limiter *loginLimiter
	now     func() time.Time
	mux     *http.ServeMux
}

// New builds a Server.
func New(cfg Config) (*Server, error) {
	if len(cfg.Users) == 0 {
		return nil, errors.New("webui: at least one user is required")
	}
	if len(cfg.SessionKey) < 32 {
		return nil, errors.New("webui: session key must be at least 32 bytes")
	}
	if cfg.Local == nil || cfg.Dial == nil {
		return nil, errors.New("webui: Local and Dial are required")
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 10 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	s := &Server{cfg: cfg, limiter: newLoginLimiter(5, 5*time.Minute), now: time.Now}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	mux := http.NewServeMux()
	static, _ := fs.Sub(staticFiles, "static")
	mux.Handle("GET /", http.FileServerFS(static))

	mux.HandleFunc("POST /api/login", s.handleLogin)
	mux.HandleFunc("POST /api/logout", s.handleLogout)
	mux.Handle("GET /api/session", s.auth(s.handleSession))

	mux.Handle("GET /api/overview", s.auth(s.handleOverview))
	mux.Handle("POST /api/leader", s.auth(s.handleTransferLeadership))
	mux.Handle("POST /api/nodes", s.auth(s.handleAddNode))
	mux.Handle("DELETE /api/nodes/{id}", s.auth(s.handleRemoveNode))
	mux.Handle("POST /api/ping", s.auth(s.handlePing))

	mux.Handle("POST /api/bots", s.auth(s.handleCreateBot))
	mux.Handle("POST /api/bots/{id}/state", s.auth(s.handleSetBotState))
	mux.Handle("DELETE /api/bots/{id}", s.auth(s.handleDeleteBot))
	mux.Handle("GET /api/bots/{id}/messages", s.auth(s.handleBotMessages))
	s.mux = mux
}

// ServeHTTP implements http.Handler: security headers for everything, then
// routing.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Referrer-Policy", "no-referrer")
	if strings.HasPrefix(r.URL.Path, "/api/") {
		h.Set("Cache-Control", "no-store")
	}
	if s.cfg.SecureCookies {
		h.Set("Strict-Transport-Security", "max-age=31536000")
	}
	s.mux.ServeHTTP(w, r)
}

type authedHandler func(w http.ResponseWriter, r *http.Request, user string)

// auth requires a valid session and, for state-changing methods, the CSRF
// header.
func (s *Server) auth(next authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := s.sessionUser(r)
		if !ok {
			writeError(w, http.StatusUnauthorized, "login required")
			return
		}
		if r.Method != http.MethodGet && r.Header.Get(csrfHeader) == "" {
			writeError(w, http.StatusForbidden, "missing "+csrfHeader+" header")
			return
		}
		next(w, r, user)
	})
}

// ---- auth handlers ----

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get(csrfHeader) == "" {
		writeError(w, http.StatusForbidden, "missing "+csrfHeader+" header")
		return
	}
	key := clientKey(r)
	if s.limiter.blocked(key, s.now()) {
		writeError(w, http.StatusTooManyRequests, "too many failed logins, try again in a few minutes")
		return
	}
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.checkPassword(req.Username, req.Password) {
		s.limiter.fail(key, s.now())
		s.cfg.Logger.Warn("web login failed", "event", "webui.login_failed", "client", key)
		writeError(w, http.StatusUnauthorized, "invalid username or password")
		return
	}
	s.limiter.reset(key)
	value, exp := s.newSession(req.Username)
	s.setSessionCookie(w, value, exp)
	s.cfg.Logger.Info("web login", "event", "webui.login", "user", req.Username, "client", key)
	writeJSON(w, http.StatusOK, map[string]any{"username": req.Username})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSession(w http.ResponseWriter, _ *http.Request, user string) {
	writeJSON(w, http.StatusOK, map[string]any{"username": user, "node_id": s.cfg.LocalNodeID})
}

// ---- helpers ----

func (s *Server) ctx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.cfg.CallTimeout)
}

func (s *Server) audit(user, action string, args ...any) {
	s.cfg.Logger.Info("web action", append([]any{"event", "webui.action", "user", user, "action", action}, args...)...)
}

const maxBody = 64 << 10

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errors.New("invalid JSON body: " + err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

var marshal = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}

// protoJSON renders a protobuf message as JSON with snake_case field names
// and enum names, for embedding into a response.
func protoJSON(m proto.Message) json.RawMessage {
	b, err := marshal.Marshal(m)
	if err != nil {
		return json.RawMessage(`null`)
	}
	return b
}

// writeGRPCError maps a gRPC error to an HTTP status with its message.
func writeGRPCError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	code := http.StatusBadGateway
	switch st.Code() {
	case codes.InvalidArgument:
		code = http.StatusBadRequest
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.AlreadyExists, codes.FailedPrecondition:
		code = http.StatusConflict
	case codes.Unavailable, codes.DeadlineExceeded:
		code = http.StatusServiceUnavailable
	case codes.ResourceExhausted:
		code = http.StatusTooManyRequests
	}
	writeError(w, code, st.Message())
}
