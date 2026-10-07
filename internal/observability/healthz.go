package observability

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// statusResponse — тело ответа /healthz и /readyz.
type statusResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Reason  string `json:"reason,omitempty"`
}

// HealthzHandler отвечает на "процесс жив". Пока процесс
// способен обработать HTTP-запрос, ответ всегда 200 — эта ручка не должна
// зависеть ни от Raft, ни от Telegram, ни от чего-либо ещё: иначе перезапуск
// живого процесса из-за недоступности внешней зависимости (ровно то, чего
// разделение healthz/readyz позволяет избежать).
func HealthzHandler(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusOK, statusResponse{Status: "ok", Service: service})
	}
}

// ReadyzHandler отвечает на "зависимости доступны" без реальной проверки —
// всегда 200. Для настоящей проверки зависимостей — ReadyzHandlerFunc
// (её и использует cmd/botmanager/main.go).
func ReadyzHandler(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeStatus(w, http.StatusOK, statusResponse{Status: "ok", Service: service})
	}
}

// ReadyzHandlerFunc is ReadyzHandler with a real dependency check instead of
// always answering 200 — for use once a dependency exists to check.
// ready is called on every request and must return quickly (no blocking
// network calls) and without side effects; it reports whether the service
// can currently do useful work and, when not, a short reason included in
// the response body. cmd/botmanager/main.go uses this once
// internal/raftcluster.Node exists: "ready" means the node has a Raft
// leader (itself or another) — on a single-node cluster that is
// "has this node finished its own first election yet", which briefly is not
// the case right after process start, exactly the situation readyz exists
// to report (readyz, unlike healthz, may legitimately answer 503).
func ReadyzHandlerFunc(service string, ready func() (bool, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ok, reason := ready()
		if !ok {
			writeStatus(w, http.StatusServiceUnavailable, statusResponse{Status: "not_ready", Service: service, Reason: reason})
			return
		}
		writeStatus(w, http.StatusOK, statusResponse{Status: "ok", Service: service})
	}
}

func writeStatus(w http.ResponseWriter, code int, body statusResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// NewHTTPMux собирает HTTP-обработчик наблюдаемости: /healthz, /readyz,
// /metrics. Это отдельный сервер от gRPC —
// слушает свой порт (см. cmd/botmanager/main.go).
func NewHTTPMux(service string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", HealthzHandler(service))
	mux.Handle("/readyz", ReadyzHandler(service))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// NewHTTPMuxWithReadyz is NewHTTPMux with /readyz wired to a real
// dependency check (see ReadyzHandlerFunc) instead of always answering 200.
func NewHTTPMuxWithReadyz(service string, ready func() (bool, string)) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", HealthzHandler(service))
	mux.Handle("/readyz", ReadyzHandlerFunc(service, ready))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
