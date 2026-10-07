package main

import (
	"bufio"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/h5vx/botmanager/internal/config"
	"github.com/h5vx/botmanager/internal/rpcserver"
	"github.com/h5vx/botmanager/internal/webui"
)

// runHashPassword reads a password from stdin (first line) and prints the
// bcrypt hash to put into web.users.
func runHashPassword(in io.Reader, out io.Writer) error {
	line, err := bufio.NewReader(in).ReadString('\n')
	if err != nil && err != io.EOF {
		return err
	}
	hash, err := webui.HashPassword(strings.TrimRight(line, "\r\n"))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, hash)
	return err
}

// newWebServer builds the optional admin web interface. It talks to this
// node's gRPC API (selfAddr) and to peers through the forwarder's
// connection pool, so it needs nothing beyond what the node already has.
// tlsServing reports whether the server must be started with TLS.
func newWebServer(cfg config.WebConfig, nodeID, selfAddr string, sec securitySetup, forwarder *rpcserver.Forwarder, logger *slog.Logger) (srv *http.Server, tlsServing bool, err error) {
	users := make(map[string][]byte, len(cfg.Users))
	for _, u := range cfg.Users {
		users[u.Username] = []byte(u.PasswordHash)
	}

	// Ключ сессий выводится из общего ключа токенов — сессия, открытая на
	// одном узле, действительна на всех. Без ключа (insecure) — случайный,
	// сессии живут до перезапуска процесса и только на этом узле.
	var sessionKey []byte
	if sec.tokenKey != nil {
		mac := hmac.New(sha256.New, sec.tokenKey)
		mac.Write([]byte("botmanager webui session v1"))
		sessionKey = mac.Sum(nil)
	} else {
		sessionKey = make([]byte, 32)
		if _, err := rand.Read(sessionKey); err != nil {
			return nil, false, err
		}
	}

	tlsServing = cfg.TLS && sec.mtls != nil
	if cfg.TLS && sec.mtls == nil {
		logger.Warn("web.tls is set but the node runs in insecure mode: serving the web interface over plain HTTP",
			"event", "botmanager.web_insecure")
	}

	local, err := forwarder.Conn(selfAddr)
	if err != nil {
		return nil, false, err
	}
	handler, err := webui.New(webui.Config{
		Users:         users,
		SessionKey:    sessionKey,
		SessionTTL:    time.Duration(cfg.SessionTTLMinutes) * time.Minute,
		SecureCookies: tlsServing,
		Local:         local,
		LocalNodeID:   nodeID,
		Dial:          func(addr string) (grpc.ClientConnInterface, error) { return forwarder.Conn(addr) },
		Logger:        logger,
	})
	if err != nil {
		return nil, false, err
	}

	srv = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	if tlsServing {
		// Только сертификат сервера: браузер клиентский сертификат не
		// предъявляет, вход — по логину и паролю.
		srv.TLSConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: sec.mtls.Server.Certificates,
		}
	}
	return srv, tlsServing, nil
}

// hashPasswordMain handles `botmanager -hash-password`.
func hashPasswordMain() {
	fmt.Fprintln(os.Stderr, "Password (read from stdin, first line):")
	if err := runHashPassword(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "botmanager:", err)
		os.Exit(1)
	}
}
