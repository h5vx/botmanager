package telegram

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"golang.org/x/net/proxy"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

// EffectiveProxy resolves the three-level proxy configuration:
// nodeDefault is this node's proxy.* config (config.yaml `proxy:`, node
// default); botOverride is Bot.Proxy. Per Bot.Proxy's own doc comment in
// raftcluster/types.go, nil means "use nodeDefault"; a non-nil botOverride
// fully replaces it — including an explicit
// &raftcluster.ProxyConfig{Enabled: false}, which means "no proxy for this
// bot regardless of the node default". There is no fourth level: this
// function is the entire resolution rule.
func EffectiveProxy(nodeDefault raftcluster.ProxyConfig, botOverride *raftcluster.ProxyConfig) raftcluster.ProxyConfig {
	if botOverride != nil {
		return *botOverride
	}
	return nodeDefault
}

// newHTTPClient builds an *http.Client for one bot from its effective proxy
// config. When disabled, it is a plain client (with a sane TLS floor).
// When enabled, Transport.DialContext goes through a SOCKS5 dialer built
// from effective.Address.
//
// The requirement — socks5h, DNS resolved on the proxy side, never
// locally — falls out of a simple invariant this function (and every
// caller) upholds: the destination host is never looked up locally before
// dialing. net/http's Transport.DialContext receives the destination as an
// unresolved "host:port" string, and golang.org/x/net/proxy's SOCKS5
// dialer (golang.org/x/net/internal/socks.Dialer.connect) only ever calls
// net.ParseIP on that host — never net.LookupHost/net.Resolver — sending it
// to the proxy as a domain name (SOCKS5 ATYP 0x03) whenever it is not
// already a literal IP, which is precisely what lets the proxy do the DNS
// resolution instead of us. See proxy_test.go for a wire-level test against
// a fake SOCKS5 server, using a hostname that cannot resolve locally, that
// proves this end to end rather than by inspection alone.
//
// No blanket http.Client.Timeout is set here — ordinary calls and
// getUpdates long polling need very different per-request timeouts, so
// every call site supplies its own via context (see client.go's do()).
// NewHTTPClient is the exported form of newHTTPClient, for callers outside
// this package that need to build a one-off *http.Client honoring the
// proxy resolution without going through Runner/NewRunnerFactory — namely
// internal/rpcserver's on-demand telegram.Client construction for
// synchronous RPCs (BotAdmin.GetChat, Messaging.EditMessage/DeleteMessage/
// PinMessage/UnpinMessage/AnswerCallback), which have no persistent
// per-bot Runner to reuse a client from. Behavior is identical to what
// Runner.Start does internally.
func NewHTTPClient(effective raftcluster.ProxyConfig) (*http.Client, error) {
	return newHTTPClient(effective)
}

func newHTTPClient(effective raftcluster.ProxyConfig) (*http.Client, error) {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}

	if effective.Enabled {
		dialer, err := socks5Dialer(effective.Address)
		if err != nil {
			return nil, err
		}
		if cd, ok := dialer.(proxy.ContextDialer); ok {
			transport.DialContext = cd.DialContext
		} else {
			// Ни одна известная реализация proxy.Dialer здесь этого не
			// требует (SOCKS5 реализует ContextDialer, см. proxy_test.go),
			// но контракт proxy.Dialer этого не гарантирует — оставляем
			// корректный, просто менее отзывчивый на отмену контекста,
			// путь на случай будущей замены дилера.
			transport.DialContext = func(_ context.Context, network, addr string) (net.Conn, error) {
				return dialer.Dial(network, addr)
			}
		}
	}

	return &http.Client{Transport: transport}, nil
}

// socks5Dialer parses address (expected scheme "socks5h", "socks5" also
// accepted since golang.org/x/net/proxy's SOCKS5 dialer behaves
// identically either way — the "h" is a naming convention some tools use
// to signal "resolve remotely", not a distinct wire protocol) and builds a
// SOCKS5 dialer for it, forwarding basic auth credentials embedded in the
// URL if present.
func socks5Dialer(address string) (proxy.Dialer, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("telegram: invalid proxy address %q: %w", address, err)
	}
	if u.Scheme != "socks5h" && u.Scheme != "socks5" {
		return nil, fmt.Errorf("telegram: unsupported proxy scheme %q (want socks5h)", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("telegram: proxy address %q has no host", address)
	}

	var auth *proxy.Auth
	if u.User != nil {
		password, _ := u.User.Password()
		auth = &proxy.Auth{User: u.User.Username(), Password: password}
	}
	return proxy.SOCKS5("tcp", u.Host, auth, proxy.Direct)
}
