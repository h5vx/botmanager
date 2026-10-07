package telegram

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/h5vx/botmanager/internal/raftcluster"
)

func TestEffectiveProxy(t *testing.T) {
	nodeDefault := raftcluster.ProxyConfig{Enabled: true, Address: "socks5h://node-proxy:1080"}

	cases := []struct {
		name     string
		override *raftcluster.ProxyConfig
		want     raftcluster.ProxyConfig
	}{
		{"nil override uses node default", nil, nodeDefault},
		{"explicit disable overrides node default", &raftcluster.ProxyConfig{Enabled: false}, raftcluster.ProxyConfig{Enabled: false}},
		{
			"explicit bot proxy overrides node default",
			&raftcluster.ProxyConfig{Enabled: true, Address: "socks5h://bot-proxy:1080"},
			raftcluster.ProxyConfig{Enabled: true, Address: "socks5h://bot-proxy:1080"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EffectiveProxy(nodeDefault, c.override)
			if got != c.want {
				t.Errorf("EffectiveProxy() = %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestNewHTTPClient_NoProxy(t *testing.T) {
	client, err := newHTTPClient(raftcluster.ProxyConfig{Enabled: false})
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport is %T, want *http.Transport", client.Transport)
	}
	if transport.DialContext != nil {
		t.Error("DialContext should be nil (default dialer) when proxy is disabled")
	}
}

func TestNewHTTPClient_RejectsNonSocks5Scheme(t *testing.T) {
	_, err := newHTTPClient(raftcluster.ProxyConfig{Enabled: true, Address: "http://not-socks:8080"})
	if err == nil {
		t.Fatal("expected an error for a non-socks5 proxy scheme")
	}
}

// TestNewHTTPClient_DialsThroughSOCKS5WithUnresolvedHost is the
// socks5h test: it stands up a minimal SOCKS5 server (raw protocol, no
// golang.org/x/net dependency on the server side — a from-scratch RFC 1928
// implementation of just enough to observe one CONNECT request) and drives
// an *http.Client built by newHTTPClient at it, requesting a hostname
// (an .invalid TLD, reserved by RFC 2606 to never resolve) that would fail
// immediately if this package ever resolved it locally before dialing.
//
// The fake server asserts the CONNECT request's address type is 0x03
// (domain name, not an IP) and that the domain bytes are exactly the
// hostname requested — proving DNS resolution never happened on this side
// of the tunnel, which is the entire meaning of "socks5h" (name
// resolution on the proxy side, not locally).
func TestNewHTTPClient_DialsThroughSOCKS5WithUnresolvedHost(t *testing.T) {
	const unresolvableHost = "definitely-does-not-resolve.invalid"

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	resultCh := make(chan socks5Observation, 1)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			resultCh <- socks5Observation{err: err}
			return
		}
		defer conn.Close()
		resultCh <- serveSOCKS5Handshake(conn)
	}()

	proxyCfg := raftcluster.ProxyConfig{Enabled: true, Address: "socks5h://" + ln.Addr().String()}
	client, err := newHTTPClient(proxyCfg)
	if err != nil {
		t.Fatalf("newHTTPClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+unresolvableHost+"/probe", nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	// Ошибка неизбежна (наш фейковый сервер не говорит по TLS) — важен сам
	// факт, что дозвон дошёл до SOCKS5-хендшейка, а не до локального
	// резолвинга, которое проверяется через resultCh ниже.
	_, _ = client.Do(req)

	select {
	case res := <-resultCh:
		if res.err != nil {
			t.Fatalf("fake SOCKS5 server: %v", res.err)
		}
		if res.atyp != socks5AddrTypeFQDN {
			t.Fatalf("CONNECT address type = 0x%02x, want 0x03 (domain name) — host was resolved locally", res.atyp)
		}
		if res.domain != unresolvableHost {
			t.Fatalf("CONNECT domain = %q, want %q", res.domain, unresolvableHost)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the fake SOCKS5 server to observe a CONNECT request")
	}
}

const (
	socks5Version        = 0x05
	socks5AddrTypeIPv4   = 0x01
	socks5AddrTypeFQDN   = 0x03
	socks5NoAuth         = 0x00
	socks5CmdConnect     = 0x01
	socks5ReplySucceeded = 0x00
)

type socks5Observation struct {
	atyp   byte
	domain string
	err    error
}

// serveSOCKS5Handshake speaks just enough RFC 1928 server-side to accept
// one no-auth CONNECT request and report what address type/host the client
// asked for. It always replies "succeeded" with a dummy bound address so
// the client's SOCKS5 library considers the tunnel open (what happens next
// on that tunnel is irrelevant to this test).
func serveSOCKS5Handshake(conn net.Conn) socks5Observation {
	conn.SetDeadline(time.Now().Add(3 * time.Second))

	// Greeting: VER, NMETHODS, METHODS...
	hdr := make([]byte, 2)
	if _, err := readFull(conn, hdr); err != nil {
		return socks5Observation{err: err}
	}
	nMethods := int(hdr[1])
	if _, err := readFull(conn, make([]byte, nMethods)); err != nil {
		return socks5Observation{err: err}
	}
	if _, err := conn.Write([]byte{socks5Version, socks5NoAuth}); err != nil {
		return socks5Observation{err: err}
	}

	// Request: VER, CMD, RSV, ATYP, ...
	req := make([]byte, 4)
	if _, err := readFull(conn, req); err != nil {
		return socks5Observation{err: err}
	}
	atyp := req[3]

	var domain string
	switch atyp {
	case socks5AddrTypeFQDN:
		lenBuf := make([]byte, 1)
		if _, err := readFull(conn, lenBuf); err != nil {
			return socks5Observation{err: err}
		}
		nameBuf := make([]byte, lenBuf[0])
		if _, err := readFull(conn, nameBuf); err != nil {
			return socks5Observation{err: err}
		}
		domain = string(nameBuf)
		if _, err := readFull(conn, make([]byte, 2)); err != nil { // port
			return socks5Observation{err: err}
		}
	case socks5AddrTypeIPv4:
		if _, err := readFull(conn, make([]byte, 4+2)); err != nil { // IPv4 + port
			return socks5Observation{err: err}
		}
	default:
		if _, err := readFull(conn, make([]byte, 16+2)); err != nil { // IPv6 + port
			return socks5Observation{err: err}
		}
	}

	reply := []byte{socks5Version, socks5ReplySucceeded, 0x00, socks5AddrTypeIPv4, 0, 0, 0, 0, 0, 0}
	if _, err := conn.Write(reply); err != nil {
		return socks5Observation{err: err}
	}

	return socks5Observation{atyp: atyp, domain: domain}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
