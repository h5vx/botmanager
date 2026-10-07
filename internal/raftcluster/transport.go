package raftcluster

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// TransportTLS secures the Raft transport with mutual TLS. Server is used
// for the listener and must require and verify client certificates; Client
// is used to dial peers and must present this node's certificate. Both
// typically share one CA, the same one that secures the gRPC API.
type TransportTLS struct {
	Server *tls.Config
	Client *tls.Config
}

// tlsStreamLayer implements raft.StreamLayer over TLS: every Raft
// connection between nodes is encrypted and both ends are authenticated by
// certificates signed by the cluster CA.
type tlsStreamLayer struct {
	net.Listener
	advertise net.Addr
	client    *tls.Config
}

func (l *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	cfg := l.client.Clone()
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(string(address))
		if err != nil {
			return nil, fmt.Errorf("raftcluster: dial %q: %w", address, err)
		}
		cfg.ServerName = host
	}
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), cfg)
}

func (l *tlsStreamLayer) Addr() net.Addr { return l.advertise }

func buildTransport(cfg Config) (raft.Transport, error) {
	if cfg.Deps.Transport != nil {
		return cfg.Deps.Transport, nil
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raftcluster: BindAddr is required when Deps.Transport is not set")
	}
	advertiseAddr := cfg.AdvertiseAddr
	if advertiseAddr == "" {
		advertiseAddr = cfg.BindAddr
	}
	advertise, err := net.ResolveTCPAddr("tcp", advertiseAddr)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: resolve advertise addr %q: %w", advertiseAddr, err)
	}

	if cfg.TLS == nil {
		t, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 3, 10*time.Second, io.Discard)
		if err != nil {
			return nil, fmt.Errorf("raftcluster: tcp transport: %w", err)
		}
		return t, nil
	}

	if cfg.TLS.Server == nil || cfg.TLS.Client == nil {
		return nil, fmt.Errorf("raftcluster: TLS requires both Server and Client configs")
	}
	if advertise.IP == nil || advertise.IP.IsUnspecified() {
		return nil, fmt.Errorf("raftcluster: advertise addr %q is not advertisable; set it explicitly", advertiseAddr)
	}
	lis, err := tls.Listen("tcp", cfg.BindAddr, cfg.TLS.Server)
	if err != nil {
		return nil, fmt.Errorf("raftcluster: tls listen %q: %w", cfg.BindAddr, err)
	}
	layer := &tlsStreamLayer{Listener: lis, advertise: advertise, client: cfg.TLS.Client}
	return raft.NewNetworkTransport(layer, 3, 10*time.Second, io.Discard), nil
}
