// Package client is the Go client for a botmanager cluster.
//
// A Client talks to any number of botmanager nodes. Any node accepts any
// call: writes received by a follower are forwarded to the Raft leader by
// the node itself, so the client never needs to know which node leads.
// Calls are spread over the healthy endpoints, and calls that are safe to
// repeat are retried automatically while the cluster is unavailable (for
// example during a leader election), see Config.RetryTimeout. Subscribe reconnects on its own and
// resumes exactly where it stopped.
//
//	tlsCfg, err := client.LoadTLS("ca.pem", "my-service.pem", "my-service-key.pem")
//	c, err := client.New(client.Config{
//		Endpoints: []string{"10.0.0.1:9090", "10.0.0.2:9090", "10.0.0.3:9090"},
//		TLS:       tlsCfg,
//	})
//	defer c.Close()
//	_, err = c.Messaging.Send(ctx, &botmanagerpb.SendRequest{IdempotencyKey: "order-42-paid", BotId: botID, ChatId: chatID, Text: "Paid"})
package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// Config configures a Client.
type Config struct {
	// Endpoints are host:port gRPC addresses of botmanager nodes. One is
	// enough; listing every node lets the client keep working when some of
	// them are down.
	Endpoints []string
	// TLS is the client-side mutual TLS configuration (see LoadTLS). Nil
	// connects without TLS, which only works against a node running in
	// insecure development mode.
	TLS *tls.Config
	// RetryTimeout bounds how long a call that is safe to repeat keeps
	// being retried while the cluster answers UNAVAILABLE (node down,
	// leader election in progress). Zero means 30s; the call's own context
	// deadline, if shorter, wins. Negative disables retries.
	RetryTimeout time.Duration
	// DialOptions are appended to the client's own dial options.
	DialOptions []grpc.DialOption
}

// Client is a connection to a botmanager cluster. The typed service clients
// are safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn

	BotAdmin    botmanagerpb.BotAdminClient
	Messaging   botmanagerpb.MessagingClient
	Maintenance botmanagerpb.MaintenanceClient
}

// serviceConfig spreads calls over all healthy endpoints.
const serviceConfig = `{"loadBalancingConfig": [{"round_robin": {}}]}`

var schemeCounter atomic.Uint64

// New connects to the cluster. It does not block: connection problems
// surface on the first call.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("botmanager client: at least one endpoint is required")
	}
	addrs := make([]resolver.Address, 0, len(cfg.Endpoints))
	for _, ep := range cfg.Endpoints {
		host, _, err := net.SplitHostPort(ep)
		if err != nil {
			return nil, fmt.Errorf("botmanager client: endpoint %q: %w", ep, err)
		}
		// ServerName — имя для проверки сертификата именно этого узла: у
		// разных узлов разные хосты, а цель соединения одна на все.
		addrs = append(addrs, resolver.Address{Addr: ep, ServerName: host})
	}

	// Собственная схема на каждый клиент: manual-резолвер регистрируется
	// через WithResolvers только для этого соединения.
	r := manual.NewBuilderWithScheme(fmt.Sprintf("botmanager-%d", schemeCounter.Add(1)))
	r.InitialState(resolver.State{Addresses: addrs})

	creds := insecure.NewCredentials()
	if cfg.TLS != nil {
		creds = credentials.NewTLS(cfg.TLS)
	}
	retryTimeout := cfg.RetryTimeout
	if retryTimeout == 0 {
		retryTimeout = 30 * time.Second
	}
	opts := []grpc.DialOption{
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(serviceConfig),
	}
	if retryTimeout > 0 {
		opts = append(opts, grpc.WithChainUnaryInterceptor(retryInterceptor(retryTimeout)))
	}
	opts = append(opts, cfg.DialOptions...)

	conn, err := grpc.NewClient(r.Scheme()+":///botmanager", opts...)
	if err != nil {
		return nil, fmt.Errorf("botmanager client: %w", err)
	}
	return &Client{
		conn:        conn,
		BotAdmin:    botmanagerpb.NewBotAdminClient(conn),
		Messaging:   botmanagerpb.NewMessagingClient(conn),
		Maintenance: botmanagerpb.NewMaintenanceClient(conn),
	}, nil
}

// Conn returns the underlying connection, e.g. for health checks.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// LoadTLS builds a client mutual-TLS configuration from PEM files: the
// cluster CA and this client's certificate and key, issued by that CA.
func LoadTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("botmanager client: read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("botmanager client: no CA certificates in " + caFile)
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("botmanager client: load key pair: %w", err)
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		RootCAs:      pool,
		Certificates: []tls.Certificate{cert},
	}, nil
}
