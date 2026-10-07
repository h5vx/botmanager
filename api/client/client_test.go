package client

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// fakeServer is a scripted botmanager node for client tests.
type fakeServer struct {
	botmanagerpb.UnimplementedMessagingServer
	botmanagerpb.UnimplementedBotAdminServer

	mu       sync.Mutex
	subReqs  []*botmanagerpb.SubscribeRequest
	sessions []func(req *botmanagerpb.SubscribeRequest, stream botmanagerpb.Messaging_SubscribeServer) error

	getBotFailures atomic.Int32
	getBotCalls    atomic.Int32
	createCalls    atomic.Int32
}

func (f *fakeServer) Subscribe(req *botmanagerpb.SubscribeRequest, stream botmanagerpb.Messaging_SubscribeServer) error {
	f.mu.Lock()
	f.subReqs = append(f.subReqs, req)
	n := len(f.subReqs)
	var session func(*botmanagerpb.SubscribeRequest, botmanagerpb.Messaging_SubscribeServer) error
	if n <= len(f.sessions) {
		session = f.sessions[n-1]
	}
	f.mu.Unlock()
	if session == nil {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return session(req, stream)
}

func (f *fakeServer) requests() []*botmanagerpb.SubscribeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*botmanagerpb.SubscribeRequest(nil), f.subReqs...)
}

func (f *fakeServer) GetBot(context.Context, *botmanagerpb.GetBotRequest) (*botmanagerpb.Bot, error) {
	f.getBotCalls.Add(1)
	if f.getBotFailures.Add(-1) >= 0 {
		return nil, status.Error(codes.Unavailable, "leader election in progress")
	}
	return &botmanagerpb.Bot{Id: "bot-1"}, nil
}

func (f *fakeServer) CreateBot(context.Context, *botmanagerpb.CreateBotRequest) (*botmanagerpb.Bot, error) {
	f.createCalls.Add(1)
	return nil, status.Error(codes.Unavailable, "unavailable")
}

func startFake(t *testing.T, f *fakeServer) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	botmanagerpb.RegisterMessagingServer(s, f)
	botmanagerpb.RegisterBotAdminServer(s, f)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

func update(seq uint64) *botmanagerpb.Update {
	return &botmanagerpb.Update{Sequence: seq, Payload: &botmanagerpb.Update_IncomingMessage{IncomingMessage: &botmanagerpb.IncomingMessage{Text: strconv.FormatUint(seq, 10)}}}
}

func sendHeader(stream botmanagerpb.Messaging_SubscribeServer, pos uint64) error {
	return stream.SendHeader(metadata.Pairs(botmanagerpb.SubscribePositionHeader, strconv.FormatUint(pos, 10)))
}

func newTestClient(t *testing.T, endpoints ...string) *Client {
	t.Helper()
	c, err := New(Config{Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestSubscribe_ResumesAfterDisconnectWithoutDuplicates: a broken stream is
// reopened after the last delivered sequence (taken from the position
// header when nothing was delivered yet), and replayed duplicates are
// skipped.
func TestSubscribe_ResumesAfterDisconnectWithoutDuplicates(t *testing.T) {
	f := &fakeServer{}
	f.sessions = []func(*botmanagerpb.SubscribeRequest, botmanagerpb.Messaging_SubscribeServer) error{
		func(_ *botmanagerpb.SubscribeRequest, s botmanagerpb.Messaging_SubscribeServer) error {
			_ = sendHeader(s, 10)
			return status.Error(codes.Unavailable, "node restarting") // ни одного события
		},
		func(_ *botmanagerpb.SubscribeRequest, s botmanagerpb.Messaging_SubscribeServer) error {
			_ = sendHeader(s, 10)
			_ = s.Send(update(11))
			_ = s.Send(update(12))
			return status.Error(codes.Unavailable, "node restarting")
		},
		func(_ *botmanagerpb.SubscribeRequest, s botmanagerpb.Messaging_SubscribeServer) error {
			_ = sendHeader(s, 12)
			_ = s.Send(update(12)) // дубль
			_ = s.Send(update(13))
			<-s.Context().Done()
			return nil
		},
	}
	c := newTestClient(t, startFake(t, f))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []uint64
	var reconnects int
	err := c.Subscribe(ctx, SubscribeOptions{BotIDs: []string{"bot-1"}, MinBackoff: time.Millisecond, OnReconnect: func(error) { reconnects++ }},
		func(u *botmanagerpb.Update) error {
			got = append(got, u.GetSequence())
			if len(got) == 3 {
				cancel()
			}
			return nil
		})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe err = %v", err)
	}
	if len(got) != 3 || got[0] != 11 || got[1] != 12 || got[2] != 13 {
		t.Fatalf("delivered = %v, want [11 12 13]", got)
	}
	reqs := f.requests()
	if reqs[0].AfterSequence != nil {
		t.Fatalf("first request after_sequence = %v, want unset", reqs[0].GetAfterSequence())
	}
	if reqs[1].AfterSequence == nil || reqs[1].GetAfterSequence() != 10 {
		t.Fatalf("second request = %+v, want after_sequence 10 from the position header", reqs[1])
	}
	if reqs[2].GetAfterSequence() != 12 || reqs[2].GetBotIds()[0] != "bot-1" {
		t.Fatalf("third request = %+v", reqs[2])
	}
	if reconnects != 2 {
		t.Fatalf("reconnects = %d, want 2", reconnects)
	}
}

func TestSubscribe_EventsLost(t *testing.T) {
	f := &fakeServer{}
	f.sessions = []func(*botmanagerpb.SubscribeRequest, botmanagerpb.Messaging_SubscribeServer) error{
		func(*botmanagerpb.SubscribeRequest, botmanagerpb.Messaging_SubscribeServer) error {
			return status.Error(codes.OutOfRange, "evicted")
		},
	}
	c := newTestClient(t, startFake(t, f))
	start := uint64(1)
	err := c.Subscribe(context.Background(), SubscribeOptions{AfterSequence: &start}, func(*botmanagerpb.Update) error { return nil })
	if !errors.Is(err, ErrEventsLost) {
		t.Fatalf("err = %v, want ErrEventsLost", err)
	}
}

func TestSubscribe_HandlerErrorStops(t *testing.T) {
	f := &fakeServer{}
	f.sessions = []func(*botmanagerpb.SubscribeRequest, botmanagerpb.Messaging_SubscribeServer) error{
		func(_ *botmanagerpb.SubscribeRequest, s botmanagerpb.Messaging_SubscribeServer) error {
			_ = sendHeader(s, 0)
			_ = s.Send(update(1))
			<-s.Context().Done()
			return nil
		},
	}
	c := newTestClient(t, startFake(t, f))
	stop := errors.New("stop")
	err := c.Subscribe(context.Background(), SubscribeOptions{}, func(*botmanagerpb.Update) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("err = %v, want handler error", err)
	}
}

// TestRetry_SafeCallsOnlyAndEndpointFailover: an UNAVAILABLE read is
// retried transparently, CreateBot is not retried, and a dead endpoint in
// the list does not break calls.
func TestRetry_SafeCallsOnlyAndEndpointFailover(t *testing.T) {
	f := &fakeServer{}
	// Больше, чем пережили бы встроенные ретраи gRPC (5 попыток).
	f.getBotFailures.Store(6)

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	c := newTestClient(t, deadAddr, startFake(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bot, err := c.BotAdmin.GetBot(ctx, &botmanagerpb.GetBotRequest{Id: "bot-1"})
	if err != nil || bot.GetId() != "bot-1" {
		t.Fatalf("GetBot = %v, %v", bot, err)
	}
	if calls := f.getBotCalls.Load(); calls != 7 {
		t.Fatalf("GetBot server calls = %d, want 7 (six retried failures)", calls)
	}

	if _, err := c.BotAdmin.CreateBot(ctx, &botmanagerpb.CreateBotRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateBot err = %v", err)
	}
	if calls := f.createCalls.Load(); calls != 1 {
		t.Fatalf("CreateBot server calls = %d, want 1 (not retried)", calls)
	}
}

func TestNew_Validation(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("no endpoints accepted")
	}
	if _, err := New(Config{Endpoints: []string{"no-port"}}); err == nil {
		t.Fatal("endpoint without port accepted")
	}
}

func TestRetry_Disabled(t *testing.T) {
	f := &fakeServer{}
	f.getBotFailures.Store(1)
	c, err := New(Config{Endpoints: []string{startFake(t, f)}, RetryTimeout: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.BotAdmin.GetBot(context.Background(), &botmanagerpb.GetBotRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want Unavailable without retry", err)
	}
}
