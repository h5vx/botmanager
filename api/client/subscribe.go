package client

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/h5vx/botmanager/api/botmanagerpb"
)

// ErrEventsLost is returned by Subscribe when the cluster no longer retains
// the events the subscription would have to resume from (the subscriber
// was away longer than the journal retention covers). Subscribe again with
// a nil AfterSequence to continue from the current position, and resync
// whatever state depends on the missed events.
var ErrEventsLost = errors.New("botmanager client: events were evicted from the journal before they could be delivered")

// SubscribeOptions configures Subscribe.
type SubscribeOptions struct {
	// BotIDs limits the stream to these bots; empty means all bots.
	BotIDs []string
	// AfterSequence resumes after a previously seen Update.sequence (for
	// example one persisted by the caller). Nil starts from the current
	// position: only events committed after the subscription is
	// established are delivered.
	AfterSequence *uint64
	// MinBackoff/MaxBackoff bound the reconnect delay (defaults 100ms/5s).
	MinBackoff, MaxBackoff time.Duration
	// OnReconnect, if set, is called with the error that broke the stream
	// before each reconnect attempt — for logging.
	OnReconnect func(err error)
}

// Subscribe delivers every update of the cluster's event stream to handle,
// in order and exactly once per sequence number, until ctx is done,
// handle returns an error, or events were lost (ErrEventsLost).
//
// Broken streams (node restart, network failure, leader change) are
// reconnected transparently — possibly to another endpoint — resuming
// after the last delivered sequence; duplicates are skipped. Every node
// serves the same replicated stream, so switching nodes is seamless.
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions, handle func(*botmanagerpb.Update) error) error {
	minB, maxB := opts.MinBackoff, opts.MaxBackoff
	if minB <= 0 {
		minB = 100 * time.Millisecond
	}
	if maxB <= 0 {
		maxB = 5 * time.Second
	}
	if maxB < minB {
		maxB = minB
	}

	var last *uint64
	if opts.AfterSequence != nil {
		v := *opts.AfterSequence
		last = &v
	}
	backoff := minB

	for {
		req := &botmanagerpb.SubscribeRequest{BotIds: opts.BotIDs, AfterSequence: last}
		delivered, err := c.subscribeOnce(ctx, req, &last, handle)
		var hErr handlerError
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case errors.As(err, &hErr):
			return hErr.err
		case status.Code(err) == codes.OutOfRange:
			return ErrEventsLost
		}
		if delivered {
			backoff = minB
		}
		if opts.OnReconnect != nil {
			opts.OnReconnect(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxB {
			backoff = maxB
		}
	}
}

type handlerError struct{ err error }

func (h handlerError) Error() string { return h.err.Error() }

// subscribeOnce runs one stream until it breaks. last is advanced as
// updates are delivered (and initialised from the server's position header
// when the caller started without a position).
func (c *Client) subscribeOnce(ctx context.Context, req *botmanagerpb.SubscribeRequest, last **uint64, handle func(*botmanagerpb.Update) error) (delivered bool, err error) {
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := c.Messaging.Subscribe(sctx, req)
	if err != nil {
		return false, err
	}
	hdr, err := stream.Header()
	if err != nil {
		return false, err
	}
	if *last == nil {
		if pos, perr := strconv.ParseUint(trimHeader(hdr.Get(botmanagerpb.SubscribePositionHeader)), 10, 64); perr == nil {
			*last = &pos
		}
	}

	for {
		upd, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				err = status.Error(codes.Unavailable, "stream closed by server")
			}
			return delivered, err
		}
		seq := upd.GetSequence()
		if *last != nil && seq <= **last {
			continue // уже доставлено до переподключения
		}
		if err := handle(upd); err != nil {
			return delivered, handlerError{err}
		}
		delivered = true
		*last = &seq
	}
}

func trimHeader(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}
