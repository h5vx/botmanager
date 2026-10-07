package botmanagerpb

// SubscribePositionHeader is the response header Messaging.Subscribe sends
// as soon as the subscription is established. Its value is the sequence the
// stream starts after (decimal): every event with a higher sequence will be
// delivered. A client that waits for the header (stream.Header()) knows that
// everything committed from that moment on reaches it, and can resume from
// that position after a reconnect even if no event has arrived yet.
const SubscribePositionHeader = "x-botmanager-after-sequence"
