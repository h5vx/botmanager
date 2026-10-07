package telegram

import (
	"testing"
	"time"
)

func TestInMemoryBus_FanOut(t *testing.T) {
	bus := NewInMemoryBus()
	ch1, cancel1 := bus.Subscribe()
	defer cancel1()
	ch2, cancel2 := bus.Subscribe()
	defer cancel2()

	want := Update{Kind: UpdateKindIncomingMessage, BotID: "bot-1", ChatID: 42, MessageID: 7}
	bus.Publish(want)

	for i, ch := range []<-chan Update{ch1, ch2} {
		select {
		case got := <-ch:
			if got != want {
				t.Errorf("subscriber %d got %+v, want %+v", i, got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out waiting for update", i)
		}
	}
}

func TestInMemoryBus_UnsubscribeStopsDelivery(t *testing.T) {
	bus := NewInMemoryBus()
	ch, cancel := bus.Subscribe()
	cancel()

	bus.Publish(Update{Kind: UpdateKindIncomingMessage})

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("received an update on an unsubscribed channel")
		}
		// closed channel read returning zero value+false would also be
		// acceptable, but Subscribe/cancel never closes the channel — this
		// branch should not be reached either way in the current
		// implementation.
	case <-time.After(50 * time.Millisecond):
		// ничего не пришло — ожидаемо.
	}
}

func TestInMemoryBus_SlowSubscriberDoesNotBlockPublish(t *testing.T) {
	bus := NewInMemoryBus()
	_, cancel := bus.Subscribe() // подписчик, который никогда не читает
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < subscriberBufSize+10; i++ {
			bus.Publish(Update{Kind: UpdateKindIncomingMessage, MessageID: int64(i)})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a slow/non-reading subscriber")
	}
}
