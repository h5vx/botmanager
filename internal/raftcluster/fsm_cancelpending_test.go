package raftcluster

import (
	"errors"
	"testing"
)

// TestApplyCancelPending covers Messaging.CancelPending semantics
// (FSM.applyCancelPending): PENDING/RETRYING transition to Cancelled,
// already-Cancelled is an idempotent no-op, SENT/FAILED are left untouched
// (the caller tells "actually cancelled" from "already terminal" by
// checking the returned Delivery.Status), a missing idempotency_key is
// ErrMessageNotFound, and an empty idempotency_key in the command itself is
// ErrInvalidCommand.
func TestApplyCancelPending(t *testing.T) {
	newFSMWithMessage := func(t *testing.T, initial DeliveryStatus) *FSM {
		t.Helper()
		fsm := NewFSM(10)
		mustApply(t, fsm, Command{Type: CommandCreateBot, CreateBot: &CreateBotCommand{ID: "bot-1", CreatedAt: t1(0)}})
		mustApply(t, fsm, Command{Type: CommandPutMessage, PutMessage: &PutMessageCommand{
			IdempotencyKey: "k1", BotID: "bot-1", ChatID: 1, Text: "hi", CreatedAt: t1(1),
		}})
		if initial != DeliveryStatusPending {
			mustApply(t, fsm, Command{Type: CommandUpdateDelivery, UpdateDelivery: &UpdateDeliveryCommand{
				IdempotencyKey: "k1", Status: initial, SentAt: t1(2),
			}})
		}
		return fsm
	}

	cases := []struct {
		name          string
		initialStatus DeliveryStatus
		wantStatus    DeliveryStatus
	}{
		{name: "pending is cancelled", initialStatus: DeliveryStatusPending, wantStatus: DeliveryStatusCancelled},
		{name: "retrying is cancelled", initialStatus: DeliveryStatusRetrying, wantStatus: DeliveryStatusCancelled},
		{name: "already cancelled stays cancelled (idempotent)", initialStatus: DeliveryStatusCancelled, wantStatus: DeliveryStatusCancelled},
		{name: "sent is left untouched", initialStatus: DeliveryStatusSent, wantStatus: DeliveryStatusSent},
		{name: "failed is left untouched", initialStatus: DeliveryStatusFailed, wantStatus: DeliveryStatusFailed},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fsm := newFSMWithMessage(t, tc.initialStatus)

			res := mustApply(t, fsm, Command{Type: CommandCancelPending, CancelPending: &CancelPendingCommand{
				IdempotencyKey: "k1", CancelledAt: t1(3),
			}})
			if res.Err != nil {
				t.Fatalf("unexpected Err: %v", res.Err)
			}
			if res.Message == nil {
				t.Fatalf("Message = nil")
			}
			if res.Message.Delivery.Status != tc.wantStatus {
				t.Fatalf("Delivery.Status = %v, want %v", res.Message.Delivery.Status, tc.wantStatus)
			}
		})
	}

	t.Run("missing idempotency_key is ErrMessageNotFound", func(t *testing.T) {
		fsm := newFSMWithMessage(t, DeliveryStatusPending)
		res := mustApply(t, fsm, Command{Type: CommandCancelPending, CancelPending: &CancelPendingCommand{
			IdempotencyKey: "does-not-exist", CancelledAt: t1(3),
		}})
		if !errors.Is(res.Err, ErrMessageNotFound) {
			t.Fatalf("Err = %v, want ErrMessageNotFound", res.Err)
		}
	})

	t.Run("empty idempotency_key in command is ErrInvalidCommand", func(t *testing.T) {
		fsm := newFSMWithMessage(t, DeliveryStatusPending)
		res := mustApply(t, fsm, Command{Type: CommandCancelPending, CancelPending: &CancelPendingCommand{
			CancelledAt: t1(3),
		}})
		if !errors.Is(res.Err, ErrInvalidCommand) {
			t.Fatalf("Err = %v, want ErrInvalidCommand", res.Err)
		}
	})
}
