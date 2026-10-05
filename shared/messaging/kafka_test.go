package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"ride-sharing/shared/retry"

	"github.com/segmentio/kafka-go"
)

var fastRetry = retry.Config{MaxRetries: 2, InitialWait: time.Millisecond, MaxWait: time.Millisecond}

func TestProcessWithRetrySucceedsAfterFailures(t *testing.T) {
	calls := 0
	handler := func(ctx context.Context, d Delivery) error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		return nil
	}
	dead := func(context.Context, Delivery, error) error {
		t.Fatal("must not dead-letter a message that eventually succeeds")
		return nil
	}

	if !processWithRetry(context.Background(), fastRetry, handler, Delivery{}, dead) {
		t.Fatal("expected the message to be committable")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestProcessWithRetryDeadLettersAfterMaxRetries(t *testing.T) {
	cause := errors.New("permanent")
	calls := 0
	handler := func(context.Context, Delivery) error { calls++; return cause }

	var deadLettered error
	dead := func(_ context.Context, _ Delivery, err error) error { deadLettered = err; return nil }

	if !processWithRetry(context.Background(), fastRetry, handler, Delivery{}, dead) {
		t.Fatal("a dead-lettered message must be committed so the partition moves on")
	}
	if calls != fastRetry.MaxRetries+1 {
		t.Fatalf("expected %d attempts, got %d", fastRetry.MaxRetries+1, calls)
	}
	if !errors.Is(deadLettered, cause) {
		t.Fatalf("expected the dead letter cause to be %v, got %v", cause, deadLettered)
	}
}

func TestProcessWithRetryKeepsMessageUncommittedOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	handler := func(context.Context, Delivery) error { cancel(); return errors.New("boom") }
	dead := func(context.Context, Delivery, error) error {
		t.Fatal("must not dead-letter while shutting down")
		return nil
	}

	if processWithRetry(ctx, fastRetry, handler, Delivery{}, dead) {
		t.Fatal("expected the message to stay uncommitted")
	}
}

func TestProcessWithRetryCommitsWhenDeadLetterFails(t *testing.T) {
	handler := func(context.Context, Delivery) error { return errors.New("permanent") }
	dead := func(context.Context, Delivery, error) error { return errors.New("kafka down") }

	if !processWithRetry(context.Background(), fastRetry, handler, Delivery{}, dead) {
		t.Fatal("expected the message to be committed so the partition is not blocked")
	}
}

func TestToDelivery(t *testing.T) {
	d := toDelivery(kafka.Message{
		Topic:     "trip.event.created",
		Key:       []byte("rider-1"),
		Partition: 2,
		Offset:    7,
		Value:     []byte(`{"ownerId":"rider-1"}`),
		Headers:   []kafka.Header{{Key: "content-type", Value: []byte("application/json")}},
	})

	if d.Topic != "trip.event.created" || d.Key != "rider-1" || d.Partition != 2 || d.Offset != 7 {
		t.Fatalf("unexpected delivery: %+v", d)
	}
	if d.Headers["content-type"] != "application/json" {
		t.Fatalf("unexpected headers: %+v", d.Headers)
	}
}
