package streambus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// mockConsumer is a simple in-memory Consumer for unit tests.
type mockConsumer struct {
	mu      sync.Mutex
	batches []map[string][]Message
	idx     int
	acked   []string
	nacked  []string
	nackErr error
}

func (m *mockConsumer) Read(_ context.Context, _ int, _ time.Duration) (map[string][]Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.idx >= len(m.batches) {
		// Block forever (caller cancels via ctx).
		time.Sleep(10 * time.Millisecond)
		return map[string][]Message{}, nil
	}
	batch := m.batches[m.idx]
	m.idx++
	return batch, nil
}

func (m *mockConsumer) Ack(_ context.Context, _ string, ids ...string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, ids...)
	return len(ids), nil
}

func (m *mockConsumer) Nack(_ context.Context, _ string, id string, _ *time.Duration) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nackErr != nil {
		return 0, m.nackErr
	}
	m.nacked = append(m.nacked, id)
	return 1, nil
}

// TestProcessor_TwoSubjects verifies that messages on two subjects are both processed.
func TestProcessor_TwoSubjects(t *testing.T) {
	mc := &mockConsumer{
		batches: []map[string][]Message{
			{
				"orders":   {{Id: "1", Value: "order-1"}},
				"payments": {{Id: "2", Value: "pay-1"}},
			},
		},
	}

	var (
		mu       sync.Mutex
		received []string
	)

	record := func(_ context.Context, subject, id string, _ any) error {
		mu.Lock()
		received = append(received, subject+"/"+id)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	p := NewProcessor(mc).
		Handle("orders", record).
		Handle("payments", record)

	_ = p.Run(ctx) // returns nil on ctx cancel

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Fatalf("expected 2 processed messages, got %d: %v", len(received), received)
	}
}

// TestProcessor_ErrStop verifies that ErrStopProcessor causes a clean shutdown.
func TestProcessor_ErrStop(t *testing.T) {
	mc := &mockConsumer{
		batches: []map[string][]Message{
			{"events": {{Id: "a", Value: "x"}}},
			{"events": {{Id: "b", Value: "y"}}}, // should never be processed
		},
	}

	calls := 0
	stopHandler := func(_ context.Context, _, _ string, _ any) error {
		calls++
		return ErrStopProcessor
	}

	p := NewProcessor(mc).Handle("events", stopHandler)

	ctx := context.Background()
	err := p.Run(ctx)

	if err != nil {
		t.Fatalf("expected nil (clean stop), got: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected handler called once, got %d", calls)
	}
	// ErrStopProcessor → NACK + stop, not ACK.
	if len(mc.nacked) != 1 || mc.nacked[0] != "a" {
		t.Fatalf("expected NACK on 'a', got acked=%v nacked=%v", mc.acked, mc.nacked)
	}
	if len(mc.acked) != 0 {
		t.Fatalf("expected no ACKs, got: %v", mc.acked)
	}
}

// TestProcessor_MissingHandler verifies that an unregistered subject returns an error.
func TestProcessor_MissingHandler(t *testing.T) {
	mc := &mockConsumer{
		batches: []map[string][]Message{
			{"unknown": {{Id: "z", Value: "data"}}},
		},
	}

	p := NewProcessor(mc) // no handlers registered

	ctx := context.Background()
	err := p.Run(ctx)

	if err == nil {
		t.Fatal("expected error for missing handler, got nil")
	}
}

// TestProcessor_EmptyHandler verifies that nil-value messages are routed to emptyHandler.
func TestProcessor_EmptyHandler(t *testing.T) {
	mc := &mockConsumer{
		batches: []map[string][]Message{
			{"events": {{Id: "del-1", Value: nil}}},
		},
	}

	emptyCalled := false
	p := NewProcessor(mc).
		Handle("events", func(_ context.Context, _, _ string, _ any) error {
			return nil
		}).
		HandleEmpty(func(_ context.Context, _, _ string, _ any) error {
			emptyCalled = true
			return nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_ = p.Run(ctx)

	if !emptyCalled {
		t.Fatal("emptyHandler was not called for nil-value message")
	}
}

// TestProcessor_StopOnError verifies WithStopOnError halts on handler errors.
func TestProcessor_StopOnError(t *testing.T) {
	someErr := errors.New("boom")

	mc := &mockConsumer{
		batches: []map[string][]Message{
			{"events": {{Id: "1", Value: "x"}}},
		},
	}

	p := NewProcessor(mc).
		Handle("events", func(_ context.Context, _, _ string, _ any) error {
			return someErr
		}).
		WithStopOnError()

	ctx := context.Background()
	err := p.Run(ctx)

	if !errors.Is(err, someErr) {
		t.Fatalf("expected %v, got %v", someErr, err)
	}
}
