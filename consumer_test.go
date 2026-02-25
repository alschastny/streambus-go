package streambus

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// ---- Basic Consumer ----

func newConsumerSetup(t *testing.T) (*StreamBus, Consumer, Consumer) {
	t.Helper()

	client := testClient(t)

	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 100
		s.MaxDelivery = 10
	})

	builder := NewBuilder("test").
		WithClient(client).
		WithSettings(settings).
		WithSerializers(map[string]Serializer{
			"subject_a": NewJSONSerializer[map[string]any](),
			"subject_b": NewJSONSerializer[map[string]any](),
		})

	bus, err := builder.CreateBus()
	if err != nil {
		t.Fatalf("CreateBus failed: %v", err)
	}

	c1, err := builder.CreateConsumer("group1", "consumer1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateConsumer failed: %v", err)
	}

	c1Clone, err := builder.CreateConsumer("group1", "consumer1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateConsumer(clone) failed: %v", err)
	}

	return bus, c1, c1Clone
}

func TestConsumer_Ack(t *testing.T) {
	bus, c1, c1Clone := newConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	c1.Read(ctx, 10, 0)
	c1.Ack(ctx, "subject_a", id1)

	result, _ := c1Clone.Read(ctx, 10, 0)
	if len(result) != 0 {
		t.Errorf("expected empty result after ack, got %v", result)
	}
}

func TestConsumer_Read(t *testing.T) {
	bus, c1, c1Clone := newConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	id2, _ := bus.Add(ctx, "subject_a", map[string]any{"k2": "v2"}, "")

	c1Clone.Read(ctx, 10, 0)

	result1, _ := c1.Read(ctx, 1, 0)
	want1 := map[string]map[string]any{"subject_a": {id1: map[string]any{"k1": "v1"}}}
	if !reflect.DeepEqual(msgMap(result1), want1) {
		t.Errorf("first read: expected %v, got %v", want1, msgMap(result1))
	}

	result2, _ := c1.Read(ctx, 1, 0)
	want2 := map[string]map[string]any{"subject_a": {id2: map[string]any{"k2": "v2"}}}
	if !reflect.DeepEqual(msgMap(result2), want2) {
		t.Errorf("second read: expected %v, got %v", want2, msgMap(result2))
	}
}

func TestConsumer_AddNackRead(t *testing.T) {
	bus, c1, _ := newConsumerSetup(t)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	id, _ := bus.Add(ctx, "subject_a", msg, "")
	if id == "" {
		t.Fatal("expected non-empty id")
	}

	want := map[string]map[string]any{"subject_a": {id: msg}}

	result1, _ := c1.Read(ctx, 10, 0)
	if !reflect.DeepEqual(msgMap(result1), want) {
		t.Errorf("read: expected %v, got %v", want, msgMap(result1))
	}

	nacked, err := c1.Nack(ctx, "subject_a", id, nil)
	if err != nil {
		t.Fatalf("Nack failed: %v", err)
	}
	if nacked != 1 {
		t.Errorf("expected nack=1, got %d", nacked)
	}

	result2, _ := c1.Read(ctx, 10, 0)
	if !reflect.DeepEqual(msgMap(result2), want) {
		t.Errorf("read after nack: expected %v, got %v", want, msgMap(result2))
	}
}

// ---- Ordered Consumer ----

func newOrderedConsumerSetup(t *testing.T) (*StreamBus, Consumer, Consumer) {
	t.Helper()

	client := testClient(t)

	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 0
		s.MaxSize = 100
		s.ExactLimits = true
		s.DeleteOnAck = true
		s.MaxDelivery = 10
	})

	builder := NewBuilder("test").
		WithClient(client).
		WithSettings(settings).
		WithSerializers(map[string]Serializer{
			"subject_a": NewJSONSerializer[map[string]any](),
			"subject_b": NewJSONSerializer[map[string]any](),
		})

	bus, err := builder.CreateBus()
	if err != nil {
		t.Fatalf("CreateBus failed: %v", err)
	}

	c1, err := builder.CreateOrderedConsumer("group1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateOrderedConsumer failed: %v", err)
	}

	c1Clone, err := builder.CreateOrderedConsumer("group1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateOrderedConsumer(clone) failed: %v", err)
	}

	return bus, c1, c1Clone
}

func TestOrderedConsumer_Ack(t *testing.T) {
	bus, c1, c1Clone := newOrderedConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	c1.Read(ctx, 10, 0)
	c1.Ack(ctx, "subject_a", id1)

	result, _ := c1Clone.Read(ctx, 10, 0)
	if len(result) != 0 {
		t.Errorf("expected empty result after ack, got %v", result)
	}
}

func TestOrderedConsumer_Read(t *testing.T) {
	bus, c1, c1Clone := newOrderedConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	id2, _ := bus.Add(ctx, "subject_a", map[string]any{"k2": "v2"}, "")

	c1Clone.Read(ctx, 10, 0)

	result1, _ := c1.Read(ctx, 1, 0)
	want1 := map[string]map[string]any{"subject_a": {id1: map[string]any{"k1": "v1"}}}
	if !reflect.DeepEqual(msgMap(result1), want1) {
		t.Errorf("first read: expected %v, got %v", want1, msgMap(result1))
	}

	result2, _ := c1.Read(ctx, 1, 0)
	want2 := map[string]map[string]any{"subject_a": {id2: map[string]any{"k2": "v2"}}}
	if !reflect.DeepEqual(msgMap(result2), want2) {
		t.Errorf("second read: expected %v, got %v", want2, msgMap(result2))
	}
}

func TestOrderedConsumer_NackThrows(t *testing.T) {
	_, c1, _ := newOrderedConsumerSetup(t)
	ctx := context.Background()

	_, err := c1.Nack(ctx, "subject_a", "0-0", nil)
	if err == nil {
		t.Fatal("expected error")
	}

	consumerErr, ok := err.(*ConsumerError)
	if !ok {
		t.Fatalf("expected *ConsumerError, got %T", err)
	}
	if consumerErr.Err != ErrNackNotAllowed {
		t.Errorf("expected ErrNackNotAllowed, got %v", consumerErr.Err)
	}
}

// ---- Ordered Strict Consumer ----

func newOrderedStrictConsumerSetup(t *testing.T) (*StreamBus, Consumer, Consumer) {
	t.Helper()

	client := testClient(t)

	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 100
		s.MaxDelivery = 10
	})

	builder := NewBuilder("test").
		WithClient(client).
		WithSettings(settings).
		WithSerializers(map[string]Serializer{
			"subject_a": NewJSONSerializer[map[string]any](),
			"subject_b": NewJSONSerializer[map[string]any](),
		})

	bus, err := builder.CreateBus()
	if err != nil {
		t.Fatalf("CreateBus failed: %v", err)
	}

	c1, err := builder.CreateOrderedStrictConsumer("group1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateOrderedStrictConsumer failed: %v", err)
	}

	c1Clone, err := builder.CreateOrderedStrictConsumer("group1", []string{"subject_a", "subject_b"})
	if err != nil {
		t.Fatalf("CreateOrderedStrictConsumer(clone) failed: %v", err)
	}

	return bus, c1, c1Clone
}

func TestOrderedStrictConsumer_NotAckThrows(t *testing.T) {
	bus, c1, c1Clone := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	c1.Read(ctx, 10, 0)

	acked, err := c1Clone.Ack(ctx, "subject_a", id1)
	if err != nil {
		t.Fatalf("clone Ack failed: %v", err)
	}
	if acked != 1 {
		t.Errorf("expected acked=1, got %d", acked)
	}

	_, err = c1.Ack(ctx, "subject_a", id1)
	if err == nil {
		t.Fatal("expected error when acking already acked message")
	}
}

func TestOrderedStrictConsumer_NackThrows(t *testing.T) {
	_, c1, _ := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	_, err := c1.Nack(ctx, "subject_a", "0-0", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOrderedStrictConsumer_PendingConsistencyThrows(t *testing.T) {
	bus, c1, c1Clone := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	id2, _ := bus.Add(ctx, "subject_a", map[string]any{"k2": "v2"}, "")

	c1Clone.Read(ctx, 10, 0)
	c1.Read(ctx, 1, 0)
	c1.Read(ctx, 1, 0)

	acked, _ := bus.Ack(ctx, "group1", "subject_a", id1)
	if acked != 1 {
		t.Fatalf("expected bus ack=1, got %d", acked)
	}

	_, err := c1.Ack(ctx, "subject_a", id2)
	if err == nil {
		t.Fatal("expected consistency error")
	}
}

func TestOrderedStrictConsumer_UnknownConsumer(t *testing.T) {
	bus, c1, _ := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	bus.CreateGroup(ctx, "group1", "0")
	bus.ReadNew(ctx, "group1", "consumer_unknown", 1, 0)

	_, err := c1.Read(ctx, 10, 0)
	if err == nil {
		t.Fatal("expected error for unknown consumer")
	}
}

func TestOrderedStrictConsumer_UnknownConsumersWhileInit(t *testing.T) {
	bus, c1, _ := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	bus.CreateGroup(ctx, "group1", "0")
	bus.ReadNew(ctx, "group1", "consumer_unknown1", 1, 0)
	bus.ReadNew(ctx, "group1", "consumer_unknown2", 1, 0)

	_, err := c1.Read(ctx, 10, 0)
	if err == nil {
		t.Fatal("expected error for unknown consumers")
	}
}

func TestOrderedStrictConsumer_UnknownConsumersWhileAck(t *testing.T) {
	bus, c1, _ := newOrderedStrictConsumerSetup(t)
	ctx := context.Background()

	id1, _ := bus.Add(ctx, "subject_a", map[string]any{"k1": "v1"}, "")
	c1.Read(ctx, 10, 0)
	bus.ReadNew(ctx, "group1", "consumer_unknown", 1, 0)

	_, err := c1.Ack(ctx, "subject_a", id1)
	if err == nil {
		t.Fatal("expected error when unknown consumer detected during ack")
	}
}
