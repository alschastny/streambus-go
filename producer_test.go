package streambus

import (
	"context"
	"testing"
	"time"
)

func setupProducerTest(t *testing.T) (Producer, *StreamBusInfo) {
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

	info, err := builder.CreateBusInfo()
	if err != nil {
		t.Fatalf("CreateBusInfo failed: %v", err)
	}

	producer, err := builder.CreateProducer("producer")
	if err != nil {
		t.Fatalf("CreateProducer failed: %v", err)
	}

	return producer, info
}

func TestProducer_Add(t *testing.T) {
	producer, info := setupProducerTest(t)
	ctx := context.Background()

	producer.Add(ctx, "subject_a", map[string]any{"k": "v"})

	length, _ := info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected length 1, got %d", length)
	}

	producer.Add(ctx, "subject_a", map[string]any{"k": "v"})
	producer.Add(ctx, "subject_a", map[string]any{"k": "v"})

	length, _ = info.GetStreamLength(ctx, "subject_a")
	if length != 3 {
		t.Errorf("expected length 3, got %d", length)
	}
}

func TestProducer_AddMany(t *testing.T) {
	producer, info := setupProducerTest(t)
	ctx := context.Background()

	messages := make([]any, 5)
	for i := range messages {
		messages[i] = map[string]any{"k": "v"}
	}

	producer.AddMany(ctx, "subject_a", messages)

	length, _ := info.GetStreamLength(ctx, "subject_a")
	if length != 5 {
		t.Errorf("expected length 5, got %d", length)
	}

	producer.AddMany(ctx, "subject_a", messages)

	length, _ = info.GetStreamLength(ctx, "subject_a")
	if length != 10 {
		t.Errorf("expected length 10, got %d", length)
	}
}
