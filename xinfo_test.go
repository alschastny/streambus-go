package streambus

import (
	"context"
	"testing"
	"time"
)

func setupXInfoTest(t *testing.T) (*StreamBus, *StreamBusInfo) {
	t.Helper()

	client := testClient(t)

	settings := &Settings{
		MinTTL:      60 * time.Second,
		MaxSize:     100,
		ExactLimits: false,
		DeleteOnAck: false,
		MaxDelivery: 10,
		AckExplicit: true,
		AckWait:     30 * time.Minute,
	}

	serializers := map[string]Serializer{
		"subject_a": testSerializer(),
		"subject_b": testSerializer(),
	}

	bus, err := NewStreamBus("test", client, settings, serializers)
	if err != nil {
		t.Fatalf("Failed to create bus: %v", err)
	}

	info := NewStreamBusInfo(client, "test")

	return bus, info
}

func TestXInfo_GetConsumers(t *testing.T) {
	bus, info := setupXInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	bus.Add(ctx, "subject_a", msg, "")
	bus.CreateGroup(ctx, "group1", "0")
	bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	// Get XInfo for existing subject
	xinfo, err := info.XInfo(ctx, "subject_a")
	if err != nil {
		t.Fatalf("XInfo failed: %v", err)
	}

	consumers, err := xinfo.GetConsumers("group1")
	if err != nil {
		t.Fatalf("GetConsumers failed: %v", err)
	}

	if len(consumers) == 0 {
		t.Error("expected at least one consumer")
	}

	// Verify consumer1 exists
	found := false
	for _, c := range consumers {
		if c.Name == "consumer1" {
			found = true
			if c.Pending != 1 {
				t.Errorf("expected 1 pending for consumer1, got %d", c.Pending)
			}
			break
		}
	}
	if !found {
		t.Error("consumer1 not found")
	}

	// Unknown subject - XInfo succeeds but GetConsumers returns empty (like PHP)
	xinfoUnknown, err := info.XInfo(ctx, "subject_unknown")
	if err != nil {
		t.Fatalf("XInfo for unknown subject should not error: %v", err)
	}
	unknownConsumers, err := xinfoUnknown.GetConsumers("group1")
	if err != nil {
		t.Fatalf("GetConsumers for unknown subject should not error: %v", err)
	}
	if len(unknownConsumers) != 0 {
		t.Errorf("expected empty consumers for unknown subject, got %d", len(unknownConsumers))
	}
}

func TestXInfo_GetConsumers_UnknownGroup(t *testing.T) {
	bus, info := setupXInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	bus.Add(ctx, "subject_a", msg, "")
	bus.CreateGroup(ctx, "group1", "0")

	xinfo, err := info.XInfo(ctx, "subject_a")
	if err != nil {
		t.Fatalf("XInfo failed: %v", err)
	}

	// Unknown group should return error or empty
	consumers, err := xinfo.GetConsumers("unknown_group")
	if err == nil && len(consumers) != 0 {
		t.Error("expected error or empty for unknown group")
	}
}

func TestXInfo_Stream(t *testing.T) {
	bus, info := setupXInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	bus.Add(ctx, "subject_a", msg, "")
	bus.CreateGroup(ctx, "group1", "0")
	bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	// Get stream info for existing subject
	xinfo, err := info.XInfo(ctx, "subject_a")
	if err != nil {
		t.Fatalf("XInfo failed: %v", err)
	}

	stream, err := xinfo.GetStream(ctx)
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}

	if stream.Length != 1 {
		t.Errorf("expected stream length 1, got %d", stream.Length)
	}

	if stream.Groups != 1 {
		t.Errorf("expected 1 group, got %d", stream.Groups)
	}
}

func TestXInfo_Stream_WithMultipleMessages(t *testing.T) {
	bus, info := setupXInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}

	// Add multiple messages
	for i := 0; i < 5; i++ {
		bus.Add(ctx, "subject_a", msg, "")
	}

	bus.CreateGroup(ctx, "group1", "0")

	xinfo, err := info.XInfo(ctx, "subject_a")
	if err != nil {
		t.Fatalf("XInfo failed: %v", err)
	}

	stream, err := xinfo.GetStream(ctx)
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	if stream.Length != 5 {
		t.Errorf("expected stream length 5, got %d", stream.Length)
	}
}

func TestXInfo_EmptyXInfoData(t *testing.T) {
	// Test with empty XInfoData (nil client/streamKey)
	xinfo := &XInfoData{}

	consumers, err := xinfo.GetConsumers("group1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(consumers) != 0 {
		t.Error("expected empty consumers for empty XInfoData")
	}
}
