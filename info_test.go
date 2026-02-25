package streambus

import (
	"context"
	"sort"
	"testing"
	"time"
)

func setupInfoTest(t *testing.T) (*StreamBus, *StreamBusInfo) {
	t.Helper()

	client := testClient(t)
	ctx := context.Background()

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

	// Create groups to ensure streams exist
	if err := bus.CreateGroup(ctx, "group1", "0"); err != nil {
		t.Fatalf("Failed to create group1: %v", err)
	}

	return bus, info
}

func TestInfo_GetSubjects(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	// Add a message to ensure stream exists
	bus.Add(ctx, "subject_a", map[string]any{"k": "v"}, "")
	bus.Add(ctx, "subject_b", map[string]any{"k": "v"}, "")

	subjects, err := info.GetSubjects(ctx)
	if err != nil {
		t.Fatalf("GetSubjects failed: %v", err)
	}

	// Sort for consistent comparison
	sort.Strings(subjects)
	expected := []string{"subject_a", "subject_b"}

	if len(subjects) != len(expected) {
		t.Errorf("expected %d subjects, got %d", len(expected), len(subjects))
	}

	for i, s := range expected {
		if subjects[i] != s {
			t.Errorf("expected subject %s, got %s", s, subjects[i])
		}
	}
}

func TestInfo_GetGroups(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	bus.CreateGroup(ctx, "group2", "0")

	groups, err := info.GetGroups(ctx, "subject_a")
	if err != nil {
		t.Fatalf("GetGroups failed: %v", err)
	}

	sort.Strings(groups)
	expected := []string{"group1", "group2"}

	if len(groups) != len(expected) {
		t.Errorf("expected %d groups, got %d", len(expected), len(groups))
	}

	for i, g := range expected {
		if groups[i] != g {
			t.Errorf("expected group %s, got %s", g, groups[i])
		}
	}

	// Unknown subject should return empty
	groups, err = info.GetGroups(ctx, "subject_unknown")
	if err == nil && len(groups) != 0 {
		t.Error("expected empty groups for unknown subject")
	}
}

func TestInfo_GetGroupPending(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k": "v"}
	bus.AddMany(ctx, "subject_a", []any{msg, msg, msg}, "")

	bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	pending, err := info.GetGroupPending(ctx, "group1", "subject_a")
	if err != nil {
		t.Fatalf("GetGroupPending failed: %v", err)
	}
	if pending != 1 {
		t.Errorf("expected 1 pending, got %d", pending)
	}

	bus.ReadNew(ctx, "group1", "consumer1", 2, 0)
	pending, _ = info.GetGroupPending(ctx, "group1", "subject_a")
	if pending != 3 {
		t.Errorf("expected 3 pending, got %d", pending)
	}

	// Unknown subject
	pending, _ = info.GetGroupPending(ctx, "group1", "subject_unknown")
	if pending != 0 {
		t.Errorf("expected 0 pending for unknown subject, got %d", pending)
	}

	// Unknown group
	pending, _ = info.GetGroupPending(ctx, "group_unknown", "subject_a")
	if pending != 0 {
		t.Errorf("expected 0 pending for unknown group, got %d", pending)
	}
}

func TestInfo_GetGroupTimeLag(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k": "v"}
	id, _ := bus.Add(ctx, "subject_a", msg, "")
	if id == "" {
		t.Fatal("expected id")
	}

	result, _ := bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result["subject_a"]) != 1 {
		t.Fatal("expected 1 message")
	}

	lag, err := info.GetGroupTimeLag(ctx, "group1", "subject_a")
	if err != nil {
		t.Fatalf("GetGroupTimeLag failed: %v", err)
	}
	// Lag should be small right after reading
	if lag < 0 {
		t.Errorf("expected non-negative lag, got %d", lag)
	}

	// Wait and add another message - lag should be >= 1 second (1000ms)
	time.Sleep(1 * time.Second)
	bus.Add(ctx, "subject_a", msg, "")

	lag, err = info.GetGroupTimeLag(ctx, "group1", "subject_a")
	if err != nil {
		t.Fatalf("GetGroupTimeLag failed: %v", err)
	}
	if lag < 1000 {
		t.Errorf("expected lag >= 1000ms after 1s wait, got %d", lag)
	}

	// Unknown group - should return lag based on 0-0 as lastDeliveredId
	lag, err = info.GetGroupTimeLag(ctx, "unknown", "subject_a")
	if err != nil {
		t.Fatalf("GetGroupTimeLag for unknown group failed: %v", err)
	}
	// Unknown group gets 0-0 as lastDeliveredId, so lag equals lastGeneratedId timestamp
	if lag == 0 {
		t.Error("expected non-zero lag for unknown group")
	}

	// Unknown subject
	lag, _ = info.GetGroupTimeLag(ctx, "group1", "subject_unknown")
	if lag != 0 {
		t.Errorf("expected 0 for unknown subject, got %d", lag)
	}
}

func TestInfo_GetStreamLength(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	msg := map[string]any{"k": "v"}
	bus.Add(ctx, "subject_a", msg, "")

	length, err := info.GetStreamLength(ctx, "subject_a")
	if err != nil {
		t.Fatalf("GetStreamLength failed: %v", err)
	}
	if length != 1 {
		t.Errorf("expected length 1, got %d", length)
	}

	bus.AddMany(ctx, "subject_a", []any{msg, msg, msg, msg}, "")

	length, _ = info.GetStreamLength(ctx, "subject_a")
	if length != 5 {
		t.Errorf("expected length 5, got %d", length)
	}

	// Unknown subject should return 0
	length, _ = info.GetStreamLength(ctx, "subject_unknown")
	if length != 0 {
		t.Errorf("expected 0 for unknown subject, got %d", length)
	}
}

func TestInfo_XInfo(t *testing.T) {
	bus, info := setupInfoTest(t)
	ctx := context.Background()

	// Add message and create group
	bus.Add(ctx, "subject_a", map[string]any{"k": "v"}, "")

	xinfo, err := info.XInfo(ctx, "subject_a")
	if err != nil {
		t.Fatalf("XInfo failed: %v", err)
	}

	if xinfo == nil {
		t.Fatal("expected non-nil XInfoData")
	}

	stream, err := xinfo.GetStream(ctx)
	if err != nil {
		t.Fatalf("GetStream failed: %v", err)
	}
	if stream == nil {
		t.Error("expected non-nil stream data")
	}
}
