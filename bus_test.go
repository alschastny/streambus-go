package streambus

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type busTestSetup struct {
	client   *redis.Client
	bus      *StreamBus
	busA     *StreamBus
	busB     *StreamBus
	info     *StreamBusInfo
	settings *Settings
}

func newBusSetup(t *testing.T, name string, settings *Settings) *busTestSetup {
	t.Helper()
	client := testClient(t)
	ctx := context.Background()

	if settings == nil {
		settings = testSettingsWithOptions(func(s *Settings) {
			s.MinTTL = 60 * time.Second
			s.MaxSize = 100
			s.MaxDelivery = 10
		})
	}

	builder := NewBuilder(name).
		WithClient(client).
		WithSettings(settings).
		WithSerializers(map[string]Serializer{
			"subject_a": testSerializer(),
			"subject_b": testSerializer(),
			"subject_c": testSerializer(),
			"subject_d": testSerializer(),
			"subject_e": testSerializer(),
			"subject_f": testSerializer(),
		})

	bus, err := builder.CreateBus()
	if err != nil {
		t.Fatalf("CreateBus failed: %v", err)
	}
	info, err := builder.CreateBusInfo()
	if err != nil {
		t.Fatalf("CreateBusInfo failed: %v", err)
	}
	busA, err := builder.WithSubjects([]string{"subject_a", "subject_b", "subject_c"}).CreateBus()
	if err != nil {
		t.Fatalf("CreateBusA failed: %v", err)
	}
	busB, err := builder.WithSubjects([]string{"subject_d", "subject_e", "subject_f"}).CreateBus()
	if err != nil {
		t.Fatalf("CreateBusB failed: %v", err)
	}

	bus.CreateGroup(ctx, "group1", "0")
	bus.CreateGroup(ctx, "group2", "0")

	return &busTestSetup{client: client, bus: bus, busA: busA, busB: busB, info: info, settings: settings}
}

// msgMap converts a ReadNew/ReadExpired/ReadPending result to subject -> id -> value.
func msgMap(result map[string][]Message) map[string]map[string]any {
	m := make(map[string]map[string]any)
	for subject, msgs := range result {
		sub := make(map[string]any, len(msgs))
		for _, msg := range msgs {
			sub[msg.Id] = msg.Value
		}
		m[subject] = sub
	}
	return m
}

func TestBus_AddWrongSubject(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()
	_, err := s.bus.Add(ctx, "unknown_subject", map[string]any{}, "")
	if err == nil {
		t.Error("expected error for unknown subject")
	}
}

func TestBus_AddMany(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msgs := makeMessages(100)
	anyMsgs := make([]any, len(msgs))
	for i, m := range msgs {
		anyMsgs[i] = m
	}

	empty, err := s.bus.AddMany(ctx, "subject_a", []any{}, "")
	if err != nil || len(empty) != 0 {
		t.Fatalf("expected empty result for empty input, got %v %v", empty, err)
	}

	ids, err := s.bus.AddMany(ctx, "subject_a", anyMsgs[:1], "")
	if err != nil || len(ids) != 1 {
		t.Fatalf("expected 1 id, got %d %v", len(ids), err)
	}
	ids2, err := s.bus.AddMany(ctx, "subject_a", anyMsgs[1:2], "")
	if err != nil {
		t.Fatalf("AddMany failed: %v", err)
	}
	ids = append(ids, ids2...)
	if len(ids) != 2 {
		t.Errorf("expected 2 ids, got %d", len(ids))
	}
	ids3, err := s.bus.AddMany(ctx, "subject_a", anyMsgs[2:], "")
	if err != nil {
		t.Fatalf("AddMany failed: %v", err)
	}
	ids = append(ids, ids3...)
	if len(ids) != 100 {
		t.Errorf("expected 100 ids, got %d", len(ids))
	}

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 100, 0)
	expected := make(map[string]any, 100)
	for i, id := range ids {
		expected[id] = msgs[i]
	}
	if !reflect.DeepEqual(msgMap(result)["subject_a"], expected) {
		t.Error("read messages do not match added messages")
	}
}

func TestBus_AddManyTTL(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = time.Second
		s.MaxSize = 0
		s.ExactLimits = true
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msgs := makeMessages(100)
	anyMsgs := make([]any, len(msgs))
	for i, m := range msgs {
		anyMsgs[i] = m
	}

	ids, err := s.bus.AddMany(ctx, "subject_a", anyMsgs, "")
	if err != nil || len(ids) != 100 {
		t.Fatalf("expected 100 ids, got %d %v", len(ids), err)
	}
	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 100, 0)
	if len(result["subject_a"]) != 100 {
		t.Errorf("expected 100 messages, got %d", len(result["subject_a"]))
	}

	time.Sleep(2 * time.Second)

	ids2, err := s.bus.AddMany(ctx, "subject_a", anyMsgs[:2], "")
	if err != nil || len(ids2) != 2 {
		t.Fatalf("expected 2 ids after TTL, got %d %v", len(ids2), err)
	}
	result2, _ := s.bus.ReadNew(ctx, "group2", "consumer1", 100, 0)
	expected := map[string]any{ids2[0]: msgs[0], ids2[1]: msgs[1]}
	if !reflect.DeepEqual(msgMap(result2)["subject_a"], expected) {
		t.Error("read messages after TTL do not match")
	}
}

func TestBus_AddManyExactMaxSize(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 0
		s.MaxSize = 100
		s.ExactLimits = true
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msgs := make([]any, 100)
	for i := range msgs {
		msgs[i] = map[string]any{"k": i}
	}

	ids, _ := s.bus.AddMany(ctx, "subject_a", msgs[:1], "")
	if len(ids) != 1 {
		t.Errorf("expected 1 id, got %d", len(ids))
	}
	ids, _ = s.bus.AddMany(ctx, "subject_a", msgs, "")
	if len(ids) != 100 {
		t.Errorf("expected 100 ids, got %d", len(ids))
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 100 {
		t.Errorf("expected stream length 100, got %d", length)
	}
	ids, _ = s.bus.AddMany(ctx, "subject_a", msgs, "")
	if len(ids) != 100 {
		t.Errorf("expected 100 ids, got %d", len(ids))
	}
	length, _ = s.info.GetStreamLength(ctx, "subject_a")
	if length != 100 {
		t.Errorf("expected stream length 100, got %d", length)
	}
}

func TestBus_AddRead(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msgA := map[string]any{"k1": "v1"}
	msgB := map[string]any{"k2": "v2"}
	idA1, _ := s.bus.Add(ctx, "subject_a", msgA, "")
	idA2, _ := s.bus.Add(ctx, "subject_a", msgB, "")
	idB1, _ := s.bus.Add(ctx, "subject_b", msgA, "")
	idB2, _ := s.bus.Add(ctx, "subject_b", msgB, "")

	result, _ := s.busA.ReadNew(ctx, "group1", "consumer1", 2, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_a": {idA1: msgA, idA2: msgB},
		"subject_b": {idB1: msgA, idB2: msgB},
	}) {
		t.Errorf("unexpected messages: %v", msgMap(result))
	}

	result, _ = s.busA.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty after all consumed by consumer1")
	}
	result, _ = s.busA.ReadNew(ctx, "group1", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for consumer2")
	}

	result, _ = s.busA.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_a": {idA1: msgA},
		"subject_b": {idB1: msgA},
	}) {
		t.Errorf("unexpected messages for group2/consumer1: %v", msgMap(result))
	}
	result, _ = s.busA.ReadNew(ctx, "group2", "consumer2", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_a": {idA2: msgB},
		"subject_b": {idB2: msgB},
	}) {
		t.Errorf("unexpected messages for group2/consumer2: %v", msgMap(result))
	}
	result, _ = s.busA.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for group2/consumer1")
	}
	result, _ = s.busA.ReadNew(ctx, "group2", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for group2/consumer2")
	}
}

func TestBus_AddReadOnlyTypes(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	idA, _ := s.bus.Add(ctx, "subject_a", msg, "")
	idB, _ := s.bus.Add(ctx, "subject_b", msg, "")
	idC, _ := s.bus.Add(ctx, "subject_c", msg, "")
	idD, _ := s.bus.Add(ctx, "subject_d", msg, "")
	idE, _ := s.bus.Add(ctx, "subject_e", msg, "")
	idF, _ := s.bus.Add(ctx, "subject_f", msg, "")

	s.busA.CreateGroup(ctx, "group1", "0")
	s.busB.CreateGroup(ctx, "group2", "0")

	result, _ := s.busA.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_a": {idA: msg},
		"subject_b": {idB: msg},
		"subject_c": {idC: msg},
	}) {
		t.Errorf("unexpected messages for busA: %v", msgMap(result))
	}
	result, _ = s.busA.ReadNew(ctx, "group1", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for busA/consumer2")
	}

	result, _ = s.busB.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_d": {idD: msg},
		"subject_e": {idE: msg},
		"subject_f": {idF: msg},
	}) {
		t.Errorf("unexpected messages for busB: %v", msgMap(result))
	}
	result, _ = s.busB.ReadNew(ctx, "group2", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for busB/consumer2")
	}
}

func TestBus_AddReadWithBlock(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	idA, _ := s.bus.Add(ctx, "subject_a", msg, "")
	idB, _ := s.bus.Add(ctx, "subject_b", msg, "")

	result, _ := s.busA.ReadNew(ctx, "group1", "consumer1", 1, 10*time.Second)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_a": {idA: msg},
		"subject_b": {idB: msg},
	}) {
		t.Errorf("unexpected messages: %v", msgMap(result))
	}

	idC, _ := s.bus.Add(ctx, "subject_c", msg, "")
	start := time.Now()
	result, _ = s.busA.ReadNew(ctx, "group1", "consumer1", 1, 10*time.Second)
	if time.Since(start) >= time.Second {
		t.Error("expected ReadNew to return quickly with existing message")
	}
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{
		"subject_c": {idC: msg},
	}) {
		t.Errorf("unexpected messages: %v", msgMap(result))
	}
}

func TestBus_AddAckRead(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	msg2 := map[string]any{"k2": "v2"}
	id2, _ := s.bus.Add(ctx, "subject_a", msg2, "")

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("unexpected messages for consumer1")
	}
	result, _ = s.bus.ReadNew(ctx, "group1", "consumer2", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id2: msg2}}) {
		t.Error("unexpected messages for consumer2")
	}

	s.bus.Ack(ctx, "group1", "subject_a", id1, id2)

	result, _ = s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty after ack")
	}

	result, _ = s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("unexpected messages for group2/consumer1")
	}
	result, _ = s.bus.ReadNew(ctx, "group2", "consumer2", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id2: msg2}}) {
		t.Error("unexpected messages for group2/consumer2")
	}

	s.bus.Ack(ctx, "group2", "subject_a", id1)

	result, _ = s.bus.ReadNew(ctx, "group2", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty after partial ack")
	}

	s.bus.Nack(ctx, "group2", "consumer2", "subject_a", id2, nil)
	result, _ = s.bus.ReadExpired(ctx, "group2", "consumer1", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id2: msg2}}) {
		t.Error("expected expired message after nack")
	}
}

func TestBus_AddAckDeleteRead(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.ExactLimits = true
		s.DeleteOnAck = true
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	msg2 := map[string]any{"k2": "v2"}
	id2, _ := s.bus.Add(ctx, "subject_a", msg2, "")

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("unexpected read result")
	}
	s.bus.Ack(ctx, "group1", "subject_a", id1)

	result, _ = s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id2: msg2}}) {
		t.Error("unexpected read result for group2")
	}
	s.bus.Ack(ctx, "group2", "subject_a", id2)

	result, _ = s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for group1")
	}
	result, _ = s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for group2")
	}

	p1, _ := s.info.GetGroupPending(ctx, "group1", "subject_a")
	if p1 != 0 {
		t.Errorf("expected 0 pending for group1, got %d", p1)
	}
	p2, _ := s.info.GetGroupPending(ctx, "group2", "subject_a")
	if p2 != 0 {
		t.Errorf("expected 0 pending for group2, got %d", p2)
	}
}

func TestBus_DeletePolicyKeepRef(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.ExactLimits = true
		s.DeleteOnAck = true
		s.DeletePolicy = DeleteModeKeepRef
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	if !s.bus.checkDeleteModesSupport(ctx) {
		t.Skip("Redis >= 8.2 required for delete modes")
	}

	msg := map[string]any{"k1": "v1"}
	id, _ := s.bus.Add(ctx, "subject_a", msg, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)

	n, err := s.bus.Ack(ctx, "group1", "subject_a", id)
	if err != nil || n != 1 {
		t.Fatalf("expected ack=1, got %d %v", n, err)
	}

	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 0 {
		t.Errorf("expected stream length 0 after KeepRef delete, got %d", length)
	}
	p1, _ := s.info.GetGroupPending(ctx, "group1", "subject_a")
	if p1 != 0 {
		t.Errorf("expected 0 pending for group1, got %d", p1)
	}
	p2, _ := s.info.GetGroupPending(ctx, "group2", "subject_a")
	if p2 != 1 {
		t.Errorf("expected 1 pending for group2 (KeepRef), got %d", p2)
	}

	pending, _, _ := s.bus.ReadPending(ctx, "group2", "consumer1", 1, nil)
	if pending["subject_a"][0].Id != id || pending["subject_a"][0].Value != nil {
		t.Error("expected deleted message with nil value in group2 PEL")
	}
}

func TestBus_DeletePolicyDelRef(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.ExactLimits = true
		s.DeleteOnAck = true
		s.DeletePolicy = DeleteModeDelRef
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	if !s.bus.checkDeleteModesSupport(ctx) {
		t.Skip("Redis >= 8.2 required for delete modes")
	}

	msg := map[string]any{"k1": "v1"}
	id, _ := s.bus.Add(ctx, "subject_a", msg, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)

	n, err := s.bus.Ack(ctx, "group1", "subject_a", id)
	if err != nil || n != 1 {
		t.Fatalf("expected ack=1, got %d %v", n, err)
	}

	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 0 {
		t.Errorf("expected stream length 0 after DelRef delete, got %d", length)
	}
	p1, _ := s.info.GetGroupPending(ctx, "group1", "subject_a")
	if p1 != 0 {
		t.Errorf("expected 0 pending for group1, got %d", p1)
	}
	p2, _ := s.info.GetGroupPending(ctx, "group2", "subject_a")
	if p2 != 0 {
		t.Errorf("expected 0 pending for group2 (DelRef removes all refs), got %d", p2)
	}
}

func TestBus_DeletePolicyAcked(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.ExactLimits = true
		s.DeleteOnAck = true
		s.DeletePolicy = DeleteModeAcked
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	if !s.bus.checkDeleteModesSupport(ctx) {
		t.Skip("Redis >= 8.2 required for delete modes")
	}

	msg := map[string]any{"k1": "v1"}
	id, _ := s.bus.Add(ctx, "subject_a", msg, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)

	n, _ := s.bus.Ack(ctx, "group1", "subject_a", id)
	if n != 1 {
		t.Errorf("expected ack=1, got %d", n)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1 (group2 hasn't acked yet), got %d", length)
	}
	p1, _ := s.info.GetGroupPending(ctx, "group1", "subject_a")
	if p1 != 0 {
		t.Errorf("expected 0 pending for group1, got %d", p1)
	}
	p2, _ := s.info.GetGroupPending(ctx, "group2", "subject_a")
	if p2 != 1 {
		t.Errorf("expected 1 pending for group2, got %d", p2)
	}

	n, _ = s.bus.Ack(ctx, "group2", "subject_a", id)
	if n != 1 {
		t.Errorf("expected ack=1, got %d", n)
	}
	length, _ = s.info.GetStreamLength(ctx, "subject_a")
	if length != 0 {
		t.Errorf("expected stream length 0 after both groups acked, got %d", length)
	}
	p2, _ = s.info.GetGroupPending(ctx, "group2", "subject_a")
	if p2 != 0 {
		t.Errorf("expected 0 pending for group2, got %d", p2)
	}
}

func TestBus_AddNackRead(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	id, _ := s.bus.Add(ctx, "subject_a", msg, "")

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("unexpected read result")
	}

	n, _ := s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id, nil)
	if n != 1 {
		t.Errorf("expected nack=1, got %d", n)
	}
	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("expected expired message after nack")
	}

	n, _ = s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id, nil)
	if n != 1 {
		t.Errorf("expected nack=1, got %d", n)
	}
	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer2", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("expected expired message for consumer2")
	}

	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if len(result) != 0 {
		t.Error("expected empty for consumer1 after consumer2 claimed")
	}

	result, _ = s.bus.ReadNew(ctx, "group2", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("expected message in group2")
	}
	result, _ = s.bus.ReadNew(ctx, "group1", "consumer2", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty for group1/consumer2 new")
	}

	n, _ = s.bus.Nack(ctx, "group2", "consumer1", "subject_a", id, nil)
	if n != 1 {
		t.Errorf("expected nack=1, got %d", n)
	}
	result, _ = s.bus.ReadExpired(ctx, "group2", "consumer2", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("expected expired message for group2/consumer2")
	}

	delay := s.settings.AckWait + time.Millisecond
	_, err := s.bus.Nack(ctx, "group1", "consumer2", "subject_a", id, &delay)
	if err == nil {
		t.Error("expected error for nackDelay > ackWait")
	}
}

func TestBus_AddNackReadOrder(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	msg2 := map[string]any{"k2": "v2"}
	id2, _ := s.bus.Add(ctx, "subject_a", msg2, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.ReadNew(ctx, "group1", "consumer2", 1, 0)

	s.bus.Nack(ctx, "group1", "consumer2", "subject_a", id2, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)

	result, _ := s.bus.ReadExpired(ctx, "group1", "consumer1", 2)
	got := msgMap(result)
	if len(got["subject_a"]) != 2 {
		t.Errorf("expected 2 expired messages, got %d", len(got["subject_a"]))
	}
	if got["subject_a"][id1] == nil || got["subject_a"][id2] == nil {
		t.Error("expected both messages in expired result")
	}
}

func TestBus_AddNackDelayed(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.MaxDelivery = 10
		s.NackDelay = 25 * time.Millisecond
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	id, _ := s.bus.Add(ctx, "subject_a", msg, "")

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("unexpected read result")
	}

	s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id, nil)

	result, _ = s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty immediately after nack with delay")
	}

	id1, _ := s.bus.Add(ctx, "subject_a", msg, "")
	result, _ = s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg}}) {
		t.Error("unexpected read result for id1")
	}

	time.Sleep(s.settings.NackDelay)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)

	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id: msg}}) {
		t.Error("expected first message expired after delay")
	}
	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if len(result) != 0 {
		t.Error("expected empty after first message claimed")
	}

	time.Sleep(s.settings.NackDelay)
	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg}}) {
		t.Error("expected second message expired after delay")
	}
}

func TestBus_NackMaxDelivery(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	s.bus.CreateGroup(ctx, "group1", "0")

	for n := 1; n <= s.settings.MaxDelivery; n++ {
		result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
		if len(result) == 0 {
			result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
		}
		if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
			t.Errorf("iteration %d: unexpected result", n)
		}
		s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)
	}

	result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	if len(result) != 0 {
		t.Error("expected empty after max delivery")
	}
	result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
	if len(result) != 0 {
		t.Error("expected empty expired after max delivery")
	}
}

func TestBus_NackMaxDeliveryWithCallback(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")

	called := false
	s.bus.SetMaxAttemptsProcessor(func(id string, msg any) error {
		called = true
		if id != id1 {
			t.Errorf("expected id %s, got %s", id1, id)
		}
		if !reflect.DeepEqual(msg, msg1) {
			t.Errorf("expected msg %v, got %v", msg1, msg)
		}
		return nil
	})

	s.bus.CreateGroup(ctx, "group1", "0")
	for n := 1; n <= s.settings.MaxDelivery; n++ {
		result, _ := s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
		if len(result) == 0 {
			result, _ = s.bus.ReadExpired(ctx, "group1", "consumer1", 1)
		}
		if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
			t.Errorf("iteration %d: unexpected result", n)
		}
		s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)
	}

	if !called {
		t.Error("expected callback to be called")
	}
}

func TestBus_NackDLQ(t *testing.T) {
	ctx := context.Background()
	client := testClient(t)

	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 0
		s.MaxSize = 100
		s.ExactLimits = true
		s.MaxDelivery = 10
	})

	builder := NewBuilder(t.Name()).
		WithClient(client).
		WithSettings(settings).
		WithSerializers(map[string]Serializer{"subject_a": testSerializer()})

	dlq, err := builder.CreateDLQBus()
	if err != nil {
		t.Fatalf("CreateDLQBus failed: %v", err)
	}
	builder.WithDLQ(dlq)
	bus, err := builder.CreateBus()
	if err != nil {
		t.Fatalf("CreateBus failed: %v", err)
	}
	dlqInfo, err := builder.CreateDLQBusInfo()
	if err != nil {
		t.Fatalf("CreateDLQBusInfo failed: %v", err)
	}

	dlq.CreateGroup(ctx, "group1", "0")
	bus.CreateGroup(ctx, "group1", "0")

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := bus.Add(ctx, "subject_a", msg1, "")
	msg2 := map[string]any{"k2": "v2"}
	bus.Add(ctx, "subject_a", msg2, "")

	length, _ := dlqInfo.GetStreamLength(ctx, "subject_a")
	if length != 0 {
		t.Errorf("expected DLQ empty, got %d", length)
	}

	for n := 1; n <= 10; n++ {
		result, _ := bus.ReadExpired(ctx, "group1", "consumer1", 1)
		if len(result) == 0 {
			result, _ = bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
		}
		if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
			t.Errorf("iteration %d: unexpected result", n)
		}
		bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)
	}

	length, _ = dlqInfo.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected DLQ length 1, got %d", length)
	}

	dlqResult, _ := dlq.ReadNew(ctx, "group1", "consumer", 1, 0)
	msgs := dlqResult["subject_a"]
	if len(msgs) != 1 || !reflect.DeepEqual(msgs[0].Value, msg1) {
		t.Errorf("unexpected DLQ message: %v", msgs)
	}

	dlqResult, _ = dlq.ReadNew(ctx, "group1", "consumer", 1, 0)
	if len(dlqResult) != 0 {
		t.Error("expected empty DLQ after reading")
	}
}

func TestBus_NackDeletedOrUnknownId(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	s.bus.CreateGroup(ctx, "group1", "0")
	n, err := s.bus.Nack(ctx, "group1", "consumer1", "subject_a", "1-1", nil)
	if err != nil || n != 0 {
		t.Errorf("expected nack=0 for unknown id, got %d %v", n, err)
	}
}

func TestBus_NoAck(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.AckExplicit = false
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	pending, _, _ := s.bus.ReadPending(ctx, "group1", "consumer1", 1, nil)
	if len(pending) != 0 {
		t.Error("expected empty pending in no-ack mode")
	}

	_, err := s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)
	if err == nil {
		t.Error("expected error for nack in no-ack mode")
	}
}

func TestBus_PendingRead(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	msg2 := map[string]any{"k2": "v2"}
	id2, _ := s.bus.Add(ctx, "subject_a", msg2, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	pending, _, _ := s.bus.ReadPending(ctx, "group1", "consumer1", 1, nil)
	if !reflect.DeepEqual(msgMap(pending), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("unexpected pending messages")
	}

	s.bus.Ack(ctx, "group1", "subject_a", id1)
	pending, _, _ = s.bus.ReadPending(ctx, "group1", "consumer1", 1, nil)
	if len(pending) != 0 {
		t.Error("expected empty pending after ack")
	}

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	// Test with deleted element
	msg3 := map[string]any{"k3": "v3"}
	id3, _ := s.bus.Add(ctx, "subject_a", msg3, "")
	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)

	streamKey := s.bus.streamKeysPrefix + "subject_a"
	s.client.XDel(ctx, streamKey, id2)

	pending, _, _ = s.bus.ReadPending(ctx, "group1", "consumer1", 2, nil)
	got := msgMap(pending)
	if got["subject_a"][id2] != nil || !reflect.DeepEqual(got["subject_a"][id3], msg3) {
		t.Errorf("expected deleted message nil and id3 present, got %v", got)
	}
}

func TestBus_PendingAfterAckWaitConsumeDelay(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 1000
		s.MaxDelivery = 10
		s.AckWait = 25 * time.Millisecond
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	msg2 := map[string]any{"k2": "v2"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")
	id2, _ := s.bus.Add(ctx, "subject_a", msg2, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.ReadNew(ctx, "group1", "consumer2", 1, 0)

	time.Sleep(s.settings.AckWait)

	pending, _, _ := s.bus.ReadPending(ctx, "group1", "consumer1", 1, nil)
	if !reflect.DeepEqual(msgMap(pending), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("expected msg1 pending for consumer1")
	}
	pending, _, _ = s.bus.ReadPending(ctx, "group1", "consumer2", 1, nil)
	if !reflect.DeepEqual(msgMap(pending), map[string]map[string]any{"subject_a": {id2: msg2}}) {
		t.Error("expected msg2 pending for consumer2")
	}

	s.bus.Ack(ctx, "group1", "subject_a", id2)
	pending, _, _ = s.bus.ReadPending(ctx, "group1", "consumer2", 1, nil)
	if len(pending) != 0 {
		t.Error("expected empty pending for consumer2 after ack")
	}

	time.Sleep(s.settings.AckWait)
	result, _ := s.bus.ReadExpired(ctx, "group1", "consumer2", 1)
	if !reflect.DeepEqual(msgMap(result), map[string]map[string]any{"subject_a": {id1: msg1}}) {
		t.Error("expected msg1 in expired")
	}
}

func TestBus_PendingCursorReads(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msgs := makeMessages(5)
	anyMsgs := make([]any, 5)
	for i, m := range msgs {
		anyMsgs[i] = m
	}
	ids, _ := s.bus.AddMany(ctx, "subject_a", anyMsgs, "")
	if len(ids) != 5 {
		t.Fatalf("expected 5 ids, got %d", len(ids))
	}
	id1, id2, id3, id4 := ids[0], ids[1], ids[2], ids[3]
	msg1, msg2, msg3, msg4 := msgs[0], msgs[1], msgs[2], msgs[3]

	s.bus.CreateGroup(ctx, "group", "0")
	s.bus.ReadNew(ctx, "group", "consumer1", 4, 0)

	pending1, cursor1, _ := s.bus.ReadPending(ctx, "group", "consumer1", 2, nil)
	if !reflect.DeepEqual(msgMap(pending1), map[string]map[string]any{
		"subject_a": {id1: msg1, id2: msg2},
	}) {
		t.Errorf("unexpected pending1: %v", msgMap(pending1))
	}

	pending2, cursor2, _ := s.bus.ReadPending(ctx, "group", "consumer1", 2, cursor1)
	if !reflect.DeepEqual(msgMap(pending2), map[string]map[string]any{
		"subject_a": {id3: msg3, id4: msg4},
	}) {
		t.Errorf("unexpected pending2: %v", msgMap(pending2))
	}
	if reflect.DeepEqual(cursor1, cursor2) {
		t.Error("expected cursor2 to differ from cursor1")
	}

	pending3, cursorLast, _ := s.bus.ReadPending(ctx, "group", "consumer1", 1, cursor2)
	if len(pending3) != 0 {
		t.Errorf("expected empty pending3, got %v", pending3)
	}
	if !reflect.DeepEqual(cursor2, cursorLast) {
		t.Error("expected cursorLast to equal cursor2 when no more messages")
	}
}

func TestBus_ReadExpired(t *testing.T) {
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MaxExpiredSubjects = 3
	})
	s := newBusSetup(t, t.Name(), settings)
	ctx := context.Background()

	msg := map[string]any{"k1": "v1"}
	idA, _ := s.bus.Add(ctx, "subject_a", msg, "")
	idB, _ := s.bus.Add(ctx, "subject_b", msg, "")
	idC, _ := s.bus.Add(ctx, "subject_c", msg, "")
	idD, _ := s.bus.Add(ctx, "subject_d", msg, "")
	idE, _ := s.bus.Add(ctx, "subject_e", msg, "")
	idF, _ := s.bus.Add(ctx, "subject_f", msg, "")

	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_a", idA, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_b", idB, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_c", idC, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_d", idD, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_e", idE, nil)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_f", idF, nil)

	result, _ := s.bus.ReadExpired(ctx, "group1", "consumer1", 10)
	if len(result) != 3 {
		t.Errorf("expected 3 subjects (maxExpiredSubjects), got %d", len(result))
	}
}

func TestBus_ReadExpiredDeleted(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	msg1 := map[string]any{"k1": "v1"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg1, "")

	s.bus.CreateGroup(ctx, "group1", "0")
	s.bus.ReadNew(ctx, "group1", "consumer1", 1, 0)
	s.bus.Nack(ctx, "group1", "consumer1", "subject_a", id1, nil)

	streamKey := s.bus.streamKeysPrefix + "subject_a"
	s.client.XDel(ctx, streamKey, id1)

	result, _ := s.bus.ReadExpired(ctx, "group1", "consumer1", 10)
	if len(result["subject_a"]) != 1 || result["subject_a"][0].Id != id1 || result["subject_a"][0].Value != nil {
		t.Errorf("expected deleted message with nil value, got %v", result)
	}
}

func TestBus_CreateGroup(t *testing.T) {
	s := newBusSetup(t, "test", nil)
	ctx := context.Background()

	if err := s.bus.CreateGroup(ctx, "test_group", "0"); err != nil {
		t.Errorf("expected no error creating group, got %v", err)
	}
	if err := s.bus.CreateGroup(ctx, "test_group", "0"); err != nil {
		t.Errorf("expected no error creating existing group, got %v", err)
	}
	if err := s.bus.CreateGroup(ctx, "", "0"); err == nil {
		t.Error("expected error for empty group name")
	}
}

func idmpBusSetup(t *testing.T, mode IdmpMode) *busTestSetup {
	t.Helper()
	settings := testSettingsWithOptions(func(s *Settings) {
		s.MinTTL = 60 * time.Second
		s.MaxSize = 100
		s.IdmpMode = mode
	})
	return newBusSetup(t, t.Name(), settings)
}

func TestBus_AddIdmpAutoDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeAuto)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msg := map[string]any{"k": "v"}
	id1, _ := s.bus.Add(ctx, "subject_a", msg, "test-producer")
	id2, _ := s.bus.Add(ctx, "subject_a", msg, "test-producer")
	if id1 != id2 {
		t.Errorf("expected same id for duplicate, got %s and %s", id1, id2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}
}

func TestBus_AddIdmpAutoManyDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeAuto)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msgs := []any{map[string]any{"k": "v1"}, map[string]any{"k": "v2"}}
	ids1, _ := s.bus.AddMany(ctx, "subject_a", msgs, "test-producer")
	ids2, _ := s.bus.AddMany(ctx, "subject_a", msgs, "test-producer")
	if !reflect.DeepEqual(ids1, ids2) {
		t.Errorf("expected same ids for duplicate batch, got %v and %v", ids1, ids2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 2 {
		t.Errorf("expected stream length 2, got %d", length)
	}
}

func TestBus_AddIdmpExplicitDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msg := map[string]any{"k": "v"}
	iid := "unique-message-id"
	id1, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg, IdmpId: iid}, "test-producer")
	id2, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg, IdmpId: iid}, "test-producer")
	if id1 != id2 {
		t.Errorf("expected same id for duplicate, got %s and %s", id1, id2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}
}

func TestBus_AddIdmpExplicitDifferentIids(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msg := map[string]any{"k": "v"}
	id1, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg, IdmpId: "iid-1"}, "test-producer")
	id2, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg, IdmpId: "iid-2"}, "test-producer")
	if id1 == id2 {
		t.Error("expected different ids for different iids")
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 2 {
		t.Errorf("expected stream length 2, got %d", length)
	}
}

func TestBus_AddIdmpExplicitMissingIid(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	_, err := s.bus.Add(ctx, "subject_a", map[string]any{"k": "v"}, "test-producer")
	if err == nil {
		t.Error("expected error for missing iid in explicit mode")
	}
}

func TestBus_AddIdmpExplicitManyDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msgs := []any{
		&Message{Value: map[string]any{"k": "v1"}, IdmpId: "iid-a"},
		&Message{Value: map[string]any{"k": "v2"}, IdmpId: "iid-b"},
	}
	ids1, _ := s.bus.AddMany(ctx, "subject_a", msgs, "test-producer")
	ids2, _ := s.bus.AddMany(ctx, "subject_a", msgs, "test-producer")
	if !reflect.DeepEqual(ids1, ids2) {
		t.Errorf("expected same ids for duplicate batch, got %v and %v", ids1, ids2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 2 {
		t.Errorf("expected stream length 2, got %d", length)
	}
}

func TestBus_AddStreamBusMessageAutoDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeAuto)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	msg := map[string]any{"k": "v"}
	id1, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg}, "test-producer")
	id2, _ := s.bus.Add(ctx, "subject_a", &Message{Value: msg}, "test-producer")
	if id1 != id2 {
		t.Errorf("expected same id, got %s and %s", id1, id2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}
}

func TestBus_AddStreamBusMessageExplicitDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	iid := "msg-iid-1"
	id1, _ := s.bus.Add(ctx, "subject_a", &Message{Value: map[string]any{"k": "v"}, IdmpId: iid}, "test-producer")
	id2, _ := s.bus.Add(ctx, "subject_a", &Message{Value: map[string]any{"k": "v"}, IdmpId: iid}, "test-producer")
	if id1 != id2 {
		t.Errorf("expected same id, got %s and %s", id1, id2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}
}

func TestBus_AddStreamBusMessageExplicitManyDeduplicates(t *testing.T) {
	s := idmpBusSetup(t, IdmpModeExplicit)
	ctx := context.Background()
	if !s.bus.checkIdmpSupport(ctx) {
		t.Skip("Redis >= 8.6 required for idempotency")
	}

	items := []any{
		&Message{Value: map[string]any{"k": "v1"}, IdmpId: "iid-a"},
		&Message{Value: map[string]any{"k": "v2"}, IdmpId: "iid-b"},
	}
	ids1, _ := s.bus.AddMany(ctx, "subject_a", items, "test-producer")
	ids2, _ := s.bus.AddMany(ctx, "subject_a", items, "test-producer")
	if !reflect.DeepEqual(ids1, ids2) {
		t.Errorf("expected same ids, got %v and %v", ids1, ids2)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 2 {
		t.Errorf("expected stream length 2, got %d", length)
	}
}

func TestBus_AddStreamBusMessageCustomId(t *testing.T) {
	s := newBusSetup(t, t.Name(), nil)
	ctx := context.Background()

	customId := "9999999999999-0"
	returned, err := s.bus.Add(ctx, "subject_a", &Message{Value: map[string]any{"k": "v"}, Id: customId}, "")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if returned != customId {
		t.Errorf("expected custom id %s, got %s", customId, returned)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 1 {
		t.Errorf("expected stream length 1, got %d", length)
	}
}

func TestBus_AddManyStreamBusMessageCustomIds(t *testing.T) {
	s := newBusSetup(t, t.Name(), nil)
	ctx := context.Background()

	items := []any{
		&Message{Value: map[string]any{"k": "v1"}, Id: "9999999999997-0"},
		&Message{Value: map[string]any{"k": "v2"}, Id: "9999999999998-0"},
		&Message{Value: map[string]any{"k": "v3"}, Id: "9999999999999-0"},
	}
	ids, err := s.bus.AddMany(ctx, "subject_a", items, "")
	if err != nil {
		t.Fatalf("AddMany failed: %v", err)
	}
	expected := []string{"9999999999997-0", "9999999999998-0", "9999999999999-0"}
	if !reflect.DeepEqual(ids, expected) {
		t.Errorf("expected custom ids %v, got %v", expected, ids)
	}
	length, _ := s.info.GetStreamLength(ctx, "subject_a")
	if length != 3 {
		t.Errorf("expected stream length 3, got %d", length)
	}
}

func TestBus_BadSerializers(t *testing.T) {
	client := testClient(t)
	settings := testSettings()

	t.Run("empty", func(t *testing.T) {
		_, err := NewStreamBus("test", client, settings, map[string]Serializer{})
		if err == nil {
			t.Error("expected error for empty serializers")
		}
	})

	t.Run("bad subject", func(t *testing.T) {
		_, err := NewStreamBus("test", client, settings, map[string]Serializer{
			"subject~-=": testSerializer(),
		})
		if err == nil {
			t.Error("expected error for invalid subject")
		}
	})
}
