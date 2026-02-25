package streambus

import (
	"testing"
)

// TestMessage is a sample struct for serializer testing.
type TestMessage struct {
	Value string `json:"value"`
}

func TestJSONSerializer_RoundTrip(t *testing.T) {
	serializer := NewJSONSerializer[TestMessage]()

	msg := TestMessage{Value: "test123"}

	// Serialize
	data, err := serializer.Serialize(msg)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	if _, ok := data["json"]; !ok {
		t.Error("expected 'json' field in serialized data")
	}

	// Deserialize
	result, err := serializer.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Type assert
	deserialized, ok := result.(TestMessage)
	if !ok {
		t.Fatalf("expected TestMessage, got %T", result)
	}

	if deserialized.Value != msg.Value {
		t.Errorf("expected value %q, got %q", msg.Value, deserialized.Value)
	}
}

func TestJSONSerializer_MapRoundTrip(t *testing.T) {
	serializer := NewJSONSerializer[map[string]any]()

	msg := map[string]any{"k1": "v1", "k2": float64(42)}

	data, err := serializer.Serialize(msg)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := serializer.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	deserialized, ok := result.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any, got %T", result)
	}

	if deserialized["k1"] != "v1" {
		t.Errorf("expected k1=v1, got %v", deserialized["k1"])
	}
	if deserialized["k2"] != float64(42) {
		t.Errorf("expected k2=42, got %v", deserialized["k2"])
	}
}

func TestJSONSerializer_SerializeError(t *testing.T) {
	serializer := NewJSONSerializer[TestMessage]()

	// Try to serialize something that can't be JSON encoded (channel)
	ch := make(chan int)
	_, err := serializer.Serialize(ch)
	if err == nil {
		t.Error("expected error when serializing channel")
	}

	// Check it's a SerializerError
	if _, ok := err.(*SerializerError); !ok {
		t.Errorf("expected SerializerError, got %T", err)
	}
}

func TestJSONSerializer_DeserializeError(t *testing.T) {
	serializer := NewJSONSerializer[TestMessage]()

	// Invalid JSON
	_, err := serializer.Deserialize(map[string]any{"json": "{"})
	if err == nil {
		t.Error("expected error for invalid JSON")
	}

	// Check it's a SerializerError
	if _, ok := err.(*SerializerError); !ok {
		t.Errorf("expected SerializerError, got %T", err)
	}
}

func TestJSONSerializer_MissingJsonField(t *testing.T) {
	serializer := NewJSONSerializer[TestMessage]()

	_, err := serializer.Deserialize(map[string]any{"other": "value"})
	if err == nil {
		t.Error("expected error for missing json field")
	}
}

func TestJSONSerializer_ImplementsInterface(t *testing.T) {
	// Verify JSONSerializer implements Serializer interface
	var _ Serializer = NewJSONSerializer[TestMessage]()
	var _ Serializer = NewJSONSerializer[map[string]any]()
}
