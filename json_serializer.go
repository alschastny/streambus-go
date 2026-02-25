package streambus

import (
	"encoding/json"
	"fmt"
)

// JSONSerializer serializes messages to/from JSON format.
// The type parameter T specifies the message type for proper deserialization.
type JSONSerializer[T any] struct{}

// NewJSONSerializer creates a new JSONSerializer for type T.
func NewJSONSerializer[T any]() *JSONSerializer[T] {
	return &JSONSerializer[T]{}
}

// Serialize converts a message to a map with a single "json" field.
func (s *JSONSerializer[T]) Serialize(msg any) (map[string]any, error) {
	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, &SerializerError{
			Op:  "serialize",
			Err: fmt.Errorf("json marshal: %w", err),
		}
	}
	return map[string]any{"json": string(jsonBytes)}, nil
}

// Deserialize converts a map back to type T.
func (s *JSONSerializer[T]) Deserialize(data map[string]any) (any, error) {
	jsonStr, ok := data["json"].(string)
	if !ok {
		return nil, &SerializerError{
			Op:  "deserialize",
			Err: fmt.Errorf("missing or invalid 'json' field"),
		}
	}

	var result T
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		return nil, &SerializerError{
			Op:  "deserialize",
			Err: fmt.Errorf("json unmarshal: %w", err),
		}
	}

	return result, nil
}
