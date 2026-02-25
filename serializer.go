package streambus

// Serializer handles message serialization and deserialization.
// It converts messages to/from a map format suitable for Redis Streams.
type Serializer interface {
	Serialize(msg any) (map[string]any, error)
	Deserialize(data map[string]any) (any, error)
}
