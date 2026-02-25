package streambus

import "fmt"

var (
	// ErrInvalidSubject is returned when a subject name is invalid.
	ErrInvalidSubject = fmt.Errorf("invalid subject name")

	// ErrSerializerNotFound is returned when no serializer is registered for a subject.
	ErrSerializerNotFound = fmt.Errorf("serializer not found for subject")

	// ErrNoSerializers is returned when no serializers are provided to the bus.
	ErrNoSerializers = fmt.Errorf("no serializers provided")

	// ErrNackNotAllowed is returned when NACK is called on a consumer that doesn't support it.
	ErrNackNotAllowed = fmt.Errorf("NACK not allowed for this consumer type")
)

// BusError represents an error that occurred during a bus operation.
type BusError struct {
	Op      string // operation that failed (e.g., "add", "read", "ack")
	Subject string // subject if applicable
	Err     error  // underlying error
}

func (e *BusError) Error() string {
	if e.Subject != "" {
		return fmt.Sprintf("streambus: %s %s: %v", e.Op, e.Subject, e.Err)
	}
	return fmt.Sprintf("streambus: %s: %v", e.Op, e.Err)
}

func (e *BusError) Unwrap() error {
	return e.Err
}

// SerializerError represents an error that occurred during serialization/deserialization.
type SerializerError struct {
	Op      string // "serialize" or "deserialize"
	Subject string // subject being processed
	Err     error  // underlying error
}

func (e *SerializerError) Error() string {
	return fmt.Sprintf("streambus: %s %s: %v", e.Op, e.Subject, e.Err)
}

func (e *SerializerError) Unwrap() error {
	return e.Err
}

// ConsumerError represents an error that occurred during consumer operations.
type ConsumerError struct {
	Op       string // operation that failed
	Group    string // consumer group
	Consumer string // consumer name
	Err      error  // underlying error
}

func (e *ConsumerError) Error() string {
	return fmt.Sprintf("streambus: consumer %s/%s %s: %v", e.Group, e.Consumer, e.Op, e.Err)
}

func (e *ConsumerError) Unwrap() error {
	return e.Err
}
