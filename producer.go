package streambus

import "context"

// Producer is the interface for publishing messages to the bus.
type Producer interface {
	// Add adds a single message to the specified subject.
	// Returns the message Id assigned by Redis.
	Add(ctx context.Context, subject string, item any) (string, error)

	// AddMany adds multiple messages to the specified subject in a batch.
	// Returns a slice of message IDs assigned by Redis.
	// More efficient than calling Add multiple times as it uses Redis pipelining.
	AddMany(ctx context.Context, subject string, items []any) ([]string, error)
}

// producer implements Producer for a StreamBus.
// producerID is forwarded on every Add/AddMany call; required when IdmpMode != IdmpModeNone.
type producer struct {
	bus        Bus
	producerID string
}

// NewProducer creates a new Producer wrapping the given bus.
// producerID identifies this producer; pass "" if idempotency is not used.
func NewProducer(bus Bus, producerID string) Producer {
	return &producer{
		bus:        bus,
		producerID: producerID,
	}
}

// Add adds a single message to the specified subject.
func (p *producer) Add(ctx context.Context, subject string, item any) (string, error) {
	return p.bus.Add(ctx, subject, item, p.producerID)
}

// AddMany adds multiple messages to the specified subject in a batch.
func (p *producer) AddMany(ctx context.Context, subject string, items []any) ([]string, error) {
	return p.bus.AddMany(ctx, subject, items, p.producerID)
}
