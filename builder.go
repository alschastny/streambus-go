package streambus

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Builder provides a fluent API for creating StreamBus components.
type Builder struct {
	name                 string
	client               *redis.Client
	settings             *Settings
	serializers          map[string]Serializer
	subjects             []string
	dlq                  Bus
	maxAttemptsProcessor func(string, any) error
}

// NewBuilder creates a new Builder with the given name.
func NewBuilder(name string) *Builder {
	return &Builder{
		name:        name,
		serializers: make(map[string]Serializer),
	}
}

// WithClient sets the Redis client.
func (b *Builder) WithClient(client *redis.Client) *Builder {
	b.client = client
	return b
}

// WithSettings sets the bus settings.
func (b *Builder) WithSettings(settings *Settings) *Builder {
	b.settings = settings
	return b
}

// WithSerializers sets the serializers map.
func (b *Builder) WithSerializers(serializers map[string]Serializer) *Builder {
	b.serializers = serializers
	return b
}

// WithSubjects filters which subjects to use when creating consumers.
func (b *Builder) WithSubjects(subjects []string) *Builder {
	b.subjects = subjects
	return b
}

// WithDLQ sets a dead letter queue.
func (b *Builder) WithDLQ(dlq Bus) *Builder {
	b.dlq = dlq
	return b
}

// WithMaxAttemptsProcessor sets a callback for messages exceeding max delivery.
func (b *Builder) WithMaxAttemptsProcessor(processor func(string, any) error) *Builder {
	b.maxAttemptsProcessor = processor
	return b
}

// clone creates a copy of the builder with different subjects.
func (b *Builder) clone(subjects []string) *Builder {
	return &Builder{
		name:                 b.name,
		client:               b.client,
		settings:             b.settings,
		serializers:          b.serializers,
		subjects:             subjects,
		dlq:                  b.dlq,
		maxAttemptsProcessor: b.maxAttemptsProcessor,
	}
}

// getSerializers returns serializers filtered by subjects if set.
func (b *Builder) getSerializers() (map[string]Serializer, error) {
	if len(b.subjects) == 0 {
		return b.serializers, nil
	}

	filtered := make(map[string]Serializer)
	for _, subject := range b.subjects {
		serializer, ok := b.serializers[subject]
		if !ok {
			return nil, fmt.Errorf("serializer not defined for subject: %s", subject)
		}
		filtered[subject] = serializer
	}
	return filtered, nil
}

// createBusWithName creates a bus with the given name (internal helper).
func (b *Builder) createBusWithName(name string, applyDLQ bool) (*StreamBus, error) {
	if b.client == nil {
		return nil, fmt.Errorf("client is not defined")
	}
	if b.settings == nil {
		return nil, fmt.Errorf("settings is not defined")
	}
	serializers, err := b.getSerializers()
	if err != nil {
		return nil, err
	}
	if len(serializers) == 0 {
		return nil, fmt.Errorf("serializers are empty")
	}

	bus, err := NewStreamBus(name, b.client, b.settings, serializers)
	if err != nil {
		return nil, err
	}

	if applyDLQ {
		if b.dlq != nil {
			bus.SetDeadLetterQueue(b.dlq)
		}
		if b.maxAttemptsProcessor != nil {
			bus.SetMaxAttemptsProcessor(b.maxAttemptsProcessor)
		}
	}

	return bus, nil
}

// createInfoWithName creates a StreamBusInfo with the given name (internal helper).
func (b *Builder) createInfoWithName(name string) (*StreamBusInfo, error) {
	if b.client == nil {
		return nil, fmt.Errorf("client is not defined")
	}
	return NewStreamBusInfo(b.client, name), nil
}

// CreateBus creates a StreamBus instance.
func (b *Builder) CreateBus() (*StreamBus, error) {
	return b.createBusWithName(b.name, true)
}

// CreateDLQBus creates a dead letter queue bus with "dlq:" prefix.
func (b *Builder) CreateDLQBus() (*StreamBus, error) {
	return b.createBusWithName("dlq:"+b.name, false)
}

// CreateBusInfo creates a StreamBusInfo for monitoring.
func (b *Builder) CreateBusInfo() (*StreamBusInfo, error) {
	return b.createInfoWithName(b.name)
}

// CreateDLQBusInfo creates a StreamBusInfo for the DLQ.
func (b *Builder) CreateDLQBusInfo() (*StreamBusInfo, error) {
	return b.createInfoWithName("dlq:" + b.name)
}

// CreateProducer creates a Producer with the given producer ID.
// producerID is forwarded on every Add/AddMany call; required when IdmpMode != IdmpModeNone.
func (b *Builder) CreateProducer(producerID string) (Producer, error) {
	bus, err := b.CreateBus()
	if err != nil {
		return nil, err
	}
	return NewProducer(bus, producerID), nil
}

// CreateConsumer creates a basic consumer.
func (b *Builder) CreateConsumer(group, consumer string, subjects []string) (Consumer, error) {
	builder := b
	if len(subjects) > 0 {
		builder = b.clone(subjects)
	}

	bus, err := builder.CreateBus()
	if err != nil {
		return nil, err
	}

	return newBasicConsumer(bus, group, consumer), nil
}

// CreateOrderedConsumer creates an ordered consumer.
func (b *Builder) CreateOrderedConsumer(group string, subjects []string) (Consumer, error) {
	builder := b
	if len(subjects) > 0 {
		builder = b.clone(subjects)
	}

	bus, err := builder.CreateBus()
	if err != nil {
		return nil, err
	}

	return newOrderedConsumer(bus, group, "consumer"), nil
}

// CreateOrderedStrictConsumer creates an ordered strict consumer.
func (b *Builder) CreateOrderedStrictConsumer(group string, subjects []string) (Consumer, error) {
	builder := b
	effectiveSubjects := subjects
	if len(subjects) == 0 {
		effectiveSubjects = b.subjects
	} else {
		builder = b.clone(subjects)
	}

	bus, err := builder.CreateBus()
	if err != nil {
		return nil, err
	}

	info, err := b.CreateBusInfo()
	if err != nil {
		return nil, err
	}

	return newOrderedStrictConsumer(bus, group, "consumer", info, effectiveSubjects)
}
