package streambus

import (
	"context"
	"fmt"
	"time"
)

// Consumer reads and acknowledges messages from the bus.
type Consumer interface {
	// Read retrieves messages. Blocks if blockDuration > 0 and no messages available.
	Read(ctx context.Context, count int, blockDuration time.Duration) (map[string][]Message, error)

	// Ack acknowledges messages for a subject.
	Ack(ctx context.Context, subject string, ids ...string) (int64, error)

	// Nack negatively acknowledges a message. Not supported by all consumer types.
	Nack(ctx context.Context, subject, id string, nackDelay *time.Duration) (int, error)
}

// baseConsumer implements the basic consumer with no order guarantees.
// Read priority: Pending → Expired → New
type baseConsumer struct {
	bus               Bus
	group             string
	consumer          string
	readPending       bool
	readPendingCursor map[string]string
	initialized       bool
}

// newBasicConsumer creates a basic consumer from an existing bus.
func newBasicConsumer(bus Bus, group, consumer string) Consumer {
	return &basicConsumer{
		baseConsumer: &baseConsumer{
			bus:         bus,
			group:       group,
			consumer:    consumer,
			readPending: true,
		},
	}
}

// newOrderedConsumer creates an ordered consumer from an existing bus.
func newOrderedConsumer(bus Bus, group, consumer string) Consumer {
	return &orderedConsumer{
		baseConsumer: &baseConsumer{
			bus:         bus,
			group:       group,
			consumer:    consumer,
			readPending: true,
		},
	}
}

// newOrderedStrictConsumer creates an ordered strict consumer from an existing bus.
func newOrderedStrictConsumer(bus Bus, group, consumer string, info Info, subjects []string) (Consumer, error) {
	if info == nil {
		return nil, fmt.Errorf("info interface required for ordered strict consumer")
	}
	if len(subjects) == 0 {
		return nil, fmt.Errorf("subjects required for ordered strict consumer")
	}
	return &orderedStrictConsumer{
		baseConsumer: &baseConsumer{
			bus:         bus,
			group:       group,
			consumer:    consumer,
			readPending: true,
		},
		info:     info,
		subjects: subjects,
		pending:  make(map[string]int),
	}, nil
}

// init initializes the consumer by creating the consumer group.
func (c *baseConsumer) init(ctx context.Context) error {
	if c.initialized {
		return nil
	}

	err := c.bus.CreateGroup(ctx, c.group, "0")
	if err != nil {
		return &ConsumerError{
			Op:       "init",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      fmt.Errorf("can't create group: %w", err),
		}
	}

	c.initialized = true
	return nil
}

// basicConsumer - no order guarantees, supports NACK.
type basicConsumer struct {
	*baseConsumer
}

func (c *basicConsumer) Read(ctx context.Context, count int, blockDuration time.Duration) (map[string][]Message, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}

	// First, try to read pending messages
	if c.readPending {
		messages, cursor, err := c.bus.ReadPending(ctx, c.group, c.consumer, count, c.readPendingCursor)
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			c.readPendingCursor = cursor
			return messages, nil
		}
		// No more pending messages
		c.readPending = false
		c.readPendingCursor = nil
	}

	// Try to read expired messages
	expired, err := c.bus.ReadExpired(ctx, c.group, c.consumer, count)
	if err != nil {
		return nil, err
	}
	if len(expired) > 0 {
		return expired, nil
	}

	// Finally, read new messages
	return c.bus.ReadNew(ctx, c.group, c.consumer, count, blockDuration)
}

func (c *basicConsumer) Ack(ctx context.Context, subject string, ids ...string) (int64, error) {
	if err := c.init(ctx); err != nil {
		return 0, err
	}
	return c.bus.Ack(ctx, c.group, subject, ids...)
}

func (c *basicConsumer) Nack(ctx context.Context, subject, id string, nackDelay *time.Duration) (int, error) {
	if err := c.init(ctx); err != nil {
		return 0, err
	}
	return c.bus.Nack(ctx, c.group, c.consumer, subject, id, nackDelay)
}

// orderedConsumer - ordered delivery, NACK not allowed.
// Read priority: Pending → New (no expired)
type orderedConsumer struct {
	*baseConsumer
}

func (c *orderedConsumer) Read(ctx context.Context, count int, blockDuration time.Duration) (map[string][]Message, error) {
	if err := c.init(ctx); err != nil {
		return nil, err
	}

	// First, try to read pending messages
	if c.readPending {
		messages, cursor, err := c.bus.ReadPending(ctx, c.group, c.consumer, count, c.readPendingCursor)
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			c.readPendingCursor = cursor
			return messages, nil
		}
		// No more pending messages
		c.readPending = false
		c.readPendingCursor = nil
	}

	// Read new messages only (no expired to maintain order)
	return c.bus.ReadNew(ctx, c.group, c.consumer, count, blockDuration)
}

func (c *orderedConsumer) Ack(ctx context.Context, subject string, ids ...string) (int64, error) {
	if err := c.init(ctx); err != nil {
		return 0, err
	}
	return c.bus.Ack(ctx, c.group, subject, ids...)
}

func (c *orderedConsumer) Nack(ctx context.Context, subject, id string, nackDelay *time.Duration) (int, error) {
	return 0, &ConsumerError{
		Op:       "nack",
		Group:    c.group,
		Consumer: c.consumer,
		Err:      ErrNackNotAllowed,
	}
}

// orderedStrictConsumer - ordered delivery with consistency checks, NACK not allowed.
type orderedStrictConsumer struct {
	*baseConsumer
	info     Info
	subjects []string
	pending  map[string]int
}

func (c *orderedStrictConsumer) Read(ctx context.Context, count int, blockDuration time.Duration) (map[string][]Message, error) {
	if err := c.initStrict(ctx); err != nil {
		return nil, err
	}

	// First, try to read pending messages
	if c.readPending {
		messages, cursor, err := c.bus.ReadPending(ctx, c.group, c.consumer, count, c.readPendingCursor)
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			c.readPendingCursor = cursor
			return messages, nil
		}
		// No more pending messages
		c.readPending = false
		c.readPendingCursor = nil
	}

	// Read new messages
	items, err := c.bus.ReadNew(ctx, c.group, c.consumer, count, blockDuration)
	if err != nil {
		return nil, err
	}

	// Track pending count
	for subject, messages := range items {
		c.pending[subject] += len(messages)
	}

	return items, nil
}

func (c *orderedStrictConsumer) Ack(ctx context.Context, subject string, ids ...string) (int64, error) {
	if err := c.initStrict(ctx); err != nil {
		return 0, err
	}

	acked, err := c.bus.Ack(ctx, c.group, subject, ids...)
	if err != nil {
		return 0, err
	}

	if acked != int64(len(ids)) {
		return 0, &ConsumerError{
			Op:       "ack",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      fmt.Errorf("not acked: expected %d, got %d", len(ids), acked),
		}
	}

	c.pending[subject] -= int(acked)

	// Check consistency
	if err := c.checkPending(ctx, subject); err != nil {
		return 0, err
	}

	return acked, nil
}

func (c *orderedStrictConsumer) Nack(ctx context.Context, subject, id string, nackDelay *time.Duration) (int, error) {
	return 0, &ConsumerError{
		Op:       "nack",
		Group:    c.group,
		Consumer: c.consumer,
		Err:      ErrNackNotAllowed,
	}
}

// initStrict initializes the strict consumer with consistency checks.
func (c *orderedStrictConsumer) initStrict(ctx context.Context) error {
	if c.initialized {
		return nil
	}

	err := c.bus.CreateGroup(ctx, c.group, "0")
	if err != nil {
		return &ConsumerError{
			Op:       "init",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      fmt.Errorf("can't create group: %w", err),
		}
	}

	// Validate only one consumer per subject
	for _, subject := range c.subjects {
		c.pending[subject] = 0

		xinfo, err := c.info.XInfo(ctx, subject)
		if err != nil {
			continue // Stream might not exist yet
		}

		consumers, err := xinfo.GetConsumers(c.group)
		if err != nil {
			continue
		}

		// Allow 0 consumers (new group) or 1 consumer (must be ourselves)
		if len(consumers) == 0 {
			continue // New group, no consumers yet - OK
		}

		if len(consumers) > 1 {
			return &ConsumerError{
				Op:       "init",
				Group:    c.group,
				Consumer: c.consumer,
				Err:      fmt.Errorf("only one consumer allowed, detected %d", len(consumers)),
			}
		}

		// Exactly 1 consumer - must be ourselves
		consumerInfo := consumers[0]
		if consumerInfo.Name != c.consumer {
			return &ConsumerError{
				Op:       "init",
				Group:    c.group,
				Consumer: c.consumer,
				Err:      fmt.Errorf("unknown consumer detected: %s", consumerInfo.Name),
			}
		}

		c.pending[subject] = int(consumerInfo.Pending)
	}

	c.initialized = true
	return nil
}

// checkPending validates the local pending count against Redis.
func (c *orderedStrictConsumer) checkPending(ctx context.Context, subject string) error {
	xinfo, err := c.info.XInfo(ctx, subject)
	if err != nil {
		return &ConsumerError{
			Op:       "checkPending",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      err,
		}
	}

	consumers, err := xinfo.GetConsumers(c.group)
	if err != nil {
		return &ConsumerError{
			Op:       "checkPending",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      err,
		}
	}

	if len(consumers) != 1 {
		return &ConsumerError{
			Op:       "checkPending",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      fmt.Errorf("only one consumer allowed, detected %d", len(consumers)),
		}
	}

	consumerInfo := consumers[0]
	if c.pending[subject] != int(consumerInfo.Pending) {
		return &ConsumerError{
			Op:       "checkPending",
			Group:    c.group,
			Consumer: c.consumer,
			Err:      fmt.Errorf("inconsistency detected: calculated pending %d != real pending %d", c.pending[subject], consumerInfo.Pending),
		}
	}

	return nil
}
