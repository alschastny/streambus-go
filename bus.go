package streambus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Message represents a message from the stream with its Id and value.
// When used as input to Add/AddMany, Id and IdmpId are optional fields
// that control the Redis stream entry ID and idempotency key respectively.
type Message struct {
	Id     string
	IdmpId string
	Value  any
}

// Bus is the core interface for StreamBus operations.
type Bus interface {
	// Add adds a single message to a subject.
	Add(ctx context.Context, subject string, item any, producerID string) (string, error)

	// AddMany adds multiple messages to a subject.
	AddMany(ctx context.Context, subject string, items []any, producerID string) ([]string, error)

	// Ack acknowledges messages
	Ack(ctx context.Context, group, subject string, ids ...string) (int, error)

	// Nack negatively acknowledges a message
	Nack(ctx context.Context, group, consumer, subject, id string, nackDelay *time.Duration) (int, error)

	// ReadNew reads new, unprocessed messages
	ReadNew(ctx context.Context, group, consumer string, count int, blockDuration time.Duration) (map[string][]Message, error)

	// ReadExpired reads expired messages via XAUTOCLAIM
	ReadExpired(ctx context.Context, group, consumer string, count int) (map[string][]Message, error)

	// ReadPending reads previously delivered but not acked messages
	ReadPending(ctx context.Context, group, consumer string, count int, cursor map[string]string) (map[string][]Message, map[string]string, error)

	// CreateGroup creates a consumer group
	CreateGroup(ctx context.Context, group string, startID string) error
}

// StreamBus implements the Bus interface using Redis Streams.
type StreamBus struct {
	name                 string
	client               *redis.Client
	settings             *Settings
	serializers          map[string]Serializer
	streamKeysPrefix     string
	streamKeys           []string
	maxSizeOperator      string
	addMethod            addMethodFunc
	tact                 bool
	deadLetterQueue      Bus
	maxAttemptsProcessor func(string, any) error
	cursor               string // XAUTOCLAIM cursor for ReadExpired randomness
	supportsDelRefModes  *bool  // nil = not checked, true/false = cached result
	supportsIdmpModes    *bool  // nil = not checked, true/false = cached result
}

type addMethodFunc func(ctx context.Context, subject string, item any, producerID string) (string, error)

var subjectRegex = regexp.MustCompile(`^[\w.-]+$`)

// NewStreamBus creates a new StreamBus instance.
func NewStreamBus(
	name string,
	client *redis.Client,
	settings *Settings,
	serializers map[string]Serializer,
) (*StreamBus, error) {
	if err := settings.Validate(); err != nil {
		return nil, &BusError{Op: "new", Err: fmt.Errorf("invalid settings: %w", err)}
	}

	bus := &StreamBus{
		name:             name,
		client:           client,
		settings:         settings,
		serializers:      make(map[string]Serializer),
		streamKeysPrefix: fmt.Sprintf("streambus:%s:", name),
		streamKeys:       make([]string, 0),
		cursor:           "0-0",
	}

	if err := bus.configureSerializers(serializers); err != nil {
		return nil, err
	}

	bus.configureSettings()

	return bus, nil
}

// configureSerializers validates and stores serializers.
func (b *StreamBus) configureSerializers(serializers map[string]Serializer) error {
	if len(serializers) == 0 {
		return &BusError{Op: "configure", Err: ErrNoSerializers}
	}

	for subject, serializer := range serializers {
		if !subjectRegex.MatchString(subject) {
			return &BusError{
				Op:      "configure",
				Subject: subject,
				Err:     ErrInvalidSubject,
			}
		}

		b.serializers[subject] = serializer
		streamKey := b.createStreamKey(subject)
		b.streamKeys = append(b.streamKeys, streamKey)
	}

	return nil
}

// configureSettings sets up the add method based on settings.
func (b *StreamBus) configureSettings() {
	if b.settings.ExactLimits {
		b.maxSizeOperator = "="
	} else {
		b.maxSizeOperator = "~"
	}

	if b.settings.MinTTL > 0 && b.settings.MaxSize > 0 {
		b.addMethod = b.addWithTact
	} else if b.settings.MinTTL > 0 {
		b.addMethod = b.addWithTTL
	} else {
		b.addMethod = b.addWithSize
	}
}

// SetDeadLetterQueue sets the DLQ for messages exceeding max delivery.
func (b *StreamBus) SetDeadLetterQueue(dlq Bus) {
	b.deadLetterQueue = dlq
}

// SetMaxAttemptsProcessor sets a callback for handling max delivery exceeded.
func (b *StreamBus) SetMaxAttemptsProcessor(processor func(string, any) error) {
	b.maxAttemptsProcessor = processor
}

// checkSubject verifies that a subject has a registered serializer.
func (b *StreamBus) checkSubject(subject string) error {
	if _, ok := b.serializers[subject]; !ok {
		return &BusError{
			Op:      "check",
			Subject: subject,
			Err:     ErrSerializerNotFound,
		}
	}
	return nil
}

// createStreamKey creates a Redis stream key for a subject.
func (b *StreamBus) createStreamKey(subject string) string {
	return b.streamKeysPrefix + subject
}

// getSubjectFromStreamKey extracts the subject from a stream key.
func (b *StreamBus) getSubjectFromStreamKey(key string) string {
	return strings.TrimPrefix(key, b.streamKeysPrefix)
}

// serialize converts a message to Redis stream format.
func (b *StreamBus) serialize(subject string, item any) (map[string]any, error) {
	serializer, ok := b.serializers[subject]
	if !ok {
		return nil, &BusError{
			Op:      "serialize",
			Subject: subject,
			Err:     ErrSerializerNotFound,
		}
	}
	return serializer.Serialize(item)
}

// unserialize converts Redis stream data back to a message.
func (b *StreamBus) unserialize(subject string, item map[string]any) (any, error) {
	serializer, ok := b.serializers[subject]
	if !ok {
		return nil, &BusError{
			Op:      "unserialize",
			Subject: subject,
			Err:     ErrSerializerNotFound,
		}
	}
	return serializer.Deserialize(item)
}

// Add adds a single message to a subject.
func (b *StreamBus) Add(ctx context.Context, subject string, item any, producerID string) (string, error) {
	if err := b.checkSubject(subject); err != nil {
		return "", err
	}
	return b.addMethod(ctx, subject, item, producerID)
}

// AddMany adds multiple messages to a subject in a pipeline.
func (b *StreamBus) AddMany(ctx context.Context, subject string, items []any, producerID string) ([]string, error) {
	if err := b.checkSubject(subject); err != nil {
		return nil, err
	}

	if len(items) == 0 {
		return []string{}, nil
	}

	streamKey := b.createStreamKey(subject)

	// Compute per-index trim specs as []any slices.
	var lastTrimArgs, penultimateTrimArgs []any
	lastIdx := len(items) - 1
	penultimateIdx := -1

	if b.settings.MaxSize > 0 && b.settings.MinTTL > 0 {
		if len(items) >= 2 {
			penultimateIdx = len(items) - 2
			serverTime, err := b.client.Time(ctx).Result()
			if err != nil {
				return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
			}
			minID := serverTime.Add(-b.settings.MinTTL).UnixMilli()
			lastTrimArgs = []any{"MINID", b.maxSizeOperator, strconv.FormatInt(minID, 10)}
			penultimateTrimArgs = []any{"MAXLEN", b.maxSizeOperator, strconv.FormatInt(b.settings.MaxSize, 10)}
		} else {
			serverTime, err := b.client.Time(ctx).Result()
			if err != nil {
				return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
			}
			b.tact = !b.tact
			if b.tact {
				minID := serverTime.Add(-b.settings.MinTTL).UnixMilli()
				lastTrimArgs = []any{"MINID", b.maxSizeOperator, strconv.FormatInt(minID, 10)}
			} else {
				lastTrimArgs = []any{"MAXLEN", b.maxSizeOperator, strconv.FormatInt(b.settings.MaxSize, 10)}
			}
		}
	} else if b.settings.MaxSize > 0 {
		lastTrimArgs = []any{"MAXLEN", b.maxSizeOperator, strconv.FormatInt(b.settings.MaxSize, 10)}
	} else {
		serverTime, err := b.client.Time(ctx).Result()
		if err != nil {
			return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
		}
		minID := serverTime.Add(-b.settings.MinTTL).UnixMilli()
		lastTrimArgs = []any{"MINID", b.maxSizeOperator, strconv.FormatInt(minID, 10)}
	}

	trimArgsForIdx := func(i int) []any {
		switch i {
		case penultimateIdx:
			return penultimateTrimArgs
		case lastIdx:
			return lastTrimArgs
		default:
			return nil
		}
	}

	pipe := b.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(items))

	for i, item := range items {
		entryID := "*"
		idmpId := ""
		if msg, ok := item.(*Message); ok {
			idmpId = msg.IdmpId
			if msg.Id != "" {
				entryID = msg.Id
			}
			item = msg.Value
		}

		data, err := b.serialize(subject, item)
		if err != nil {
			return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
		}

		xArgs, err := b.buildXAddArgs(ctx, streamKey, data, trimArgsForIdx(i), entryID, idmpId, producerID)
		if err != nil {
			return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
		}
		cmds[i] = pipe.XAdd(ctx, xArgs)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
	}

	ids := make([]string, len(items))
	for i, cmd := range cmds {
		id, err := cmd.Result()
		if err != nil {
			return nil, &BusError{Op: "addMany", Subject: subject, Err: err}
		}
		ids[i] = id
	}

	return ids, nil
}

// buildXAddArgs builds an XAddArgs from trim spec, entry ID, and idempotency fields.
// trimArgs should be []any{trimType, operator, value} or nil.
func (b *StreamBus) buildXAddArgs(ctx context.Context, stream string, data map[string]any, trimArgs []any, entryID, idmpId, producerID string) (*redis.XAddArgs, error) {
	args := &redis.XAddArgs{Stream: stream, ID: entryID, Values: data}

	if len(trimArgs) >= 3 {
		trimType := trimArgs[0].(string)
		operator := trimArgs[1].(string)
		value := trimArgs[2].(string)

		if trimType == "MAXLEN" {
			maxLen, _ := strconv.ParseInt(value, 10, 64)
			args.MaxLen = maxLen
		} else {
			args.MinID = value
		}
		args.Approx = operator == "~"
	}

	if b.checkDeleteModesSupport(ctx) {
		args.Mode = string(b.settings.DeletePolicy)
	}

	if b.settings.IdmpMode != IdmpModeNone {
		if !b.checkIdmpSupport(ctx) {
			return nil, fmt.Errorf("redis server does not support idempotency (requires 8.6+)")
		}
		if producerID == "" {
			return nil, fmt.Errorf("producerID is required when IdmpMode is not None")
		}
		switch b.settings.IdmpMode {
		case IdmpModeAuto:
			args.ProducerID = producerID
			args.IdempotentAuto = true
		case IdmpModeExplicit:
			if idmpId == "" {
				return nil, fmt.Errorf("IdmpId is required in Explicit idmp mode")
			}
			args.ProducerID = producerID
			args.IdempotentID = idmpId
		default:
			return nil, fmt.Errorf("unknown IdmpMode: %d", b.settings.IdmpMode)
		}
	}

	return args, nil
}

// addWithTTL adds a message with TTL-based trimming.
func (b *StreamBus) addWithTTL(ctx context.Context, subject string, item any, producerID string) (string, error) {
	entryID := "*"
	idmpId := ""
	if msg, ok := item.(*Message); ok {
		idmpId = msg.IdmpId
		if msg.Id != "" {
			entryID = msg.Id
		}
		item = msg.Value
	}

	data, err := b.serialize(subject, item)
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	serverTime, err := b.client.Time(ctx).Result()
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	minID := serverTime.Add(-b.settings.MinTTL).UnixMilli()
	streamKey := b.createStreamKey(subject)
	trimArgs := []any{"MINID", b.maxSizeOperator, strconv.FormatInt(minID, 10)}

	args, err := b.buildXAddArgs(ctx, streamKey, data, trimArgs, entryID, idmpId, producerID)
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	id, err := b.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	return id, nil
}

// addWithSize adds a message with size-based trimming.
func (b *StreamBus) addWithSize(ctx context.Context, subject string, item any, producerID string) (string, error) {
	entryID := "*"
	idmpId := ""
	if msg, ok := item.(*Message); ok {
		idmpId = msg.IdmpId
		if msg.Id != "" {
			entryID = msg.Id
		}
		item = msg.Value
	}

	data, err := b.serialize(subject, item)
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	streamKey := b.createStreamKey(subject)
	trimArgs := []any{"MAXLEN", b.maxSizeOperator, strconv.FormatInt(b.settings.MaxSize, 10)}

	args, err := b.buildXAddArgs(ctx, streamKey, data, trimArgs, entryID, idmpId, producerID)
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	id, err := b.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", &BusError{Op: "add", Subject: subject, Err: err}
	}

	return id, nil
}

// addWithTact alternates between TTL and size-based trimming.
func (b *StreamBus) addWithTact(ctx context.Context, subject string, item any, producerID string) (string, error) {
	b.tact = !b.tact
	if b.tact {
		return b.addWithTTL(ctx, subject, item, producerID)
	}
	return b.addWithSize(ctx, subject, item, producerID)
}

// Ack acknowledges messages.
func (b *StreamBus) Ack(ctx context.Context, group, subject string, ids ...string) (int, error) {
	if !b.settings.AckExplicit {
		return 0, &BusError{Op: "ack", Subject: subject, Err: fmt.Errorf("ack mode not enabled")}
	}

	if err := b.checkSubject(subject); err != nil {
		return 0, err
	}

	key := b.createStreamKey(subject)

	// Use XACKDEL when DeleteOnAck is enabled and Redis supports it (8.2+)
	if b.settings.DeleteOnAck && b.checkDeleteModesSupport(ctx) {
		res, err := b.client.XAckDel(ctx, key, group, string(b.settings.DeletePolicy), ids...).Result()
		if err != nil {
			return 0, &BusError{Op: "ack", Subject: subject, Err: err}
		}
		var count int
		for _, v := range res {
			if n, ok := v.(int64); ok && n > 0 {
				count++
			}
		}
		return count, nil
	}

	result, err := b.client.XAck(ctx, key, group, ids...).Result()
	if err != nil {
		return 0, &BusError{Op: "ack", Subject: subject, Err: err}
	}

	// Delete on ack if configured (fallback for Redis < 8.2)
	if b.settings.DeleteOnAck {
		b.client.XDel(ctx, key, ids...)
	}

	return int(result), nil
}

// checkDeleteModesSupport checks if Redis supports KEEPREF | DELREF | ACKED (version >= 8.2).
// Result is cached after first check.
func (b *StreamBus) checkDeleteModesSupport(ctx context.Context) bool {
	if b.supportsDelRefModes != nil {
		return *b.supportsDelRefModes
	}

	supported := false
	info, err := b.client.Info(ctx, "server").Result()
	if err == nil {
		supported = redisSupportsDeleteModes(info)
	}
	b.supportsDelRefModes = &supported
	return supported
}

// checkIdmpSupport checks if Redis supports idempotent publishing (version >= 8.6).
// Result is cached after first check.
func (b *StreamBus) checkIdmpSupport(ctx context.Context) bool {
	if b.supportsIdmpModes != nil {
		return *b.supportsIdmpModes
	}

	supported := false
	info, err := b.client.Info(ctx, "server").Result()
	if err == nil {
		supported = redisSupportsIdempotency(info)
	}
	b.supportsIdmpModes = &supported
	return supported
}

// Nack negatively acknowledges a message.
func (b *StreamBus) Nack(ctx context.Context, group, consumer, subject, id string, nackDelay *time.Duration) (int, error) {
	if !b.settings.AckExplicit {
		return 0, &BusError{Op: "nack", Subject: subject, Err: fmt.Errorf("ack mode not enabled")}
	}

	delay := b.settings.NackDelay
	if nackDelay != nil {
		delay = *nackDelay
		if delay > b.settings.AckWait {
			return 0, &BusError{Op: "nack", Subject: subject, Err: fmt.Errorf("nackDelay > ackWait")}
		}
	}

	if err := b.checkSubject(subject); err != nil {
		return 0, err
	}

	key := b.createStreamKey(subject)

	// Check if message still belongs to consumer
	pendingInfo, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   key,
		Group:    group,
		Start:    id,
		End:      id,
		Count:    1,
		Consumer: consumer,
	}).Result()

	if err != nil || len(pendingInfo) == 0 {
		return 0, nil
	}

	item := pendingInfo[0]
	deliveredCount := item.RetryCount // RetryCount is the number of times delivered

	// Check max delivery
	if b.settings.MaxDelivery > 0 && deliveredCount >= int64(b.settings.MaxDelivery) {
		var messageData any

		// Fetch message if we have DLQ or processor
		if b.maxAttemptsProcessor != nil || b.deadLetterQueue != nil {
			msgs, err := b.client.XRange(ctx, key, id, id).Result()
			if err == nil && len(msgs) > 0 {
				messageData, _ = b.unserialize(subject, msgs[0].Values)
			}
		}

		// Ack the message
		acked, err := b.Ack(ctx, group, subject, id)
		if err != nil || acked == 0 {
			return 0, err
		}

		// Send to DLQ
		if b.deadLetterQueue != nil && messageData != nil {
			_, _ = b.deadLetterQueue.Add(ctx, subject, messageData, "")
		}

		// Call processor
		if b.maxAttemptsProcessor != nil {
			return 1, b.maxAttemptsProcessor(id, messageData)
		}

		return 1, nil
	}

	// Calculate new idle time so the message becomes available after `delay` ms.
	// Setting IDLE = AckWait (not +1) intentionally keeps the message ineligible for
	// XAUTOCLAIM until at least 1ms of real time passes, preventing same-iteration redelivery.
	lastDeliveredMs := item.Idle.Milliseconds()
	newIdleTime := max(0, max(lastDeliveredMs, b.settings.AckWait.Milliseconds())-delay.Milliseconds())

	// XCLAIM to __NACK__ consumer with IDLE option to set the new idle time
	// Using raw command because XClaimArgs doesn't support the IDLE option
	// XCLAIM key group consumer min-idle-time id [IDLE ms] [JUSTID]
	result, err := b.client.Do(ctx, "XCLAIM", key, group, "__NACK__", lastDeliveredMs, id, "IDLE", newIdleTime, "JUSTID").Result()
	if err != nil {
		return 0, &BusError{Op: "nack", Subject: subject, Err: err}
	}

	// Parse result - JUSTID returns array of message IDs
	if ids, ok := result.([]interface{}); ok && len(ids) > 0 {
		if claimedID, ok := ids[0].(string); ok && claimedID == id {
			return 1, nil
		}
	}

	return 0, nil
}

// ReadNew reads new, unprocessed messages via XREADGROUP.
func (b *StreamBus) ReadNew(ctx context.Context, group, consumer string, count int, blockDuration time.Duration) (map[string][]Message, error) {
	// Prepare stream args
	streams := make([]string, 0, len(b.streamKeys)*2)
	streams = append(streams, b.streamKeys...)
	for range b.streamKeys {
		streams = append(streams, ">")
	}

	blockMs := int64(-1)
	if blockDuration > 0 {
		blockMs = blockDuration.Milliseconds()
	}

	noAck := !b.settings.AckExplicit

	xstreams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    int64(count),
		Block:    time.Duration(blockMs) * time.Millisecond,
		NoAck:    noAck,
	}).Result()

	if errors.Is(err, redis.Nil) {
		return make(map[string][]Message), nil
	}
	if err != nil {
		return nil, &BusError{Op: "readNew", Err: err}
	}

	// Parse and unserialize results
	result := make(map[string][]Message)
	for _, xstream := range xstreams {
		subject := b.getSubjectFromStreamKey(xstream.Stream)
		result[subject] = make([]Message, 0, len(xstream.Messages))
		for _, msg := range xstream.Messages {
			unserialized, err := b.unserialize(subject, msg.Values)
			if err != nil {
				return nil, err
			}
			result[subject] = append(result[subject], Message{
				Id:    msg.ID,
				Value: unserialized,
			})
		}
	}

	return result, nil
}

// ReadExpired reads expired messages via XAUTOCLAIM.
func (b *StreamBus) ReadExpired(ctx context.Context, group, consumer string, count int) (map[string][]Message, error) {
	keys := make([]string, len(b.streamKeys))
	copy(keys, b.streamKeys)

	// Limit subjects if configured
	if b.settings.MaxExpiredSubjects > 0 && len(keys) > b.settings.MaxExpiredSubjects {
		rand.Shuffle(len(keys), func(i, j int) {
			keys[i], keys[j] = keys[j], keys[i]
		})
		keys = keys[:b.settings.MaxExpiredSubjects]
	}

	result := make(map[string][]Message)
	remaining := count

	for _, key := range keys {
		if remaining <= 0 {
			break
		}

		// Use raw command because go-redis XAutoClaim doesn't expose deleted entries
		// from Redis 7.0+ response format: [cursor, [messages...], [deleted_ids...]]
		// The cursor provides randomness - it persists across calls so multiple consumers
		// don't always fight over the same first expired message
		rawResult, err := b.client.Do(ctx,
			"XAUTOCLAIM", key, group, consumer,
			b.settings.AckWait.Milliseconds(), b.cursor,
			"COUNT", remaining,
		).Result()

		if err != nil {
			continue
		}

		subject := b.getSubjectFromStreamKey(key)
		messages := make([]Message, 0)

		// Parse the raw XAUTOCLAIM response
		if arr, ok := rawResult.([]any); ok && len(arr) >= 2 {
			// First element: new cursor for next call
			if newCursor, ok := arr[0].(string); ok {
				b.cursor = newCursor
			}

			// Second element: claimed messages
			if claimedMsgs, ok := arr[1].([]any); ok {
				for _, rawMsg := range claimedMsgs {
					if msgArr, ok := rawMsg.([]any); ok && len(msgArr) >= 2 {
						id, _ := msgArr[0].(string)
						if valuesArr, ok := msgArr[1].([]any); ok {
							values := make(map[string]any)
							for i := 0; i+1 < len(valuesArr); i += 2 {
								if k, ok := valuesArr[i].(string); ok {
									values[k] = valuesArr[i+1]
								}
							}
							if len(values) == 0 {
								// Empty values - deleted message
								messages = append(messages, Message{Id: id, Value: nil})
							} else {
								unserialized, err := b.unserialize(subject, values)
								if err != nil {
									continue
								}
								messages = append(messages, Message{Id: id, Value: unserialized})
							}
						}
					}
				}
			}

			// Third element (Redis 7.0+): deleted message IDs
			if len(arr) >= 3 {
				if deletedIDs, ok := arr[2].([]any); ok {
					for _, rawID := range deletedIDs {
						if id, ok := rawID.(string); ok {
							messages = append(messages, Message{Id: id, Value: nil})
						}
					}
				}
			}
		}

		if len(messages) > 0 {
			result[subject] = messages
			remaining -= len(messages)
		}
	}

	return result, nil
}

// ReadPending reads previously delivered but not acked messages.
func (b *StreamBus) ReadPending(ctx context.Context, group, consumer string, count int, cursor map[string]string) (map[string][]Message, map[string]string, error) {
	// Create a new cursor map to avoid modifying the input
	newCursor := make(map[string]string)
	if cursor == nil {
		for _, key := range b.streamKeys {
			newCursor[key] = "0"
		}
	} else {
		for k, v := range cursor {
			newCursor[k] = v
		}
	}
	cursor = newCursor

	// Prepare stream args
	streams := make([]string, 0, len(b.streamKeys)*2)
	streams = append(streams, b.streamKeys...)
	for _, key := range b.streamKeys {
		if c, ok := cursor[key]; ok {
			streams = append(streams, c)
		} else {
			streams = append(streams, "0")
		}
	}

	noAck := !b.settings.AckExplicit

	xstreams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    int64(count),
		NoAck:    noAck,
	}).Result()

	if errors.Is(err, redis.Nil) {
		return make(map[string][]Message), cursor, nil
	}
	if err != nil {
		return nil, nil, &BusError{Op: "readPending", Err: err}
	}

	// Parse and unserialize results
	result := make(map[string][]Message)
	for _, xstream := range xstreams {
		if len(xstream.Messages) == 0 {
			continue
		}

		subject := b.getSubjectFromStreamKey(xstream.Stream)
		messages := make([]Message, 0, len(xstream.Messages))

		var lastID string
		for _, msg := range xstream.Messages {
			lastID = msg.ID
			if msg.Values == nil {
				messages = append(messages, Message{
					Id:    msg.ID,
					Value: nil,
				})
			} else {
				unserialized, err := b.unserialize(subject, msg.Values)
				if err != nil {
					return nil, nil, err
				}
				messages = append(messages, Message{
					Id:    msg.ID,
					Value: unserialized,
				})
			}
		}

		result[subject] = messages
		cursor[xstream.Stream] = lastID
	}

	return result, cursor, nil
}

// CreateGroup creates a consumer group for all subjects.
func (b *StreamBus) CreateGroup(ctx context.Context, group string, startID string) error {
	if group == "" {
		return &BusError{Op: "createGroup", Err: fmt.Errorf("group name can't be empty")}
	}

	if startID == "" {
		startID = "0"
	}

	for _, key := range b.streamKeys {
		// Check if stream exists
		exists, err := b.client.Exists(ctx, key).Result()
		if err != nil {
			return &BusError{Op: "createGroup", Err: err}
		}

		if exists > 0 {
			// Check if group already exists
			groups, err := b.client.XInfoGroups(ctx, key).Result()
			if err == nil {
				groupExists := false
				for _, g := range groups {
					if g.Name == group {
						groupExists = true
						break
					}
				}
				if groupExists {
					continue
				}
			}
		}

		// Try to create group (MKSTREAM creates stream if it doesn't exist)
		err = b.client.XGroupCreateMkStream(ctx, key, group, startID).Err()
		if err != nil {
			// Check if error is because group already exists
			if strings.Contains(err.Error(), "BUSYGROUP") {
				continue
			}
			return &BusError{Op: "createGroup", Err: err}
		}

		b.applyIdmpConfig(ctx, key)
	}

	return nil
}

// applyIdmpConfig configures the IDMP map for a stream via XCFGSET (Redis 8.6+).
func (b *StreamBus) applyIdmpConfig(ctx context.Context, streamKey string) {
	if b.settings.IdmpMode == IdmpModeNone || !b.checkIdmpSupport(ctx) {
		return
	}

	if b.settings.IdmpDurationSec > 0 || b.settings.IdmpMaxSize > 0 {
		b.client.XCfgSet(ctx, &redis.XCfgSetArgs{
			Stream:   streamKey,
			Duration: int64(b.settings.IdmpDurationSec),
			MaxSize:  int64(b.settings.IdmpMaxSize),
		})
	}
}
