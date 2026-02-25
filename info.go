package streambus

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Info provides monitoring and inspection capabilities for the bus.
type Info interface {
	// GetSubjects returns all subjects in the bus.
	GetSubjects(ctx context.Context) ([]string, error)

	// GetGroups returns consumer groups for a subject.
	GetGroups(ctx context.Context, subject string) ([]string, error)

	// GetGroupPending returns pending message count for a group.
	GetGroupPending(ctx context.Context, group, subject string) (int64, error)

	// GetGroupTimeLag returns time lag in milliseconds.
	GetGroupTimeLag(ctx context.Context, group, subject string) (int64, error)

	// GetStreamLength returns the stream length.
	GetStreamLength(ctx context.Context, subject string) (int64, error)

	// XInfo returns raw Redis XINFO data for a subject.
	XInfo(ctx context.Context, subject string) (*XInfoData, error)
}

// XInfoData wraps Redis XINFO STREAM response.
type XInfoData struct {
	client    *redis.Client
	streamKey string
}

// ConsumerInfo represents consumer information from XINFO.
type ConsumerInfo struct {
	Name    string
	Pending int64
	Idle    int64
}

// GroupInfo represents group information from XINFO.
type GroupInfo struct {
	Name            string
	Consumers       int64
	Pending         int64
	LastDeliveredID string
}

// GetConsumers returns consumer information for a group.
func (x *XInfoData) GetConsumers(group string) ([]ConsumerInfo, error) {
	if x.client == nil || x.streamKey == "" {
		return []ConsumerInfo{}, nil
	}

	ctx := context.Background()

	// Check if stream exists
	exists, err := x.client.Exists(ctx, x.streamKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return []ConsumerInfo{}, nil
	}

	consumers, err := x.client.XInfoConsumers(ctx, x.streamKey, group).Result()
	if err != nil {
		return nil, err
	}

	result := make([]ConsumerInfo, len(consumers))
	for i, c := range consumers {
		result[i] = ConsumerInfo{
			Name:    c.Name,
			Pending: c.Pending,
			Idle:    c.Idle.Milliseconds(),
		}
	}

	return result, nil
}

// GetGroups returns group information.
func (x *XInfoData) GetGroups() ([]GroupInfo, error) {
	if x.client == nil || x.streamKey == "" {
		return []GroupInfo{}, nil
	}

	ctx := context.Background()

	// Check if stream exists
	exists, err := x.client.Exists(ctx, x.streamKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return []GroupInfo{}, nil
	}

	groups, err := x.client.XInfoGroups(ctx, x.streamKey).Result()
	if err != nil {
		return nil, err
	}

	result := make([]GroupInfo, len(groups))
	for i, g := range groups {
		result[i] = GroupInfo{
			Name:            g.Name,
			Consumers:       g.Consumers,
			Pending:         g.Pending,
			LastDeliveredID: g.LastDeliveredID,
		}
	}

	return result, nil
}

// GetStream returns stream information.
func (x *XInfoData) GetStream(ctx context.Context) (*redis.XInfoStream, error) {
	if x.client == nil || x.streamKey == "" {
		return nil, nil
	}

	// Check if stream exists
	exists, err := x.client.Exists(ctx, x.streamKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}

	return x.client.XInfoStream(ctx, x.streamKey).Result()
}

// GetStreamFull returns full stream information with entries.
func (x *XInfoData) GetStreamFull(ctx context.Context, count int) (*redis.XInfoStreamFull, error) {
	if x.client == nil || x.streamKey == "" {
		return nil, nil
	}

	// Check if stream exists
	exists, err := x.client.Exists(ctx, x.streamKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil
	}

	return x.client.XInfoStreamFull(ctx, x.streamKey, count).Result()
}

// StreamBusInfo implements the Info interface.
type StreamBusInfo struct {
	client           *redis.Client
	streamKeysPrefix string
}

// NewStreamBusInfo creates a new StreamBusInfo instance.
func NewStreamBusInfo(client *redis.Client, busName string) *StreamBusInfo {
	return &StreamBusInfo{
		client:           client,
		streamKeysPrefix: fmt.Sprintf("streambus:%s:", busName),
	}
}

// XInfo returns raw Redis XINFO data for a subject.
func (i *StreamBusInfo) XInfo(ctx context.Context, subject string) (*XInfoData, error) {
	key := i.streamKeysPrefix + subject
	return &XInfoData{
		client:    i.client,
		streamKey: key,
	}, nil
}

// GetSubjects returns all subjects in the bus.
func (i *StreamBusInfo) GetSubjects(ctx context.Context) ([]string, error) {
	keys, err := i.client.Keys(ctx, i.streamKeysPrefix+"*").Result()
	if err != nil {
		return nil, &BusError{Op: "getSubjects", Err: err}
	}

	subjects := make([]string, len(keys))
	for idx, key := range keys {
		subjects[idx] = key[len(i.streamKeysPrefix):]
	}

	sort.Strings(subjects)
	return subjects, nil
}

// GetGroups returns consumer groups for a subject.
func (i *StreamBusInfo) GetGroups(ctx context.Context, subject string) ([]string, error) {
	key := i.streamKeysPrefix + subject

	// Check if stream exists
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		return nil, &BusError{Op: "getGroups", Subject: subject, Err: err}
	}
	if exists == 0 {
		return []string{}, nil
	}

	groups, err := i.client.XInfoGroups(ctx, key).Result()
	if err != nil {
		return nil, &BusError{Op: "getGroups", Subject: subject, Err: err}
	}

	names := make([]string, len(groups))
	for idx, group := range groups {
		names[idx] = group.Name
	}

	return names, nil
}

// GetGroupPending returns pending message count for a group.
func (i *StreamBusInfo) GetGroupPending(ctx context.Context, group, subject string) (int64, error) {
	key := i.streamKeysPrefix + subject

	// Check if stream exists
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getGroupPending", Subject: subject, Err: err}
	}
	if exists == 0 {
		return 0, nil
	}

	groups, err := i.client.XInfoGroups(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getGroupPending", Subject: subject, Err: err}
	}

	for _, g := range groups {
		if g.Name == group {
			return g.Pending, nil
		}
	}

	return 0, nil
}

// GetGroupTimeLag returns time lag in milliseconds.
func (i *StreamBusInfo) GetGroupTimeLag(ctx context.Context, group, subject string) (int64, error) {
	key := i.streamKeysPrefix + subject

	// Check if stream exists
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getGroupTimeLag", Subject: subject, Err: err}
	}
	if exists == 0 {
		return 0, nil
	}

	// Get stream info
	streamInfo, err := i.client.XInfoStream(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getGroupTimeLag", Subject: subject, Err: err}
	}

	lastGeneratedID := streamInfo.LastGeneratedID
	if lastGeneratedID == "" {
		lastGeneratedID = "0-0"
	}

	// Get group info
	lastDeliveredID := "0-0"
	groups, err := i.client.XInfoGroups(ctx, key).Result()
	if err == nil {
		for _, g := range groups {
			if g.Name == group {
				lastDeliveredID = g.LastDeliveredID
				break
			}
		}
	}

	// Parse timestamps from IDs (format: <timestamp>-<sequence>)
	lastGenerated := parseStreamIDTimestamp(lastGeneratedID)
	lastDelivered := parseStreamIDTimestamp(lastDeliveredID)

	if lastDelivered > lastGenerated {
		return 0, nil
	}
	return lastGenerated - lastDelivered, nil
}

// parseStreamIDTimestamp extracts the timestamp (milliseconds) from a Redis stream Id.
func parseStreamIDTimestamp(id string) int64 {
	if idx := strings.Index(id, "-"); idx > 0 {
		if ts, err := strconv.ParseInt(id[:idx], 10, 64); err == nil {
			return ts
		}
	}
	return 0
}

// GetStreamLength returns the stream length.
func (i *StreamBusInfo) GetStreamLength(ctx context.Context, subject string) (int64, error) {
	key := i.streamKeysPrefix + subject

	// Check if stream exists
	exists, err := i.client.Exists(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getStreamLength", Subject: subject, Err: err}
	}
	if exists == 0 {
		return 0, nil
	}

	length, err := i.client.XLen(ctx, key).Result()
	if err != nil {
		return 0, &BusError{Op: "getStreamLength", Subject: subject, Err: err}
	}

	return length, nil
}
