package streambus

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

// testClient creates a Redis client for testing.
// Uses environment variables REDIS_HOST and REDIS_PORT, defaults to localhost:6379.
// Uses database 15 for isolation from other data.
func testClient(t *testing.T) *redis.Client {
	t.Helper()

	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}

	client := redis.NewClient(&redis.Options{
		Addr: host + ":" + port,
		DB:   15,
	})

	// Verify connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Flush DB before each test
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush DB: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		client.FlushDB(ctx)
		client.Close()
	})

	return client
}

// makeMessages creates a map of test messages.
func makeMessages(count int) []map[string]any {
	messages := make([]map[string]any, count)
	for i := 0; i < count; i++ {
		messages[i] = map[string]any{
			"k" + strconv.Itoa(i+1): "v" + strconv.Itoa(i+1),
		}
	}
	return messages
}

// testSerializer returns a JSON serializer for testing.
func testSerializer() Serializer {
	return NewJSONSerializer[map[string]any]()
}

// testSettings returns default test settings.
func testSettings() *Settings {
	return DefaultSettings()
}

// testSettingsWithOptions returns settings with custom options.
func testSettingsWithOptions(opts ...func(*Settings)) *Settings {
	s := DefaultSettings()
	for _, opt := range opts {
		opt(s)
	}
	return s
}
