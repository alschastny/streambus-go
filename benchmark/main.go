// benchmark is a standalone program that measures StreamBus throughput.
//
// It runs the following scenarios:
//
//  1. Add              – 50 000 individual single-message writes
//  2. AddMany          – 1 000 000 messages sent in pipeline batches of 1 000
//  3. SingleHandler    – produce 10 000 messages on one subject, consume with one handler
//  4. MultipleHandlers – produce 10 000 × 3 subjects, consume with a basic consumer
//  5. MultipleHandlersOrdered – same as 4 but with an ordered consumer
//  6. ParallelProduce  – 5 goroutines each writing 1 000 000 messages concurrently
//  7. ParallelConsume  – 1 000 000 pre-filled messages drained by 6 concurrent consumers
//
// Scenarios 3–5 are repeated for batch sizes: 1, 10, 100, 1000.
//
// Usage:
//
//	go run ./benchmark                        # localhost:6379
//	REDIS_ADDR=redis:6379 go run ./benchmark
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alschastny/streambus-go"
	"github.com/redis/go-redis/v9"
)

// ── tunables ─────────────────────────────────────────────────────────────────

const (
	busName     = "benchmark"
	addLimit    = 50_000
	manyLimit   = 1_000_000
	manyBatch   = 1_000
	handleLimit = 10_000

	// ParallelProduce (mirrors ProduceBenchmarkTest.php)
	pProducerMsgs  = 1_000_000 // per producer goroutine
	pProducerBatch = 5_000

	// ParallelConsume (mirrors ConsumeBenchmarkTest.php)
	pConsumeMsgs  = 1_000_000 // total messages pre-filled
	pConsumeBatch = 1_000
)

var (
	singleSubject    = []string{"subject_a"}
	multipleSubjects = []string{"subject_a", "subject_b", "subject_c"}
	batchSizes       = []int{1, 10, 100, 1000}
)

// ── entry point ───────────────────────────────────────────────────────────────

func main() {
	procs := flag.Int("procs", 0, "GOMAXPROCS value (default: use current runtime value)")
	flag.Parse()

	if *procs > 0 {
		runtime.GOMAXPROCS(*procs)
	}
	parallelism := runtime.GOMAXPROCS(0)

	client := newClient()
	defer func() { _ = client.Close() }()

	fmt.Println("StreamBus Go Benchmark")
	fmt.Println("======================")
	fmt.Printf("CPUs: %d  GOMAXPROCS: %d  REDIS_ADDR: %s\n\n",
		runtime.NumCPU(), runtime.GOMAXPROCS(0), os.Getenv("REDIS_ADDR"))

	// ── 1. Add ────────────────────────────────────────────────────────────────
	flush(client)
	bus := newBus(client, singleSubject)
	msgs, elapsed := benchAdd(bus)
	printResult("Add (individual)", msgs, elapsed)

	// ── 2. AddMany ────────────────────────────────────────────────────────────
	flush(client)
	bus = newBus(client, singleSubject)
	msgs, elapsed = benchAddMany(bus)
	printResult("AddMany (pipeline 1 000)", msgs, elapsed)

	fmt.Println()

	// ── 3. SingleHandler ──────────────────────────────────────────────────────
	for _, batch := range batchSizes {
		flush(client)
		bus = newBus(client, singleSubject)
		produce(bus, singleSubject, handleLimit)
		consumer, err := streambus.NewBuilder(busName).
			WithClient(client).
			WithSettings(benchSettings()).
			WithSerializers(serializers(singleSubject)).
			CreateConsumer("group", "consumer", singleSubject)
		must(err)

		msgs, elapsed = benchConsume(consumer, singleSubject, handleLimit, batch)
		printResult(fmt.Sprintf("SingleHandler        batch=%4d", batch), msgs, elapsed)
	}

	fmt.Println()

	// ── 4. MultipleHandlers (basic consumer) ──────────────────────────────────
	for _, batch := range batchSizes {
		flush(client)
		bus = newBus(client, multipleSubjects)
		produce(bus, multipleSubjects, handleLimit)
		consumer, err := streambus.NewBuilder(busName).
			WithClient(client).
			WithSettings(benchSettings()).
			WithSerializers(serializers(multipleSubjects)).
			CreateConsumer("group", "consumer", multipleSubjects)
		must(err)

		total := handleLimit * len(multipleSubjects)
		msgs, elapsed = benchConsume(consumer, multipleSubjects, total, batch)
		printResult(fmt.Sprintf("MultipleHandlers     batch=%4d", batch), msgs, elapsed)
	}

	fmt.Println()

	// ── 5. MultipleHandlersOrdered ────────────────────────────────────────────
	for _, batch := range batchSizes {
		flush(client)
		bus = newBus(client, multipleSubjects)
		produce(bus, multipleSubjects, handleLimit)
		consumer, err := streambus.NewBuilder(busName).
			WithClient(client).
			WithSettings(benchSettings()).
			WithSerializers(serializers(multipleSubjects)).
			CreateOrderedConsumer("group", multipleSubjects)
		must(err)

		total := handleLimit * len(multipleSubjects)
		msgs, elapsed = benchConsume(consumer, multipleSubjects, total, batch)
		printResult(fmt.Sprintf("MultipleHandlersOrdered batch=%4d", batch), msgs, elapsed)
	}

	fmt.Println()

	// ── 6. ParallelProduce ────────────────────────────────────────────────────
	flush(client)
	msgs, elapsed = benchParallelProduce(client, parallelism)
	printResult(fmt.Sprintf("ParallelProduce (%d goroutines)", parallelism), msgs, elapsed)

	fmt.Println()

	// ── 7. ParallelConsume ────────────────────────────────────────────────────
	flush(client)
	bus = newBus(client, singleSubject)
	produce(bus, singleSubject, pConsumeMsgs)
	msgs, elapsed = benchParallelConsume(client, parallelism)
	printResult(fmt.Sprintf("ParallelConsume (%d goroutines)", parallelism), msgs, elapsed)
}

// ── benchmark helpers ─────────────────────────────────────────────────────────

// benchAdd measures individual Add throughput.
func benchAdd(bus *streambus.StreamBus) (int, time.Duration) {
	msg := map[string]any{"k": "v"}
	ctx := context.Background()

	start := time.Now()
	for i := 0; i < addLimit; i++ {
		if _, err := bus.Add(ctx, "subject_a", msg, ""); err != nil {
			log.Printf("Add error: %v", err)
		}
	}
	return addLimit, time.Since(start)
}

// benchAddMany measures pipelined AddMany throughput.
func benchAddMany(bus *streambus.StreamBus) (int, time.Duration) {
	msg := map[string]any{"k": "v"}
	ctx := context.Background()

	chunk := make([]any, manyBatch)
	for i := range chunk {
		chunk[i] = msg
	}

	start := time.Now()
	for i := 0; i < manyLimit/manyBatch; i++ {
		if _, err := bus.AddMany(ctx, "subject_a", chunk, ""); err != nil {
			log.Printf("AddMany error: %v", err)
		}
	}
	return manyLimit, time.Since(start)
}

// benchConsume assumes messages are already in the stream.
func benchConsume(consumer streambus.Consumer, subjects []string, total, batch int) (int, time.Duration) {
	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(_ context.Context, _, _ string, _ any) error {
		if count.Add(1) >= int64(total) {
			cancel()
		}
		return nil
	}

	p := streambus.NewProcessor(consumer).WithBatch(batch)
	for _, s := range subjects {
		p.Handle(s, handler)
	}

	start := time.Now()
	_ = p.Run(ctx)
	elapsed := time.Since(start)

	return int(count.Load()), elapsed
}

// benchParallelProduce runs pProducers goroutines concurrently, each writing
// pProducerMsgs messages in batches of pProducerBatch to subject_a.
// Each goroutine owns its own StreamBus instance to avoid contention.
func benchParallelProduce(client *redis.Client, n int) (int, time.Duration) {
	msg := map[string]any{"k": "v"}
	chunk := make([]any, pProducerBatch)
	for i := range chunk {
		chunk[i] = msg
	}

	ctx := context.Background()
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < n; i++ {
		bus := newBus(client, singleSubject)
		wg.Add(1)
		go func() {
			defer wg.Done()
			remaining := pProducerMsgs
			for remaining > 0 {
				size := min(remaining, pProducerBatch)
				if _, err := bus.AddMany(ctx, "subject_a", chunk[:size], ""); err != nil {
					log.Printf("parallel produce error: %v", err)
				}
				remaining -= size
			}
		}()
	}

	wg.Wait()
	return n * pProducerMsgs, time.Since(start)
}

// benchParallelConsume assumes subject_a is pre-filled with pConsumeMsgs messages.
// It starts pConsumers goroutines in the same consumer group and measures the
// wall-clock time until every message has been acknowledged.
func benchParallelConsume(client *redis.Client, n int) (int, time.Duration) {
	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < n; i++ {
		consumer, err := streambus.NewBuilder(busName).
			WithClient(client).
			WithSettings(benchSettings()).
			WithSerializers(serializers(singleSubject)).
			CreateConsumer("group1", fmt.Sprintf("consumer%d", i+1), singleSubject)
		must(err)

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				msgs, err := consumer.Read(ctx, pConsumeBatch, 0)
				if err != nil || len(msgs) == 0 {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				for subject, messages := range msgs {
					ids := make([]string, len(messages))
					for j, m := range messages {
						ids[j] = m.Id
					}
					_, _ = consumer.Ack(ctx, subject, ids...) //nolint:errcheck
					if count.Add(int64(len(messages))) >= pConsumeMsgs {
						cancel()
						return
					}
				}
			}
		}()
	}

	wg.Wait()
	return int(count.Load()), time.Since(start)
}

// ── setup helpers ─────────────────────────────────────────────────────────────

// produce fills each subject with n messages using pipeline batches of manyBatch.
func produce(bus *streambus.StreamBus, subjects []string, n int) {
	msg := map[string]any{"k": "v"}
	ctx := context.Background()

	chunk := make([]any, manyBatch)
	for i := range chunk {
		chunk[i] = msg
	}

	for _, subject := range subjects {
		remaining := n
		for remaining > 0 {
			size := min(remaining, manyBatch)
			if _, err := bus.AddMany(ctx, subject, chunk[:size], ""); err != nil {
				log.Fatalf("produce %s: %v", subject, err)
			}
			remaining -= size
		}
	}
}

func newClient() *redis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	if err := client.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis not reachable at %s: %v", addr, err)
	}
	return client
}

func newBus(client *redis.Client, subjects []string) *streambus.StreamBus {
	bus, err := streambus.NewStreamBus(busName, client, benchSettings(), serializers(subjects))
	must(err)
	return bus
}

func benchSettings() *streambus.Settings {
	return &streambus.Settings{
		MinTTL:      24 * time.Hour,
		MaxSize:     1_000_000,
		ExactLimits: false,
		DeleteOnAck: false,
		MaxDelivery: 10,
		AckExplicit: true,
		AckWait:     30 * time.Minute,
	}
}

func serializers(subjects []string) map[string]streambus.Serializer {
	m := make(map[string]streambus.Serializer, len(subjects))
	for _, s := range subjects {
		m[s] = streambus.NewJSONSerializer[map[string]any]()
	}
	return m
}

func flush(client *redis.Client) {
	if err := client.FlushDB(context.Background()).Err(); err != nil {
		log.Fatalf("FlushDB: %v", err)
	}
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// ── output ────────────────────────────────────────────────────────────────────

func printResult(name string, msgs int, elapsed time.Duration) {
	mps := math.Round(float64(msgs) / elapsed.Seconds())
	fmt.Printf("%-44s  %9d msgs  %8.3f s  %10.0f msg/s\n", name, msgs, elapsed.Seconds(), mps)
}
