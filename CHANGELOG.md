# Changelog

## v0.3.0

### New features

- Added `Processor` — routes messages from a `Consumer` to registered handlers with a processing loop.
  - `NewProcessor(consumer)` with fluent configuration: `Handle`, `HandleEmpty`, `WithBatch`, `WithBlockDuration`, `WithAckMode`, `WithWorkers`, `WithStopOnError`, `WithLogger`.
  - Subjects within a batch are dispatched concurrently (one goroutine per subject); messages within a subject are processed sequentially.
  - Worker concurrency can be bounded with `WithWorkers(n)`.
  - `ErrStopProcessor` sentinel returned by a handler triggers a graceful shutdown.
- Added `benchmark` — standalone throughput benchmark program
  - `-procs` flag to control `GOMAXPROCS` and goroutine count; `REDIS_ADDR` env var for Redis address.

## v0.2.0

### Breaking changes

- `Bus.Ack` and `Consumer.Ack` return `int` instead of `int64`.

### Improvements

- All errors returned from `bus.go` are now `*BusError` instances, enabling consistent error inspection via `errors.As`.
- All errors returned from `consumer.go` are now `*ConsumerError` instances, enabling consistent error inspection via `errors.As`.
- `NewStreamBus` now returns a `*BusError` for settings validation failures and missing serializers.

## v0.1.0

Initial public release.