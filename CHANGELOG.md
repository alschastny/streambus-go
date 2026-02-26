# Changelog

## v0.2.0

### Breaking changes

- `Bus.Ack` and `Consumer.Ack` return `int` instead of `int64`.

### Improvements

- All errors returned from `bus.go` are now `*BusError` instances, enabling consistent error inspection via `errors.As`.
- All errors returned from `consumer.go` are now `*ConsumerError` instances, enabling consistent error inspection via `errors.As`.
- `NewStreamBus` now returns a `*BusError` for settings validation failures and missing serializers.

## v0.1.0

Initial public release.