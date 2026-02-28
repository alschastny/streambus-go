package streambus

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// HandlerFunc processes one message.
// Return nil                  → ACK
// Return ErrStopProcessor     → NACK + graceful stop
// Return other error          → NACK + log + continue (or stop if WithStopOnError or nack=false)
type HandlerFunc func(ctx context.Context, subject, id string, msg any) error

// Processor routes messages from a Consumer to registered handlers and runs a processing loop.
type Processor struct {
	consumer      Consumer
	handlers      map[string]HandlerFunc
	emptyHandler  HandlerFunc
	batch         int
	blockDuration time.Duration
	ack           bool
	nack          bool
	workers       int
	stopOnError   bool
	logger        *slog.Logger
}

// NewProcessor creates a Processor wrapping the given consumer.
// Defaults: batch=1, ack=true, nack=true.
func NewProcessor(consumer Consumer) *Processor {
	return &Processor{
		consumer: consumer,
		handlers: make(map[string]HandlerFunc),
		batch:    1,
		ack:      true,
		nack:     true,
		logger:   slog.Default(),
	}
}

// Handle registers a handler for the given subject.
func (p *Processor) Handle(subject string, h HandlerFunc) *Processor {
	p.handlers[subject] = h
	return p
}

// HandleEmpty registers a handler for nil/deleted message entries.
func (p *Processor) HandleEmpty(h HandlerFunc) *Processor {
	p.emptyHandler = h
	return p
}

// WithBatch sets the number of messages per Read call.
func (p *Processor) WithBatch(n int) *Processor {
	p.batch = n
	return p
}

// WithBlockDuration sets the block wait duration when the stream is empty.
func (p *Processor) WithBlockDuration(d time.Duration) *Processor {
	p.blockDuration = d
	return p
}

// WithAckMode controls whether the processor automatically ACKs and NACKs.
func (p *Processor) WithAckMode(ack, nack bool) *Processor {
	p.ack = ack
	p.nack = nack
	return p
}

// WithWorkers limits the number of concurrent goroutines per batch (0 = unlimited).
func (p *Processor) WithWorkers(n int) *Processor {
	p.workers = n
	return p
}

// WithStopOnError causes the processor to stop on any handler error (not just ErrStopProcessor).
func (p *Processor) WithStopOnError() *Processor {
	p.stopOnError = true
	return p
}

// WithLogger sets the logger used for handler errors.
func (p *Processor) WithLogger(l *slog.Logger) *Processor {
	p.logger = l
	return p
}

// Run starts the processing loop. It returns nil when ctx is cancelled, or a fatal error.
func (p *Processor) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		msgs, err := p.consumer.Read(ctx, p.batch, p.blockDuration)
		if err != nil {
			// Context cancellation is a clean stop.
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		if len(msgs) == 0 {
			p.logger.Debug("nothing read")
			continue
		}

		if err := p.dispatchBatch(ctx, msgs); err != nil {
			if errors.Is(err, ErrStopProcessor) {
				p.logger.Info("interrupted")
				return nil
			}
			return err
		}
	}
}

// dispatchBatch distributes subjects to goroutines (one goroutine per subject).
func (p *Processor) dispatchBatch(ctx context.Context, msgs map[string][]Message) error {
	if len(msgs) == 1 {
		// Fast path: no goroutine overhead for a single subject.
		for subject, messages := range msgs {
			return p.processSubject(ctx, subject, messages)
		}
	}

	// Multiple subjects: run in parallel, bounded by workers semaphore.
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)

	var sem chan struct{}
	if p.workers > 0 {
		sem = make(chan struct{}, p.workers)
	}

	for subject, messages := range msgs {
		subject, messages := subject, messages // capture loop vars

		if sem != nil {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				break
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if sem != nil {
				defer func() { <-sem }()
			}

			err := p.processSubject(ctx, subject, messages)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// processSubject processes all messages for a single subject sequentially.
func (p *Processor) processSubject(ctx context.Context, subject string, messages []Message) error {
	handler, ok := p.handlers[subject]
	if !ok {
		return &ProcessorError{Op: "handle", Subject: subject, Err: ErrNoHandler}
	}

	p.logger.Info("handling", "subject", subject, "count", len(messages))

	success, fail := 0, 0
	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return nil
		}

		h := handler
		if msg.Value == nil {
			p.logger.Warn("message is NULL", "subject", subject, "id", msg.Id)
			if p.emptyHandler != nil {
				h = p.emptyHandler
			}
		}

		handlerErr := h(ctx, subject, msg.Id, msg.Value)
		ackCtx := context.WithoutCancel(ctx)
		if handlerErr == nil {
			success++
			if p.ack {
				if _, err := p.consumer.Ack(ackCtx, subject, msg.Id); err != nil {
					return &ProcessorError{Op: "ack", Subject: subject, ID: msg.Id, Err: err}
				}
				p.logger.Debug("ack", "subject", subject, "id", msg.Id)
			}
			continue
		}

		// Handler returned an error — NACK regardless of whether it is ErrStopProcessor.
		fail++
		if p.nack {
			if _, err := p.consumer.Nack(ackCtx, subject, msg.Id, nil); err != nil {
				return &ProcessorError{Op: "nack", Subject: subject, ID: msg.Id, Err: err}
			}
			p.logger.Debug("nack", "subject", subject, "id", msg.Id)
		}

		if errors.Is(handlerErr, ErrStopProcessor) {
			return ErrStopProcessor
		}

		p.logger.Error("exception", "subject", subject, "id", msg.Id, "err", handlerErr)

		if p.stopOnError || !p.nack {
			return &ProcessorError{Op: "handle", Subject: subject, ID: msg.Id, Err: handlerErr}
		}
	}

	p.logger.Info("handled", "subject", subject, "count", len(messages), "success", success, "fail", fail)

	return nil
}
