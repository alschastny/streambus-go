package streambus

import (
	"fmt"
	"time"
)

// IdmpMode defines the idempotency mode for stream messages (Redis 8.6+).
type IdmpMode int

const (
	IdmpModeNone     IdmpMode = iota // No idempotency (default)
	IdmpModeAuto                     // IDMPAUTO: Redis deduplicates by content hash
	IdmpModeExplicit                 // IDMP: caller supplies idempotent Id per message
)

type DeleteMode string

const (
	DeleteModeKeepRef = "KEEPREF" // Default
	DeleteModeDelRef  = "DELREF"
	DeleteModeAcked   = "ACKED"
)

// Settings holds configuration for the StreamBus.
type Settings struct {
	// Retention policy
	MinTTL       time.Duration // Minimum message TTL in stream
	MaxSize      int64         // Maximum messages stored per subject
	ExactLimits  bool          // Apply exact limits (vs approximate)
	DeleteOnAck  bool          // Delete message from bus with ACK operation
	DeletePolicy DeleteMode    // One of KEEPREF, DELREF, ACKED

	// Delivery policy
	MaxDelivery int // Maximum attempts to deliver a message (0 = no limit)

	// Ack policy
	AckExplicit bool          // Should client acknowledge each message it reads
	AckWait     time.Duration // Maximum time to process message before redelivery
	NackDelay   time.Duration // Time to delay next delivery message attempt

	// Other
	MaxExpiredSubjects int // Maximum number of subjects to read expired messages per call (0 = no limit)

	// Idempotency policy (Redis 8.6+)
	IdmpMode        IdmpMode // Idempotency mode
	IdmpDurationSec int      // IDMP map entry duration in seconds (0 = server default)
	IdmpMaxSize     int      // IDMP map maximum size (0 = server default)
}

// DefaultSettings returns Settings with sensible defaults matching the PHP implementation.
func DefaultSettings() *Settings {
	return &Settings{
		MinTTL:             24 * time.Hour, // 86400 seconds
		MaxSize:            1000000,
		ExactLimits:        false,
		DeletePolicy:       DeleteModeKeepRef,
		DeleteOnAck:        false,
		MaxDelivery:        0,
		AckExplicit:        true,
		AckWait:            30 * time.Minute, // 30 * 60 * 1000 ms
		NackDelay:          0,
		MaxExpiredSubjects: 0,
	}
}

// Validate checks settings consistency and returns an error if invalid.
func (s *Settings) Validate() error {
	// Apply defaults for fields whose zero value is not a valid sentinel.
	if s.DeletePolicy == "" {
		s.DeletePolicy = DeleteModeKeepRef
	}

	if s.MinTTL < 0 {
		return fmt.Errorf("negative MinTTL")
	}

	if s.MaxSize < 0 {
		return fmt.Errorf("negative MaxSize")
	}

	if s.MinTTL == 0 && s.MaxSize == 0 {
		return fmt.Errorf("MinTTL and MaxSize both zero")
	}

	if s.AckWait < 0 {
		return fmt.Errorf("negative AckWait")
	}

	if s.NackDelay < 0 {
		return fmt.Errorf("negative NackDelay")
	}

	if s.NackDelay > s.AckWait {
		return fmt.Errorf("NackDelay > AckWait")
	}

	if s.MaxExpiredSubjects < 0 {
		return fmt.Errorf("negative MaxExpiredSubjects")
	}

	switch s.DeletePolicy {
	case DeleteModeKeepRef, DeleteModeDelRef, DeleteModeAcked:
		// valid
	default:
		return fmt.Errorf("invalid DeletePolicy %q: must be KEEPREF, DELREF, or ACKED", s.DeletePolicy)
	}

	if s.DeleteOnAck && !s.AckExplicit {
		return fmt.Errorf("DeleteOnAck and AckExplicit can't be used together")
	}

	if s.IdmpDurationSec < 0 {
		return fmt.Errorf("negative IdmpDurationSec")
	}

	if s.IdmpMaxSize < 0 {
		return fmt.Errorf("negative IdmpMaxSize")
	}

	return nil
}
