package streambus

import (
	"testing"
	"time"
)

func TestSettings_Validate_NegativeMinTTL(t *testing.T) {
	s := &Settings{
		MinTTL:  -1 * time.Second,
		MaxSize: 100,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for negative MinTTL")
	}
}

func TestSettings_Validate_NegativeMaxSize(t *testing.T) {
	s := &Settings{
		MinTTL:  time.Hour,
		MaxSize: -1,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for negative MaxSize")
	}
}

func TestSettings_Validate_BothZero(t *testing.T) {
	s := &Settings{
		MinTTL:  0,
		MaxSize: 0,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error when both MinTTL and MaxSize are zero")
	}
}

func TestSettings_Validate_NegativeAckWait(t *testing.T) {
	s := &Settings{
		MinTTL:  time.Hour,
		MaxSize: 100,
		AckWait: -1 * time.Second,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for negative AckWait")
	}
}

func TestSettings_Validate_NegativeNackDelay(t *testing.T) {
	s := &Settings{
		MinTTL:    time.Hour,
		MaxSize:   100,
		AckWait:   time.Minute,
		NackDelay: -1 * time.Second,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for negative NackDelay")
	}
}

func TestSettings_Validate_NackDelayGreaterThanAckWait(t *testing.T) {
	s := &Settings{
		MinTTL:    time.Hour,
		MaxSize:   100,
		AckWait:   5 * time.Second,
		NackDelay: 10 * time.Second,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error when NackDelay > AckWait")
	}
}

func TestSettings_Validate_NegativeMaxExpiredSubjects(t *testing.T) {
	s := &Settings{
		MinTTL:             time.Hour,
		MaxSize:            100,
		MaxExpiredSubjects: -1,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error for negative MaxExpiredSubjects")
	}
}

func TestSettings_Validate_DeleteOnAckWithoutAckExplicit(t *testing.T) {
	s := &Settings{
		MinTTL:      time.Hour,
		MaxSize:     100,
		AckExplicit: false,
		DeleteOnAck: true,
	}
	err := s.Validate()
	if err == nil {
		t.Error("expected error when DeleteOnAck is true but AckExplicit is false")
	}
}

func TestSettings_Validate_ValidSettings(t *testing.T) {
	s := DefaultSettings()
	err := s.Validate()
	if err != nil {
		t.Errorf("expected no error for valid default settings, got: %v", err)
	}
}

func TestSettings_Validate_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Settings)
		wantErr bool
	}{
		{
			name:    "negative MinTTL",
			modify:  func(s *Settings) { s.MinTTL = -1 * time.Second },
			wantErr: true,
		},
		{
			name:    "negative MaxSize",
			modify:  func(s *Settings) { s.MaxSize = -1 },
			wantErr: true,
		},
		{
			name:    "MinTTL and MaxSize both zero",
			modify:  func(s *Settings) { s.MinTTL = 0; s.MaxSize = 0 },
			wantErr: true,
		},
		{
			name:    "negative AckWait",
			modify:  func(s *Settings) { s.AckWait = -1 * time.Second },
			wantErr: true,
		},
		{
			name:    "negative NackDelay",
			modify:  func(s *Settings) { s.NackDelay = -1 * time.Second },
			wantErr: true,
		},
		{
			name:    "NackDelay greater than AckWait",
			modify:  func(s *Settings) { s.NackDelay = 10 * time.Second; s.AckWait = 5 * time.Second },
			wantErr: true,
		},
		{
			name:    "negative MaxExpiredSubjects",
			modify:  func(s *Settings) { s.MaxExpiredSubjects = -1 },
			wantErr: true,
		},
		{
			name:    "DeleteOnAck without AckExplicit",
			modify:  func(s *Settings) { s.AckExplicit = false; s.DeleteOnAck = true },
			wantErr: true,
		},
		{
			name:    "valid with only MinTTL",
			modify:  func(s *Settings) { s.MinTTL = time.Hour; s.MaxSize = 0 },
			wantErr: false,
		},
		{
			name:    "valid with only MaxSize",
			modify:  func(s *Settings) { s.MinTTL = 0; s.MaxSize = 1000 },
			wantErr: false,
		},
		{
			name:    "valid with both MinTTL and MaxSize",
			modify:  func(s *Settings) { s.MinTTL = time.Hour; s.MaxSize = 1000 },
			wantErr: false,
		},
		{
			name:    "negative IdmpDurationSec",
			modify:  func(s *Settings) { s.IdmpDurationSec = -1 },
			wantErr: true,
		},
		{
			name:    "negative IdmpMaxSize",
			modify:  func(s *Settings) { s.IdmpMaxSize = -1 },
			wantErr: true,
		},
		{
			name:    "valid auto idmp with producerId",
			modify:  func(s *Settings) { s.IdmpMode = IdmpModeAuto },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := DefaultSettings()
			tt.modify(s)
			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
