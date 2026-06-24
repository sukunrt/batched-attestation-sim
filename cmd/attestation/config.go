package main

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ethp2p/simlab/cmd/attestation/node"
)

type SimConfig struct {
	GossipsubParams               node.GossipsubParams `yaml:"gossipsub_params"`
	NumTopics                     int                  `yaml:"num_topics"`
	NumSlots                      int                  `yaml:"num_slots"`
	SlotDurationSeconds           int                  `yaml:"slot_duration_seconds"`
	AttestationDataSize           int                  `yaml:"attestation_data_size"`
	SignatureSize                 int                  `yaml:"signature_size"`
	AttestationValidationDelayMs  int                  `yaml:"attestation_validation_delay_ms"`
	AttestationValidationStdDevMs int                  `yaml:"attestation_validation_std_dev_ms"`
	ValidationBatchWindowMs       int                  `yaml:"validation_batch_window_ms"`
	PerAttestationValidationUs    int                  `yaml:"per_attestation_validation_us"`
	NumAttestors                  int                  `yaml:"num_attestors"`
	PublishDelayMeanMs            int                  `yaml:"publish_delay_mean_ms"`
	StopTimeMinutes               float64              `yaml:"stop_time_minutes"`
	LogLevel                      string               `yaml:"log_level"`
	BandwidthLogFrequencyMs       int                  `yaml:"bandwidth_log_frequency_ms"`

	// Partial messages config
	PublishIntervalMs         int     `yaml:"publish_interval_ms"`
	MaxPeersPerAttestation    int     `yaml:"max_peers_per_attestation"`
	DivergentAttestorFraction float64 `yaml:"divergent_attestor_fraction"`

	// Partial-messages path (lists of attestor IDs + ephemeral iwant).
	UsePartialMessages bool `yaml:"use_partial_messages"`

	// Partial-priority path: size-capped, least-forwarded-first forwarding.
	// An alternative to the default partial push, over the same libp2p
	// partial-messages extension. MaxAttestationsPerMessage caps attestations
	// per outgoing data message (0 = default).
	PartialPriorityMode       bool `yaml:"partial_priority"`
	MaxAttestationsPerMessage int  `yaml:"max_attestations_per_message"`

	// SendAvailableWithData (partial-priority only) piggybacks our validated
	// bitmap onto the first data message to each mesh peer per tick, so peers
	// stop forwarding duplicates back. The bitmap is never sent without data.
	SendAvailableWithData bool `yaml:"send_available_with_data"`
}

func (s *SimConfig) PublishInterval() time.Duration {
	if s.PublishIntervalMs <= 0 {
		return 20 * time.Millisecond
	}
	return time.Duration(s.PublishIntervalMs) * time.Millisecond
}

func (s *SimConfig) EffectiveMaxPeersPerAttestation() int {
	if s.MaxPeersPerAttestation <= 0 {
		return s.GossipsubParams.D * 2
	}
	return s.MaxPeersPerAttestation
}

func (s *SimConfig) EffectiveMaxAttestationsPerMessage() int {
	if s.MaxAttestationsPerMessage <= 0 {
		return node.MaxAttestationsPerMessage
	}
	return s.MaxAttestationsPerMessage
}

func (s *SimConfig) BandwidthLogFrequency() time.Duration {
	return time.Duration(s.BandwidthLogFrequencyMs) * time.Millisecond
}

func LoadConfig(path string) (*SimConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var root struct {
		Simulation SimConfig `yaml:"simulation"`
	}
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	return &root.Simulation, nil
}

// ValidationDelayFunc returns a function that produces validation delays.
//
// Models BLS signature verification latency as:
//
//	total = floor + lognormal_sample
//
// where:
//
//	floor = delay_ms  (irreducible CPU cost of pairing operations)
//	lognormal_mean = delay_ms * 0.2  (so E[total] = delay_ms * 1.2)
//
// The log-normal is parameterized from the desired mean (m) and
// standard deviation (s = std_dev_ms):
//
//	σ² = ln(1 + s²/m²)
//	μ  = ln(m) - σ²/2
//
// When std_dev_ms == 0, returns a fixed delay (no randomness).
func (s *SimConfig) ValidationDelayFunc() func() time.Duration {
	delayMs := float64(s.AttestationValidationDelayMs)
	stdDevMs := float64(s.AttestationValidationStdDevMs)

	if stdDevMs <= 0 {
		fixed := time.Duration(s.AttestationValidationDelayMs) * time.Millisecond
		return func() time.Duration { return fixed }
	}

	floor := delayMs
	m := delayMs * 0.2 // lognormal mean
	sigma2 := math.Log(1 + (stdDevMs*stdDevMs)/(m*m))
	sigma := math.Sqrt(sigma2)
	mu := math.Log(m) - sigma2/2

	return func() time.Duration {
		sample := math.Exp(mu + sigma*rand.NormFloat64())
		totalMs := floor + sample
		return time.Duration(totalMs * float64(time.Millisecond))
	}
}

func (s *SimConfig) PublishDelayFunc() func() time.Duration {
	if s.PublishDelayMeanMs <= 0 {
		return func() time.Duration { return 0 }
	}
	mean := float64(s.PublishDelayMeanMs)
	return func() time.Duration {
		delay := rand.ExpFloat64() * mean
		return time.Duration(delay * float64(time.Millisecond))
	}
}

func (s *SimConfig) PerAttestationValidation() time.Duration {
	if s.PerAttestationValidationUs <= 0 {
		return 100 * time.Microsecond
	}
	return time.Duration(s.PerAttestationValidationUs) * time.Microsecond
}

func (s *SimConfig) ValidationBatchWindow() time.Duration {
	if s.ValidationBatchWindowMs <= 0 {
		return 5 * time.Millisecond
	}
	return time.Duration(s.ValidationBatchWindowMs) * time.Millisecond
}

func (s *SimConfig) SlotDuration() time.Duration {
	return time.Duration(s.SlotDurationSeconds) * time.Second
}
