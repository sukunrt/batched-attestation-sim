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

	// att_propagation path: a native libp2p protocol (no gossipsub) with three
	// persistent per-topic streams (push / bitmap / control). Mutually exclusive
	// with use_partial_messages and partial_priority. Zero bitmap-mesh sizes are
	// literal; the other zero-valued tunables fall back to spec defaults.
	AttPropagation bool `yaml:"att_propagation"`

	// DisableBitmapSends keeps the bitmap mesh formed but suppresses outbound
	// bitmap advertisements. Bitmap-mesh peers still receive spare-capacity data.
	DisableBitmapSends bool `yaml:"disable_bitmap_sends"`

	// §C1 push-mesh sizes (default 4/5/5: Dlow=top-up trigger, D=Dhigh=hard cap).
	AttPropPushDlow  int `yaml:"attprop_push_dlow"`
	AttPropPushD     int `yaml:"attprop_push_d"`
	AttPropPushDhigh int `yaml:"attprop_push_dhigh"`

	// §C1 bitmap-mesh sizes (default 14/16/16).
	AttPropBitmapDlow  int `yaml:"attprop_bitmap_dlow"`
	AttPropBitmapD     int `yaml:"attprop_bitmap_d"`
	AttPropBitmapDhigh int `yaml:"attprop_bitmap_dhigh"`

	// §F1 send budget B (default 4), initial holder-count index capacity
	// (default 30).
	AttPropSendBudgetB    int `yaml:"attprop_send_budget_b"`
	AttPropMaxPeersPerAtt int `yaml:"attprop_max_peers_per_att"`

	// §F4 tick (default 20ms), §D2 bitmap floor (default 50ms),
	// §C2 heartbeat (default 700ms), §C7 prune backoff (default 60s).
	AttPropTickIntervalMs        int `yaml:"attprop_tick_interval_ms"`
	AttPropBitmapFloorIntervalMs int `yaml:"attprop_bitmap_floor_interval_ms"`
	AttPropHeartbeatIntervalMs   int `yaml:"attprop_heartbeat_interval_ms"`
	AttPropPruneBackoffSeconds   int `yaml:"attprop_prune_backoff_seconds"`
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

// AttPropConfig resolves the att_propagation tunables into a node.AttPropParams.
// Bitmap mesh sizes are literal, including zero; the other zero-valued tunables
// fall back to spec defaults (§C1/§D2/§F). Only the values are resolved here;
// the topic list / committee size / timing are filled by the caller from the
// rest of the config.
func (s *SimConfig) AttPropConfig() node.AttPropParams {
	pick := func(v, def int) int {
		if v <= 0 {
			return def
		}
		return v
	}
	ms := func(v, defMs int) time.Duration {
		return time.Duration(pick(v, defMs)) * time.Millisecond
	}
	return node.AttPropParams{
		PushDlow:            pick(s.AttPropPushDlow, 4),
		PushD:               pick(s.AttPropPushD, 5),
		PushDhigh:           pick(s.AttPropPushDhigh, 5),
		BitmapDlow:          s.AttPropBitmapDlow,
		BitmapD:             s.AttPropBitmapD,
		BitmapDhigh:         s.AttPropBitmapDhigh,
		DisableBitmapSends:  s.DisableBitmapSends,
		SendBudgetB:         pick(s.AttPropSendBudgetB, 4),
		MaxAttsPerMessage:   s.EffectiveMaxAttestationsPerMessage(),
		MaxPeersPerAtt:      pick(s.AttPropMaxPeersPerAtt, 30),
		TickInterval:        ms(s.AttPropTickIntervalMs, 20),
		BitmapFloorInterval: ms(s.AttPropBitmapFloorIntervalMs, 50),
		HeartbeatInterval:   ms(s.AttPropHeartbeatIntervalMs, 700),
		PruneBackoff:        time.Duration(pick(s.AttPropPruneBackoffSeconds, 60)) * time.Second,
	}
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
