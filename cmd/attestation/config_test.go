package main

import (
	"testing"
	"time"

	"github.com/ethp2p/simlab/cmd/attestation/node"
)

func TestCheckModeExclusion(t *testing.T) {
	// att_propagation alone or with no other mode is fine; the partial modes
	// coexist (resolved by precedence elsewhere). Only att_prop + a partial mode
	// is rejected.
	ok := [][3]bool{
		{false, false, false}, // classic
		{true, false, false},  // partial only
		{false, true, false},  // priority only
		{true, true, false},   // both partial (precedence picks priority)
		{false, false, true},  // att_propagation only
	}
	for _, c := range ok {
		if err := checkModeExclusion(c[0], c[1], c[2]); err != nil {
			t.Fatalf("checkModeExclusion(%v) = %v, want nil", c, err)
		}
	}

	bad := [][3]bool{
		{true, false, true}, // partial + att_prop
		{false, true, true}, // priority + att_prop
		{true, true, true},  // both + att_prop
	}
	for _, c := range bad {
		if err := checkModeExclusion(c[0], c[1], c[2]); err == nil {
			t.Fatalf("checkModeExclusion(%v) = nil, want error", c)
		}
	}
}

func TestAttPropConfigDefaults(t *testing.T) {
	// An empty SimConfig must resolve to the spec defaults (§C1/§D2/§F).
	got := (&SimConfig{}).AttPropConfig()
	want := node.AttPropParams{
		PushDlow: 4, PushD: 5, PushDhigh: 5,
		BitmapDlow: 0, BitmapD: 0, BitmapDhigh: 0,
		SendBudgetB: 4, MaxAttsPerMessage: node.MaxAttestationsPerMessage, MaxPeersPerAtt: 30,
		TickInterval:        20 * time.Millisecond,
		BitmapFloorInterval: 50 * time.Millisecond,
		HeartbeatInterval:   700 * time.Millisecond,
		PruneBackoff:        60 * time.Second,
	}
	if got != want {
		t.Fatalf("AttPropConfig() defaults = %+v, want %+v", got, want)
	}

	// Explicit overrides win, including literal bitmap mesh sizes.
	cfg := &SimConfig{
		AttPropPushD: 7, AttPropBitmapD: 20, AttPropSendBudgetB: 2,
		AttPropTickIntervalMs: 50, AttPropPruneBackoffSeconds: 30,
		MaxAttestationsPerMessage: 12,
	}
	o := cfg.AttPropConfig()
	if o.PushD != 7 || o.BitmapD != 20 || o.SendBudgetB != 2 ||
		o.TickInterval != 50*time.Millisecond || o.PruneBackoff != 30*time.Second ||
		o.MaxAttsPerMessage != 12 {
		t.Fatalf("AttPropConfig() overrides not applied: %+v", o)
	}

	cfg.DisableBitmapSends = true
	if !cfg.AttPropConfig().DisableBitmapSends {
		t.Fatalf("AttPropConfig() did not preserve DisableBitmapSends")
	}
}

func TestPublishDelayFunc_Zero(t *testing.T) {
	cfg := &SimConfig{PublishDelayMeanMs: 0}
	f := cfg.PublishDelayFunc()
	for range 100 {
		if d := f(); d != 0 {
			t.Fatalf("expected 0 delay, got %v", d)
		}
	}
}

func TestPublishDelayFunc_Negative(t *testing.T) {
	cfg := &SimConfig{PublishDelayMeanMs: -10}
	f := cfg.PublishDelayFunc()
	if d := f(); d != 0 {
		t.Fatalf("expected 0 delay for negative mean, got %v", d)
	}
}

func TestPublishDelayFunc_Exponential(t *testing.T) {
	const (
		meanMs  = 300
		samples = 10_000
	)
	cfg := &SimConfig{PublishDelayMeanMs: meanMs}
	f := cfg.PublishDelayFunc()

	var total time.Duration
	for range samples {
		d := f()
		if d < 0 {
			t.Fatalf("got negative delay: %v", d)
		}
		total += d
	}

	gotMean := total.Milliseconds() / samples
	// Allow 20% tolerance on the mean
	lo, hi := int64(float64(meanMs)*0.8), int64(float64(meanMs)*1.2)
	if gotMean < lo || gotMean > hi {
		t.Fatalf("sample mean %dms outside [%d, %d]ms", gotMean, lo, hi)
	}
}

func TestParseGossipsubParams(t *testing.T) {
	want := node.GossipsubParams{D: 12, Dlow: 8, Dhigh: 16}
	// Key order should not matter; whitespace is tolerated.
	for _, s := range []string{
		"Dlow:8,D:12,Dhigh:16",
		"D:12,Dhigh:16,Dlow:8",
		" Dlow : 8 , D : 12 , Dhigh : 16 ",
	} {
		got, err := parseGossipsubParams(s)
		if err != nil {
			t.Fatalf("parseGossipsubParams(%q) errored: %v", s, err)
		}
		if got != want {
			t.Fatalf("parseGossipsubParams(%q) = %+v, want %+v", s, got, want)
		}
	}

	for _, s := range []string{
		"D:12,Dhigh:16",        // missing Dlow
		"Dlow:8,D:x,Dhigh:16",  // non-integer
		"Dlow:8,D:12,Dhi:16",   // unknown key
		"Dlow:0,D:12,Dhigh:16", // non-positive
		"Dlow:12,D:8,Dhigh:16", // Dlow > D
		"Dlow:8,D:12,Dhigh:10", // D > Dhigh
	} {
		if _, err := parseGossipsubParams(s); err == nil {
			t.Fatalf("parseGossipsubParams(%q) = nil error, want failure", s)
		}
	}
}
