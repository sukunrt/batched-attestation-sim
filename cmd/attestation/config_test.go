package main

import (
	"testing"
	"time"

	"github.com/ethp2p/simlab/cmd/attestation/node"
)

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
