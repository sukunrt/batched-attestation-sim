package main

import (
	"testing"
	"time"
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
