// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import "testing"

// The Rust port implements ValuesAtQuantiles as a per-quantile map over
// ValueAtQuantile, and ValuesAtQuantilesInto via a snapshot. These tests pin
// the assumption that makes that valid: Go's batch and live variants return
// exactly what ValueAtQuantile returns per quantile. If Go's optimized batch
// ever diverges from the single-quantile path, this fails and the Rust port
// must be revisited.

var parityQuantiles = []float64{0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.9999, 0.99999, 1}

func parityHistograms() []*Histogram {
	hs := []*Histogram{
		New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05}),
		New(Params{Lo: 1000, Hi: 1e9, ErrorBound: 0.10}),
		New(StandardParams),
	}
	// Uniform spread.
	for i := int64(1); i <= 1000; i++ {
		hs[0].Record(i)
	}
	// Geometric spread.
	for i := int64(1); i <= 1000; i++ {
		hs[1].Record(i * 1_000_000)
	}
	// Out-of-range mix.
	hs[2].Record(0)
	hs[2].Record(-5)
	for i := int64(1); i <= 500; i++ {
		hs[2].Record(i * 1_000_000)
	}
	return hs
}

func TestValuesAtQuantilesMatchesIndividual(t *testing.T) {
	for hi, h := range parityHistograms() {
		snap := h.Snapshot()
		batch := snap.ValuesAtQuantiles(parityQuantiles)
		for i, q := range parityQuantiles {
			if got, want := batch[i], snap.ValueAtQuantile(q); got != want {
				t.Errorf("hist %d q=%g: batch=%v individual=%v", hi, q, got, want)
			}
		}
	}
}

func TestValuesAtQuantilesIntoMatchesSnapshot(t *testing.T) {
	for hi, h := range parityHistograms() {
		snap := h.Snapshot()
		want := snap.ValuesAtQuantiles(parityQuantiles)
		var buf [16]float64
		got := h.ValuesAtQuantilesInto(buf[:0], parityQuantiles)
		if len(got) != len(want) {
			t.Fatalf("hist %d: len got=%d want=%d", hi, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("hist %d q=%g: into=%v snapshot=%v", hi, parityQuantiles[i], got[i], want[i])
			}
		}
	}
}
