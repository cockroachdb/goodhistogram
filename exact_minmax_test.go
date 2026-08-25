// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExactMinMaxSummary(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})

	// Includes an underflow (50), an overflow (20000), a zero, a negative,
	// and in-range values. Min/Max and Sum must account for all of them.
	values := []int64{500, 50, 20000, 0, -5, 1000, 300}
	var wantSum int64
	for _, v := range values {
		h.Record(v)
		wantSum += v
	}

	s := h.Summary()
	require.Equal(t, uint64(len(values)), s.Count)
	require.Equal(t, wantSum, s.Sum)
	require.Equal(t, int64(-5), s.Min, "min must include out-of-range/negative values")
	require.Equal(t, int64(20000), s.Max, "max must include overflow values")
}

func TestExactMinMaxEmpty(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})

	s := h.Summary()
	require.Equal(t, uint64(0), s.Count)
	require.Equal(t, int64(0), s.Sum)
	require.Equal(t, int64(0), s.Min)
	require.Equal(t, int64(0), s.Max)

	snap := h.Snapshot()
	require.Equal(t, uint64(0), snap.TotalCount)
	require.Equal(t, 0.0, snap.ValueAtQuantile(0))
	require.Equal(t, 0.0, snap.ValueAtQuantile(0.5))
	require.Equal(t, 0.0, snap.ValueAtQuantile(1.0))
}

func TestExactMinMaxSingleZero(t *testing.T) {
	// Recording a single 0 must be distinguishable from empty: min==max==0 but
	// with Count==1. The minVal<=maxVal invariant makes this work.
	h := NewWithExactMinMax(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
	h.Record(0)

	s := h.Summary()
	require.Equal(t, uint64(1), s.Count)
	require.Equal(t, int64(0), s.Min)
	require.Equal(t, int64(0), s.Max)
}

func TestExactMinMaxQuantileEndpoints(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
	// 40 is below lo, 50000 is above hi — both are the true extremes and must
	// be returned exactly at q=0 / q=1 even though they fall outside [lo, hi].
	h.Record(40)
	h.Record(50000)
	for i := int64(200); i <= 1000; i += 10 {
		h.Record(i)
	}

	snap := h.Snapshot()

	require.Equal(t, float64(40), snap.ValueAtQuantile(0),
		"q=0 must return exact min, even below lo")
	require.Equal(t, float64(50000), snap.ValueAtQuantile(1.0),
		"q=1 must return exact max, even above hi")

	// Interior quantiles must delegate unchanged to the base estimate.
	for _, q := range []float64{0.25, 0.5, 0.75, 0.99} {
		require.Equalf(t, snap.Snapshot.ValueAtQuantile(q), snap.ValueAtQuantile(q),
			"interior q=%.2f must match base estimate", q)
	}
}

func TestExactMinMaxBatchEndpoints(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
	h.Record(40)
	h.Record(50000)
	for i := int64(200); i <= 1000; i += 10 {
		h.Record(i)
	}
	snap := h.Snapshot()

	qs := []float64{0, 0.5, 0.99, 1.0}
	batch := snap.ValuesAtQuantiles(qs)
	for i, q := range qs {
		require.InDeltaf(t, snap.ValueAtQuantile(q), batch[i], 1e-9, "q=%.2f", q)
	}
	require.Equal(t, float64(40), batch[0])
	require.Equal(t, float64(50000), batch[3])
}

func TestExactMinMaxMerge(t *testing.T) {
	mk := func(vals ...int64) ExactSnapshot {
		h := NewWithExactMinMax(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		for _, v := range vals {
			h.Record(v)
		}
		return h.Snapshot()
	}

	a := mk(10, 500, 3000)
	b := mk(50, 90000)

	m := a.Merge(&b)
	require.Equal(t, uint64(5), m.TotalCount)
	require.Equal(t, int64(10), m.Min, "min-of-mins")
	require.Equal(t, int64(90000), m.Max, "max-of-maxes")

	// Merging with an empty operand preserves the non-empty extremes.
	empty := mk()
	m2 := a.Merge(&empty)
	require.Equal(t, int64(10), m2.Min)
	require.Equal(t, int64(3000), m2.Max)
	require.Equal(t, a.TotalCount, m2.TotalCount)

	// Empty on the left as well.
	m3 := empty.Merge(&a)
	require.Equal(t, int64(10), m3.Min)
	require.Equal(t, int64(3000), m3.Max)
}

func TestExactMinMaxReset(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
	for i := int64(1); i <= 500; i++ {
		h.Record(i)
	}
	h.Reset()

	s := h.Summary()
	require.Equal(t, uint64(0), s.Count)
	require.Equal(t, int64(0), s.Min)
	require.Equal(t, int64(0), s.Max)

	// Recording after reset tracks fresh extremes.
	h.Record(42)
	h.Record(7)
	s = h.Summary()
	require.Equal(t, int64(7), s.Min)
	require.Equal(t, int64(42), s.Max)
}

func TestExactMinMaxConcurrent(t *testing.T) {
	h := NewWithExactMinMax(Params{Lo: 1, Hi: 1e9, ErrorBound: 0.05})
	const goroutines = 8
	const perG = 20000

	// Each goroutine records values in [2, 1e6]; goroutine 0 additionally
	// records a known global min (1) and max (1e9) exactly once.
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			if seed == 0 {
				h.Record(1)
				h.Record(1e9)
			}
			for i := 0; i < perG; i++ {
				h.Record(rng.Int63n(1e6-1) + 2)
			}
		}(int64(g))
	}
	wg.Wait()

	s := h.Summary()
	require.Equal(t, uint64(goroutines*perG+2), s.Count)
	require.Equal(t, int64(1), s.Min)
	require.Equal(t, int64(1e9), s.Max)
}

func TestExactMinMaxEmbeddingIntact(t *testing.T) {
	// The wrapper must not disturb any base behavior: sum/count, the base
	// Snapshot, Mean, and the Prometheus export all still work.
	h := NewWithExactMinMax(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
	h.Record(100)
	h.Record(200)
	h.Record(300)

	// Base snapshot via the embedded Histogram is unchanged.
	base := h.Histogram.Snapshot()
	require.Equal(t, uint64(3), base.TotalCount)
	require.Equal(t, int64(600), base.TotalSum)

	// ExactSnapshot promotes Snapshot methods (Mean, export) unchanged.
	snap := h.Snapshot()
	require.InDelta(t, 200.0, snap.Mean(), 1e-9)
	ph := snap.ToPrometheusHistogram()
	require.Equal(t, uint64(3), ph.GetSampleCount())
	require.Equal(t, float64(600), ph.GetSampleSum())
	require.Equal(t, h.Schema(), ph.GetSchema())
}
