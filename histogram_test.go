// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPickSchema(t *testing.T) {
	tests := []struct {
		desiredError float64
		wantSchema   int32
	}{
		{0.35, 0},  // 33.3% error for schema 0
		{0.10, 2},  // 8.6% error for schema 2
		{0.05, 3},  // 4.3% error for schema 3
		{0.03, 4},  // 2.17% error for schema 4
		{0.02, 5},  // schema 4 is 2.17% > 2%, so need schema 5 (1.08%)
		{0.015, 5}, // 1.08% for schema 5
		{0.005, 7}, // schema 6 is 0.54% > 0.5%, so need schema 7 (0.27%)
		{0.003, 7}, // 0.27% for schema 7
		{0.002, 8}, // 0.14% for schema 8
		{0.001, 8}, // still schema 8 (finest available)
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("error=%.3f", tt.desiredError), func(t *testing.T) {
			got := pickSchema(tt.desiredError)
			require.Equal(t, tt.wantSchema, got)
			// Verify the actual error is at or below desired, unless we're at
			// schema 8 (the finest available) and the desired error is below
			// what's achievable.
			actualErr := schemaRelativeError(got)
			if got < 8 {
				require.LessOrEqualf(t, actualErr, tt.desiredError,
					"schema %d error %.6f exceeds desired %.6f", got, actualErr, tt.desiredError)
			}
		})
	}
}

func TestNewConfig(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		cfg := newConfig(1e4, 1e16, 0.05)
		require.Equal(t, int32(3), cfg.schema) // 4.3% error
		require.Greater(t, cfg.numBuckets, 0)
		require.Equal(t, cfg.numBuckets+1, len(cfg.boundaries))
		require.Equal(t, 1e4, cfg.boundaries[0])
		require.Equal(t, 1e16, cfg.boundaries[cfg.numBuckets])

		// Boundaries should be monotonically increasing.
		for i := 1; i < len(cfg.boundaries); i++ {
			require.Greaterf(t, cfg.boundaries[i], cfg.boundaries[i-1],
				"boundary[%d]=%v not > boundary[%d]=%v", i, cfg.boundaries[i], i-1, cfg.boundaries[i-1])
		}
	})

	t.Run("no zero-width bucket when lo is on boundary", func(t *testing.T) {
		// When lo lands exactly on a Prometheus bucket boundary (e.g., powers
		// of 2), the first bucket must not be zero-width.
		for _, lo := range []float64{1.0, 2.0, 4.0, 8.0, 1024.0} {
			cfg := newConfig(lo, lo*1e6, 0.05)
			require.Greaterf(t, cfg.boundaries[1], cfg.boundaries[0],
				"lo=%v: first bucket is zero-width [%v, %v]", lo, cfg.boundaries[0], cfg.boundaries[1])
		}
	})

	t.Run("panics on invalid input", func(t *testing.T) {
		require.Panics(t, func() { newConfig(0, 100, 0.05) })
		require.Panics(t, func() { newConfig(-1, 100, 0.05) })
		require.Panics(t, func() { newConfig(100, 50, 0.05) })
		require.Panics(t, func() { newConfig(1, 100, 0) })
	})
}

func TestRecordAndSnapshot(t *testing.T) {
	h := New(Params{Lo: 1e3, Hi: 1e9, ErrorBound: 0.05})

	// Record a range of values.
	values := []int64{1000, 5000, 10000, 100000, 1000000, 10000000, 100000000, 999999999}
	var expectedSum int64
	for _, v := range values {
		h.Record(v)
		expectedSum += v
	}

	snap := h.Snapshot()
	require.Equal(t, uint64(len(values)), snap.TotalCount)
	require.Equal(t, expectedSum, snap.TotalSum)

	// All values should be in some bucket.
	var totalInBuckets uint64
	for _, c := range snap.Counts {
		totalInBuckets += c
	}
	require.Equal(t, uint64(len(values)), totalInBuckets)
	require.Equal(t, uint64(0), snap.ZeroCount)
	require.Equal(t, uint64(0), snap.Underflow)
	require.Equal(t, uint64(0), snap.Overflow)
}

func TestRecordBoundaryValues(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})

	// Record value exactly at lo.
	h.Record(100)
	// Record value exactly at hi.
	h.Record(10000)

	snap := h.Snapshot()
	require.Equal(t, uint64(2), snap.TotalCount)
	require.Equal(t, uint64(0), snap.Underflow)
	require.Equal(t, uint64(0), snap.Overflow)
}

func TestRecordOutOfRange(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})

	// Below range.
	h.Record(50)
	// Above range.
	h.Record(20000)
	// Zero.
	h.Record(0)
	// Negative.
	h.Record(-5)

	snap := h.Snapshot()
	require.Equal(t, uint64(4), snap.TotalCount)
	require.Equal(t, uint64(1), snap.Underflow)
	require.Equal(t, uint64(1), snap.Overflow)
	require.Equal(t, uint64(2), snap.ZeroCount) // 0 and -5
}

func TestRecordAccuracy(t *testing.T) {
	// Verify that each recorded value lands in a bucket whose boundaries
	// contain the value (within floating-point tolerance).
	h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
	cfg := h.cfg

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		// Log-uniform distribution over [1, 1e6].
		v := int64(math.Exp(rng.Float64() * math.Log(1e6)))
		if v < 1 {
			v = 1
		}
		h.Record(v)
	}

	snap := h.Snapshot()
	// For each non-zero bucket, verify the bucket boundaries make sense
	// relative to the schema's error bound.
	gamma := math.Pow(2, math.Pow(2, float64(-cfg.schema)))
	for i, c := range snap.Counts {
		if c == 0 {
			continue
		}
		lo := cfg.boundaries[i]
		hi := cfg.boundaries[i+1]
		ratio := hi / lo
		// The ratio should be approximately gamma (within floating point).
		// Allow up to 2x gamma for edge buckets that may be clamped.
		require.LessOrEqualf(t, ratio, gamma*1.01,
			"bucket %d [%.2f, %.2f] ratio %.4f exceeds gamma %.4f", i, lo, hi, ratio, gamma)
	}
}

func TestReset(t *testing.T) {
	t.Run("zeroes all counters", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		// Record a mix of in-range, underflow, overflow, and zero values.
		for i := int64(500); i <= 600; i++ {
			h.Record(i)
		}
		h.Record(0)
		h.Record(-1)
		h.Record(50)
		h.Record(20000)

		// Sanity check: non-empty before reset.
		snap := h.Snapshot()
		require.Greater(t, snap.TotalCount, uint64(0))

		h.Reset()

		snap = h.Snapshot()
		require.Equal(t, uint64(0), snap.TotalCount)
		require.Equal(t, uint64(0), snap.ZeroCount)
		require.Equal(t, uint64(0), snap.Underflow)
		require.Equal(t, uint64(0), snap.Overflow)
		require.Equal(t, int64(0), snap.TotalSum)
		for i, c := range snap.Counts {
			require.Equalf(t, uint64(0), c, "counts[%d] not zero after reset", i)
		}
	})

	t.Run("records correctly after reset", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 500; i++ {
			h.Record(i)
		}
		h.Reset()

		// Record new values and verify they're tracked correctly.
		h.Record(100)
		h.Record(200)
		h.Record(300)

		snap := h.Snapshot()
		require.Equal(t, uint64(3), snap.TotalCount)
		require.Equal(t, int64(600), snap.TotalSum)
		require.Equal(t, uint64(0), snap.Underflow)
		require.Equal(t, uint64(0), snap.Overflow)
		require.Equal(t, uint64(0), snap.ZeroCount)
	})

	t.Run("no allocation", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		allocs := testing.AllocsPerRun(100, func() {
			h.Reset()
		})
		require.Equal(t, 0.0, allocs)
	})
}

func TestConcurrentRecord(t *testing.T) {
	h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
	const goroutines = 8
	const recordsPerGoroutine = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < recordsPerGoroutine; i++ {
				v := rng.Int63n(1e6) + 1
				h.Record(v)
			}
		}(int64(g))
	}
	wg.Wait()

	snap := h.Snapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), snap.TotalCount)
}

func TestQuantileUniform(t *testing.T) {
	h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
	// Record values 1..1000.
	for i := int64(1); i <= 1000; i++ {
		h.Record(i)
	}

	snap := h.Snapshot()
	schema := h.Schema()
	gamma := math.Pow(2, math.Pow(2, float64(-schema)))
	maxRelError := (gamma - 1) / (gamma + 1)

	// Test quantiles.
	for _, q := range []float64{0.50, 0.75, 0.90, 0.95, 0.99} {
		got := snap.ValueAtQuantile(q)
		expected := q * 1000.0
		relErr := math.Abs(got-expected) / expected
		// Allow a generous bound: the histogram error plus some interpolation error.
		require.LessOrEqualf(t, relErr, maxRelError*3,
			"q%.0f: got=%.1f expected=%.1f relErr=%.4f", q*100, got, expected, relErr)
	}
}

func TestQuantileExponential(t *testing.T) {
	h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
	rng := rand.New(rand.NewSource(42))
	n := 100000
	// Exponential distribution with rate=1, values in nanoseconds.
	values := make([]float64, n)
	for i := 0; i < n; i++ {
		v := rng.ExpFloat64() * 10000 // mean = 10000
		if v < 1 {
			v = 1
		}
		if v > 1e6 {
			v = 1e6
		}
		values[i] = v
		h.Record(int64(v))
	}

	snap := h.Snapshot()
	// Verify median is in a reasonable range.
	median := snap.ValueAtQuantile(0.50)
	// Median of Exp(1/10000) ≈ 10000 * ln(2) ≈ 6931
	require.InDelta(t, 6931, median, 2000, "median=%.1f", median)
}

func TestQuantileEdgeCases(t *testing.T) {
	t.Run("empty histogram", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		snap := h.Snapshot()
		require.Equal(t, 0.0, snap.ValueAtQuantile(0.50))
	})

	t.Run("single value", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		h.Record(500)
		snap := h.Snapshot()
		got := snap.ValueAtQuantile(0.50)
		// Should be somewhere in the bucket containing 500.
		require.InDelta(t, 500, got, 100)
	})

	t.Run("q0 and q100", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.GreaterOrEqual(t, snap.ValueAtQuantile(0), 1.0)
		require.LessOrEqual(t, snap.ValueAtQuantile(1.0), 1000.0)
	})

	t.Run("all zeros", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := 0; i < 100; i++ {
			h.Record(0)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(100), snap.ZeroCount)
		require.Equal(t, uint64(100), snap.TotalCount)
		// All observations are zeros (below lo), so every quantile
		// should clamp to lo.
		require.Equal(t, 1.0, snap.ValueAtQuantile(0.50))
		require.Equal(t, 1.0, snap.ValueAtQuantile(0.99))
	})

	t.Run("all underflow", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		for i := int64(1); i <= 50; i++ {
			h.Record(i) // all below lo=100
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(50), snap.Underflow)
		require.Equal(t, uint64(50), snap.TotalCount)
		// Every quantile should clamp to lo, including p100.
		require.Equal(t, 100.0, snap.ValueAtQuantile(0))
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.50))
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.99))
		require.Equal(t, 100.0, snap.ValueAtQuantile(1.0))
	})

	t.Run("all overflow", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 100, ErrorBound: 0.05})
		for i := int64(200); i <= 300; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(101), snap.Overflow)
		require.Equal(t, uint64(101), snap.TotalCount)
		// Every quantile should clamp to hi, including p0.
		require.Equal(t, 100.0, snap.ValueAtQuantile(0))
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.50))
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.99))
		require.Equal(t, 100.0, snap.ValueAtQuantile(1.0))
	})

	t.Run("underflow with in-range values", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		// 80 underflow values, 20 in-range values.
		for i := int64(1); i <= 80; i++ {
			h.Record(i) // underflow
		}
		for i := int64(500); i <= 519; i++ {
			h.Record(i) // in range
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(80), snap.Underflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p50 is at rank 50. With 80 underflow values, rank 50
		// falls in the underflow region — should clamp to lo.
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.50))

		// p90 is at rank 90. Past the 80 underflow values, it falls
		// in the in-range buckets containing 500–519.
		p90 := snap.ValueAtQuantile(0.90)
		require.Greater(t, p90, 100.0, "p90 should be in the in-range region")
		require.InDelta(t, 510, p90, 50, "p90 should be near the in-range values")
	})

	t.Run("overflow with in-range values", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 100, ErrorBound: 0.05})
		// 50 in-range values, 50 overflow values.
		for i := int64(1); i <= 50; i++ {
			h.Record(i) // in range
		}
		for i := int64(200); i <= 249; i++ {
			h.Record(i) // overflow
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(50), snap.Overflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p50 falls in the in-range buckets.
		p50 := snap.ValueAtQuantile(0.50)
		require.Greater(t, p50, 0.0)
		require.LessOrEqual(t, p50, 100.0)

		// p99 should be in the overflow region — clamps to hi.
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.99))
	})

	t.Run("mixed zeros underflow overflow and in-range", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		// 10 zeros, 10 underflow, 60 in-range, 20 overflow.
		for i := 0; i < 10; i++ {
			h.Record(0)
		}
		for i := int64(1); i <= 10; i++ {
			h.Record(i)
		}
		for i := int64(500); i <= 559; i++ {
			h.Record(i)
		}
		for i := int64(20000); i <= 20019; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		require.Equal(t, uint64(10), snap.ZeroCount)
		require.Equal(t, uint64(10), snap.Underflow)
		require.Equal(t, uint64(20), snap.Overflow)
		require.Equal(t, uint64(100), snap.TotalCount)

		// p10 is at rank 10, which falls at the boundary between
		// zeros and underflow — should clamp to lo.
		require.Equal(t, 100.0, snap.ValueAtQuantile(0.10))

		// p50 is at rank 50. With 20 sub-lo values (zeros +
		// underflow), rank 50 falls in the in-range buckets.
		p50 := snap.ValueAtQuantile(0.50)
		require.Greater(t, p50, 100.0)
		require.Less(t, p50, 10000.0)

		// p95 is at rank 95. With 80 non-overflow values, rank 95
		// falls in the overflow region — should clamp to hi.
		require.Equal(t, 10000.0, snap.ValueAtQuantile(0.95))
	})
}

func TestValuesAtQuantiles(t *testing.T) {
	t.Run("matches individual calls", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 10000; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		qs := []float64{0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.0}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9,
				"q=%.3f: single=%.6f batch=%.6f", q, single, batch[i])
		}
	})

	t.Run("preserves input order", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		// Deliberately unsorted quantiles.
		qs := []float64{0.99, 0.25, 0.75, 0.50, 0.10}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9,
				"q=%.2f at index %d", q, i)
		}
	})

	t.Run("empty histogram", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		snap := h.Snapshot()
		results := snap.ValuesAtQuantiles([]float64{0, 0.5, 1.0})
		for i, r := range results {
			require.Equalf(t, 0.0, r, "index %d", i)
		}
	})

	t.Run("empty quantiles slice", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		h.Record(500)
		snap := h.Snapshot()
		require.Empty(t, snap.ValuesAtQuantiles([]float64{}))
		require.Empty(t, snap.ValuesAtQuantiles(nil))
	})

	t.Run("single quantile", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		batch := snap.ValuesAtQuantiles([]float64{0.50})
		single := snap.ValueAtQuantile(0.50)
		require.InDelta(t, single, batch[0], 1e-9)
	})

	t.Run("all zeros", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := 0; i < 100; i++ {
			h.Record(0)
		}
		snap := h.Snapshot()
		qs := []float64{0, 0.50, 0.99, 1.0}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9, "q=%.2f", q)
		}
	})

	t.Run("all underflow", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		for i := int64(1); i <= 50; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		qs := []float64{0, 0.50, 0.99, 1.0}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9, "q=%.2f", q)
		}
	})

	t.Run("all overflow", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 100, ErrorBound: 0.05})
		for i := int64(200); i <= 300; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		qs := []float64{0, 0.50, 0.99, 1.0}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9, "q=%.2f", q)
		}
	})

	t.Run("mixed underflow overflow and in-range", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		for i := 0; i < 10; i++ {
			h.Record(0)
		}
		for i := int64(1); i <= 10; i++ {
			h.Record(i)
		}
		for i := int64(500); i <= 559; i++ {
			h.Record(i)
		}
		for i := int64(20000); i <= 20019; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		qs := []float64{0, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0}
		batch := snap.ValuesAtQuantiles(qs)
		for i, q := range qs {
			single := snap.ValueAtQuantile(q)
			require.InDeltaf(t, single, batch[i], 1e-9, "q=%.2f", q)
		}
	})

	t.Run("duplicate quantiles", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		qs := []float64{0.50, 0.50, 0.50}
		batch := snap.ValuesAtQuantiles(qs)
		single := snap.ValueAtQuantile(0.50)
		for i := range qs {
			require.InDeltaf(t, single, batch[i], 1e-9, "index %d", i)
		}
	})
}

func TestMeanAndTotal(t *testing.T) {
	h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
	h.Record(100)
	h.Record(200)
	h.Record(300)

	snap := h.Snapshot()
	require.InDelta(t, 200.0, snap.Mean(), 0.001)

	count, sum := snap.Total()
	require.Equal(t, int64(3), count)
	require.InDelta(t, 600.0, sum, 0.001)
}

// decodeDeltaCounts decodes a Prometheus native histogram's delta-encoded
// positive bucket counts, returning the absolute count for each bucket
// across all spans. This is the inverse of the encoding in export.go.
func decodeDeltaCounts(
	spans []*prometheusgo.BucketSpan, deltas []int64,
) (keys []int, counts []uint64) {
	var prevCount int64
	deltaIdx := 0
	for _, sp := range spans {
		key := int(sp.GetOffset())
		if len(keys) > 0 {
			// Subsequent spans: offset is relative to end of previous span.
			key += keys[len(keys)-1] + 1
		}
		for j := 0; j < int(sp.GetLength()); j++ {
			prevCount += deltas[deltaIdx]
			deltaIdx++
			keys = append(keys, key)
			counts = append(counts, uint64(prevCount))
			key++
		}
	}
	return keys, counts
}

func TestPrometheusExport(t *testing.T) {
	t.Run("sample count and sum", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()
		require.Equal(t, uint64(100), ph.GetSampleCount())
		require.Equal(t, float64(5050), ph.GetSampleSum())
	})

	t.Run("schema matches config", func(t *testing.T) {
		for _, errBound := range []float64{0.10, 0.05, 0.02, 0.005} {
			h := New(Params{Lo: 1, Hi: 1000, ErrorBound: errBound})
			h.Record(500)
			snap := h.Snapshot()
			ph := snap.ToPrometheusHistogram()
			require.Equal(t, h.Schema(), ph.GetSchema(),
				"errBound=%f", errBound)
		}
	})

	t.Run("zero bucket", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		h.Record(0)
		h.Record(0)
		h.Record(-5)
		h.Record(500)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(3), ph.GetZeroCount())
		require.Equal(t, math.SmallestNonzeroFloat64, ph.GetZeroThreshold())
		require.Equal(t, uint64(4), ph.GetSampleCount())
	})

	t.Run("conventional buckets include out-of-range observations", func(t *testing.T) {
		h := New(Params{Lo: 100, Hi: 10000, ErrorBound: 0.05})
		// 2 zeros, 3 underflow, 5 in-range, 4 overflow = 14 total.
		h.Record(0)
		h.Record(0)
		for i := int64(1); i <= 3; i++ {
			h.Record(i) // underflow
		}
		for i := int64(500); i <= 504; i++ {
			h.Record(i) // in-range
		}
		for i := int64(20000); i <= 20003; i++ {
			h.Record(i) // overflow
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// Every conventional bucket's cumulative count must include zeros
		// and underflow (5 observations below all upper bounds).
		require.GreaterOrEqual(t, ph.Bucket[0].GetCumulativeCount(), uint64(5),
			"first bucket must include zeros + underflow")

		// +Inf bucket must equal SampleCount.
		infBucket := ph.Bucket[len(ph.Bucket)-1]
		require.Equal(t, math.Inf(1), infBucket.GetUpperBound())
		require.Equal(t, uint64(14), infBucket.GetCumulativeCount())
		require.Equal(t, uint64(14), ph.GetSampleCount())
	})

	t.Run("conventional buckets are cumulative", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		for i := int64(1); i <= 100; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.NotEmpty(t, ph.Bucket)
		var prev uint64
		for i, b := range ph.Bucket {
			require.GreaterOrEqualf(t, b.GetCumulativeCount(), prev,
				"bucket[%d] cumulative count %d < previous %d",
				i, b.GetCumulativeCount(), prev)
			prev = b.GetCumulativeCount()
		}
		// +Inf bucket must be last and contain all observations.
		lastBucket := ph.Bucket[len(ph.Bucket)-1]
		require.Equal(t, math.Inf(1), lastBucket.GetUpperBound())
		require.Equal(t, uint64(100), lastBucket.GetCumulativeCount())
	})

	t.Run("conventional bucket upper bounds are monotonic", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		var prevUB float64
		for i, b := range ph.Bucket {
			require.Greaterf(t, b.GetUpperBound(), prevUB,
				"bucket[%d] upper bound %.4f not > previous %.4f",
				i, b.GetUpperBound(), prevUB)
			prevUB = b.GetUpperBound()
		}
	})

	t.Run("native delta decoding reconstructs counts", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		rng := rand.New(rand.NewSource(42))
		const n = 10000
		for i := 0; i < n; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(n), total,
			"sum of decoded native bucket counts must equal total observations")
	})

	t.Run("native spans have non-zero lengths", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		for i := int64(1); i <= 1000; i++ {
			h.Record(i)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		for i, sp := range ph.PositiveSpan {
			require.Greaterf(t, sp.GetLength(), uint32(0),
				"span[%d] has zero length", i)
		}
	})

	t.Run("native bucket keys are monotonically increasing", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 5000; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		keys, _ := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		for i := 1; i < len(keys); i++ {
			require.Greaterf(t, keys[i], keys[i-1],
				"key[%d]=%d not > key[%d]=%d", i, keys[i], i-1, keys[i-1])
		}
	})

	t.Run("sparse encoding with widely separated values", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		for i := 0; i < 100; i++ {
			h.Record(10)
			h.Record(100000)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// Should have at least 2 spans for the two clusters of values.
		require.GreaterOrEqual(t, len(ph.PositiveSpan), 2,
			"expected at least 2 spans for widely separated values")

		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(200), total)
	})

	t.Run("single observation", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		h.Record(42)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(1), ph.GetSampleCount())
		require.Equal(t, float64(42), ph.GetSampleSum())
		require.Len(t, ph.PositiveSpan, 1)
		require.Equal(t, uint32(1), ph.PositiveSpan[0].GetLength())
		require.Equal(t, []int64{1}, ph.PositiveDelta)
	})

	t.Run("empty histogram", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		require.Equal(t, uint64(0), ph.GetSampleCount())
		require.Equal(t, float64(0), ph.GetSampleSum())
		require.Empty(t, ph.PositiveSpan)
		require.Empty(t, ph.PositiveDelta)
		// Conventional buckets include all in-range buckets plus +Inf.
		require.NotEmpty(t, ph.Bucket)
		infBucket := ph.Bucket[len(ph.Bucket)-1]
		require.Equal(t, math.Inf(1), infBucket.GetUpperBound())
		require.Equal(t, uint64(0), infBucket.GetCumulativeCount())
	})

	t.Run("conventional and native counts agree", func(t *testing.T) {
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		rng := rand.New(rand.NewSource(99))
		const n = 5000
		for i := 0; i < n; i++ {
			h.Record(int64(rng.Float64()*999999) + 1)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// +Inf bucket must equal SampleCount.
		infBucket := ph.Bucket[len(ph.Bucket)-1]
		require.Equal(t, math.Inf(1), infBucket.GetUpperBound())
		require.Equal(t, uint64(n), infBucket.GetCumulativeCount())

		// Total from native buckets.
		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var nativeTotal uint64
		for _, c := range counts {
			nativeTotal += c
		}

		// With only in-range values, native total equals the
		// second-to-last conventional bucket (last before +Inf).
		convInRange := ph.Bucket[len(ph.Bucket)-2].GetCumulativeCount()
		require.Equal(t, convInRange, nativeTotal,
			"conventional in-range and native bucket totals must agree")
		require.Equal(t, uint64(n), nativeTotal)
	})

	t.Run("native keys align with prometheus schema", func(t *testing.T) {
		// Verify that the bucket keys produced by our export match what
		// Prometheus would compute for the same values via promBucketKey,
		// allowing off-by-one for values that land on a straddling entry
		// in the lookup table (where we round up to the next bucket).
		h := New(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05})
		schema := h.Schema()

		// Record specific values and verify their keys.
		testValues := []int64{1, 2, 4, 8, 16, 100, 1000, 10000, 100000, 999999}
		for _, v := range testValues {
			h.Record(v)
		}
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		keys, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)

		// Build a set of expected keys from promBucketKey, including
		// key+1 for each value to account for the lookup table's
		// round-up behavior at straddling boundaries.
		expectedKeys := make(map[int]bool)
		for _, v := range testValues {
			k := promBucketKey(float64(v), schema)
			expectedKeys[k] = true
			expectedKeys[k+1] = true
		}

		// Every key with count > 0 in the export should be in the expected set.
		for i, k := range keys {
			if counts[i] > 0 {
				require.Truef(t, expectedKeys[k],
					"exported key %d (count=%d) not in expected set %v",
					k, counts[i], expectedKeys)
			}
		}
	})

	t.Run("negative deltas are valid", func(t *testing.T) {
		// When bucket counts decrease (e.g., a tall bucket followed by a short
		// one), deltas should be negative. Verify this doesn't break decoding.
		h := New(Params{Lo: 1, Hi: 1000, ErrorBound: 0.05})
		// Put 1000 observations in one bucket, then 1 in an adjacent one.
		for i := 0; i < 1000; i++ {
			h.Record(100)
		}
		h.Record(200)
		snap := h.Snapshot()
		ph := snap.ToPrometheusHistogram()

		// There should be at least one negative delta.
		hasNegative := false
		for _, d := range ph.PositiveDelta {
			if d < 0 {
				hasNegative = true
				break
			}
		}
		require.True(t, hasNegative,
			"expected negative deltas when bucket counts decrease")

		// Decoding should still produce correct total.
		_, counts := decodeDeltaCounts(ph.PositiveSpan, ph.PositiveDelta)
		var total uint64
		for _, c := range counts {
			total += c
		}
		require.Equal(t, uint64(1001), total)
	})
}
