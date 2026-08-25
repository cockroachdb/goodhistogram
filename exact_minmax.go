// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"math"
	"sync/atomic"
)

// WithExactMinMax wraps a Histogram, additionally tracking the exact minimum
// and maximum recorded values. The base Histogram already tracks sum and count
// exactly, so this completes an exact summary of {count, sum, min, max}.
//
// The wrapper is opt-in and additive: it embeds *Histogram, so every existing
// method (Snapshot, Schema, ToPrometheusHistogram, ...) continues to work
// unchanged, and the base recording path is not modified for callers who don't
// need exact extremes.
//
// "min" and "max" mean the smallest and largest values ever passed to Record —
// the extreme datapoints — not the configured [lo, hi] range. They include
// values that fall outside [lo, hi] (counted in Underflow/Overflow) and
// zero/negative values (counted in ZeroCount), mirroring how the exact sum
// already accounts for every observation. As a result Max may exceed cfg.hi and
// Min may be below cfg.lo — this is intentional, and is why the bucket
// interpolation cannot recover them.
type WithExactMinMax struct {
	*Histogram

	// minVal and maxVal hold the exact extremes across all recorded values.
	// They are updated on the hot path via load-then-CAS: once the extremes
	// stabilize, every subsequent Record only reads them (no write), so the
	// cache line settles into shared state and stays cheap under contention.
	//
	// Before any value is recorded, minVal is MaxInt64 and maxVal is MinInt64.
	// These sentinels are identities for min/max, and the invariant
	// minVal <= maxVal holds if and only if at least one value has been
	// recorded — which is how readers distinguish "empty" from "recorded".
	minVal atomic.Int64
	maxVal atomic.Int64
}

// NewWithExactMinMax creates a Histogram that also tracks exact min/max. Params
// behave exactly as in New; the config is shared via the same cache.
func NewWithExactMinMax(p Params) *WithExactMinMax {
	h := &WithExactMinMax{Histogram: New(p)}
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
	return h
}

// Record adds a value to the histogram and updates the exact min/max. Cost is
// the base Record plus two load-then-CAS updates; after warmup these degrade to
// two relaxed loads. Lock-free and allocation-free, like the base.
func (h *WithExactMinMax) Record(v int64) {
	h.Histogram.Record(v)

	for {
		old := h.maxVal.Load()
		if v <= old {
			break
		}
		if h.maxVal.CompareAndSwap(old, v) {
			break
		}
	}
	for {
		old := h.minVal.Load()
		if v >= old {
			break
		}
		if h.minVal.CompareAndSwap(old, v) {
			break
		}
	}
}

// Reset zeroes all counters and clears the tracked min/max.
func (h *WithExactMinMax) Reset() {
	h.Histogram.Reset()
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
}

// Summary is an exact point-in-time summary of the recorded values. Min and Max
// are meaningful only when Count > 0.
type Summary struct {
	Count uint64
	Sum   int64
	Min   int64
	Max   int64
}

// Summary returns the exact count, sum, min, and max. Reads are independent
// (not a single atomic snapshot), matching the consistency model of the rest of
// the library.
func (h *WithExactMinMax) Summary() Summary {
	var count uint64
	for i := range h.counts {
		count += h.counts[i].Load()
	}
	count += h.ZeroCount.Load() + h.Underflow.Load() + h.Overflow.Load()

	s := Summary{Count: count, Sum: h.sum.Load()}
	// minVal <= maxVal iff at least one value has been recorded.
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		s.Min, s.Max = mn, mx
	}
	return s
}

// ExactSnapshot is a Snapshot augmented with the exact min/max. It overrides
// quantile estimation at the extremes to return the true recorded min (q<=0)
// and max (q>=1) instead of the bucket-clamped estimates.
type ExactSnapshot struct {
	Snapshot

	// Min and Max are the exact smallest and largest recorded values. They are
	// meaningful only when TotalCount > 0 (otherwise both are 0).
	Min int64
	Max int64
}

// Snapshot returns a point-in-time ExactSnapshot. The bucket snapshot and the
// min/max are read independently, the same non-atomic trade-off as the base
// Snapshot.
func (h *WithExactMinMax) Snapshot() ExactSnapshot {
	es := ExactSnapshot{Snapshot: h.Histogram.Snapshot()}
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		es.Min, es.Max = mn, mx
	}
	return es
}

// ValueAtQuantile returns the estimated value at quantile q. At the extremes it
// returns the exact recorded min (q<=0) and max (q>=1); for interior quantiles
// it delegates to the bucket-based trapezoidal estimate. Because the extremes
// are exact, this can return values outside [lo, hi].
func (s *ExactSnapshot) ValueAtQuantile(q float64) float64 {
	if s.TotalCount == 0 {
		return 0
	}
	if q <= 0 {
		return float64(s.Min)
	}
	if q >= 1 {
		return float64(s.Max)
	}
	return s.Snapshot.ValueAtQuantile(q)
}

// ValuesAtQuantiles is the batch form of ValueAtQuantile, returning exact
// extremes for any q<=0 or q>=1 entries and delegating the rest.
func (s *ExactSnapshot) ValuesAtQuantiles(qs []float64) []float64 {
	res := s.Snapshot.ValuesAtQuantiles(qs)
	if s.TotalCount == 0 {
		return res
	}
	for i, q := range qs {
		switch {
		case q <= 0:
			res[i] = float64(s.Min)
		case q >= 1:
			res[i] = float64(s.Max)
		}
	}
	return res
}

// Merge returns a new ExactSnapshot combining s and other: bucket counts are
// summed (as in Snapshot.Merge) and the extremes are combined (min-of-mins,
// max-of-maxes), skipping empty operands. Both snapshots must share the same
// config.
func (s *ExactSnapshot) Merge(other *ExactSnapshot) ExactSnapshot {
	m := ExactSnapshot{Snapshot: s.Snapshot.Merge(&other.Snapshot)}
	switch {
	case s.TotalCount == 0:
		m.Min, m.Max = other.Min, other.Max
	case other.TotalCount == 0:
		m.Min, m.Max = s.Min, s.Max
	default:
		m.Min = min(s.Min, other.Min)
		m.Max = max(s.Max, other.Max)
	}
	return m
}
