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

// WithExactMinMax wraps a Histogram, additionally tracking the exact min and
// max recorded values. Extremes span every observation, including out-of-range
// (Underflow/Overflow) and zero/negative values, so Max may exceed hi and Min
// may be below lo.
type WithExactMinMax struct {
	*Histogram

	// Before any Record, minVal is MaxInt64 and maxVal is MinInt64; these are
	// identities for min/max, and minVal <= maxVal iff a value has been
	// recorded (which is how readers tell "empty" from "recorded").
	minVal atomic.Int64
	maxVal atomic.Int64
}

func NewWithExactMinMax(p Params) *WithExactMinMax {
	h := &WithExactMinMax{Histogram: New(p)}
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
	return h
}

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

func (h *WithExactMinMax) Reset() {
	h.Histogram.Reset()
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
}

// Summary is an exact summary of the recorded values. Min and Max are
// meaningful only when Count > 0.
type Summary struct {
	Count uint64
	Sum   int64
	Min   int64
	Max   int64
}

func (h *WithExactMinMax) Summary() Summary {
	var count uint64
	for i := range h.counts {
		count += h.counts[i].Load()
	}
	count += h.ZeroCount.Load() + h.Underflow.Load() + h.Overflow.Load()

	s := Summary{Count: count, Sum: h.sum.Load()}
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		s.Min, s.Max = mn, mx
	}
	return s
}

// ExactSnapshot is a Snapshot with exact extremes. Min and Max are meaningful
// only when TotalCount > 0.
type ExactSnapshot struct {
	Snapshot
	Min int64
	Max int64
}

func (h *WithExactMinMax) Snapshot() ExactSnapshot {
	es := ExactSnapshot{Snapshot: h.Histogram.Snapshot()}
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		es.Min, es.Max = mn, mx
	}
	return es
}

// ValueAtQuantile returns the exact min at q<=0 and max at q>=1 (which may fall
// outside [lo, hi]); interior quantiles use the base estimate.
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
