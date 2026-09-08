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

	prometheusgo "github.com/prometheus/client_model/go"
)

// WithExactMinMax wraps a Histogram, additionally tracking the exact min and
// max recorded values. Extremes span every observation, including out-of-range
// (Underflow/Overflow) and zero/negative values, so Max may exceed hi and Min
// may be below lo. A WithExactMinMax must not be copied after first use.
type WithExactMinMax struct {
	histogram Histogram

	// Before any Record, minVal is MaxInt64 and maxVal is MinInt64; these are
	// identities for min/max, and minVal <= maxVal iff a value has been
	// recorded (which is how readers tell "empty" from "recorded").
	minVal atomic.Int64
	maxVal atomic.Int64
}

func NewWithExactMinMax(p Params) *WithExactMinMax {
	h := &WithExactMinMax{}
	h.histogram.init(p)
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
	return h
}

func (h *WithExactMinMax) Record(v int64) {
	h.histogram.Record(v)

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
	h.histogram.Reset()
	h.minVal.Store(math.MaxInt64)
	h.maxVal.Store(math.MinInt64)
}

// Schema returns the Prometheus native histogram schema (0–8).
func (h *WithExactMinMax) Schema() int32 {
	return h.histogram.Schema()
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
	for i := range h.histogram.counts {
		count += h.histogram.counts[i].Load()
	}
	count += h.histogram.ZeroCount.Load() + h.histogram.Underflow.Load() + h.histogram.Overflow.Load()

	s := Summary{Count: count, Sum: h.histogram.sum.Load()}
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		s.Min, s.Max = mn, mx
	}
	return s
}

// ExactSnapshot is a Snapshot with exact extremes. Min and Max are meaningful
// only when Summary().Count > 0. Subtraction is not supported because exact
// extremes cannot be recovered by subtracting snapshots.
type ExactSnapshot struct {
	snapshot Snapshot
	Min      int64
	Max      int64
}

func (h *WithExactMinMax) Snapshot() ExactSnapshot {
	es := ExactSnapshot{snapshot: h.histogram.Snapshot()}
	if mn, mx := h.minVal.Load(), h.maxVal.Load(); mn <= mx {
		es.Min, es.Max = mn, mx
	}
	return es
}

// ValueAtQuantile returns the exact min at q<=0 and max at q>=1 (which may fall
// outside [lo, hi]); interior quantiles use the base estimate.
func (s *ExactSnapshot) ValueAtQuantile(q float64) float64 {
	if s.snapshot.TotalCount == 0 {
		return 0
	}
	if q <= 0 {
		return float64(s.Min)
	}
	if q >= 1 {
		return float64(s.Max)
	}
	return s.snapshot.ValueAtQuantile(q)
}

func (s *ExactSnapshot) ValuesAtQuantiles(qs []float64) []float64 {
	res := s.snapshot.ValuesAtQuantiles(qs)
	if s.snapshot.TotalCount == 0 {
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
	m := ExactSnapshot{snapshot: s.snapshot.Merge(&other.snapshot)}
	switch {
	case s.snapshot.TotalCount == 0:
		m.Min, m.Max = other.Min, other.Max
	case other.snapshot.TotalCount == 0:
		m.Min, m.Max = s.Min, s.Max
	default:
		m.Min = min(s.Min, other.Min)
		m.Max = max(s.Max, other.Max)
	}
	return m
}

// Summary returns the exact count, sum, and extremes in the snapshot.
func (s *ExactSnapshot) Summary() Summary {
	return Summary{Count: s.snapshot.TotalCount, Sum: s.snapshot.TotalSum, Min: s.Min, Max: s.Max}
}

// Schema returns the Prometheus native histogram schema (0–8).
func (s *ExactSnapshot) Schema() int32 {
	return s.snapshot.Schema()
}

// Mean returns the arithmetic mean, or zero for an empty snapshot.
func (s *ExactSnapshot) Mean() float64 {
	return s.snapshot.Mean()
}

// Total returns the observation count and sum.
func (s *ExactSnapshot) Total() (int64, float64) {
	return s.snapshot.Total()
}

// ToPrometheusHistogram exports the bucket counts, sum, and count. Exact
// extremes are not represented in the Prometheus histogram format.
func (s *ExactSnapshot) ToPrometheusHistogram() *prometheusgo.Histogram {
	return s.snapshot.ToPrometheusHistogram()
}
