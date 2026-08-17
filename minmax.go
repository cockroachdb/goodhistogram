// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import "sync/atomic"

// This file holds experimental variants of Record that additionally track the
// exact minimum and maximum values observed. They exist purely for the
// performance evaluation of exact extreme tracking on the lock-free hot path
// and are compared A/B against the baseline Record in minmax_benchmark_test.go.
//
// Tracking exact extremes cannot use a fetch-and-add like the bucket counters
// and sum; it needs a compare-and-swap loop guarded by a relaxed load. The
// load short-circuits the common steady-state case (the value is not a new
// extreme), so the CAS only fires when a new extreme is actually seen. The
// interesting cost is therefore (a) two extra shared-atomic loads on every
// Record and (b) contended CAS retries when extremes churn.

// updateMin lowers dst toward v using a load-guarded CAS loop.
func updateMin(dst *atomic.Int64, v int64) {
	for {
		old := dst.Load()
		if v >= old {
			return
		}
		if dst.CompareAndSwap(old, v) {
			return
		}
	}
}

// updateMax raises dst toward v using a load-guarded CAS loop.
func updateMax(dst *atomic.Int64, v int64) {
	for {
		old := dst.Load()
		if v <= old {
			return
		}
		if dst.CompareAndSwap(old, v) {
			return
		}
	}
}

// RecordMinMax records v and additionally tracks the exact min and max in two
// atomics packed inline in the Histogram struct (adjacent to sum, so they may
// share a cache line with the other hot counters).
func (h *Histogram) RecordMinMax(v int64) {
	updateMin(&h.min, v)
	updateMax(&h.max, v)
	h.Record(v)
}

// RecordMinMaxPadded records v and tracks exact min and max in cache-line
// padded atomics, isolating the false-sharing cost from the inline variant.
func (h *Histogram) RecordMinMaxPadded(v int64) {
	updateMin(&h.minP.v, v)
	updateMax(&h.maxP.v, v)
	h.Record(v)
}

// Min returns the exact minimum recorded via a RecordMinMax variant, or
// MaxInt64 if none.
func (h *Histogram) Min() int64 { return h.min.Load() }

// Max returns the exact maximum recorded via a RecordMinMax variant, or
// MinInt64 if none.
func (h *Histogram) Max() int64 { return h.max.Load() }
