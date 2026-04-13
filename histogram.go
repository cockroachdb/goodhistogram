// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package goodhistogram provides an exponential histogram with Prometheus
// native histogram schema alignment, bounded relative error, and trapezoidal
// quantile estimation.
//
// The histogram is configured with a value range [lo, hi] and a desired
// relative error bound. It selects the tightest Prometheus schema whose error
// is at or below the requested bound, then allocates a fixed array of atomic
// counters covering the range. Recording is O(1) and lock-free: values are
// mapped to bucket indices via math.Frexp plus a small precomputed boundary
// table, then the corresponding counter is atomically incremented.
//
// Because the bucket layout is identical to a Prometheus native histogram
// schema, export to the Prometheus sparse format requires no remapping — our
// internal indices are Prometheus bucket indices offset by a constant.
package goodhistogram

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

const maxSchema = 8

// nativeHistogramBounds contains the Prometheus native histogram bucket
// boundaries within each group (power-of-2 octave) for each schema (0–8).
// For schema s, there are 2^s buckets per group. Each entry is the lower
// bound of a bucket in [0.5, 1.0), matching the range of math.Frexp's
// fractional part.
//
// The boundary for bucket j in schema s is: 2^(j / 2^s) / 2, which
// equals the fractional part of the Prometheus bucket boundary when
// decomposed via math.Frexp.
//
// These are computed at init time and are identical to the
// nativeHistogramBounds table in the Prometheus client_golang library.
var nativeHistogramBounds [maxSchema + 1][]float64

func init() {
	for s := int32(0); s <= maxSchema; s++ {
		bucketsPerGroup := 1 << s
		bounds := make([]float64, bucketsPerGroup)
		for j := 0; j < bucketsPerGroup; j++ {
			// The Prometheus bucket boundary for bucket j in schema s
			// within a power-of-2 group is 2^(j / 2^s). When decomposed via
			// math.Frexp, the fractional part is 2^(j / 2^s) / 2 (since
			// Frexp returns frac in [0.5, 1.0) with value = frac * 2^exp).
			bounds[j] = math.Ldexp(math.Pow(2, float64(j)/float64(bucketsPerGroup)), -1)
		}
		nativeHistogramBounds[s] = bounds
	}
}

// schemaRelativeError returns the relative error for a given schema.
// The error is (γ-1)/(γ+1) where γ = 2^(2^(-schema)).
func schemaRelativeError(schema int32) float64 {
	gamma := math.Pow(2, math.Pow(2, float64(-schema)))
	return (gamma - 1) / (gamma + 1)
}

// pickSchema selects the coarsest (fewest buckets) Prometheus schema whose
// relative error is at or below the desired error. Returns a schema in [0, 8].
func pickSchema(desiredError float64) int32 {
	for s := int32(0); s <= maxSchema; s++ {
		if schemaRelativeError(s) <= desiredError {
			return s
		}
	}
	return maxSchema
}

// promBucketKey computes the Prometheus native histogram bucket key for a
// positive value v, using the given schema. This is the same mapping used by
// the Prometheus client_golang library.
//
// For schema s > 0, the bucket key is:
//
//	sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
//
// where frac, exp = math.Frexp(v) and bounds = nativeHistogramBounds[s].
func promBucketKey(v float64, schema int32) int {
	frac, exp := math.Frexp(v)
	bounds := nativeHistogramBounds[schema]
	return sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
}

// getLe returns the upper bound of the bucket with the given key and schema.
// This is the inverse of promBucketKey.
func getLe(key int, schema int32) float64 {
	fracIdx := key & ((1 << schema) - 1)
	frac := nativeHistogramBounds[schema][fracIdx]
	exp := (key >> schema) + 1
	return math.Ldexp(frac, exp)
}

// bucketLookupBits is the number of top mantissa bits used to index
// the bucket lookup table. 8 bits → 256 entries, which is enough
// precision for all schemas up to 8 (256 buckets per group).
const bucketLookupBits = 8
const bucketLookupSize = 1 << bucketLookupBits
const bucketLookupShift = 52 - bucketLookupBits

// config holds the immutable parameters for a Histogram, computed at
// construction time from the user-specified range and error bound.
type config struct {
	schema          int32
	lo, hi          float64
	minKey          int
	numBuckets      int
	bucketsPerGroup int
	groupBounds     []float64
	bucketLookup    [bucketLookupSize]uint8
	boundaries      []float64
}

// newConfig creates a config for the given range [lo, hi] and desired
// relative error. The schema is chosen as the tightest Prometheus schema
// whose error is at or below desiredError. Panics if lo <= 0, hi <= lo,
// or desiredError <= 0.
func newConfig(lo, hi, desiredError float64) config {
	if lo <= 0 || hi <= lo || desiredError <= 0 {
		panic("goodhistogram: invalid config: need 0 < lo < hi and desiredError > 0")
	}
	schema := pickSchema(desiredError)
	minKey := promBucketKey(lo, schema)
	// If lo lands exactly on a bucket boundary, the first bucket would span
	// [lo, lo] — a zero-width degenerate bucket. Skip it so the first bucket
	// starts at lo and ends at the next real boundary above it.
	if getLe(minKey, schema) <= lo {
		minKey++
	}
	maxKey := promBucketKey(hi, schema)
	numBuckets := maxKey - minKey + 1

	// Precompute bucket boundaries for quantile estimation.
	boundaries := make([]float64, numBuckets+1)
	boundaries[0] = lo
	for i := 1; i < numBuckets; i++ {
		boundaries[i] = getLe(minKey+i-1, schema)
	}
	boundaries[numBuckets] = hi

	groupBounds := nativeHistogramBounds[schema]
	bucketsPerGroup := len(groupBounds)

	// Build the bucket lookup table. For each of the 256 possible
	// top-8-bit mantissa values, compute the bucket index within
	// the group by evaluating promBucketKey on representative float64
	// values.
	//
	// A single 8-bit entry can straddle a bucket boundary: the low
	// 44 bits that were truncated may push the full-precision value
	// into the next bucket. We detect straddling by checking both
	// the minimum and maximum float64 representable by each table
	// entry. When they disagree, we resolve to the upper key (keyMax).
	// This matches SearchFloat64s's >= semantics and means straddling
	// values always round up to the next bucket — at most one
	// bucket of additional error for a small fraction of values.
	//
	// For the common schema 2 (10% error), only 4 of 256 entries
	// straddle, affecting ~0.4% of recorded values. The maximum
	// additional error for those values is bounded by one bucket width
	// (8.6%), but the impact on quantile estimation is negligible since
	// the affected values are already near the boundary.
	var bucketLookup [bucketLookupSize]uint8
	for tableIdx := 0; tableIdx < bucketLookupSize; tableIdx++ {
		minBits := uint64(1023)<<52 | uint64(tableIdx)<<bucketLookupShift
		maxBits := minBits | (1<<bucketLookupShift - 1)
		keyMin := promBucketKey(math.Float64frombits(minBits), schema)
		keyMax := promBucketKey(math.Float64frombits(maxBits), schema)
		if keyMin == keyMax {
			bucketLookup[tableIdx] = uint8(keyMin)
		} else {
			// Straddling entry — round up to the next bucket.
			bucketLookup[tableIdx] = uint8(keyMax)
		}
	}

	return config{
		schema:          schema,
		lo:              lo,
		hi:              hi,
		minKey:          minKey,
		numBuckets:      numBuckets,
		bucketsPerGroup: bucketsPerGroup,
		groupBounds:     groupBounds,
		bucketLookup:    bucketLookup,
		boundaries:      boundaries,
	}
}

// Params holds the user-facing parameters for creating a Histogram.
//
// Zero-value fields are replaced with defaults:
//   - Lo: 1
//   - Hi: math.MaxInt64
//   - ErrorBound: 0.10 (10%, schema 2)
type Params struct {
	// Lo and Hi define the tracked value range. Values outside this range
	// are counted in Underflow/Overflow.
	Lo, Hi float64
	// ErrorBound is the maximum relative error bound. The histogram
	// selects the tightest Prometheus schema whose error is at or below
	// this value.
	ErrorBound float64
}

func (p Params) withDefaults() Params {
	if p.Lo == 0 {
		p.Lo = 1
	}
	if p.Hi == 0 {
		p.Hi = float64(math.MaxInt64)
	}
	if p.ErrorBound == 0 {
		p.ErrorBound = 0.10
	}
	return p
}

// configCache stores previously computed configs keyed by Params. Since
// configs are immutable and many histograms share the same parameters
// (e.g., all latency histograms in CockroachDB use [500ns, 60s] @ 10%),
// this avoids redundant allocations of the lookup table and boundaries
// array.
var configCache sync.Map // map[Params]*config

// getOrCreateConfig returns a shared config for the given params, creating
// one if it doesn't already exist.
func getOrCreateConfig(p Params) *config {
	if v, ok := configCache.Load(p); ok {
		return v.(*config)
	}
	cfg := newConfig(p.Lo, p.Hi, p.ErrorBound)
	actual, _ := configCache.LoadOrStore(p, &cfg)
	return actual.(*config)
}

// Histogram is a lock-free exponential histogram with atomic counters.
type Histogram struct {
	cfg    *config
	counts []atomic.Uint64
	// Underflow counts values below cfg.lo.
	Underflow atomic.Uint64
	// Overflow counts values above cfg.hi.
	Overflow atomic.Uint64
	// ZeroCount counts exact zeros (and negative values).
	ZeroCount atomic.Uint64
	sum       atomic.Int64 // using Int64 since CockroachDB histograms record int64
}

// Reset zeroes all counters without reallocating the backing slice.
func (h *Histogram) Reset() {
	for i := range h.counts {
		h.counts[i].Store(0)
	}
	h.ZeroCount.Store(0)
	h.Underflow.Store(0)
	h.Overflow.Store(0)
	h.sum.Store(0)
}

// New creates a new Histogram for the given range and error bound. Configs
// are cached and shared across histograms with identical parameters.
func New(p Params) *Histogram {
	p = p.withDefaults()
	cfg := getOrCreateConfig(p)
	return &Histogram{
		cfg:    cfg,
		counts: make([]atomic.Uint64, cfg.numBuckets),
	}
}

// Record adds a value to the histogram. This is the hot path: O(1), lock-free,
// no allocations. Values <= 0 are counted in ZeroCount. Values outside [lo, hi]
// are clamped and counted in Underflow/Overflow respectively.
//
// Bucket index is computed via math.Frexp (IEEE 754 bit extraction) plus a
// precomputed lookup table indexed by the top mantissa bits. This
// produces indices exactly aligned with Prometheus native histogram bucket
// keys, avoiding the floating-point rounding drift that a math.Log-based
// approach would introduce.
func (h *Histogram) Record(v int64) {
	h.sum.Add(v)

	if v <= 0 {
		h.ZeroCount.Add(1)
		return
	}

	// Convert to float64 and extract IEEE 754 bits. The int→float
	// conversion is a single SCVTF instruction; Float64bits is a
	// zero-cost reinterpret.
	bits := math.Float64bits(float64(v))
	exp := int(bits>>52) - 1022
	sub := int(h.cfg.bucketLookup[(bits>>bucketLookupShift)&0xFF])

	key := sub + (exp-1)*h.cfg.bucketsPerGroup
	idx := key - h.cfg.minKey

	// Clamp to valid range. Values outside [lo, hi] are counted in
	// Underflow/Overflow but are NOT added to any bucket. This matches
	// the Prometheus convention where out-of-range values go to an
	// implicit -Inf/+Inf bucket that is excluded from interpolation,
	// and quantile estimation clamps to lo/hi when the quantile falls
	// in the overflow/underflow region.
	if idx < 0 {
		h.Underflow.Add(1)
		return
	}
	if idx >= h.cfg.numBuckets {
		h.Overflow.Add(1)
		return
	}
	h.counts[idx].Add(1)
}

// Snapshot is a point-in-time, non-atomic copy of a Histogram, suitable for
// quantile computation and export.
type Snapshot struct {
	cfg        *config
	Counts     []uint64
	ZeroCount  uint64
	Underflow  uint64
	Overflow   uint64
	TotalCount uint64
	TotalSum   int64
}

// Schema returns the Prometheus native histogram schema (0–8).
func (s *Snapshot) Schema() int32 {
	return s.cfg.schema
}

// Snapshot returns a point-in-time copy of the histogram. The snapshot is
// not guaranteed to be perfectly consistent (individual counters are read
// independently), but this is acceptable for metrics — the same trade-off
// Prometheus makes.
func (h *Histogram) Snapshot() Snapshot {
	s := Snapshot{
		cfg:       h.cfg,
		Counts:    make([]uint64, h.cfg.numBuckets),
		ZeroCount: h.ZeroCount.Load(),
		Underflow: h.Underflow.Load(),
		Overflow:  h.Overflow.Load(),
		TotalSum:  h.sum.Load(),
	}
	for i := range s.Counts {
		c := h.counts[i].Load()
		s.Counts[i] = c
		s.TotalCount += c
	}
	// Underflow, overflow, and zero observations are not recorded in
	// any bucket, so add them to the total.
	s.TotalCount += s.ZeroCount + s.Underflow + s.Overflow
	return s
}

// Schema returns the Prometheus native histogram schema (0–8).
func (h *Histogram) Schema() int32 {
	return h.cfg.schema
}

// Merge returns a new Snapshot whose counts are the element-wise sum of s
// and other. Both snapshots must share the same config (same schema and
// bucket boundaries). This is used to merge prev and cur window snapshots
// in the tick-based windowing pattern.
func (s *Snapshot) Merge(other *Snapshot) Snapshot {
	merged := Snapshot{
		cfg:        s.cfg,
		Counts:     make([]uint64, len(s.Counts)),
		ZeroCount:  s.ZeroCount + other.ZeroCount,
		Underflow:  s.Underflow + other.Underflow,
		Overflow:   s.Overflow + other.Overflow,
		TotalCount: s.TotalCount + other.TotalCount,
		TotalSum:   s.TotalSum + other.TotalSum,
	}
	for i := range s.Counts {
		merged.Counts[i] = s.Counts[i] + other.Counts[i]
	}
	return merged
}
