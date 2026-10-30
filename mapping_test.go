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
	"math/rand"
	"testing"
)

// TestBucketLookupExact checks the lookup table against promBucketKey at
// every schema: at both ends of every table entry, on and next to every
// bucket boundary, at powers of two, and at random values.
func TestBucketLookupExact(t *testing.T) {
	for schema := int32(0); schema <= 8; schema++ {
		cfg := newConfig(1, 1<<40, schemaRelativeError(schema))
		if cfg.schema != schema {
			t.Fatalf("got schema %d, want %d", cfg.schema, schema)
		}
		check := func(v float64) {
			t.Helper()
			bits := math.Float64bits(v)
			got := cfg.subBucket(bits) + (int(bits>>52)-1022-1)*cfg.bucketsPerGroup
			if want := promBucketKey(v, schema); got != want {
				t.Fatalf("schema %d v=%v: lookup key %d, prom key %d", schema, v, got, want)
			}
		}
		for _, exp := range []int{0, 1, 10, 33} {
			scale := math.Ldexp(1, exp)
			for i := uint64(0); i < bucketLookupSize; i++ {
				lo := math.Float64frombits(uint64(1023)<<52 | i<<bucketLookupShift)
				hi := math.Float64frombits(math.Float64bits(lo) | (1<<bucketLookupShift - 1))
				check(lo * scale)
				check(hi * scale)
			}
			for _, b := range cfg.groupBounds {
				edge := b * 2 * scale
				check(edge)
				check(math.Nextafter(edge, 0))
				check(math.Nextafter(edge, math.Inf(1)))
			}
		}
		for exp := 0; exp < 63; exp++ {
			p := float64(int64(1) << exp)
			check(p)
			check(p + 1)
			if exp > 0 {
				check(p - 1)
			}
		}
		rng := rand.New(rand.NewSource(int64(schema)))
		for i := 0; i < 200000; i++ {
			check(math.Exp(rng.Float64() * 40))
		}
	}
}

// TestBucketMappingConsistency compares the Record() path (which uses
// the bucket lookup table for O(1) bucket resolution) against the
// promBucketKey path (which uses math.Frexp + sort.SearchFloat64s).
// The two must agree exactly.
func TestBucketMappingConsistency(t *testing.T) {
	cfg := newConfig(500, 60e9, 0.10) // schema 2

	// recordBucketIndex matches Record() exactly: lookup table only.
	recordBucketIndex := func(v int64) int {
		fv := float64(v)
		bits := math.Float64bits(fv)
		exp := int(bits>>52) - 1022
		sub := cfg.subBucket(bits)
		key := sub + (exp-1)*cfg.bucketsPerGroup
		idx := key - cfg.minKey
		if idx < 0 {
			idx = 0
		} else if idx >= cfg.numBuckets {
			idx = cfg.numBuckets - 1
		}
		return idx
	}

	// promBucketIndex uses promBucketKey (the reference implementation).
	promBucketIndex := func(v int64) int {
		fv := float64(v)
		key := promBucketKey(fv, cfg.schema)
		idx := key - cfg.minKey
		if idx < 0 {
			idx = 0
		} else if idx >= cfg.numBuckets {
			idx = cfg.numBuckets - 1
		}
		return idx
	}

	// Test random values across the range.
	rng := rand.New(rand.NewSource(42))
	logLo := math.Log(500)
	logHi := math.Log(60e9)
	const numSamples = 1000000
	mismatches := 0
	for i := 0; i < numSamples; i++ {
		v := int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
		if v <= 0 {
			continue
		}
		if refIdx, recIdx := promBucketIndex(v), recordBucketIndex(v); refIdx != recIdx {
			if mismatches < 10 {
				t.Errorf("v=%d: ref=%d rec=%d", v, refIdx, recIdx)
			}
			mismatches++
		}
	}
	if mismatches > 0 {
		t.Errorf("%d / %d values mapped to the wrong bucket", mismatches, numSamples)
	}

	// Verify that quantile estimates match the reference bucketing.
	t.Run("full-histogram-comparison", func(t *testing.T) {
		h := New(Params{Lo: 500, Hi: 60e9, ErrorBound: 0.10})

		rng := rand.New(rand.NewSource(99))
		values := make([]int64, 100000)
		for i := range values {
			values[i] = int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
			if values[i] <= 0 {
				values[i] = 1
			}
		}
		for _, v := range values {
			h.Record(v)
		}
		newSnap := h.Snapshot()

		// Build counts using the reference (promBucketKey) path.
		refCounts := make([]uint64, cfg.numBuckets)
		for _, v := range values {
			idx := promBucketIndex(v)
			refCounts[idx]++
		}

		// Compare quantile estimates between the two bucket distributions.
		refSnap := Snapshot{
			PrometheusSchema: cfg.schema,
			LowestTrackable:  cfg.lo,
			HighestTrackable: cfg.hi,
			Counts:           refCounts,
			TotalCount:       newSnap.TotalCount,
			TotalSum:         newSnap.TotalSum,
		}
		for _, q := range []float64{0.50, 0.75, 0.90, 0.95, 0.99, 0.999} {
			refQ := refSnap.ValueAtQuantile(q)
			recQ := newSnap.ValueAtQuantile(q)
			if refQ == 0 {
				continue
			}
			relErr := math.Abs(recQ-refQ) / refQ
			if relErr > 0.05 {
				t.Errorf("p%.1f: ref=%.6f rec=%.6f relErr=%.4f (>5%%)", q*100, refQ, recQ, relErr)
			}
		}
	})

	// Show the exponent calculation difference for edge cases.
	t.Run("exponent-comparison", func(t *testing.T) {
		edgeCases := []int64{1, 2, 3, 4, 7, 8, 15, 16, 1023, 1024, 2047, 2048}
		for _, v := range edgeCases {
			fv := float64(v)
			_, frexpExp := math.Frexp(fv)
			bits := math.Float64bits(fv)
			bitsExp := int(bits>>52) - 1022
			if frexpExp != bitsExp {
				t.Logf("v=%d: frexp_exp=%d bits_exp=%d (DIFFER)", v, frexpExp, bitsExp)
			} else {
				t.Logf("v=%d: frexp_exp=%d bits_exp=%d (match)", v, frexpExp, bitsExp)
			}
		}
	})

	// Show bucket comparison for values near group boundaries.
	t.Run("bucket-boundary-comparison", func(t *testing.T) {
		bounds := cfg.groupBounds
		for i := 0; i < len(bounds); i++ {
			// Generate a value right at a bucket boundary.
			boundary := bounds[i] * 2 // undo the /2 from Frexp normalization
			// Test values just below, at, and just above.
			for _, offset := range []float64{-1e-10, 0, 1e-10} {
				testVal := boundary + offset
				bits := math.Float64bits(testVal)
				lookupIdx := cfg.subBucket(bits)
				oldKey := promBucketKey(testVal, cfg.schema)
				newKey := lookupIdx + (int(bits>>52)-1022-1)*cfg.bucketsPerGroup

				if oldKey != newKey && i < 5 {
					t.Logf("boundary[%d]=%.15f offset=%+e: oldKey=%d newKey=%d (DIFFER)",
						i, boundary, offset, oldKey, newKey)
				}
			}
		}
	})
}
