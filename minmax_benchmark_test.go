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
	"math/rand"
	"sort"
	"testing"
)

// --------------------------------------------------------------------------
// Performance evaluation: exact min/max tracking on the hot Record path.
//
// Baseline is the existing Record. The variants add a load-guarded CAS loop
// for both the exact min and the exact max:
//
//   record          - baseline, no extreme tracking
//   record-minmax   - min/max in atomics packed inline next to sum
//   record-padded   - min/max in cache-line-padded atomics (no false sharing)
//
// The cost of extreme tracking depends entirely on how often a new extreme is
// seen, which is a property of input *ordering*, not the distribution's shape:
//
//   steady   - shuffled log-uniform values. After a brief warm-up the extremes
//              stop moving, so every CAS is skipped by the guard load. This is
//              the common metrics case (latencies bounce around a stable range).
//   ascending- monotonically increasing values: every single Record sets a new
//              max, forcing a CAS on every call. Adversarial worst case.
//   descending- monotonically decreasing: every Record sets a new min.
// --------------------------------------------------------------------------

type ordering struct {
	name string
	gen  func(rng *rand.Rand, n int) []int64
}

var minMaxOrderings = []ordering{
	{
		name: "steady",
		gen: func(rng *rand.Rand, n int) []int64 {
			return makeInt64Values(rng, n) // shuffled log-uniform
		},
	},
	{
		name: "ascending",
		gen: func(rng *rand.Rand, n int) []int64 {
			v := makeInt64Values(rng, n)
			sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			return v
		},
	},
	{
		name: "descending",
		gen: func(rng *rand.Rand, n int) []int64 {
			v := makeInt64Values(rng, n)
			sort.Slice(v, func(i, j int) bool { return v[i] > v[j] })
			return v
		},
	},
}

// recordVariants maps a name to the record method under test.
var recordVariants = []struct {
	name string
	fn   func(h *Histogram, v int64)
}{
	{"record", (*Histogram).Record},
	{"record-minmax", (*Histogram).RecordMinMax},
	{"record-padded", (*Histogram).RecordMinMaxPadded},
}

// BenchmarkMinMaxSingleThread measures per-Record cost single-threaded, across
// input orderings. Isolates the raw instruction/CAS cost with no contention.
func BenchmarkMinMaxSingleThread(b *testing.B) {
	const nVals = 100_000
	for _, ord := range minMaxOrderings {
		vals := ord.gen(rand.New(rand.NewSource(42)), nVals)
		for _, variant := range recordVariants {
			b.Run(fmt.Sprintf("order=%s/%s", ord.name, variant.name), func(b *testing.B) {
				h := newGoodHist()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					variant.fn(h, vals[i%len(vals)])
				}
			})
		}
	}
}

// BenchmarkMinMaxContention measures per-Record cost under contention. This is
// where extreme tracking is expected to hurt most: the shared min/max atomics
// bounce between cores' caches even when the guard load skips the CAS, and
// under the ascending/descending orderings the CAS itself is heavily contended.
func BenchmarkMinMaxContention(b *testing.B) {
	const nVals = 100_000
	for _, numG := range []int{50, 100} {
		for _, ord := range minMaxOrderings {
			vals := ord.gen(rand.New(rand.NewSource(42)), nVals)
			for _, variant := range recordVariants {
				b.Run(fmt.Sprintf("g=%d/order=%s/%s", numG, ord.name, variant.name), func(b *testing.B) {
					h := newGoodHist()
					b.SetParallelism(numG)
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						i := 0
						for pb.Next() {
							variant.fn(h, vals[i%len(vals)])
							i++
						}
					})
				})
			}
		}
	}
}
