// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"bufio"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// pmaxSampleParams matches the configuration used by the workload that
// produced the testdata sample sets: latencies in nanoseconds over the range
// 1µs to 10s with a 10% relative error bound (Prometheus schema 2).
var pmaxSampleParams = Params{Lo: 1_000, Hi: 10e9, ErrorBound: 0.10}

// pmaxQuantiles are the quantiles reported for each sample set.
var pmaxQuantiles = []struct {
	label string
	q     float64
}{
	{"p50", 0.5},
	{"p90", 0.9},
	{"p99", 0.99},
	{"p99.9", 0.999},
	{"pMax", 1.0},
}

// readSampleFile reads a testdata file containing one integer sample per line.
func readSampleFile(t *testing.T, path string) []int64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	var samples []int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		v, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			t.Fatalf("%s: parsing %q: %v", path, line, err)
		}
		samples = append(samples, v)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(samples) == 0 {
		t.Fatalf("%s: no samples", path)
	}
	return samples
}

// exactQuantile returns the q-th quantile of sorted by the nearest-rank
// definition: the smallest value whose 1-based rank is at least q*len(sorted).
// This matches the rank convention ValueAtQuantile uses (rank = q*TotalCount),
// so exactQuantile(sorted, 1) is the true maximum.
func exactQuantile(sorted []int64, q float64) float64 {
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx])
}

// TestPMaxQuantization records two similar-but-distinct sets of latency
// samples and prints each estimated quantile against the exact quantile
// computed from the raw samples.
//
// p50 through p99.9 track the exact values closely, because those ranks fall
// strictly inside the bucket array and are resolved by trapezoidal
// interpolation. pMax does not: ValueAtQuantile(1.0) takes the
// rank >= TotalCount branch, which performs no interpolation and returns the
// upper boundary of the last non-empty bucket. Two sample sets whose maxima
// land in the same bucket therefore report an identical pMax even when every
// other quantile differs substantially. With schema 2 the buckets are ~18.9%
// wide, so that collision window is large.
//
// The two sample sets below are exactly such a pair.
func TestPMaxQuantization(t *testing.T) {
	// Bound on the relative error of the interpolated quantiles. pMax is
	// deliberately exempt: it is a bucket ceiling, not an estimate, and is the
	// behavior this test exists to document.
	const interpolatedTolerance = 0.10

	sets := []struct {
		name string
		file string
	}{
		{"A", "pmax_samples_a.txt"},
		{"B", "pmax_samples_b.txt"},
	}

	const msPerNs = 1e6

	// pmax and trueMax are collected across sets for the cross-set comparison.
	pmax := make([]float64, len(sets))
	trueMax := make([]float64, len(sets))

	qs := make([]float64, len(pmaxQuantiles))
	for i, spec := range pmaxQuantiles {
		qs[i] = spec.q
	}

	for si, set := range sets {
		samples := readSampleFile(t, filepath.Join("testdata", set.file))

		h := New(pmaxSampleParams)
		for _, v := range samples {
			h.Record(v)
		}
		snap := h.Snapshot()
		estimated := snap.ValuesAtQuantiles(qs)

		sorted := slices.Clone(samples)
		slices.Sort(sorted)

		t.Logf("set %s (n=%d)", set.name, len(samples))
		t.Logf("  %-6s %11s %11s %9s", "", "exact", "estimated", "error")
		for i, spec := range pmaxQuantiles {
			exact := exactQuantile(sorted, spec.q)
			est := estimated[i]
			relErr := (est - exact) / exact

			t.Logf("  %-6s %9.3fms %9.3fms %+8.1f%%",
				spec.label, exact/msPerNs, est/msPerNs, relErr*100)

			if spec.q == 1 {
				pmax[si] = est
				trueMax[si] = exact
				// The reported max must never fall below an observed sample.
				if est < exact {
					t.Errorf("set %s: pMax %.0fns is below the true max %.0fns",
						set.name, est, exact)
				}
				continue
			}
			if math.Abs(relErr) > interpolatedTolerance {
				t.Errorf("set %s: %s estimate %.0fns is %.1f%% off exact %.0fns, "+
					"exceeding the %.0f%% bound",
					set.name, spec.label, est, relErr*100, exact,
					interpolatedTolerance*100)
			}
		}
	}

	if trueMax[0] != trueMax[1] && pmax[0] == pmax[1] {
		t.Logf("true maxima differ by %.3fms but both report pMax=%.3fms",
			(trueMax[1]-trueMax[0])/msPerNs, pmax[0]/msPerNs)
	}
}
