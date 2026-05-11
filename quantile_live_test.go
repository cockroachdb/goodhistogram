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
    "testing"
)

// TestQueryQuantilesAgreesWithSnapshot checks that QueryQuantiles produces
// the same numbers as Snapshot().ValuesAtQuantiles() across distributions.
// They should agree exactly because both use the same trapezoidal
// interpolation and matching boundary-density logic (including the
// existing right-edge dR=0 quirk).
func TestQueryQuantilesAgreesWithSnapshot(t *testing.T) {
    qs := []float64{0.0, 0.001, 0.01, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999, 1.0}

    for _, dist := range distributions {
        t.Run(dist.name, func(t *testing.T) {
            rng := rand.New(rand.NewSource(42))
            vals := dist.genFn(rng, 100_000)

            h := newGoodHist()
            for _, v := range vals {
                h.Record(int64(v))
            }

            snap := h.Snapshot()
            want := snap.ValuesAtQuantiles(qs)

            var buf [16]float64
            got := h.QueryQuantiles(buf[:0], qs)

            for i, q := range qs {
                if math.Abs(got[i]-want[i]) > 1e-6*math.Max(1, math.Abs(want[i])) {
                    t.Errorf("q=%g: QueryQuantiles=%g, ValuesAtQuantiles=%g (diff=%g)",
                        q, got[i], want[i], got[i]-want[i])
                }
            }
        })
    }
}

// TestQueryQuantilesEdges checks zero-count and edge-only inputs.
func TestQueryQuantilesEdges(t *testing.T) {
    t.Run("empty histogram", func(t *testing.T) {
        h := newGoodHist()
        var buf [4]float64
        got := h.QueryQuantiles(buf[:0], []float64{0.5, 0.99})
        for i, v := range got {
            if v != 0 {
                t.Errorf("empty histogram q[%d]: got %g, want 0", i, v)
            }
        }
    })

    t.Run("only underflow", func(t *testing.T) {
        // Use values clearly below lo's octave to guarantee underflow.
        h := newGoodHist()
        h.Record(1)
        h.Record(10)
        var buf [3]float64
        got := h.QueryQuantiles(buf[:0], []float64{0.0, 0.5, 1.0})
        snap := h.Snapshot()
        want := snap.ValuesAtQuantiles([]float64{0.0, 0.5, 1.0})
        for i, v := range got {
            if v != want[i] {
                t.Errorf("only-underflow q[%d]: QueryQuantiles=%g, ValuesAtQuantiles=%g", i, v, want[i])
            }
        }
    })

    t.Run("only overflow", func(t *testing.T) {
        // Use values clearly above hi's octave to guarantee overflow.
        h := newGoodHist()
        h.Record(int64(benchHi * 4))
        h.Record(int64(benchHi * 8))
        var buf [3]float64
        got := h.QueryQuantiles(buf[:0], []float64{0.0, 0.5, 1.0})
        snap := h.Snapshot()
        want := snap.ValuesAtQuantiles([]float64{0.0, 0.5, 1.0})
        for i, v := range got {
            if v != want[i] {
                t.Errorf("only-overflow q[%d]: QueryQuantiles=%g, ValuesAtQuantiles=%g", i, v, want[i])
            }
        }
    })

    t.Run("empty qs", func(t *testing.T) {
        h := newGoodHist()
        h.Record(1000)
        got := h.QueryQuantiles(nil, nil)
        if len(got) != 0 {
            t.Errorf("got len=%d, want 0", len(got))
        }
    })
}

// TestQueryQuantilesAllocFree verifies zero allocations when dst has cap.
func TestQueryQuantilesAllocFree(t *testing.T) {
    h := newGoodHist()
    rng := rand.New(rand.NewSource(42))
    for i := 0; i < 10_000; i++ {
        h.Record(int64(benchLo + rng.Float64()*benchRange))
    }
    qs := []float64{0.5, 0.99}
    var buf [4]float64

    allocs := testing.AllocsPerRun(100, func() {
        _ = h.QueryQuantiles(buf[:0], qs)
    })
    if allocs != 0 {
        t.Errorf("QueryQuantiles allocated %v times per run, want 0", allocs)
    }
}

// BenchmarkQueryPath compares the existing Snapshot+ValuesAtQuantiles path
// against the new QueryQuantiles path. Single-thread is the apples-to-apples
// comparison since allocation/copy cost is what we're targeting.
func BenchmarkQueryPath(b *testing.B) {
    for _, nObs := range []int{1_000, 100_000} {
        b.Run(fmt.Sprintf("n=%d", nObs), func(b *testing.B) {
            h := newGoodHist()
            rng := rand.New(rand.NewSource(42))
            for i := 0; i < nObs; i++ {
                h.Record(int64(benchLo + rng.Float64()*benchRange))
            }
            qs := []float64{0.5, 0.99}

            b.Run("SnapshotValuesAtQuantiles", func(b *testing.B) {
                b.ReportAllocs()
                for i := 0; i < b.N; i++ {
                    snap := h.Snapshot()
                    _ = snap.ValuesAtQuantiles(qs)
                }
            })
            b.Run("QueryQuantiles", func(b *testing.B) {
                b.ReportAllocs()
                var buf [4]float64
                for i := 0; i < b.N; i++ {
                    _ = h.QueryQuantiles(buf[:0], qs)
                }
            })
        })
    }
}

func BenchmarkQueryPathThreeQuantiles(b *testing.B) {
    h := newGoodHist()
    rng := rand.New(rand.NewSource(42))
    for i := 0; i < 10_000; i++ {
        h.Record(int64(benchLo + rng.Float64()*benchRange))
    }
    qs := []float64{0.5, 0.95, 0.99}

    b.Run("SnapshotValuesAtQuantiles", func(b *testing.B) {
        b.ReportAllocs()
        for i := 0; i < b.N; i++ {
            snap := h.Snapshot()
            _ = snap.ValuesAtQuantiles(qs)
        }
    })
    b.Run("QueryQuantiles", func(b *testing.B) {
        b.ReportAllocs()
        var buf [4]float64
        for i := 0; i < b.N; i++ {
            _ = h.QueryQuantiles(buf[:0], qs)
        }
    })
}
