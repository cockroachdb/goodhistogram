// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

// QueryQuantiles writes estimated values at the given quantiles into dst and
// returns dst[:len(qs)]. It reads live atomic counters directly without
// materializing a Snapshot.
//
// The result reflects the same eventual consistency Snapshot() already
// accepts: counters are read independently and may observe a slightly
// inconsistent total. The inconsistency window here is wider than
// Snapshot+ValuesAtQuantiles because counters are loaded twice (once to
// total, once during the walk); for monotonic counters the only effect is
// that cumulative bucket count may exceed the precomputed total at the
// tail, which is harmless.
//
// qs MUST be sorted in ascending order. dst must have cap >= len(qs); pass a
// stack-backed slice (e.g. var buf [4]float64; h.QueryQuantiles(buf[:0], qs))
// to make the call fully alloc-free.
func (h *Histogram) QueryQuantiles(dst, qs []float64) []float64 {
    dst = dst[:len(qs)]
    if len(qs) == 0 {
        return dst
    }
    cfg := h.cfg
    n := len(h.counts)

    // Pass 1: load scalars and sum the in-range total.
    zeroCount := h.ZeroCount.Load()
    underflow := h.Underflow.Load()
    overflow := h.Overflow.Load()
    var inRange uint64
    for i := 0; i < n; i++ {
        inRange += h.counts[i].Load()
    }
    total := zeroCount + underflow + overflow + inRange

    if total == 0 {
        for i := range dst {
            dst[i] = 0
        }
        return dst
    }

    fTotal := float64(total)
    belowLo := float64(zeroCount + underflow)
    fInRange := float64(inRange)

    // Classify each quantile. Since qs is sorted ascending and rank = q*total
    // is monotonic, low-edges come first, walk-eligible middle next, then
    // high-edges. We resolve edges directly into dst and remember the walk
    // range as [walkStart, walkEnd).
    walkStart := len(qs)
    walkEnd := len(qs)
    for i, q := range qs {
        rank := q * fTotal
        switch {
        case rank <= 0:
            if zeroCount+underflow > 0 {
                dst[i] = cfg.lo
            } else {
                dst[i] = cfg.hi
                for j := 0; j < n; j++ {
                    if h.counts[j].Load() > 0 {
                        dst[i] = cfg.boundaries[j]
                        break
                    }
                }
            }
        case rank >= fTotal:
            if overflow > 0 {
                dst[i] = cfg.hi
            } else {
                dst[i] = cfg.lo
                for j := n - 1; j >= 0; j-- {
                    if h.counts[j].Load() > 0 {
                        dst[i] = cfg.boundaries[j+1]
                        break
                    }
                }
            }
        case rank <= belowLo:
            dst[i] = cfg.lo
        case rank-belowLo > fInRange:
            dst[i] = cfg.hi
        default:
            if i < walkStart {
                walkStart = i
            }
            walkEnd = i + 1
        }
    }

    if walkStart >= walkEnd {
        return dst
    }

    // Pass 2: forward walk with a 3-count sliding window. We re-load each
    // bucket once (peeking ahead by 1) so we have prev/curr/next counts for
    // computing boundary densities on the fly — no scratch slices.
    //
    // boundaryDensity[i]   = (avgDensity[i-1] + avgDensity[i]) / 2
    // boundaryDensity[i+1] = (avgDensity[i]   + avgDensity[i+1]) / 2
    // Edge: at i==0, dL = avgDensity[0]. At i==n-1, dR = 0 (matches the
    // existing ValuesAtQuantiles behavior; see note above the file).

    var prevCount, currCount, nextCount uint64
    var prevW, currW, nextW float64

    currCount = h.counts[0].Load()
    currW = cfg.boundaries[1] - cfg.boundaries[0]
    if n > 1 {
        nextCount = h.counts[1].Load()
        nextW = cfg.boundaries[2] - cfg.boundaries[1]
    }

    var cumCount float64
    wi := walkStart

    for i := 0; i < n && wi < walkEnd; i++ {
        fc := float64(currCount)
        nextCum := cumCount + fc

        // Process all walk-eligible quantiles whose adjusted rank falls
        // in [cumCount, nextCum].
        for wi < walkEnd {
            adjRank := qs[wi]*fTotal - belowLo
            if nextCum < adjRank {
                break
            }
            localRank := adjRank - cumCount
            lo := cfg.boundaries[i]
            if currW <= 0 || fc == 0 {
                dst[wi] = lo
                wi++
                continue
            }
            currD := fc / currW
            var dL, dR float64
            if i == 0 {
                dL = currD
            } else {
                var prevD float64
                if prevW > 0 && prevCount > 0 {
                    prevD = float64(prevCount) / prevW
                }
                dL = (prevD + currD) / 2.0
            }
            if i < n-1 {
                var nextD float64
                if nextW > 0 && nextCount > 0 {
                    nextD = float64(nextCount) / nextW
                }
                dR = (currD + nextD) / 2.0
            }
            // dR remains 0 at i==n-1 to match existing behavior.
            dst[wi] = trapezoidalSolve(lo, currW, fc, dL, dR, localRank)
            wi++
        }

        cumCount = nextCum

        // Slide window forward.
        prevCount, prevW = currCount, currW
        currCount, currW = nextCount, nextW
        if i+2 < n {
            nextCount = h.counts[i+2].Load()
            nextW = cfg.boundaries[i+3] - cfg.boundaries[i+2]
        } else {
            nextCount = 0
            nextW = 0
        }
    }

    // Safety net: any walk-eligible quantiles not yet resolved (shouldn't
    // happen with monotonic counters, but counters can grow between the two
    // passes, so the walk may technically fall short).
    for ; wi < walkEnd; wi++ {
        dst[wi] = cfg.boundaries[n]
    }

    return dst
}
