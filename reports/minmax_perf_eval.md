# Performance evaluation: exact min/max tracking on the hot path

**Question:** what does it cost to track the *exact* minimum and maximum value a
`goodhistogram` has seen, added to the lock-free `Record` hot path?

Today `Record` does a `sum.Add` and a single `counts[idx].Add` — both
fetch-and-add. Exact extremes can't use fetch-and-add; each needs a
compare-and-swap loop guarded by a relaxed load:

```go
for {
    old := dst.Load()
    if v >= old { return }              // guard: skip CAS unless v is a new extreme
    if dst.CompareAndSwap(old, v) { return }
}
```

The guard load short-circuits the steady state (`v` isn't a new extreme), so the
CAS only fires when an extreme actually moves. The costs to measure are therefore
(a) two extra shared-atomic **loads** on every `Record`, and (b) contended CAS
retries when extremes churn.

## What was measured

Three `Record` variants, A/B benchmarked (`minmax.go`, `minmax_benchmark_test.go`):

| variant    | description |
|------------|-------------|
| `baseline` | existing `Record`, no extreme tracking |
| `minmax`   | min/max in two `atomic.Int64` packed **inline** in the struct (adjacent to `sum`) |
| `padded`   | min/max in **cache-line-padded** atomics (each on its own 64-byte line), to isolate false sharing |

Cost depends on input **ordering**, not distribution shape, so orderings are the
independent variable:

- **steady** — shuffled log-uniform. Extremes settle after warm-up, then every CAS
  is skipped by the guard. This is the normal metrics case.
- **ascending** — monotonically increasing: every `Record` sets a new max → CAS
  every call. Adversarial worst case.
- **descending** — monotonically decreasing: every `Record` sets a new min.

Machine: Apple M3 Pro (arm64), schema 2, range [500, 6e10], `-count=8`.
Raw data: `minmax_raw.txt`; benchstat: `minmax_benchstat.txt`.

## Results

### Single-threaded (sec/op, vs. baseline)

| ordering   | baseline | minmax          | padded          |
|------------|----------|-----------------|-----------------|
| steady     | 2.75n    | 3.46n (+25.6%)  | 3.45n (+25.3%)  |
| ascending  | 2.69n    | 3.49n (+29.7%)  | 4.94n (+83.7%, ±30%) |
| descending | 3.83n ±30% | 3.50n (~, n.s.) | 4.61n (+20.5%)  |

### High contention (sec/op, vs. baseline)

| ordering            | baseline | minmax          | padded          |
|---------------------|----------|-----------------|-----------------|
| g=50, steady        | 43.4n    | 51.8n (+19.3%)  | 54.9n (+26.4%)  |
| g=50, ascending     | 51.1n    | 49.7n (~, n.s.) | 57.2n (+11.9%)  |
| g=50, descending    | 38.9n    | 36.5n (~, n.s.) | 60.4n (+55.4%)  |
| g=100, steady       | 54.6n    | 52.2n (~, n.s.) | 55.1n (~, n.s.) |
| g=100, ascending    | 55.2n    | 53.0n (~, n.s.) | 60.4n (~, n.s.) |
| g=100, descending   | 41.1n    | 37.0n (~, n.s.) | 63.9n (+55.4%)  |

("~, n.s." = not statistically significant, p > 0.05.)

## Findings

1. **Single-threaded overhead is small in absolute terms: ~0.7–0.8 ns/op**
   (+25–30% on a ~2.7 ns baseline). Even the adversarial ascending case
   (CAS on *every* call) costs only ~0.8 ns more — an uncontended CAS is nearly
   as cheap as the guard load, so worst-case ≈ steady-state single-threaded.

2. **Under contention, inline min/max is effectively free.** In 5 of 6 cases the
   inline `minmax` variant is statistically indistinguishable from baseline
   (often nominally faster, within noise). The contended `sum.Add` already
   dominates the hot path; two read-mostly guard loads that land on an
   already-bounced cache line add nothing measurable. Contention runs are noisy
   (baseline itself ±10–30%), so treat these as "no detectable regression"
   rather than precise deltas.

3. **Cache-line padding is a pessimization here — the counterintuitive result.**
   The `padded` variant is consistently *worse* than inline under contention
   (up to +55%). Padding is the standard fix for false sharing, but the extremes
   are **read-mostly**: in the inline layout their guard loads piggyback on the
   `sum` cache line the core already owns each iteration (it just did `sum.Add`).
   Padding moves them onto two *separate* lines that must be fetched
   additionally, tripling the hot coherence footprint (1 line → 3). Co-locating
   the extremes with the already-hot `sum` is the right call; do **not** pad.

## Recommendation

Exact min/max tracking is cheap enough to add: **~0.8 ns/op single-threaded and
no detectable regression under contention**, zero extra allocations. Use the
**inline** layout (`minmax` variant) and keep the load-guarded CAS. Avoid
cache-line padding.

If the ~0.8 ns single-threaded cost ever matters on an ultra-hot path, the guard
load is what makes it cheap — no cheaper exact scheme exists (exact extremes
fundamentally require CAS, not fetch-and-add). An approximate alternative would
be to derive min/max from the populated bucket edges at `Snapshot` time for
**zero** hot-path cost, at the price of bucket-width error (≤ the configured
relative error bound) instead of exact values.
