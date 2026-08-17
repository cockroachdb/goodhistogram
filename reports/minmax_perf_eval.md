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

Run on two machines, schema 2, range [500, 6e10], `-count=8`:
- **arm64** — Apple M3 Pro (laptop). Raw: `minmax_raw_arm64.txt`; benchstat: `minmax_benchstat_arm64.txt`.
- **amd64** — GCE worker `gceworker-briandillmann`, 24 vCPU x86_64, Go 1.25.5.
  Raw: `minmax_raw_amd64.txt`; benchstat: `minmax_benchstat_amd64.txt`.

## Results — arm64 (Apple M3 Pro)

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

("~, n.s." = not statistically significant, p > 0.05.) The M3's contention
numbers are noisy (baseline ±10–40%), which is why several deltas land as n.s.

## Results — amd64 (GCE worker, 24 vCPU x86_64)

Far lower variance (±0–2%), so every delta below is significant (p < 0.001).
Note the ~8× higher absolute per-op cost than the M3 — the GCE vCPU has much
lower single-thread throughput than Apple silicon; the *relative* overhead is
what transfers.

### Single-threaded (sec/op, vs. baseline)

| ordering   | baseline | minmax          | padded          |
|------------|----------|-----------------|-----------------|
| steady     | 20.86n   | 23.00n (+10.3%) | 22.71n (+8.9%)  |
| ascending  | 20.86n   | 22.98n (+10.2%) | 22.68n (+8.7%)  |
| descending | 20.88n   | 23.02n (+10.3%) | 22.67n (+8.6%)  |

### High contention (sec/op, vs. baseline)

| ordering            | baseline | minmax          | padded           |
|---------------------|----------|-----------------|------------------|
| g=50, steady        | 39.70n   | 43.83n (+10.4%) | 44.18n (+11.3%)  |
| g=50, ascending     | 39.00n   | 43.16n (+10.7%) | 43.38n (+11.2%)  |
| g=50, descending    | 38.91n   | 42.97n (+10.4%) | 43.22n (+11.1%)  |
| g=100, steady       | 39.57n   | 43.68n (+10.4%) | 42.93n (+8.5%)   |
| g=100, ascending    | 39.29n   | 42.75n (+8.8%)  | 44.46n (+13.2%)  |
| g=100, descending   | 38.27n   | 42.78n (+11.8%) | 44.74n (+16.9%)  |

## Findings

1. **The overhead is real but small and roughly constant per op.** In absolute
   terms it's ~0.7–0.8 ns/op on the M3 and ~2.1 ns/op single-threaded / ~4 ns/op
   under contention on the GCE x86 worker. As a fraction of `Record` it lands at
   **+10% on x86** and +25–30% single-threaded on the (much faster, so
   higher-fraction) M3. The x86 run is the trustworthy one for the *relative*
   number: its variance is ±0–2% so every delta is significant, whereas the M3's
   contention noise (±10–40%) swallowed the signal and made several deltas read
   as "no change."

2. **Input ordering doesn't matter — the guard load makes the CAS nearly free.**
   On x86, steady / ascending / descending all cost within ~1% of each other,
   single-threaded *and* under 50–100-goroutine contention. Even the adversarial
   monotonic orderings (a new extreme on many calls) don't blow up: an
   uncontended CAS is about as cheap as the guard load it follows, and under
   contention each goroutine replays the same array, so the shared extreme
   settles after the first pass and later CAS attempts short-circuit. (A truly
   unbounded monotonic stream across all goroutines would contend harder; not
   tested here.)

3. **Cache-line padding is not worth it, and on arm64 it actively hurts.** On
   the M3 the padded variant was consistently *worse* than inline under
   contention (up to +55%); on x86 it's roughly a wash (±a few %, occasionally
   worse at g=100). The extremes are **read-mostly**, so in the inline layout
   their guard loads piggyback on the `sum` cache line the core already touches
   each iteration (it just did `sum.Add`); padding moves them onto separate
   lines that must be fetched additionally. Co-locating with `sum` is at least
   as good everywhere and strictly better on arm64 — do **not** pad.

## Recommendation

Exact min/max tracking is cheap enough to add: a **consistent ~10% `Record`
overhead on x86** (~2–4 ns/op) and low-single-digit ns on Apple silicon, with
zero extra allocations. Use the **inline** layout (`minmax` variant) with the
load-guarded CAS; skip the padding.

If that ~10% ever matters on an ultra-hot path, note the guard load is already
what keeps it cheap — no cheaper *exact* scheme exists (exact extremes
fundamentally need CAS, not fetch-and-add). The zero-hot-path-cost alternative
is to derive approximate min/max from the populated bucket edges at `Snapshot`
time, trading exactness for bucket-width error (≤ the configured relative error
bound).
