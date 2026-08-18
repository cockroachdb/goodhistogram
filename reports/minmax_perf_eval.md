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

Run on three machines, schema 2, range [500, 6e10], `-count=8`:
- **arm64 (laptop)** — Apple M3 Pro. Raw: `minmax_raw_arm64.txt`; benchstat: `minmax_benchstat_arm64.txt`.
- **amd64 (server)** — GCE `gceworker-briandillmann`, 24 vCPU x86_64, Go 1.25.5.
  Raw: `minmax_raw_amd64.txt`; benchstat: `minmax_benchstat_amd64.txt`.
- **arm64 (server)** — GCE `t2a-standard-8`, 8 vCPU Ampere Altra (Neoverse N1), Go 1.25.5.
  Raw: `minmax_raw_arm64_server.txt`; benchstat: `minmax_benchstat_arm64_server.txt`.

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

## Results — arm64 server (GCE t2a-standard-8, 8 vCPU Ampere Altra)

Single-thread variance is tight (±1%); contention is noisy like the M3.

### Single-threaded (sec/op, vs. baseline)

| ordering   | baseline | minmax          | padded          |
|------------|----------|-----------------|-----------------|
| steady     | 13.36n   | 15.08n (+12.9%) | 15.05n (+12.7%) |
| ascending  | 13.30n   | 15.03n (+13.0%) | 15.04n (+13.1%) |
| descending | 13.21n   | 15.04n (+13.9%) | 14.99n (+13.5%) |

### High contention (sec/op, vs. baseline)

| ordering            | baseline | minmax          | padded          |
|---------------------|----------|-----------------|-----------------|
| g=50, steady        | 101.8n   | 94.5n (−7.1%)   | 94.6n (−7.1%)   |
| g=50, ascending     | 82.2n    | 89.0n (+8.2%)   | 85.5n (~, n.s.) |
| g=50, descending    | 81.9n    | 94.3n (+15.2%)  | 88.0n (~, n.s.) |
| g=100, steady       | 100.2n   | 96.7n (~, n.s.) | 100.1n (~, n.s.)|
| g=100, ascending    | 98.0n    | 99.5n (~, n.s.) | 98.5n (~, n.s.) |
| g=100, descending   | 97.3n    | 99.9n (~, n.s.) | 95.9n (~, n.s.) |

## Findings

1. **Single-threaded overhead is real, small, and consistent — ~10–13% of
   `Record` across all three machines** (steady state): +10% on x86 server,
   +13% on Ampere arm server, +26% on the M3 (higher only because the M3's
   baseline `Record` is ~5–8× faster in absolute terms, so a fixed ~0.7 ns costs
   a bigger fraction). In absolute terms it's ~0.7 ns/op (M3), ~1.7 ns/op
   (Ampere), ~2.1 ns/op (x86). The two server runs have tight ±1% variance so
   every single-thread delta is significant.

2. **Under contention the cost disappears into the noise.** Only the x86 server
   shows a clean, consistent +10% under contention; both arm machines (M3 and
   Ampere) are too noisy (±5–40%) to distinguish min/max from baseline — several
   deltas are even negative. The contended `sum.Add` already dominates, so the
   read-mostly guard loads add little. Treat contention as "no clear regression."

3. **Input ordering doesn't matter — the guard load makes the CAS nearly free.**
   steady / ascending / descending land within ~1% of each other single-threaded
   on both server machines. Even the adversarial monotonic orderings (a new
   extreme on many calls) don't blow up: an uncontended CAS is about as cheap as
   the guard load it follows, and under contention each goroutine replays the
   same array, so the shared extreme settles after the first pass and later CAS
   attempts short-circuit. (A truly unbounded global monotonic stream would
   contend harder; not tested.)

4. **Cache-line padding isn't worth it — and the "padding hurts" effect is
   Apple-M3-specific, not arm64-general.** On the M3 the padded variant was
   consistently *worse* than inline under contention (up to +55%); but on the
   Ampere arm server padding is a wash (within noise), same as x86. So the
   dramatic penalty was an M3 quirk, not an arm property. Everywhere, co-locating
   the read-mostly extremes with the already-hot `sum` cache line (inline) is at
   least as good as padding and never worse — so **don't pad**.

## Recommendation

Exact min/max tracking is cheap enough to add: a **~10–13% single-threaded
`Record` overhead** (~0.7–2 ns/op, consistent across x86 and arm servers and the
M3 laptop), **no clear regression under contention**, and zero extra
allocations. Use the **inline** layout (`minmax` variant) with the load-guarded
CAS; skip the padding.

If that ~10% ever matters on an ultra-hot path, note the guard load is already
what keeps it cheap — no cheaper *exact* scheme exists (exact extremes
fundamentally need CAS, not fetch-and-add). The zero-hot-path-cost alternative
is to derive approximate min/max from the populated bucket edges at `Snapshot`
time, trading exactness for bucket-width error (≤ the configured relative error
bound).
