# goodhistogram

A fast, lock-free exponential histogram for Go that produces
Prometheus native histogram output with bounded relative error
and trapezoidal quantile estimation.

## Performance vs Prometheus native histograms

Schema 2, range [500ns, 60s]. Speed benchmarks on Intel Xeon @ 2.80GHz, 24
cores, x86_64, Linux. Accuracy across 12 distributions, 100k observations each.

| | goodhistogram | Prometheus native |
|---|---:|---:|
| Memory per histogram | 0.85 KB | 19 KB |
| Record (1 thread) | 20 ns/op | 132 ns/op |
| Mean quantile error | 0.35% | 1.00% |

The full results of benchmarks on a Macbook and GCE worker can be seen in
[reports/benchmark_comparison.txt](reports/benchmark_comparison.txt).

## Motivation

CockroachDB maintains hundreds of histograms per node for
latency distributions, operation sizes (log entries, compaction bytes), and
resource utilization. These histograms sit on the hot path of every SQL
statement, every KV request, and every Raft proposal.

We'd been using Prometheus' classic histogram implementation to track and
report on this data with good results, albeit with a little more overhead than
we would have liked. Recent tests however to move Prometheus' new native
histogram formats once again surfaced these overhead concerns and the necessity
of them came into question. 

As a result of this thinking, we designed goodhistogram. goodhistogram is a
purpose-built replacement that:

- Matches Prometheus native bucket and export format.
- Strips memory and compute requirements to the bare minimum required.
- Introduces new recording and quantile computation improvements.

It's designed to be lightweight, fast and accurate as can be to meet the needs
of a performance sensitive database. Additionally, goodhistograms are mergeable,
compressible, and thread safe, making them ideal for high throughput
distributed systems.

## Getting started

### Install

```
go get github.com/cockroachdb/goodhistogram
```

### Create and record

```go
h := goodhistogram.New(goodhistogram.Params{
    Lo:           500,    // lower bound of tracked range (e.g. 500ns)
    Hi:           60e9,   // upper bound (e.g. 60s in nanoseconds)
    ErrorBound: 0.10,   // 10% relative error → schema 2
})

h.Record(1_500_000)  // 1.5ms
h.Record(42_000)     // 42µs
```

### Take a snapshot and compute quantiles

```go
snap := h.Snapshot()

p99 := snap.ValueAtQuantile(0.99)
mean := snap.Mean()
count, sum := snap.Total()
```

### Register with Prometheus

A histogram can be registered with a Prometheus registry via
`ToPrometheusCollector`. Recording is done directly on the histogram;
the collector only participates in the scrape path.

```go
h := goodhistogram.New(goodhistogram.Params{
    Lo:         500,
    Hi:         60e9,
    ErrorBound: 0.10,
})

desc := prometheus.NewDesc("request_duration_ns", "Request duration in nanoseconds", nil, nil)
prometheus.MustRegister(h.ToPrometheusCollector(desc))

// Hot path — record directly on the histogram.
h.Record(durationNs)
```

The collector exports both conventional cumulative buckets (for
backward compatibility with classic Prometheus) and native histogram
sparse fields (schema, spans, deltas). Because the internal bucket
indices are Prometheus bucket keys by construction, export is a direct
copy with no remapping.

### Labeled histograms (HistogramVec)

For multi-dimensional histograms partitioned by label values, use
`HistogramVec`. It implements `prometheus.Collector` so the entire
vec can be registered with a registry.

```go
vec := goodhistogram.NewHistogramVec(
    goodhistogram.Params{Lo: 500, Hi: 60e9, ErrorBound: 0.10},
    "request_duration_ns", "Request duration in nanoseconds",
    []string{"method", "path"},
)
prometheus.MustRegister(vec)

// Hot path — WithLabelValues returns a *Histogram for direct recording.
vec.WithLabelValues("GET", "/api").Record(durationNs)
```

### Export to Prometheus proto

If you need the proto directly (e.g. for remote write or custom
export), use `ToPrometheusHistogram` on a snapshot:

```go
ph := snap.ToPrometheusHistogram() // *prometheusgo.Histogram
```


## How it works

goodhistogram takes inspiration from [Prometheus native
histograms](https://prometheus.io/docs/specs/native_histograms/),
[DDSketch](https://www.datadoghq.com/blog/engineering/computing-accurate-percentiles-with-ddsketch/),
and [base2histogram](https://blog.openacid.com/algo/histogram/). At a high
level, it borrows the exponential bucketing scheme from Prometheus and
DDSketch, whose error bounds act as the input to the constructor, it uses the
bucket grouping concept found both in base2histogram and prometheus for fast
recording. It then uses the trapezoidal interpolation described by
base2histogram to significantly improve quantile estimation.

What makes it different from these libraries is the following:
 - It uses an array for optimal runtime operations, while preserving the ability for sparse output.
 - It performs constant time lookups with a log-linear bucketing function.
 - It allows configuring a precise error bound, which determines bucket density.
 - It only allocates buckets within a parameterized expected range, minimizing memory.

### Bucket layout

goodhistogram uses the same exponential bucket scheme as Prometheus
native histograms. For a given schema *s*, each power-of-two octave is
divided into 2^*s* buckets with boundaries at 2^(*j*/2^*s*). The
schema is chosen as the coarsest one whose relative error
(*gamma* - 1)/(*gamma* + 1) is at or below the requested `ErrorBound`,
where *gamma* = 2^(2^(-*s*)).

At construction, a fixed array of `atomic.Uint64` counters is allocated
covering the range `[Lo, Hi]`. The number of buckets is determined by
the Prometheus bucket keys of `Lo` and `Hi`:

```
numBuckets = promBucketKey(Hi, schema) - promBucketKey(Lo, schema) + 1
```

### Recording (the hot path)

`Record(v)` maps an `int64` to a bucket index in O(1) without locks or
allocations:

1. Convert to `float64` and extract the IEEE 754 bits.
2. Compute the exponent from the biased exponent field: `exp = bits>>52 - 1022`.
3. Look up the bucket index from a precomputed 256-entry table indexed by the
   top 8 bits of the mantissa.
4. Combine: `key = bucket + (exp-1) * bucketsPerGroup`, then
   `idx = key - minKey`.
5. `counts[idx].Add(1)`.

The novelty of this implementation is in the precomputed bucket lookup table.
This enables both constant-time bucket lookup, in addition to prometheus
compatible export, which serves as a helpful collection of features from
incompatible implementations.

### Quantile estimation

`ValueAtQuantile` uses trapezoidal interpolation rather than the
uniform-density (log-linear) interpolation used by Prometheus:

1. Compute average density in each bucket: count / width.
2. Estimate density at each boundary by averaging adjacent buckets.
3. Within the target bucket, model the density as varying linearly from
   left to right (a trapezoid). The CDF is the integral of this linear
   function, which is a quadratic.
4. Solve the quadratic to find the value at the target rank.

This produces more accurate estimates when the true density is not uniform
within a bucket, which is nearly always the case for real-world distributions.
This approach is described well in the base2histogram link cited above.

### Sacrifices in Accuracy

As a final note on the design, goodhistogram takes two tracks which
theoretically affect quantile accuracy. The first is that it does not lock on
snapshot, which means that snapshots can be inconsistent. Inconsistent in this
case means that the `TotalSum` seen for the histogram can be greater than the sum
of its parts. In practice, the likelihood of this is low enough, and the order
of the effect small enough, that no detectable difference would be seen.

Bucket lookup is imprecise as well. Because of the techniques used to make
bucket lookup constant, ~0.45% of the samples will be incorrectly rounded to a
neighboring bucket. The resulting distortion however is small (.02%) compared
to the performance gains that it allows for. The distortions seen are similar
to the ones seen in non-linear bucketing formats, which too optimize for lookup
performance.

## Performance Notes

It should be noted that the performance on this structure is likely to look
much closer to the single-threaded example than the concurrently accessed one.
This is because the benchmarks operate on a throughput that is much higher than
would be seen on any histogram in CockroachDB. This means the likelihood of any
two requests entering the critical region simultaneously is exceedingly low, at
least with the database as written today.

I should also note that these results are not the actual floor observed. The
benchmarks above were run on x86, as it is today the most common chip
architecture found on cloud machines. If one re-runs the test on ARM-based
machines the differences between goodhistogram and Prometheus widen:

| | goodhistogram | Prometheus native |
|---|---:|---:|
| Record (1 thread) | 12.9 ns/op | 103 ns/op |

This is the result of ARM's optimistic atomic instructions, which allow
increments without fencing. The gap widens even further on a Mac ARM
machine:

| | goodhistogram | Prometheus native |
|---|---:|---:|
| Record (1 thread) | 2.6 ns/op | 58.1 ns/op |
