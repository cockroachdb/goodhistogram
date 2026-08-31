# goodhistogram (Rust)

Rust implementation of [`github.com/cockroachdb/goodhistogram`](https://github.com/cockroachdb/goodhistogram):
a lock-free, Prometheus-aligned exponential histogram.

This crate lives in the same repository as the canonical Go implementation. The
two are kept in lockstep by a shared conformance fixture generated from the Go
code (`../testdata/conformance.txt`); `tests/conformance.rs` replays it against
this crate. When you change bucketing or quantile behavior in either language,
regenerate the fixture (`go test ./go/ -run TestConformance -rewrite` from the repo
root) and both test suites must agree.

## Usage

```toml
[dependencies]
goodhistogram = { git = "https://github.com/cockroachdb/goodhistogram" }
```

```rust
use goodhistogram::{Histogram, Params};

let h = Histogram::new(Params { lo: 1_000.0, hi: 60e9, ..Default::default() });
h.record(1_500_000); // 1.5ms in nanoseconds

let snap = h.snapshot();
let p99 = snap.value_at_quantile(0.99);
```

The core library has no dependencies.

## Metrics-ecosystem integration (optional)

Two off-by-default features expose goodhistogram to the common Rust metrics
crates. Both are **duration** histograms: values are observed in seconds, stored
internally in nanoseconds, and exported in seconds; they export the full bucket
set so `histogram_quantile()` works against the scraped metric.

### `prometheus` — tikv [`prometheus`](https://crates.io/crates/prometheus) crate

```toml
goodhistogram = { git = "https://github.com/cockroachdb/goodhistogram", features = ["prometheus"] }
```

```rust
use goodhistogram::{LATENCY_PARAMS, prometheus::PromHistogram};

let h = PromHistogram::new("op_duration_seconds", "op latency", LATENCY_PARAMS);
h.observe(0.0015); // seconds
registry.register(Box::new(h))?;
```

`PromHistogram`/`PromHistogramVec` implement `prometheus::core::Collector` — a
near-drop-in replacement for `prometheus::Histogram`.

### `prometheus-client` — OpenMetrics-native [`prometheus-client`](https://crates.io/crates/prometheus-client) crate

```toml
goodhistogram = { git = "https://github.com/cockroachdb/goodhistogram", features = ["prometheus-client"] }
```

```rust
use goodhistogram::{LATENCY_PARAMS, prometheus_client::HistogramCollector};

let h = HistogramCollector::new("op_duration_seconds", "op latency", LATENCY_PARAMS);
h.observe(0.0015);
registry.register_collector(Box::new(h));
```

`HistogramCollector` implements `prometheus_client::collector::Collector`.

_Planned:_ a `metrics`-facade recorder for backend-agnostic integration.
