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
