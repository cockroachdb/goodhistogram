// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use std::sync::Arc;
use std::thread;

#[test]
fn test_pick_schema() {
    // The error bound is the worst-case bucket error (γ-1), so a given bound
    // selects one schema finer than the DDSketch midpoint convention would.
    assert_eq!(pick_schema(0.35), 2);
    assert_eq!(pick_schema(0.10), 3);
    assert_eq!(pick_schema(0.05), 4);
    assert_eq!(pick_schema(0.03), 5);
    assert_eq!(pick_schema(0.02), 6);
    assert_eq!(pick_schema(0.005), 8);
    assert_eq!(pick_schema(0.002), 8);
    assert_eq!(pick_schema(0.001), 8);
}

#[test]
fn test_schema_error_bounds_match() {
    // The table is literal because schema_relative_error isn't const-evaluable;
    // keep each entry exactly equal to it so callers that pin a schema don't
    // silently drift to another one.
    for s in 0..=MAX_SCHEMA {
        assert_eq!(
            SCHEMA_ERROR_BOUNDS[s as usize],
            schema_relative_error(s),
            "schema {s}"
        );
        assert_eq!(pick_schema(SCHEMA_ERROR_BOUNDS[s as usize]), s);
    }
    assert_eq!(STANDARD_ERROR_BOUND, SCHEMA_ERROR_BOUNDS[2]);
}

#[test]
fn test_config_boundaries_monotonic() {
    let cfg = Config::new(1e4, 1e16, 0.05);
    assert!(cfg.num_buckets > 0);
    assert_eq!(cfg.boundaries.len(), cfg.num_buckets + 1);
    // Lo and hi only select buckets; the boundaries are those buckets' real
    // edges.
    let bounds = native_histogram_bounds(cfg.schema);
    let lo_key = prom_bucket_key(1e4, cfg.schema, &bounds);
    let hi_key = prom_bucket_key(1e16, cfg.schema, &bounds);
    assert_eq!(cfg.boundaries[0], get_le(lo_key - 1, cfg.schema, &bounds));
    assert!(cfg.boundaries[0] < 1e4);
    assert_eq!(
        cfg.boundaries[cfg.num_buckets],
        get_le(hi_key, cfg.schema, &bounds)
    );
    assert!(cfg.boundaries[cfg.num_buckets] > 1e16);
    for i in 1..cfg.boundaries.len() {
        assert!(
            cfg.boundaries[i] > cfg.boundaries[i - 1],
            "boundary[{}]={} not > boundary[{}]={}",
            i,
            cfg.boundaries[i],
            i - 1,
            cfg.boundaries[i - 1]
        );
    }
}

#[test]
fn test_no_zero_width_first_bucket() {
    for lo in [1.0, 2.0, 4.0, 8.0, 1024.0] {
        let cfg = Config::new(lo, lo * 1e6, 0.05);
        assert!(
            cfg.boundaries[1] > cfg.boundaries[0],
            "lo={lo}: first bucket is zero-width [{}, {}]",
            cfg.boundaries[0],
            cfg.boundaries[1]
        );
    }
}

#[test]
#[should_panic]
fn test_config_panics_zero_lo() {
    Config::new(0.0, 100.0, 0.05);
}

#[test]
#[should_panic]
fn test_config_panics_negative_lo() {
    Config::new(-1.0, 100.0, 0.05);
}

#[test]
#[should_panic]
fn test_config_panics_hi_le_lo() {
    Config::new(100.0, 50.0, 0.05);
}

#[test]
fn test_record_and_snapshot() {
    let h = Histogram::new(Params {
        lo: 1e3,
        hi: 1e9,
        error_bound: 0.05,
    });

    let values = [
        1000i64, 5000, 10000, 100000, 1000000, 10000000, 100000000, 999999999,
    ];
    let expected_sum: i64 = values.iter().sum();
    for &v in &values {
        h.record(v);
    }

    let snap = h.snapshot();
    assert_eq!(snap.total_count, values.len() as u64);
    assert_eq!(snap.total_sum, expected_sum);
    assert_eq!(snap.zero_count, 0);
    assert_eq!(snap.underflow, 0);
    assert_eq!(snap.overflow, 0);
}

#[test]
fn test_record_out_of_range() {
    let h = Histogram::new(Params {
        lo: 1000.0,
        hi: 1_000_000.0,
        error_bound: 0.10,
    });

    h.record(500); // underflow
    h.record(2_000_000); // overflow
    h.record(0); // zero
    h.record(-42); // negative → zero

    let snap = h.snapshot();
    assert_eq!(snap.total_count, 4);
    assert_eq!(snap.underflow, 1);
    assert_eq!(snap.overflow, 1);
    assert_eq!(snap.zero_count, 2);
    assert_eq!(snap.total_sum, (500 + 2_000_000) - 42);
}

#[test]
fn test_concurrent_record() {
    let h = Arc::new(Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.05,
    }));

    const THREADS: usize = 8;
    const RECORDS_PER_THREAD: usize = 10_000;

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let h = Arc::clone(&h);
        handles.push(thread::spawn(move || {
            // Simple deterministic sequence per thread.
            for i in 0..RECORDS_PER_THREAD {
                let v = ((t * RECORDS_PER_THREAD + i) % 999_999 + 1) as i64;
                h.record(v);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let snap = h.snapshot();
    assert_eq!(snap.total_count, (THREADS * RECORDS_PER_THREAD) as u64);
}

#[test]
fn test_quantile_uniform() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1000.0,
        error_bound: 0.05,
    });
    for i in 1..=1000i64 {
        h.record(i);
    }

    let snap = h.snapshot();
    let schema = pick_schema(0.05);
    let max_rel_error = schema_relative_error(schema);

    for q in [0.50, 0.75, 0.90, 0.95, 0.99] {
        let got = snap.value_at_quantile(q);
        let expected = q * 1000.0;
        let rel_err = (got - expected).abs() / expected;
        assert!(
            rel_err <= max_rel_error * 3.0,
            "q{}: got={:.1} expected={:.1} relErr={:.4} maxAllowed={:.4}",
            (q * 100.0) as u32,
            got,
            expected,
            rel_err,
            max_rel_error * 3.0
        );
    }
}

#[test]
fn test_quantile_edge_cases() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1000.0,
        error_bound: 0.10,
    });

    // Empty histogram.
    let snap = h.snapshot();
    assert_eq!(snap.value_at_quantile(0.50), 0.0);

    // Single value.
    h.record(500);
    let snap = h.snapshot();
    let p50 = snap.value_at_quantile(0.50);
    assert!((p50 - 500.0).abs() / 500.0 < 0.15, "single value p50={p50}");

    // q=0 and q=1.
    let p0 = snap.value_at_quantile(0.0);
    let p100 = snap.value_at_quantile(1.0);
    assert!(p0 > 0.0, "p0 should be positive");
    assert!(p100 > 0.0, "p100 should be positive");
}

#[test]
fn test_reset() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.10,
    });
    h.record(100);
    h.record(200);
    h.record(0); // zero
    h.record(-1); // negative → zero
    h.record(2_000_000); // overflow

    let snap = h.snapshot();
    assert_eq!(snap.total_count, 5);

    h.reset();
    let snap = h.snapshot();
    assert_eq!(snap.total_count, 0);
    assert_eq!(snap.total_sum, 0);
    assert_eq!(snap.zero_count, 0);
    assert_eq!(snap.underflow, 0);
    assert_eq!(snap.overflow, 0);
}

#[test]
fn test_mean_and_total() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.10,
    });

    // Empty.
    assert!(h.snapshot().mean().is_nan());

    h.record(100);
    h.record(200);
    h.record(300);

    let snap = h.snapshot();
    assert!((snap.mean() - 200.0).abs() < 0.01);
    let (count, sum) = snap.total();
    assert_eq!(count, 3);
    assert!((sum - 600.0).abs() < 0.01);
}

#[test]
fn test_frexp_values() {
    let cases = [
        (1.0, 0.5, 1),
        (2.0, 0.5, 2),
        (4.0, 0.5, 3),
        (8.0, 0.5, 4),
        (0.5, 0.5, 0),
        (0.25, 0.5, -1),
        (3.0, 0.75, 2),
        (6.0, 0.75, 3),
    ];
    for (v, want_frac, want_exp) in cases {
        let (frac, exp) = frexp(v);
        assert!(
            (frac - want_frac).abs() < 1e-10,
            "frexp({v}): frac={frac}, want {want_frac}"
        );
        assert_eq!(exp, want_exp, "frexp({v}): exp={exp}, want {want_exp}");
    }
}

#[test]
fn test_record_and_quantile() {
    let h = Histogram::new(Params {
        lo: 1_000.0,
        hi: 10e9,
        error_bound: 0.10,
    });

    // Record 1000 values from 1ms to 1s.
    for i in 1..=1000 {
        h.record(i * 1_000_000); // i milliseconds in nanoseconds
    }

    let snap = h.snapshot();
    assert_eq!(snap.total_count, 1000);

    let p50 = snap.value_at_quantile(0.50);
    let p99 = snap.value_at_quantile(0.99);

    // p50 should be near 500ms = 500_000_000 ns (within 15% error).
    assert!(
        (p50 - 500_000_000.0).abs() / 500_000_000.0 < 0.15,
        "p50={p50}, expected ~500_000_000"
    );
    // p99 should be near 990ms = 990_000_000 ns.
    assert!(
        (p99 - 990_000_000.0).abs() / 990_000_000.0 < 0.15,
        "p99={p99}, expected ~990_000_000"
    );
}

#[test]
fn test_schema_accessors() {
    for (eb, want) in [(0.35, 2), (0.10, 3), (0.05, 4)] {
        let h = Histogram::new(Params {
            lo: 1.0,
            hi: 1e9,
            error_bound: eb,
        });
        assert_eq!(h.schema(), want, "error_bound {eb}");
        assert_eq!(h.snapshot().schema(), want);
    }
}

#[test]
fn test_default_error_bound_is_schema_3() {
    // Matches Go: an unset error bound defaults to 10% (schema 3).
    assert_eq!(DEFAULT_ERROR_BOUND, 0.10);
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e9,
        error_bound: 0.0,
    });
    assert_eq!(h.schema(), 3);
    assert_eq!(Params::default().error_bound, 0.10);
}

#[test]
fn test_values_at_quantiles_matches_individual() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1000.0,
        error_bound: 0.05,
    });
    for i in 1..=1000i64 {
        h.record(i);
    }
    let snap = h.snapshot();
    let qs = [0.0, 0.25, 0.5, 0.9, 0.99, 1.0];
    let batch = snap.values_at_quantiles(&qs);
    assert_eq!(batch.len(), qs.len());
    for (i, &q) in qs.iter().enumerate() {
        assert_eq!(batch[i], snap.value_at_quantile(q), "q={q}");
    }
}

#[test]
fn test_values_at_quantiles_into_matches_batch() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1000.0,
        error_bound: 0.05,
    });
    for i in 1..=1000i64 {
        h.record(i);
    }
    let qs = [0.0, 0.5, 0.99, 1.0];
    let mut dst = Vec::new();
    h.values_at_quantiles_into(&mut dst, &qs);
    assert_eq!(dst, h.snapshot().values_at_quantiles(&qs));
}

#[test]
fn test_merge() {
    let p = Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.1,
    };
    let a = Histogram::new(p);
    let b = Histogram::new(p);
    a.record(100);
    a.record(200);
    b.record(200);
    b.record(0); // zero
    b.record(5_000_000); // overflow

    let (sa, sb) = (a.snapshot(), b.snapshot());
    let m = sa.merge(&sb);
    assert_eq!(m.total_count, sa.total_count + sb.total_count);
    assert_eq!(m.total_sum, sa.total_sum + sb.total_sum);
    assert_eq!(m.zero_count, sb.zero_count);
    assert_eq!(m.overflow, sb.overflow);
    for i in 0..m.counts.len() {
        assert_eq!(m.counts[i], sa.counts[i] + sb.counts[i]);
    }
}

#[test]
fn test_sub() {
    let p = Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.1,
    };
    let cumulative = Histogram::new(p);
    cumulative.record(100);
    let baseline = cumulative.snapshot(); // 1 observation
    cumulative.record(200);
    cumulative.record(300);
    let cur = cumulative.snapshot(); // 3 observations

    // Windowed view: current minus baseline = the 2 newer observations.
    let window = cur.sub(&baseline);
    assert_eq!(window.total_count, 2);
    assert_eq!(window.total_sum, 500);
    // merge is the inverse of sub.
    let restored = window.merge(&baseline);
    assert_eq!(restored.counts, cur.counts);
    assert_eq!(restored.total_count, cur.total_count);
}

#[test]
fn test_presets_schema() {
    assert_eq!(Histogram::new(COARSE_PARAMS).schema(), 1);
    assert_eq!(Histogram::new(STANDARD_PARAMS).schema(), 2);
    assert_eq!(Histogram::new(FINE_PARAMS).schema(), 3);
    // The time/size presets leave the default error bound (schema 3).
    for p in [
        HIRES_LATENCY_PARAMS,
        IO_LATENCY_PARAMS,
        RESPONSE_TIME_PARAMS,
        LONG_RUNNING_PARAMS,
        DATA_SIZE_PARAMS,
        MEMORY_USAGE_PARAMS,
    ] {
        assert_eq!(Histogram::new(p).schema(), 3);
    }
}

#[test]
fn test_conventional_buckets() {
    let h = Histogram::new(Params {
        lo: 1000.0,
        hi: 1_000_000.0,
        error_bound: 0.10,
    });
    h.record(500); // underflow
    h.record(0); // zero
    h.record(50_000); // in range
    h.record(2_000_000); // overflow

    let snap = h.snapshot();
    let buckets = snap.conventional_buckets();

    // One entry per bucket plus the trailing +Inf.
    assert_eq!(buckets.len(), snap.counts.len() + 1);

    // Cumulative counts are monotonically non-decreasing.
    for w in buckets.windows(2) {
        assert!(w[1].1 >= w[0].1, "cumulative counts must not decrease");
    }

    // Zero + underflow are below every finite bucket, so the first bucket's
    // cumulative count already includes them.
    assert_eq!(buckets[0].1, snap.zero_count + snap.underflow);

    // The final bucket is +Inf and holds every observation, overflow included.
    let last = buckets.last().unwrap();
    assert!(last.0.is_infinite());
    assert_eq!(last.1, snap.total_count);
    assert_eq!(last.1, 4);
}

#[test]
fn test_to_openmetrics() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1000.0,
        error_bound: 0.10,
    });
    h.record(10);
    h.record(100);

    let text = h.snapshot().to_openmetrics("latency");
    assert!(text.starts_with("# TYPE latency histogram\n"));
    assert!(text.contains("latency_bucket{le=\"+Inf\"} 2"));
    assert!(text.contains("latency_sum 110"));
    assert!(text.contains("latency_count 2"));
    // Every bucket line carries an le label.
    for line in text.lines().filter(|l| l.contains("_bucket{")) {
        assert!(line.contains("le=\""), "malformed bucket line: {line}");
    }
}

#[test]
fn test_boundaries_align_with_counts() {
    let h = Histogram::new(Params {
        lo: 1e3,
        hi: 1e9,
        error_bound: 0.10,
    });

    let values = [1_500i64, 20_000, 375_000, 4_000_000, 88_000_000];
    for &v in &values {
        h.record(v);
    }

    let snap = h.snapshot();
    let bounds = snap.boundaries();
    // One more boundary than bucket: bucket i spans (bounds[i], bounds[i+1]].
    assert_eq!(bounds.len(), snap.counts.len() + 1);
    assert_eq!(snap.underflow, 0);
    assert_eq!(snap.overflow, 0);

    // Every recorded value must land in a bucket whose advertised range
    // actually contains it. A reader of the bucket dump takes these labels
    // literally, so an off-by-one here would misreport where the mass sits
    // rather than fail loudly.
    let mut populated = 0;
    for (i, &c) in snap.counts.iter().enumerate() {
        if c == 0 {
            continue;
        }
        populated += c;
        let (lo, hi) = (bounds[i], bounds[i + 1]);
        let matches = values
            .iter()
            .filter(|&&v| v as f64 > lo && v as f64 <= hi)
            .count() as u64;
        assert_eq!(
            matches, c,
            "bucket {i} spanning ({lo}, {hi}] holds {c} but {matches} values fall in it"
        );
    }
    assert_eq!(populated, values.len() as u64);
}

/// The record path's key for a value, computed the way `Histogram::record` does.
fn lookup_key(cfg: &Config, v: f64) -> i32 {
    let bits = v.to_bits();
    let exp = ((bits >> 52) & 0x7FF) as i32 - 1022;
    let i = ((bits >> BUCKET_LOOKUP_SHIFT) as usize) & (BUCKET_LOOKUP_SIZE - 1);
    let sub = cfg.bucket_lookup[i] as i32 + ((bits & MANTISSA_MASK) > cfg.bucket_edge[i]) as i32;
    sub + (exp - 1) * cfg.buckets_per_group as i32
}

#[test]
fn test_bucket_lookup_exact() {
    for schema in 0..=MAX_SCHEMA {
        let cfg = Config::new(1.0, (1u64 << 40) as f64, schema_relative_error(schema));
        assert_eq!(cfg.schema, schema);
        let bounds = native_histogram_bounds(schema);
        let check = |v: f64| {
            let want = prom_bucket_key(v, schema, &bounds);
            assert_eq!(lookup_key(&cfg, v), want, "schema {schema} v={v}");
        };
        for exp in [0, 1, 10, 33] {
            let scale = ldexp(1.0, exp);
            for i in 0..BUCKET_LOOKUP_SIZE as u64 {
                let lo = f64::from_bits((1023u64 << 52) | (i << BUCKET_LOOKUP_SHIFT));
                let hi = f64::from_bits(lo.to_bits() | ((1u64 << BUCKET_LOOKUP_SHIFT) - 1));
                check(lo * scale);
                check(hi * scale);
            }
            for &b in &bounds {
                let edge = b * 2.0 * scale;
                check(edge);
                check(f64::from_bits(edge.to_bits() - 1));
                check(f64::from_bits(edge.to_bits() + 1));
            }
        }
        for exp in 0..63 {
            let p = (1u64 << exp) as f64;
            check(p);
            check(p + 1.0);
            if exp > 0 {
                check(p - 1.0);
            }
        }
    }
}

/// Lo is exclusive and hi inclusive, like Prometheus's (lower, upper] buckets:
/// recording lo underflows exactly when lo is a bucket's upper edge (a power of
/// two), and otherwise lands in the lowest bucket.
#[test]
fn test_lo_exclusive_hi_inclusive() {
    let cases = [
        ("coarse", COARSE_PARAMS, true),
        ("standard", STANDARD_PARAMS, true),
        ("fine", FINE_PARAMS, true),
        ("hires latency", HIRES_LATENCY_PARAMS, false),
        ("io latency", IO_LATENCY_PARAMS, false),
        ("response time", RESPONSE_TIME_PARAMS, false),
        ("long running", LONG_RUNNING_PARAMS, false),
        ("data size", DATA_SIZE_PARAMS, true),
        ("memory usage", MEMORY_USAGE_PARAMS, true),
        (
            "pow2 schema8",
            Params {
                lo: 1024.0,
                hi: 4096.0,
                error_bound: schema_relative_error(8),
            },
            true,
        ),
        (
            "odd schema6",
            Params {
                lo: 1000.0,
                hi: 5000.0,
                error_bound: schema_relative_error(6),
            },
            false,
        ),
    ];
    for (name, p, lo_on_edge) in cases {
        let h = Histogram::new(p);
        let bottom = h.cfg.boundaries[0];
        if lo_on_edge {
            assert_eq!(bottom, p.lo, "{name}");
        } else {
            assert!(bottom < p.lo, "{name}");
        }
        let hi = if p.hi < i64::MAX as f64 {
            p.hi as i64
        } else {
            i64::MAX
        };
        h.record(p.lo as i64);
        h.record(hi);
        let snap = h.snapshot();
        if lo_on_edge {
            assert_eq!(snap.underflow, 1, "{name}");
            assert_eq!(snap.counts[0], 0, "{name}");
        } else {
            assert_eq!(snap.underflow, 0, "{name}");
            assert_eq!(snap.counts[0], 1, "{name}");
        }
        assert_eq!(snap.overflow, 0, "{name}");
        assert_eq!(*snap.counts.last().unwrap(), 1, "{name}");
    }
}

/// When hi is exactly a bucket's upper edge, the top boundary is hi itself.
#[test]
fn test_top_equals_hi_on_bucket_edge() {
    for p in [DATA_SIZE_PARAMS, MEMORY_USAGE_PARAMS] {
        let h = Histogram::new(p);
        assert_eq!(*h.cfg.boundaries.last().unwrap(), p.hi);
    }
}

/// A value above hi but inside hi's bucket is counted in range, so the export
/// and quantiles report it against the bucket's real top edge, not hi.
#[test]
fn test_value_above_hi_in_hi_bucket() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 100.0,
        error_bound: 0.1,
    });
    h.record(105);
    let snap = h.snapshot();
    assert_eq!(snap.overflow, 0);
    let bounds = native_histogram_bounds(3);
    let top = get_le(prom_bucket_key(100.0, 3, &bounds), 3, &bounds);
    let buckets = snap.conventional_buckets();
    let last = buckets[buckets.len() - 2];
    assert_eq!(last, (top, 1));
    assert!(snap.value_at_quantile(1.0) >= 105.0);
    let mut live = Vec::new();
    h.values_at_quantiles_into(&mut live, &[1.0]);
    assert!(live[0] >= 105.0);
}

/// Small deterministic generator for the randomized tests (no rand dependency).
struct XorShift(u64);

impl XorShift {
    /// Uniform in [0, 1).
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[test]
fn test_record_boundary_values() {
    let h = Histogram::new(Params {
        lo: 100.0,
        hi: 10_000.0,
        error_bound: 0.05,
    });
    h.record(100);
    h.record(10_000);
    let snap = h.snapshot();
    assert_eq!(snap.total_count, 2);
    assert_eq!(snap.underflow, 0);
    assert_eq!(snap.overflow, 0);
}

/// Every populated bucket spans one schema step: its upper edge is gamma times
/// its lower edge.
#[test]
fn test_record_accuracy() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.05,
    });
    let mut rng = XorShift(42);
    for _ in 0..10_000 {
        let v = (rng.next_f64() * 1e6f64.ln()).exp() as i64;
        h.record(v.max(1));
    }
    let snap = h.snapshot();
    let gamma = f64::powf(2.0, f64::powf(2.0, -(h.cfg.schema as f64)));
    for (i, &c) in snap.counts.iter().enumerate() {
        if c == 0 {
            continue;
        }
        let ratio = h.cfg.boundaries[i + 1] / h.cfg.boundaries[i];
        assert!(
            ratio <= gamma * 1.01,
            "bucket {i} ratio {ratio} exceeds gamma {gamma}"
        );
    }
}

#[test]
fn test_quantile_exponential() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.05,
    });
    let mut rng = XorShift(7);
    for _ in 0..100_000 {
        // Exponential with mean 10000, clamped to the histogram's range.
        let v = -(1.0 - rng.next_f64()).ln() * 10_000.0;
        h.record(v.clamp(1.0, 1e6) as i64);
    }
    // Median of Exp(1/10000) is 10000 * ln(2) ~= 6931.
    let median = h.snapshot().value_at_quantile(0.5);
    assert!((median - 6931.0).abs() <= 2000.0, "median={median}");
}

/// Regression test for the rightmost-bucket density bias: the last bucket's
/// right-edge density must be its own average density, not zero, or high
/// interior quantiles get pulled toward its lower edge.
#[test]
fn test_quantile_top_bucket_density() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.05,
    });
    for _ in 0..1000 {
        h.record(1_000_000);
    }
    let snap = h.snapshot();
    let n = snap.counts.len();
    assert_eq!(snap.counts[n - 1], 1000);
    let b = snap.boundaries();
    let mid = (b[n - 1] + b[n]) / 2.0;
    let median = snap.value_at_quantile(0.5);
    assert!(
        median >= mid,
        "median {median} should be >= top-bucket midpoint {mid}"
    );
    assert_eq!(snap.values_at_quantiles(&[0.5])[0], median);
}

#[test]
fn test_native_export() {
    let h = Histogram::new(Params {
        lo: 1.0,
        hi: 1e6,
        error_bound: 0.5,
    });
    let bounds = native_histogram_bounds(h.schema());
    let key = |v: f64| prom_bucket_key(v, h.schema(), &bounds);
    // Two adjacent buckets, a gap, then one more; plus a zero, which goes in
    // the zero bucket.
    for v in [100, 100, 150, 5000] {
        h.record(v);
    }
    h.record(0);
    assert_eq!(key(150.0), key(100.0) + 1);
    let native = h.snapshot().native();
    assert_eq!(native.schema, h.schema());
    assert_eq!(native.zero_threshold, f64::MIN_POSITIVE * f64::EPSILON);
    assert_eq!(native.zero_count, 1);
    assert_eq!(
        native.positive_spans,
        vec![(key(100.0), 2), (key(5000.0) - key(150.0) - 1, 1)]
    );
    assert_eq!(native.positive_deltas, vec![2, -1, 0]);
}

#[test]
fn test_native_export_out_of_range() {
    // A native histogram has no +Inf bucket, so underflow goes in the bucket
    // directly below the lowest bucket and overflow in the bucket directly
    // above the highest, keeping the bucket counts plus the zero count equal
    // to the sample count.
    let h = Histogram::new(Params {
        lo: 1000.0,
        hi: 1e6,
        error_bound: 0.1,
    });
    let bounds = native_histogram_bounds(h.schema());
    let key = |v: f64| prom_bucket_key(v, h.schema(), &bounds);
    for _ in 0..5 {
        h.record(1); // underflow
    }
    h.record(5000);
    for _ in 0..3 {
        h.record(10_000_000); // overflow
    }
    h.record(0);
    let snap = h.snapshot();
    assert_eq!((snap.underflow, snap.overflow), (5, 3));
    let native = snap.native();
    assert_eq!(native.zero_threshold, f64::MIN_POSITIVE * f64::EPSILON);
    assert_eq!(native.zero_count, 1);

    let below = snap.min_key - 1;
    let above = snap.min_key + snap.counts.len() as i32;
    assert_eq!(
        native.positive_spans,
        vec![
            (below, 1),
            (key(5000.0) - below - 1, 1),
            (above - key(5000.0) - 1, 1)
        ]
    );
    assert_eq!(native.positive_deltas, vec![5, -4, 2]);
    // The lowest bucket's lower edge is the top of the underflow bucket, and
    // the overflow bucket starts at the highest bucket's upper edge.
    let b = snap.boundaries();
    assert_eq!(key(b[0]), below);
    assert_eq!(key(b[b.len() - 1]) + 1, above);
    assert_eq!(native.zero_count + 5 + 1 + 3, snap.total_count);
}

#[test]
fn test_config_shared_across_histograms() {
    let p = Params {
        lo: 7.0,
        hi: 7e6,
        error_bound: 0.07,
    };
    let a = Histogram::new(p);
    let b = Histogram::new(p);
    assert!(Arc::ptr_eq(&a.cfg, &b.cfg));
    // Defaulted fields resolve before the lookup, so they share too.
    let c = Histogram::new(Params {
        lo: 0.0,
        hi: 0.0,
        error_bound: 0.0,
    });
    let d = Histogram::new(Params::default());
    assert!(Arc::ptr_eq(&c.cfg, &d.cfg));
    // Invalid params panic without poisoning the cache for later callers.
    let bad = std::panic::catch_unwind(|| {
        Histogram::new(Params {
            lo: 10.0,
            hi: 1.0,
            error_bound: 0.1,
        })
    });
    assert!(bad.is_err());
    assert!(Arc::ptr_eq(&Histogram::new(p).cfg, &a.cfg));
}
