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
fn test_standard_error_bound_matches() {
    // STANDARD_ERROR_BOUND is a const literal because schema_relative_error is
    // not const-evaluable; keep the literal exactly equal to it so callers that
    // pin schema 2 do not silently drift to another schema.
    assert_eq!(STANDARD_ERROR_BOUND, schema_relative_error(2));
    assert_eq!(pick_schema(STANDARD_ERROR_BOUND), 2);
}

#[test]
fn test_config_boundaries_monotonic() {
    let cfg = Config::new(1e4, 1e16, 0.05);
    assert!(cfg.num_buckets > 0);
    assert_eq!(cfg.boundaries.len(), cfg.num_buckets + 1);
    assert_eq!(cfg.boundaries[0], 1e4);
    assert_eq!(cfg.boundaries[cfg.num_buckets], 1e16);
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
