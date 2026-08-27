// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Lock-free exponential histogram with Prometheus-aligned bucket layout.
//!
//! This is the Rust implementation of `github.com/cockroachdb/goodhistogram`.
//! It lives in the same repository as the Go implementation and is kept in
//! lockstep with it by a shared conformance fixture (`testdata/conformance.txt`,
//! generated from the Go code); see `tests/conformance.rs`.
//!
//! Recording is O(1) and lock-free: values are mapped to bucket indices via
//! IEEE 754 bit extraction plus a precomputed lookup table, then the
//! corresponding atomic counter is incremented.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

const MAX_SCHEMA: i32 = 8;

/// Number of top mantissa bits used for the bucket lookup table.
const BUCKET_LOOKUP_BITS: u32 = 8;
const BUCKET_LOOKUP_SIZE: usize = 1 << BUCKET_LOOKUP_BITS;
const BUCKET_LOOKUP_SHIFT: u32 = 52 - BUCKET_LOOKUP_BITS;

/// User-facing parameters for creating a [`Histogram`].
#[derive(Clone, Copy)]
pub struct Params {
    /// Lower bound of tracked range. Values below are counted in underflow.
    pub lo: f64,
    /// Upper bound of tracked range. Values above are counted in overflow.
    pub hi: f64,
    /// Maximum relative error bound. Selects the tightest Prometheus schema
    /// whose error is at or below this value.
    pub error_bound: f64,
}

/// Worst-case relative error of Prometheus schema 2 (~18.9%), i.e.
/// `schema_relative_error(2)` as a literal usable in const context. Passing
/// this as `error_bound` pins a histogram to schema 2, the default bucket
/// granularity. A rounder value such as 0.10 falls into schema 3 and doubles
/// the bucket count. The `test_standard_error_bound_matches` test guards the
/// literal against drift.
pub const STANDARD_ERROR_BOUND: f64 = 0.18920711500272103;

impl Default for Params {
    fn default() -> Self {
        Params {
            lo: 1.0,
            hi: i64::MAX as f64,
            error_bound: STANDARD_ERROR_BOUND,
        }
    }
}

/// Latency histogram params: 1µs to 60s at Prometheus schema 2.
pub const LATENCY_PARAMS: Params = Params {
    lo: 1_000.0,
    hi: 60e9,
    error_bound: STANDARD_ERROR_BOUND,
};

/// Immutable configuration computed from Params.
struct Config {
    min_key: i32,
    num_buckets: usize,
    buckets_per_group: usize,
    bucket_lookup: [u8; BUCKET_LOOKUP_SIZE],
    boundaries: Vec<f64>,
}

/// Compute the native histogram bucket boundaries for a given schema.
fn native_histogram_bounds(schema: i32) -> Vec<f64> {
    let buckets_per_group = 1usize << schema;
    let mut bounds = Vec::with_capacity(buckets_per_group);
    for j in 0..buckets_per_group {
        // 2^(j / 2^schema) / 2 — matches the Prometheus bucket boundary
        // decomposed via frexp.
        let val = f64::powf(2.0, j as f64 / buckets_per_group as f64);
        bounds.push(f64::from_bits(val.to_bits() - (1u64 << 52))); // ldexp(val, -1)
    }
    bounds
}

/// Returns the worst-case relative error for a schema: γ-1 where
/// γ = 2^(2^(-schema)). This is DDSketch's midpoint error (γ-1)/(γ+1) doubled,
/// because `value_at_quantile` reports anywhere in a bucket [b, γ·b], not just
/// the midpoint, so a value at b can be reported at γ·b.
fn schema_relative_error(schema: i32) -> f64 {
    let gamma = f64::powf(2.0, f64::powf(2.0, -(schema as f64)));
    gamma - 1.0
}

/// Selects the coarsest schema whose error ≤ desired_error.
fn pick_schema(desired_error: f64) -> i32 {
    for s in 0..=MAX_SCHEMA {
        if schema_relative_error(s) <= desired_error {
            return s;
        }
    }
    MAX_SCHEMA
}

/// Computes the Prometheus native histogram bucket key for a positive value.
fn prom_bucket_key(v: f64, _schema: i32, bounds: &[f64]) -> i32 {
    let (frac, exp) = frexp(v);
    let idx = bounds.partition_point(|&b| b < frac);
    idx as i32 + (exp - 1) * bounds.len() as i32
}

/// Returns the upper bound of the bucket with the given key and schema.
fn get_le(key: i32, schema: i32, bounds: &[f64]) -> f64 {
    let frac_idx = (key & ((1 << schema) - 1)) as usize;
    let frac = bounds[frac_idx];
    let exp = (key >> schema) + 1;
    ldexp(frac, exp)
}

/// Decomposes a float64 into fraction and exponent: value = frac * 2^exp,
/// where 0.5 ≤ frac < 1.0. Matches Go's math.Frexp.
fn frexp(v: f64) -> (f64, i32) {
    if v == 0.0 || v.is_nan() || v.is_infinite() {
        return (v, 0);
    }
    let bits = v.to_bits();
    let biased_exp = ((bits >> 52) & 0x7FF) as i32;
    // Replace exponent with 1022 (biased) → actual exponent 1022 - 1023 = -1
    // so frac = v * 2^(-exp) with 0.5 ≤ frac < 1.0.
    let frac_bits = (bits & !(0x7FFu64 << 52)) | (1022u64 << 52);
    let frac = f64::from_bits(frac_bits);
    let exp = biased_exp - 1022;
    (frac, exp)
}

/// Scales a float64 by 2^exp. Matches Go's math.Ldexp.
fn ldexp(frac: f64, exp: i32) -> f64 {
    frac * f64::powi(2.0, exp)
}

impl Config {
    fn new(lo: f64, hi: f64, desired_error: f64) -> Self {
        assert!(lo > 0.0 && hi > lo && desired_error > 0.0);
        let schema = pick_schema(desired_error);
        let group_bounds = native_histogram_bounds(schema);
        let mut min_key = prom_bucket_key(lo, schema, &group_bounds);
        if get_le(min_key, schema, &group_bounds) <= lo {
            min_key += 1;
        }
        let max_key = prom_bucket_key(hi, schema, &group_bounds);
        let num_buckets = (max_key - min_key + 1) as usize;

        // Precompute bucket boundaries.
        let mut boundaries = Vec::with_capacity(num_buckets + 1);
        boundaries.push(lo);
        for i in 1..num_buckets {
            boundaries.push(get_le(min_key + i as i32 - 1, schema, &group_bounds));
        }
        boundaries.push(hi);

        let buckets_per_group = group_bounds.len();

        // Build bucket lookup table.
        let mut bucket_lookup = [0u8; BUCKET_LOOKUP_SIZE];
        for (table_idx, slot) in bucket_lookup.iter_mut().enumerate() {
            let min_bits = (1023u64 << 52) | ((table_idx as u64) << BUCKET_LOOKUP_SHIFT);
            let max_bits = min_bits | ((1u64 << BUCKET_LOOKUP_SHIFT) - 1);
            let _key_min = prom_bucket_key(f64::from_bits(min_bits), schema, &group_bounds);
            let key_max = prom_bucket_key(f64::from_bits(max_bits), schema, &group_bounds);
            *slot = key_max as u8;
        }

        Config {
            min_key,
            num_buckets,
            buckets_per_group,
            bucket_lookup,
            boundaries,
        }
    }
}

/// Lock-free exponential histogram with atomic counters.
pub struct Histogram {
    cfg: Config,
    counts: Vec<AtomicU64>,
    underflow: AtomicU64,
    overflow: AtomicU64,
    zero_count: AtomicU64,
    sum: AtomicI64,
}

impl Histogram {
    /// Creates a new Histogram for the given range and error bound.
    pub fn new(p: Params) -> Self {
        let p = Params {
            lo: if p.lo == 0.0 { 1.0 } else { p.lo },
            hi: if p.hi == 0.0 { i64::MAX as f64 } else { p.hi },
            error_bound: if p.error_bound == 0.0 {
                STANDARD_ERROR_BOUND
            } else {
                p.error_bound
            },
        };
        let cfg = Config::new(p.lo, p.hi, p.error_bound);
        let counts: Vec<AtomicU64> = (0..cfg.num_buckets).map(|_| AtomicU64::new(0)).collect();
        Histogram {
            cfg,
            counts,
            underflow: AtomicU64::new(0),
            overflow: AtomicU64::new(0),
            zero_count: AtomicU64::new(0),
            sum: AtomicI64::new(0),
        }
    }

    /// Records a value. O(1), lock-free.
    pub fn record(&self, v: i64) {
        self.sum.fetch_add(v, Ordering::Relaxed);

        if v <= 0 {
            self.zero_count.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let bits = (v as f64).to_bits();
        let exp = ((bits >> 52) & 0x7FF) as i32 - 1022;
        let sub = self.cfg.bucket_lookup[((bits >> BUCKET_LOOKUP_SHIFT) & 0xFF) as usize] as i32;
        let key = sub + (exp - 1) * self.cfg.buckets_per_group as i32;
        let idx = key - self.cfg.min_key;

        if idx < 0 {
            self.underflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if idx as usize >= self.cfg.num_buckets {
            self.overflow.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counts[idx as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Zeroes all counters without reallocating.
    pub fn reset(&self) {
        for c in &self.counts {
            c.store(0, Ordering::Relaxed);
        }
        self.zero_count.store(0, Ordering::Relaxed);
        self.underflow.store(0, Ordering::Relaxed);
        self.overflow.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
    }

    /// Returns a point-in-time snapshot.
    pub fn snapshot(&self) -> Snapshot<'_> {
        let mut counts = Vec::with_capacity(self.cfg.num_buckets);
        let mut total_count = 0u64;
        for c in &self.counts {
            let v = c.load(Ordering::Relaxed);
            counts.push(v);
            total_count += v;
        }
        let zero_count = self.zero_count.load(Ordering::Relaxed);
        let underflow = self.underflow.load(Ordering::Relaxed);
        let overflow = self.overflow.load(Ordering::Relaxed);
        total_count += zero_count + underflow + overflow;

        Snapshot {
            boundaries: &self.cfg.boundaries,
            counts,
            zero_count,
            underflow,
            overflow,
            total_count,
            total_sum: self.sum.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time snapshot of a Histogram, suitable for quantile queries.
pub struct Snapshot<'a> {
    boundaries: &'a [f64],
    pub counts: Vec<u64>,
    pub zero_count: u64,
    pub underflow: u64,
    pub overflow: u64,
    pub total_count: u64,
    pub total_sum: i64,
}

impl Snapshot<'_> {
    /// Returns the bucket boundaries. `counts[i]` holds the number of values
    /// that fell in `(boundaries[i], boundaries[i + 1]]`, so this slice is one
    /// longer than `counts`.
    ///
    /// Percentiles summarize a distribution the caller has already assumed the
    /// shape of. Reading the buckets is how a caller checks that assumption —
    /// a multi-modal distribution is invisible in any set of quantiles but
    /// obvious in the counts.
    pub fn boundaries(&self) -> &[f64] {
        self.boundaries
    }

    /// Returns the estimated value at quantile q ∈ [0, 1] using trapezoidal
    /// interpolation.
    pub fn value_at_quantile(&self, q: f64) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }
        let rank = q * self.total_count as f64;
        if rank <= 0.0 {
            if self.zero_count + self.underflow > 0 {
                return self.boundaries[0]; // lo
            }
            for (i, &c) in self.counts.iter().enumerate() {
                if c > 0 {
                    return self.boundaries[i];
                }
            }
            return *self.boundaries.last().unwrap();
        }
        if rank >= self.total_count as f64 {
            if self.overflow > 0 {
                return *self.boundaries.last().unwrap();
            }
            for i in (0..self.counts.len()).rev() {
                if self.counts[i] > 0 {
                    return self.boundaries[i + 1];
                }
            }
            return self.boundaries[0];
        }

        let below_lo = (self.zero_count + self.underflow) as f64;
        if rank <= below_lo {
            return self.boundaries[0];
        }
        let rank = rank - below_lo;

        let n = self.counts.len();

        // Compute average density in each bucket.
        let mut avg_density = vec![0.0f64; n];
        for (i, slot) in avg_density.iter_mut().enumerate() {
            let w = self.boundaries[i + 1] - self.boundaries[i];
            if w > 0.0 && self.counts[i] > 0 {
                *slot = self.counts[i] as f64 / w;
            }
        }

        // Estimate density at each boundary. At the outer edges there is no
        // neighbor on one side, so use the adjacent bucket's density directly
        // rather than averaging with zero — otherwise the rightmost-bucket
        // interpolation gets biased low, which matters a lot for p99 in
        // long-tailed distributions.
        let mut boundary_density = vec![0.0f64; n + 1];
        boundary_density[0] = avg_density[0];
        boundary_density[n] = avg_density[n - 1];
        for i in 1..n {
            boundary_density[i] = (avg_density[i - 1] + avg_density[i]) / 2.0;
        }

        let mut cum_count = 0.0f64;
        for i in 0..n {
            let fc = self.counts[i] as f64;
            if cum_count + fc >= rank {
                let local_rank = rank - cum_count;
                let lo = self.boundaries[i];
                let hi = self.boundaries[i + 1];
                let w = hi - lo;
                if w <= 0.0 || fc == 0.0 {
                    return lo;
                }
                let d_l = boundary_density[i];
                let d_r = boundary_density[i + 1];
                return trapezoidal_solve(lo, w, fc, d_l, d_r, local_rank);
            }
            cum_count += fc;
        }
        *self.boundaries.last().unwrap()
    }

    /// Returns the mean of all recorded values. Returns NaN if empty.
    pub fn mean(&self) -> f64 {
        if self.total_count == 0 {
            return f64::NAN;
        }
        self.total_sum as f64 / self.total_count as f64
    }

    /// Returns total count and sum.
    pub fn total(&self) -> (i64, f64) {
        (self.total_count as i64, self.total_sum as f64)
    }

    /// Returns the classic (non-native) Prometheus histogram buckets:
    /// cumulative `(upper_bound, count)` pairs with a trailing
    /// `(+Inf, total_count)`. `bucket[i].0` is `boundaries()[i + 1]`.
    ///
    /// Mirrors the Go implementation's `conventionalBuckets`: zero and
    /// underflow observations sit below every finite upper bound, so they are
    /// included in every bucket's cumulative count; overflow observations
    /// appear only in the final `+Inf` bucket, whose count equals the total.
    ///
    /// This is the classic projection of the histogram; the native
    /// (exponential) bucket encoding is not produced here.
    pub fn conventional_buckets(&self) -> Vec<(f64, u64)> {
        let mut buckets = Vec::with_capacity(self.counts.len() + 1);
        // Zeros and underflow are below every finite upper bound.
        let mut cum = self.zero_count + self.underflow;
        for (i, &c) in self.counts.iter().enumerate() {
            cum += c;
            buckets.push((self.boundaries[i + 1], cum));
        }
        buckets.push((f64::INFINITY, self.total_count));
        buckets
    }

    /// Renders a simplified OpenMetrics/Prometheus text exposition: a `# TYPE`
    /// line, cumulative `<name>_bucket{le="..."}` lines (ending in `+Inf`),
    /// `<name>_sum`, and `<name>_count`. `sum` is the raw recorded sum in the
    /// histogram's own units; unit conversion (e.g. ns→s) is the caller's job.
    ///
    /// This is the classic projection; native bucket encoding is not emitted.
    pub fn to_openmetrics(&self, name: &str) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        let _ = writeln!(out, "# TYPE {name} histogram");
        for (ub, cum) in self.conventional_buckets() {
            if ub.is_infinite() {
                let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {cum}");
            } else {
                let _ = writeln!(out, "{name}_bucket{{le=\"{ub}\"}} {cum}");
            }
        }
        let _ = writeln!(out, "{name}_sum {}", self.total_sum);
        let _ = writeln!(out, "{name}_count {}", self.total_count);
        out
    }
}

/// Solves for the value x in [lo, lo+w] where the area under a linear
/// density function from lo to x equals local_rank.
fn trapezoidal_solve(lo: f64, w: f64, fc: f64, d_l: f64, d_r: f64, local_rank: f64) -> f64 {
    let raw_area = w * (d_l + d_r) / 2.0;
    if raw_area <= 0.0 {
        return lo + w * (local_rank / fc);
    }
    let scale = fc / raw_area;
    let s_l = d_l * scale;
    let s_r = d_r * scale;

    if (s_r - s_l).abs() < 1e-12 * (s_l + s_r) {
        return lo + w * (local_rank / fc);
    }

    let a = (s_r - s_l) / (2.0 * w);
    let b = s_l;
    let c = -local_rank;
    let disc = b * b - 4.0 * a * c;
    if disc < 0.0 {
        return lo + w * (local_rank / fc);
    }

    let x = if b >= 0.0 {
        (2.0 * c) / (-b - disc.sqrt())
    } else {
        (-b + disc.sqrt()) / (2.0 * a)
    };

    lo + x.clamp(0.0, w)
}

#[cfg(test)]
mod tests;
