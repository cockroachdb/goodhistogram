// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Adapter exposing goodhistogram to the OpenMetrics-native
//! [`prometheus-client`] crate.
//!
//! [`HistogramCollector`] implements `prometheus_client::collector::Collector`,
//! so a goodhistogram can be registered with a `prometheus_client::registry`
//! and rendered in the OpenMetrics text format. Like the tikv adapter these are
//! duration histograms: observed in seconds, stored internally in nanoseconds,
//! exported in seconds.
//!
//! Enable with the `prometheus-client` cargo feature.

use crate::{Histogram, Params};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::MetricType;
use std::sync::Arc;

const NANOS_PER_SEC: f64 = 1e9;

/// A `prometheus-client` collector backed by a goodhistogram. Observe in
/// seconds; the OpenMetrics exposition reports seconds.
#[derive(Clone)]
pub struct HistogramCollector {
    name: String,
    help: String,
    h: Arc<Histogram>,
}

// The Collector trait requires Debug; implement it by hand so the core
// Histogram type need not derive Debug (and we avoid dumping the lookup table).
impl std::fmt::Debug for HistogramCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HistogramCollector")
            .field("name", &self.name)
            .field("help", &self.help)
            .finish_non_exhaustive()
    }
}

impl HistogramCollector {
    /// Creates a collector named `name`. `params` are in internal (nanosecond)
    /// units.
    pub fn new(name: impl Into<String>, help: impl Into<String>, params: Params) -> Self {
        HistogramCollector {
            name: name.into(),
            help: help.into(),
            h: Arc::new(Histogram::new(params)),
        }
    }

    /// Observes a duration in seconds.
    pub fn observe(&self, seconds: f64) {
        self.h.record((seconds * NANOS_PER_SEC) as i64);
    }
}

impl Collector for HistogramCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let snap = self.h.snapshot();
        let sum = snap.total_sum as f64 / NANOS_PER_SEC;
        let count = snap.total_count;

        // prometheus-client's encode_histogram wants PER-BUCKET counts (it
        // accumulates them itself) and renders exactly the buckets it is given,
        // including the terminal +Inf. This is the opposite of the tikv crate,
        // which wants cumulative counts and synthesizes +Inf from the count.
        //
        // Zero and underflow observations sit below every finite bound, so they
        // go in the first bucket; overflow observations go in +Inf. Upper bounds
        // are converted from nanoseconds to seconds.
        let bounds = snap.boundaries();
        let mut buckets: Vec<(f64, u64)> = Vec::with_capacity(snap.counts.len() + 1);
        for (i, &c) in snap.counts.iter().enumerate() {
            let per_bucket = if i == 0 {
                c + snap.zero_count + snap.underflow
            } else {
                c
            };
            buckets.push((bounds[i + 1] / NANOS_PER_SEC, per_bucket));
        }
        buckets.push((f64::INFINITY, snap.overflow));

        let metric_encoder =
            encoder.encode_descriptor(&self.name, &self.help, None, MetricType::Histogram)?;
        HistogramValue {
            sum,
            count,
            buckets,
        }
        .encode(metric_encoder)?;
        Ok(())
    }
}

/// Bridges our snapshot to `EncodeMetric::encode_histogram`.
struct HistogramValue {
    sum: f64,
    count: u64,
    buckets: Vec<(f64, u64)>,
}

impl EncodeMetric for HistogramValue {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::MetricEncoder,
    ) -> Result<(), std::fmt::Error> {
        encoder.encode_histogram::<()>(self.sum, self.count, &self.buckets, None)
    }

    fn metric_type(&self) -> MetricType {
        MetricType::Histogram
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LATENCY_PARAMS;
    use prometheus_client::registry::Registry;

    #[test]
    fn renders_openmetrics_histogram() {
        let h = HistogramCollector::new("op_duration_seconds", "op latency", LATENCY_PARAMS);
        h.observe(0.0015);
        h.observe(0.02);
        h.observe(5.0);

        let mut registry = Registry::default();
        registry.register_collector(Box::new(h));

        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &registry).unwrap();

        // prometheus-client renders the terminal bucket as le="inf". Its
        // cumulative count must equal the sample count.
        assert!(
            out.contains("op_duration_seconds_bucket{le=\"inf\"} 3"),
            "{out}"
        );
        assert!(out.contains("op_duration_seconds_count 3"));
        assert!(out.contains("op_duration_seconds_sum 5.0215"));
    }

    #[test]
    fn folds_underflow_and_overflow() {
        // 1µs–1s range. 1ns is below lo (underflow); 10s is above hi (overflow).
        let params = Params {
            lo: 1_000.0,
            hi: 1e9,
            error_bound: 0.1,
        };
        let h = HistogramCollector::new("d_seconds", "help", params);
        h.observe(1e-9); // underflow
        h.observe(0.5); // in range
        h.observe(10.0); // overflow

        let mut registry = Registry::default();
        registry.register_collector(Box::new(h));
        let mut out = String::new();
        prometheus_client::encoding::text::encode(&mut out, &registry).unwrap();

        // Underflow lands below every finite bound, so the first bucket's
        // cumulative count already includes it; the +Inf cumulative is the total.
        let first = out
            .lines()
            .find(|l| l.contains("d_seconds_bucket{le="))
            .unwrap();
        assert!(
            first.ends_with(" 1"),
            "first bucket should include underflow: {first}"
        );
        assert!(out.contains("d_seconds_bucket{le=\"inf\"} 3"), "{out}");
        assert!(out.contains("d_seconds_count 3"));
    }
}
