// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Adapter exposing goodhistogram as a metric for the tikv [`prometheus`] crate.
//!
//! [`PromHistogram`] and [`PromHistogramVec`] implement
//! `prometheus::core::Collector`, so a goodhistogram can be registered in a
//! `prometheus::Registry` and scraped like any other histogram — a near-drop-in
//! replacement for `prometheus::Histogram`.
//!
//! Following Prometheus convention these are **duration** histograms: values are
//! observed in **seconds**, recorded internally in nanoseconds (goodhistogram's
//! integer domain), and exported in seconds. Construct them with
//! nanosecond-scaled [`Params`] such as [`crate::LATENCY_PARAMS`].
//!
//! Unlike a count/sum-only wrapper, this exports the full cumulative bucket set,
//! so `histogram_quantile()` works against the scraped metric.
//!
//! Enable with the `prometheus` cargo feature.

use crate::{Histogram, Params, Snapshot};
use prometheus::core::{Collector, Desc};
use prometheus::proto::{
    Bucket, Histogram as ProtoHistogram, LabelPair, Metric, MetricFamily, MetricType,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const NANOS_PER_SEC: f64 = 1e9;

/// Builds a Prometheus proto histogram from a snapshot, converting the
/// nanosecond internal units to seconds. The `+Inf` bucket is omitted here: the
/// exposition encoder emits it from the sample count.
fn proto_histogram(snap: &Snapshot<'_>) -> ProtoHistogram {
    let mut ph = ProtoHistogram::default();
    ph.set_sample_count(snap.total_count);
    ph.set_sample_sum(snap.total_sum as f64 / NANOS_PER_SEC);

    let mut buckets = Vec::new();
    for (upper_bound, cumulative) in snap.conventional_buckets() {
        if upper_bound.is_infinite() {
            continue;
        }
        let mut b = Bucket::default();
        b.set_upper_bound(upper_bound / NANOS_PER_SEC);
        b.set_cumulative_count(cumulative);
        buckets.push(b);
    }
    ph.set_bucket(buckets);
    ph
}

/// A Prometheus histogram backed by goodhistogram. Observes seconds, records
/// nanoseconds internally, exports seconds.
pub struct PromHistogram {
    desc: Desc,
    h: Arc<Histogram>,
}

impl PromHistogram {
    /// Creates a histogram named `name`. `params` are in the histogram's
    /// internal (nanosecond) units.
    pub fn new(name: &str, help: &str, params: Params) -> Self {
        let desc = Desc::new(name.to_string(), help.to_string(), vec![], HashMap::new())
            .expect("invalid metric description");
        PromHistogram {
            desc,
            h: Arc::new(Histogram::new(params)),
        }
    }

    /// Observes a duration in seconds (Prometheus convention).
    pub fn observe(&self, seconds: f64) {
        self.h.record((seconds * NANOS_PER_SEC) as i64);
    }
}

impl Collector for PromHistogram {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut metric = Metric::default();
        metric.set_histogram(proto_histogram(&self.h.snapshot()));

        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(MetricType::HISTOGRAM);
        mf.set_metric(vec![metric]);
        vec![mf]
    }
}

/// Handle returned by [`PromHistogramVec::with_label_values`] for recording.
pub struct LabeledHistogram {
    h: Arc<Histogram>,
}

impl LabeledHistogram {
    /// Observes a duration in seconds.
    pub fn observe(&self, seconds: f64) {
        self.h.record((seconds * NANOS_PER_SEC) as i64);
    }
}

/// A labeled Prometheus histogram backed by per-label-set goodhistogram
/// instances.
pub struct PromHistogramVec {
    params: Params,
    desc: Desc,
    label_names: Vec<String>,
    hists: RwLock<Vec<Entry>>,
}

struct Entry {
    labels: Vec<String>,
    h: Arc<Histogram>,
}

fn find<'a>(entries: &'a [Entry], lvs: &[&str]) -> Option<&'a Entry> {
    entries
        .iter()
        .find(|e| e.labels.len() == lvs.len() && e.labels.iter().zip(lvs).all(|(a, b)| a == b))
}

impl PromHistogramVec {
    /// Creates a labeled histogram. `params` are in internal (nanosecond) units.
    pub fn new(name: &str, help: &str, params: Params, label_names: &[&str]) -> Self {
        let owned: Vec<String> = label_names.iter().map(|s| s.to_string()).collect();
        let desc = Desc::new(
            name.to_string(),
            help.to_string(),
            owned.clone(),
            HashMap::new(),
        )
        .expect("invalid metric description");
        PromHistogramVec {
            params,
            desc,
            label_names: owned,
            hists: RwLock::new(Vec::new()),
        }
    }

    /// Returns a handle for the given label values, creating the histogram if it
    /// does not yet exist.
    pub fn with_label_values(&self, lvs: &[&str]) -> LabeledHistogram {
        if let Some(e) = find(&self.hists.read().unwrap(), lvs) {
            return LabeledHistogram {
                h: Arc::clone(&e.h),
            };
        }
        let mut hists = self.hists.write().unwrap();
        // Re-check after taking the write lock.
        if let Some(e) = find(&hists, lvs) {
            return LabeledHistogram {
                h: Arc::clone(&e.h),
            };
        }
        let h = Arc::new(Histogram::new(self.params));
        hists.push(Entry {
            labels: lvs.iter().map(|s| s.to_string()).collect(),
            h: Arc::clone(&h),
        });
        LabeledHistogram { h }
    }
}

impl Collector for PromHistogramVec {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let hists = self.hists.read().unwrap();
        if hists.is_empty() {
            return vec![];
        }
        let mut metrics = Vec::with_capacity(hists.len());
        for e in hists.iter() {
            let mut metric = Metric::default();
            let mut label_pairs = Vec::with_capacity(self.label_names.len());
            for (name, value) in self.label_names.iter().zip(e.labels.iter()) {
                let mut lp = LabelPair::default();
                lp.set_name(name.clone());
                lp.set_value(value.clone());
                label_pairs.push(lp);
            }
            metric.set_label(label_pairs);
            metric.set_histogram(proto_histogram(&e.h.snapshot()));
            metrics.push(metric);
        }
        let mut mf = MetricFamily::default();
        mf.set_name(self.desc.fq_name.clone());
        mf.set_help(self.desc.help.clone());
        mf.set_field_type(MetricType::HISTOGRAM);
        mf.set_metric(metrics);
        vec![mf]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LATENCY_PARAMS;
    use prometheus::{Encoder, Registry, TextEncoder};

    fn scrape(reg: &Registry) -> String {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&reg.gather(), &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn exposes_full_buckets_and_inf() {
        let h = PromHistogram::new("op_duration_seconds", "help", LATENCY_PARAMS);
        h.observe(0.0015); // 1.5ms
        h.observe(0.02); // 20ms
        h.observe(5.0); // 5s

        let reg = Registry::new();
        reg.register(Box::new(h)).unwrap();
        let out = scrape(&reg);

        // Full bucket set, an +Inf bucket equal to the count, sum in seconds.
        assert!(out.contains("op_duration_seconds_bucket{"));
        assert!(out.contains("op_duration_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(out.contains("op_duration_seconds_count 3"));
        assert!(out.contains("op_duration_seconds_sum 5.0215"));
    }

    #[test]
    fn labeled_histograms_scrape_per_label_set() {
        let v = PromHistogramVec::new("rpc_seconds", "help", LATENCY_PARAMS, &["method"]);
        v.with_label_values(&["get"]).observe(0.001);
        v.with_label_values(&["put"]).observe(0.002);
        v.with_label_values(&["get"]).observe(0.003); // same handle reused

        let reg = Registry::new();
        reg.register(Box::new(v)).unwrap();
        let out = scrape(&reg);

        assert!(out.contains("rpc_seconds_count{method=\"get\"} 2"));
        assert!(out.contains("rpc_seconds_count{method=\"put\"} 1"));
    }
}
