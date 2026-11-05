// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! A collection of histograms partitioned by label values, mirroring Go's
//! `HistogramVec`. This is the core data structure (no Prometheus dependency);
//! the Prometheus-registerable equivalent is `prometheus::PromHistogramVec`
//! behind the `prometheus` feature.

use crate::{Histogram, Params};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A set of [`Histogram`]s that share the same [`Params`], keyed by label
/// values. Recording is done on the `Arc<Histogram>` returned by
/// [`HistogramVec::with_label_values`].
pub struct HistogramVec {
    params: Params,
    label_names: Vec<String>,
    inner: RwLock<HashMap<Vec<String>, Arc<Histogram>>>,
}

impl HistogramVec {
    /// Creates a new vec whose children all use `params`.
    pub fn new(params: Params, label_names: &[&str]) -> Self {
        HistogramVec {
            params,
            label_names: label_names.iter().map(|s| s.to_string()).collect(),
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// The label names this vec is partitioned by.
    pub fn label_names(&self) -> &[String] {
        &self.label_names
    }

    /// Returns the histogram for the given label values, creating it if it does
    /// not yet exist. Panics if the number of values doesn't match the number
    /// of label names (matching Go's `WithLabelValues`).
    pub fn with_label_values(&self, lvs: &[&str]) -> Arc<Histogram> {
        self.check_len(lvs);
        let key: Vec<String> = lvs.iter().map(|s| s.to_string()).collect();

        if let Some(h) = self.inner.read().unwrap().get(&key) {
            return Arc::clone(h);
        }
        let mut m = self.inner.write().unwrap();
        // Re-check after taking the write lock.
        if let Some(h) = m.get(&key) {
            return Arc::clone(h);
        }
        let h = Arc::new(Histogram::new(self.params));
        m.insert(key, Arc::clone(&h));
        h
    }

    /// Removes the histogram for the given label values. Returns true if it
    /// existed. Panics on label-count mismatch (matching Go).
    pub fn delete_label_values(&self, lvs: &[&str]) -> bool {
        self.check_len(lvs);
        let key: Vec<String> = lvs.iter().map(|s| s.to_string()).collect();
        self.inner.write().unwrap().remove(&key).is_some()
    }

    /// Removes all child histograms.
    pub fn reset(&self) {
        self.inner.write().unwrap().clear();
    }

    fn check_len(&self, lvs: &[&str]) {
        assert_eq!(
            lvs.len(),
            self.label_names.len(),
            "goodhistogram: expected {} label values, got {}",
            self.label_names.len(),
            lvs.len()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> Params {
        Params {
            lo: 1.0,
            hi: 1e6,
            error_bound: 0.1,
        }
    }

    #[test]
    fn same_labels_return_same_histogram() {
        let v = HistogramVec::new(params(), &["op"]);
        let a = v.with_label_values(&["read"]);
        a.record(100);
        let b = v.with_label_values(&["read"]); // same handle
        b.record(200);
        assert_eq!(a.snapshot().total_count, 2);
        assert_eq!(b.snapshot().total_count, 2);

        // A different label set is independent.
        let c = v.with_label_values(&["write"]);
        c.record(300);
        assert_eq!(c.snapshot().total_count, 1);
        assert_eq!(a.snapshot().total_count, 2);
    }

    #[test]
    fn delete_and_reset() {
        let v = HistogramVec::new(params(), &["op"]);
        v.with_label_values(&["read"]).record(1);
        v.with_label_values(&["write"]).record(1);

        assert!(v.delete_label_values(&["read"]));
        assert!(!v.delete_label_values(&["read"])); // already gone

        // A recreated label set starts fresh.
        assert_eq!(v.with_label_values(&["read"]).snapshot().total_count, 0);

        v.reset();
        assert_eq!(v.with_label_values(&["write"]).snapshot().total_count, 0);
    }

    #[test]
    #[should_panic]
    fn wrong_label_count_panics() {
        let v = HistogramVec::new(params(), &["op"]);
        v.with_label_values(&["read", "extra"]);
    }
}
