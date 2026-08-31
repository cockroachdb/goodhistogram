// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Verifies the Rust `Params` presets match the Go ones.
//!
//! `../datatest/presets.txt` is generated from Go (`go test ./go/ -run
//! TestPresets -rewrite`). This test looks up each Rust preset const by the
//! shared Go name and asserts identical lo/hi, error_bound (within tolerance for
//! powf drift), schema, and bucket count.

use goodhistogram::*;

const ERROR_BOUND_REL_TOL: f64 = 1e-9;

fn presets() -> Vec<(&'static str, Params)> {
    vec![
        ("CoarseParams", COARSE_PARAMS),
        ("StandardParams", STANDARD_PARAMS),
        ("FineParams", FINE_PARAMS),
        ("HiResLatencyParams", HIRES_LATENCY_PARAMS),
        ("IOLatencyParams", IO_LATENCY_PARAMS),
        ("ResponseTimeParams", RESPONSE_TIME_PARAMS),
        ("LongRunningParams", LONG_RUNNING_PARAMS),
        ("DataSizeParams", DATA_SIZE_PARAMS),
        ("MemoryUsageParams", MEMORY_USAGE_PARAMS),
    ]
}

struct Golden {
    lo: f64,
    hi: f64,
    error_bound: f64,
    schema: i32,
    num_buckets: usize,
}

#[test]
fn presets_match_go() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../datatest/presets.golden");
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        panic!("cannot read {path}: {e}\ngenerate with `go test ./go/ -run TestPresets -rewrite`")
    });
    let golden = parse(&text);

    for (name, p) in presets() {
        let g = golden
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, g)| g)
            .unwrap_or_else(|| panic!("preset {name} missing from golden"));

        assert_eq!(p.lo, g.lo, "{name} lo");
        assert_eq!(p.hi, g.hi, "{name} hi");
        let rel = (p.error_bound - g.error_bound).abs() / g.error_bound.abs().max(1.0);
        assert!(
            rel <= ERROR_BOUND_REL_TOL,
            "{name} error_bound: rust={} go={}",
            p.error_bound,
            g.error_bound
        );

        let h = Histogram::new(p);
        let snap = h.snapshot();
        assert_eq!(snap.schema(), g.schema, "{name} schema");
        assert_eq!(snap.counts.len(), g.num_buckets, "{name} num_buckets");
    }
}

fn parse(text: &str) -> Vec<(String, Golden)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = line.split_whitespace().collect();
        assert!(f.len() == 7 && f[0] == "preset", "malformed line: {line}");
        out.push((
            f[1].to_string(),
            Golden {
                lo: f[2].parse().unwrap(),
                hi: f[3].parse().unwrap(),
                error_bound: f[4].parse().unwrap(),
                schema: f[5].parse().unwrap(),
                num_buckets: f[6].parse().unwrap(),
            },
        ));
    }
    out
}
