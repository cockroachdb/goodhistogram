// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Replays the shared `datatest/` scenarios against this crate.
//!
//! Each `../datatest/*.txt` file has a hand-authored input section (params +
//! values) and a golden section generated from the canonical Go implementation
//! (`go test ./go/ -run TestDatatest -rewrite`): the OpenMetrics exposition plus
//! a set of quantiles. This test records the same values in Rust and asserts it
//! reproduces the golden — bucket counts and sum exactly, boundaries and
//! quantiles within a small tolerance for cross-language float (libm) drift.
//!
//! Go owns the golden; Rust only verifies. See datatest/README.md.

use goodhistogram::{Histogram, Params};
use std::path::PathBuf;

/// Bucket upper bounds derive from `powf`, whose last bit can differ between
/// Go's and Rust's math libraries; compare with a relative tolerance far tighter
/// than a bucket width but loose enough to absorb that.
const BOUNDARY_REL_TOL: f64 = 1e-9;

/// Quantiles add a `sqrt` and divisions on top of the boundaries, so allow a
/// little more slack.
const QUANTILE_REL_TOL: f64 = 1e-6;

#[derive(Default)]
struct Scenario {
    lo: f64,
    hi: f64,
    error_bound: f64,
    values: Vec<i64>,
    buckets: Vec<(f64, u64)>, // (le, cumulative count), ending in (+Inf, total)
    sum: i64,
    count: u64,
    schema: i32,
    quantiles: Vec<(f64, f64)>,
}

#[test]
fn datatest_matches_go() {
    let dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../datatest"));
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "txt"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no scenarios in {}", dir.display());

    for path in &files {
        let name = path.file_stem().unwrap().to_string_lossy();
        let text = std::fs::read_to_string(path).unwrap();
        let sc = parse_scenario(&text, &name);
        check_scenario(&sc, &name);
    }
}

fn check_scenario(sc: &Scenario, name: &str) {
    let h = Histogram::new(Params {
        lo: sc.lo,
        hi: sc.hi,
        error_bound: sc.error_bound,
    });
    for &v in &sc.values {
        h.record(v);
    }
    let snap = h.snapshot();

    // Exposition: same buckets, boundaries within tolerance, cumulative counts
    // exact.
    let got = snap.conventional_buckets();
    assert_eq!(
        got.len(),
        sc.buckets.len(),
        "[{name}] bucket count: rust={} go={}",
        got.len(),
        sc.buckets.len()
    );
    for (i, (&(g_le, g_cum), &(e_le, e_cum))) in got.iter().zip(&sc.buckets).enumerate() {
        assert!(
            close(g_le, e_le, BOUNDARY_REL_TOL),
            "[{name}] bucket[{i}] le: rust={g_le} go={e_le}"
        );
        assert_eq!(g_cum, e_cum, "[{name}] bucket[{i}] cumulative count");
    }

    assert_eq!(snap.total_sum, sc.sum, "[{name}] sum");
    assert_eq!(snap.total_count, sc.count, "[{name}] count");
    assert_eq!(snap.schema(), sc.schema, "[{name}] schema");

    for &(q, want) in &sc.quantiles {
        let got = snap.value_at_quantile(q);
        assert!(
            close(got, want, QUANTILE_REL_TOL),
            "[{name}] quantile q={q}: rust={got} go={want}"
        );
    }
}

/// Equal within a relative tolerance, with an absolute floor of 1.0 so values
/// near zero don't demand impossible precision. Infinities must match exactly.
fn close(a: f64, b: f64, rel: f64) -> bool {
    if a.is_infinite() || b.is_infinite() {
        return a == b;
    }
    if a == b {
        return true;
    }
    let scale = a.abs().max(b.abs()).max(1.0);
    (a - b).abs() <= rel * scale
}

fn parse_scenario(text: &str, name: &str) -> Scenario {
    let mut sc = Scenario::default();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split_whitespace();
        let key = it.next().unwrap();
        match key {
            "lo" => sc.lo = f(&mut it, name),
            "hi" => sc.hi = f(&mut it, name),
            "error_bound" => sc.error_bound = f(&mut it, name),
            "values" => sc.values = it.map(|t| parse::<i64>(t, name)).collect(),
            "schema" => sc.schema = parse(it.next().expect("schema value"), name),
            "quantile" => {
                let q = f(&mut it, name);
                let v = f(&mut it, name);
                sc.quantiles.push((q, v));
            }
            _ if key.contains("_bucket{le=") => {
                let le = parse_le(key, name);
                let cum = parse::<u64>(it.next().expect("bucket count"), name);
                sc.buckets.push((le, cum));
            }
            _ if key.ends_with("_count") => sc.count = parse(it.next().unwrap(), name),
            _ if key.ends_with("_sum") => sc.sum = parse(it.next().unwrap(), name),
            other => panic!("[{name}] unknown key {other:?}"),
        }
    }
    sc
}

/// Extracts the `le` value from a token like `goodhistogram_bucket{le="1024"}`
/// or `..._bucket{le="+Inf"}`.
fn parse_le(token: &str, name: &str) -> f64 {
    let start = token.find("le=\"").expect("le=\" prefix") + 4;
    let rest = &token[start..];
    let end = rest
        .find("\"}")
        .unwrap_or_else(|| panic!("[{name}] malformed le token {token:?}"));
    let s = &rest[..end];
    if s == "+Inf" {
        f64::INFINITY
    } else {
        parse(s, name)
    }
}

fn f<'a>(it: &mut impl Iterator<Item = &'a str>, name: &str) -> f64 {
    parse(
        it.next()
            .unwrap_or_else(|| panic!("[{name}] missing value")),
        name,
    )
}

fn parse<T: std::str::FromStr>(s: &str, name: &str) -> T {
    s.parse().unwrap_or_else(|_| {
        panic!(
            "[{name}] cannot parse {s:?} as {}",
            std::any::type_name::<T>()
        )
    })
}
