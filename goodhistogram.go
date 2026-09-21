// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package goodhistogram provides an exponential histogram with Prometheus
// native histogram schema alignment, bounded relative error, and trapezoidal
// quantile estimation.
//
// The histogram is configured with a value range [lo, hi] and a desired
// relative error bound. It selects the tightest Prometheus schema whose error
// is at or below the requested bound, then allocates a fixed array of atomic
// counters covering the range. Recording is O(1) and lock-free: values are
// mapped to bucket indices via math.Frexp plus a small precomputed boundary
// table, then the corresponding counter is atomically incremented.
//
// Because the bucket layout is identical to a Prometheus native histogram
// schema, export to the Prometheus sparse format requires no remapping — our
// internal indices are Prometheus bucket indices offset by a constant.
package goodhistogram

import (
	"time"

	"github.com/cockroachdb/goodhistogram/internal"
)

// Params holds the user-facing parameters for creating a Histogram.
//
// Zero-value fields are replaced with defaults:
//   - Lo: 1
//   - Hi: math.MaxInt64
//   - ErrorBound: 0.10 (10%, schema 3)
type Params = internal.Params

// Histogram is a lock-free exponential histogram with atomic counters.
type Histogram = internal.Histogram

// Snapshot is a point-in-time, non-atomic copy of a Histogram, suitable for
// quantile computation, export, and serialization. The zero value represents
// an unset snapshot with no observations or layout. UnmarshalJSON validates
// the decoded layout and counts; JSON null leaves the snapshot unchanged.
type Snapshot = internal.Snapshot

// WithExactMinMax wraps a Histogram, additionally tracking the exact min and
// max recorded values. Extremes span every observation, including out-of-range
// (Underflow/Overflow) and zero/negative values, so Max may exceed hi and Min
// may be below lo. A WithExactMinMax must not be copied after first use.
type WithExactMinMax = internal.WithExactMinMax

// Summary is an exact summary of the recorded values. Min and Max are
// meaningful only when Count > 0.
type Summary = internal.Summary

// ExactSnapshot is a Snapshot with exact extremes. Min and Max are meaningful
// only when Summary().Count > 0. Subtraction is not supported because exact
// extremes cannot be recovered by subtracting snapshots.
type ExactSnapshot = internal.ExactSnapshot

// PrometheusCollector wraps a Histogram as a prometheus.Collector, allowing it
// to be registered with a Prometheus registry for scraping. Recording is done
// directly on the Histogram; the collector only participates in the export path.
type PrometheusCollector = internal.PrometheusCollector

// HistogramVec is a collection of Histograms partitioned by label values.
// It implements prometheus.Collector so the entire vec can be registered
// with a Prometheus registry. Recording is done on the individual
// *Histogram returned by WithLabelValues.
type HistogramVec = internal.HistogramVec

// Windowed is a histogram that supports both cumulative and windowed snapshots.
// It maintains a single cumulative histogram and two baseline snapshots that
// are rotated on a configurable interval. The windowed snapshot is computed by
// subtracting the older baseline from the current cumulative state, providing
// a view of recent activity spanning 1x-2x the window duration.
//
// Recording is identical in cost to a plain Histogram (~20ns): values are
// atomically added to the single underlying histogram with no locking. The
// mutex only protects baseline rotation, which happens lazily on
// WindowedSnapshot() calls or explicitly via Tick().
type Windowed = internal.Windowed

// WindowedCollector wraps a Windowed histogram as a prometheus.Collector,
// allowing it to be registered with a Prometheus registry. Collect() exports
// cumulative data, as Prometheus expects monotonically increasing counters.
type WindowedCollector = internal.WindowedCollector

// WindowedVec is a collection of Windowed histograms partitioned by label
// values. It implements prometheus.Collector so the entire vec can be
// registered with a Prometheus registry. Recording is done on the individual
// *Windowed returned by WithLabelValues.
type WindowedVec = internal.WindowedVec

// Resolution presets cover the full range [1, math.MaxInt64] and differ only
// in accuracy and memory — pick by the fidelity you need. Memory is the
// per-histogram counts array; the config (boundaries, lookup table) is shared.
var (
	// CoarseParams: schema 1, ~41.4% error, 126 buckets, ~1 KB/histogram.
	CoarseParams = internal.CoarseParams

	// StandardParams: schema 2, ~18.9% error, 252 buckets, ~2 KB/histogram.
	StandardParams = internal.StandardParams

	// FineParams: schema 3, ~9.05% error, 504 buckets, ~4 KB/histogram.
	FineParams = internal.FineParams
)

// Common Params presets, modeled after the bucket tiers in CockroachDB's
// pkg/util/metric/histogram_buckets.go and Prometheus DefBuckets.
//
// Time-based presets expect values in nanoseconds, matching Go's
// time.Duration. Record with int64(duration).
var (
	// HiResLatencyParams covers high-resolution latency from 1us to 5m.
	// Use for: end-to-end request latencies where you need visibility into
	// both fast-path sub-millisecond operations and slow tail outliers.
	HiResLatencyParams = internal.HiResLatencyParams

	// IOLatencyParams covers fast I/O operations from 10us to 10s.
	// Use for: RPC latencies, raft operations, disk I/O, network round-trips.
	IOLatencyParams = internal.IOLatencyParams

	// ResponseTimeParams covers request/response latencies from 1ms to 30s.
	// Use for: SQL query execution, HTTP handlers, API response times.
	ResponseTimeParams = internal.ResponseTimeParams

	// LongRunningParams covers long-running operations from 500ms to 1h.
	// Use for: backups, restores, migrations, bulk ingestion jobs.
	LongRunningParams = internal.LongRunningParams

	// DataSizeParams covers data payload sizes from 1KB to 16MB (in bytes).
	// Use for: message sizes, request/response bodies, SST sizes.
	DataSizeParams = internal.DataSizeParams

	// MemoryUsageParams covers memory tracking from 1B to 64MB (in bytes).
	// Use for: memory allocations, buffer sizes, cache entry sizes.
	MemoryUsageParams = internal.MemoryUsageParams
)

// New creates a new Histogram for the given range and error bound. Configs
// are cached and shared across histograms with identical parameters.
func New(p Params) *Histogram {
	return internal.New(p)
}

// NewWithExactMinMax creates a Histogram that also tracks exact extremes.
func NewWithExactMinMax(p Params) *WithExactMinMax {
	return internal.NewWithExactMinMax(p)
}

// NewHistogramVec creates a new HistogramVec. All child histograms share the
// same Params. The desc is created internally from name, help, and labelNames.
func NewHistogramVec(p Params, name, help string, labelNames []string) *HistogramVec {
	return internal.NewHistogramVec(p, name, help, labelNames)
}

// NewWindowed creates a new Windowed histogram. The params configure the
// bucket layout (same as New), and window is the rotation interval. Panics
// if window <= 0.
func NewWindowed(p Params, window time.Duration) *Windowed {
	return internal.NewWindowed(p, window)
}

// NewWindowedVec creates a new WindowedVec. All child histograms share the
// same Params and window duration.
func NewWindowedVec(
	p Params, window time.Duration, name, help string, labelNames []string,
) *WindowedVec {
	return internal.NewWindowedVec(p, window, name, help, labelNames)
}
