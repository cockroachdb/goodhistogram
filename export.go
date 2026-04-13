// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"math"

	prometheusgo "github.com/prometheus/client_model/go"
)

// ToPrometheusHistogram converts a Snapshot to a prometheusgo.Histogram,
// populating both conventional bucket fields (for backward compatibility) and
// native histogram sparse fields (for efficient Prometheus scraping).
func (s *Snapshot) ToPrometheusHistogram() *prometheusgo.Histogram {
	h := &prometheusgo.Histogram{}

	sampleCount := s.TotalCount
	sampleSum := float64(s.TotalSum)
	h.SampleCount = &sampleCount
	h.SampleSum = &sampleSum

	// Conventional buckets: cumulative counts with upper bounds.
	h.Bucket = s.conventionalBuckets()

	// Native histogram fields.
	s.populateNativeFields(h)

	return h
}

// conventionalBuckets returns the conventional (non-native) Prometheus
// histogram buckets with cumulative counts and a +Inf bucket.
//
// Zero and underflow observations are below all bucket upper bounds, so
// they are included in the cumulative count of every bucket. The final
// +Inf bucket has CumulativeCount == SampleCount, as required by the
// Prometheus exposition format.
func (s *Snapshot) conventionalBuckets() []*prometheusgo.Bucket {
	buckets := make([]*prometheusgo.Bucket, 0, len(s.Counts)+1)
	// Zeros and underflow values are below all bucket upper bounds,
	// so they contribute to every bucket's cumulative count.
	cumCount := s.ZeroCount + s.Underflow
	for i, c := range s.Counts {
		cumCount += c
		ub := s.cfg.boundaries[i+1]
		cc := cumCount
		buckets = append(buckets, &prometheusgo.Bucket{
			CumulativeCount: &cc,
			UpperBound:      &ub,
		})
	}
	// +Inf bucket: all observations including overflow.
	infUB := math.Inf(1)
	infCC := s.TotalCount
	buckets = append(buckets, &prometheusgo.Bucket{
		CumulativeCount: &infCC,
		UpperBound:      &infUB,
	})
	return buckets
}

// populateNativeFields fills in the Prometheus native histogram sparse
// fields: Schema, ZeroThreshold, ZeroCount, PositiveSpan, PositiveDelta.
//
// Because our bucket indices are Prometheus schema-aligned by construction,
// the mapping from internal indices to Prometheus bucket keys is a simple
// offset addition: promKey = internalIndex + config.minKey.
func (s *Snapshot) populateNativeFields(h *prometheusgo.Histogram) {
	schema := s.cfg.schema
	h.Schema = &schema

	// Zero bucket: values at or below zero.
	zt := math.SmallestNonzeroFloat64
	zc := s.ZeroCount
	h.ZeroThreshold = &zt
	h.ZeroCount = &zc

	// Build positive spans and delta-encoded counts from the bucket array.
	// We walk the array, grouping consecutive non-zero buckets into spans
	// separated by gaps of zero-count buckets.
	type span struct {
		offset int32    // gap from end of previous span (or absolute for first)
		counts []uint64 // non-zero counts in this span
	}
	var spans []span
	inSpan := false
	gapSinceLastSpan := 0

	for i, c := range s.Counts {
		if c > 0 {
			if !inSpan {
				// Start a new span.
				var offset int32
				if len(spans) == 0 {
					// First span: offset is the absolute Prometheus bucket key.
					offset = int32(s.cfg.minKey + i)
				} else {
					// Subsequent spans: offset is the gap since the previous span ended.
					offset = int32(gapSinceLastSpan)
				}
				spans = append(spans, span{offset: offset})
				inSpan = true
			}
			spans[len(spans)-1].counts = append(spans[len(spans)-1].counts, c)
			gapSinceLastSpan = 0
		} else {
			if inSpan {
				inSpan = false
				gapSinceLastSpan = 1
			} else {
				gapSinceLastSpan++
			}
		}
	}

	// Convert to prometheusgo types.
	h.PositiveSpan = make([]*prometheusgo.BucketSpan, len(spans))
	totalDeltaCount := 0
	for _, sp := range spans {
		totalDeltaCount += len(sp.counts)
	}
	h.PositiveDelta = make([]int64, 0, totalDeltaCount)

	var prevCount int64
	for i, sp := range spans {
		length := uint32(len(sp.counts))
		offset := sp.offset
		h.PositiveSpan[i] = &prometheusgo.BucketSpan{
			Offset: &offset,
			Length: &length,
		}
		for _, c := range sp.counts {
			delta := int64(c) - prevCount
			h.PositiveDelta = append(h.PositiveDelta, delta)
			prevCount = int64(c)
		}
	}
}
