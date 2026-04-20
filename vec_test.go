// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"sort"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestHistogramVecGather(t *testing.T) {
	vec := NewHistogramVec(
		Params{Lo: 100, Hi: 1e9},
		"request_duration_ns", "Request duration", []string{"method", "path"},
	)
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(vec))

	vec.WithLabelValues("GET", "/api").Record(1000)
	vec.WithLabelValues("GET", "/api").Record(2000)
	vec.WithLabelValues("POST", "/api").Record(5000)

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "request_duration_ns", *families[0].Name)

	metrics := families[0].Metric
	require.Len(t, metrics, 2)

	// Sort by first label value for deterministic assertions.
	sort.Slice(metrics, func(i, j int) bool {
		return *metrics[i].Label[0].Value < *metrics[j].Label[0].Value
	})

	// GET /api: 2 observations, sum = 3000
	require.Equal(t, "method", *metrics[0].Label[0].Name)
	require.Equal(t, "GET", *metrics[0].Label[0].Value)
	require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
	require.Equal(t, float64(3000), *metrics[0].Histogram.SampleSum)

	// POST /api: 1 observation, sum = 5000
	require.Equal(t, "POST", *metrics[1].Label[0].Value)
	require.Equal(t, uint64(1), *metrics[1].Histogram.SampleCount)
	require.Equal(t, float64(5000), *metrics[1].Histogram.SampleSum)
}

func TestHistogramVecSamePointer(t *testing.T) {
	vec := NewHistogramVec(
		Params{Lo: 100, Hi: 1e9},
		"test", "test", []string{"x"},
	)
	h1 := vec.WithLabelValues("a")
	h2 := vec.WithLabelValues("a")
	require.True(t, h1 == h2, "WithLabelValues should return the same *Histogram")
}

func TestHistogramVecWrongLabelCountPanics(t *testing.T) {
	vec := NewHistogramVec(
		Params{Lo: 100, Hi: 1e9},
		"test", "test", []string{"method", "path"},
	)
	require.Panics(t, func() { vec.WithLabelValues("GET") })
}

func TestHistogramVecDeleteAndReset(t *testing.T) {
	vec := NewHistogramVec(
		Params{Lo: 100, Hi: 1e9},
		"test", "test", []string{"x"},
	)
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(vec))

	vec.WithLabelValues("a").Record(100)
	vec.WithLabelValues("b").Record(200)

	require.True(t, vec.DeleteLabelValues("a"))
	require.False(t, vec.DeleteLabelValues("a"))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families[0].Metric, 1)
	require.Equal(t, "b", *families[0].Metric[0].Label[0].Value)

	vec.Reset()
	families, err = reg.Gather()
	require.NoError(t, err)
	require.Empty(t, families)
}

func TestHistogramVecConcurrent(t *testing.T) {
	vec := NewHistogramVec(
		Params{Lo: 100, Hi: 1e9},
		"test", "test", []string{"x"},
	)

	var wg sync.WaitGroup
	labels := []string{"a", "b", "c", "d"}
	for _, l := range labels {
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func(label string) {
				defer wg.Done()
				h := vec.WithLabelValues(label)
				for i := 0; i < 1000; i++ {
					h.Record(500)
				}
			}(l)
		}
	}
	wg.Wait()

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(vec))
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families[0].Metric, 4)
	for _, m := range families[0].Metric {
		require.Equal(t, uint64(10000), *m.Histogram.SampleCount)
	}
}
