// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestPrometheusCollectorRegistration(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 1e9})
	desc := prometheus.NewDesc("test_hist", "test", nil, nil)
	collector := h.ToPrometheusCollector(desc)

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(collector))

	h.Record(500)
	h.Record(1000)
	h.Record(2000)

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "test_hist", *families[0].Name)

	metrics := families[0].Metric
	require.Len(t, metrics, 1)
	require.Equal(t, uint64(3), *metrics[0].Histogram.SampleCount)
	require.Equal(t, float64(3500), *metrics[0].Histogram.SampleSum)
}

func TestPrometheusCollectorMatchesSnapshot(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 1e9})
	for _, v := range []int64{150, 999, 5000, 100000, 5000000} {
		h.Record(v)
	}

	// Collect via the PrometheusCollector.
	desc := prometheus.NewDesc("test_hist", "test", nil, nil)
	collector := h.ToPrometheusCollector(desc)
	var collected prometheusgo.Metric
	require.NoError(t, collector.Write(&collected))

	// Export directly via Snapshot.
	snap := h.Snapshot()
	direct := snap.ToPrometheusHistogram()

	require.Equal(t, direct.GetSampleCount(), collected.Histogram.GetSampleCount())
	require.Equal(t, direct.GetSampleSum(), collected.Histogram.GetSampleSum())
	require.Equal(t, direct.GetSchema(), collected.Histogram.GetSchema())
	require.Equal(t, len(direct.GetBucket()), len(collected.Histogram.GetBucket()))
	require.Equal(t, len(direct.GetPositiveSpan()), len(collected.Histogram.GetPositiveSpan()))
}
