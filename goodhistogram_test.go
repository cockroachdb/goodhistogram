// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/goodhistogram"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Exercise the original import path from a consumer package, including the
// aliased types returned by constructors and methods.
func TestPublicAPI(t *testing.T) {
	p := goodhistogram.Params{Lo: 100, Hi: 1e9}
	var h *goodhistogram.Histogram = goodhistogram.New(p)
	var w *goodhistogram.Windowed = goodhistogram.NewWindowed(p, time.Minute)
	var v *goodhistogram.HistogramVec = goodhistogram.NewHistogramVec(p, "vec", "test", []string{"label"})
	var wv *goodhistogram.WindowedVec = goodhistogram.NewWindowedVec(p, time.Minute, "windowed_vec", "test", []string{"label"})

	for _, hist := range []interface {
		Record(int64)
		Snapshot() goodhistogram.Snapshot
	}{h, w, v.WithLabelValues("a"), wv.WithLabelValues("a")} {
		hist.Record(500)
		hist.Record(1000)
		snap := hist.Snapshot()
		require.Equal(t, uint64(2), snap.TotalCount)
		require.Equal(t, int64(1500), snap.TotalSum)
	}

	var collector *goodhistogram.PrometheusCollector = h.ToPrometheusCollector(prometheus.NewDesc("hist", "test", nil, nil))
	var windowedCollector *goodhistogram.WindowedCollector = w.ToPrometheusCollector(prometheus.NewDesc("windowed", "test", nil, nil))
	reg := prometheus.NewRegistry()
	for _, c := range []prometheus.Collector{collector, windowedCollector, v, wv} {
		require.NoError(t, reg.Register(c))
	}
	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 4)
	for _, family := range families {
		require.Len(t, family.Metric, 1)
		require.Equal(t, uint64(2), family.Metric[0].Histogram.GetSampleCount())
		require.Equal(t, float64(1500), family.Metric[0].Histogram.GetSampleSum())
	}
}

func TestPublicPresets(t *testing.T) {
	for name, p := range map[string]goodhistogram.Params{
		"Coarse":       goodhistogram.CoarseParams,
		"Standard":     goodhistogram.StandardParams,
		"Fine":         goodhistogram.FineParams,
		"HiResLatency": goodhistogram.HiResLatencyParams,
		"IOLatency":    goodhistogram.IOLatencyParams,
		"ResponseTime": goodhistogram.ResponseTimeParams,
		"LongRunning":  goodhistogram.LongRunningParams,
		"DataSize":     goodhistogram.DataSizeParams,
		"MemoryUsage":  goodhistogram.MemoryUsageParams,
	} {
		t.Run(name, func(t *testing.T) {
			h := goodhistogram.New(p)
			h.Record(int64(p.Lo))
			snap := h.Snapshot()
			require.Equal(t, uint64(1), snap.TotalCount)
			require.Zero(t, snap.Underflow)
			require.Zero(t, snap.Overflow)
		})
	}
}
