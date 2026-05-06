// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

// WindowedCollector wraps a Windowed histogram as a prometheus.Collector,
// allowing it to be registered with a Prometheus registry. Collect() exports
// cumulative data, as Prometheus expects monotonically increasing counters.
type WindowedCollector struct {
	w    *Windowed
	desc *prometheus.Desc
}

// ToPrometheusCollector returns a WindowedCollector that exposes this
// windowed histogram to a Prometheus registry. The exported data is
// cumulative (all-time), not windowed, since Prometheus expects monotonic
// counters.
func (w *Windowed) ToPrometheusCollector(desc *prometheus.Desc) *WindowedCollector {
	return &WindowedCollector{w: w, desc: desc}
}

// Describe implements prometheus.Collector.
func (c *WindowedCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements prometheus.Collector.
func (c *WindowedCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- c
}

// Desc implements prometheus.Metric.
func (c *WindowedCollector) Desc() *prometheus.Desc {
	return c.desc
}

// Write implements prometheus.Metric.
func (c *WindowedCollector) Write(m *prometheusgo.Metric) error {
	snap := c.w.Snapshot()
	m.Histogram = snap.ToPrometheusHistogram()
	return nil
}
