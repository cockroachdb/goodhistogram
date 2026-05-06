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

// PrometheusCollector wraps a Histogram as a prometheus.Collector, allowing it
// to be registered with a Prometheus registry for scraping. Recording is done
// directly on the Histogram; the collector only participates in the export path.
type PrometheusCollector struct {
	h    *Histogram
	desc *prometheus.Desc
}

// ToPrometheusCollector returns a PrometheusCollector that exposes this
// histogram to a Prometheus registry. The returned collector implements
// prometheus.Collector and can be passed to prometheus.MustRegister.
func (h *Histogram) ToPrometheusCollector(desc *prometheus.Desc) *PrometheusCollector {
	return &PrometheusCollector{h: h, desc: desc}
}

// Describe implements prometheus.Collector.
func (p *PrometheusCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- p.desc
}

// Collect implements prometheus.Collector. A fresh histogramMetric is
// created per scrape so the Collector and Metric roles are separate.
func (p *PrometheusCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- &histogramMetric{
		desc: p.desc,
		h:    p.h,
	}
}

// ToPrometheusMetric returns a point-in-time snapshot as a
// prometheusgo.Metric. This bridges to CockroachDB's PrometheusExportable
// interface without going through the Collector/Metric channel path.
func (p *PrometheusCollector) ToPrometheusMetric() *prometheusgo.Metric {
	snap := p.h.Snapshot()
	return &prometheusgo.Metric{
		Histogram: snap.ToPrometheusHistogram(),
	}
}
