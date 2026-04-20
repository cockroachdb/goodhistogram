// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"fmt"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

// HistogramVec is a collection of Histograms partitioned by label values.
// It implements prometheus.Collector so the entire vec can be registered
// with a Prometheus registry. Recording is done on the individual
// *Histogram returned by WithLabelValues.
type HistogramVec struct {
	params     Params
	desc       *prometheus.Desc
	labelNames []string

	mu         sync.RWMutex
	histograms map[string]*labeledHistogram
}

type labeledHistogram struct {
	h          *Histogram
	labelPairs []*prometheusgo.LabelPair
}

// NewHistogramVec creates a new HistogramVec. All child histograms share the
// same Params. The desc is created internally from name, help, and labelNames.
func NewHistogramVec(p Params, name, help string, labelNames []string) *HistogramVec {
	return &HistogramVec{
		params:     p,
		desc:       prometheus.NewDesc(name, help, labelNames, nil),
		labelNames: labelNames,
		histograms: make(map[string]*labeledHistogram),
	}
}

// WithLabelValues returns the Histogram for the given label values,
// creating it if it doesn't exist. Panics if the number of values
// doesn't match the number of label names.
func (v *HistogramVec) WithLabelValues(lvs ...string) *Histogram {
	if len(lvs) != len(v.labelNames) {
		panic(fmt.Sprintf(
			"goodhistogram: expected %d label values, got %d",
			len(v.labelNames), len(lvs),
		))
	}
	key := strings.Join(lvs, "\x00")

	v.mu.RLock()
	if lh, ok := v.histograms[key]; ok {
		v.mu.RUnlock()
		return lh.h
	}
	v.mu.RUnlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	if lh, ok := v.histograms[key]; ok {
		return lh.h
	}
	h := New(v.params)
	v.histograms[key] = &labeledHistogram{
		h:          h,
		labelPairs: makeLabelPairs(v.labelNames, lvs),
	}
	return h
}

// DeleteLabelValues removes the Histogram for the given label values.
// Returns true if the entry existed.
func (v *HistogramVec) DeleteLabelValues(lvs ...string) bool {
	key := strings.Join(lvs, "\x00")
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.histograms[key]
	delete(v.histograms, key)
	return ok
}

// Reset removes all child histograms.
func (v *HistogramVec) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.histograms = make(map[string]*labeledHistogram)
}

// Describe implements prometheus.Collector.
func (v *HistogramVec) Describe(ch chan<- *prometheus.Desc) {
	ch <- v.desc
}

// Collect implements prometheus.Collector.
func (v *HistogramVec) Collect(ch chan<- prometheus.Metric) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, lh := range v.histograms {
		ch <- &histogramMetric{
			desc:       v.desc,
			h:          lh.h,
			labelPairs: lh.labelPairs,
		}
	}
}

// histogramMetric implements prometheus.Metric for a single labeled histogram.
type histogramMetric struct {
	desc       *prometheus.Desc
	h          *Histogram
	labelPairs []*prometheusgo.LabelPair
}

func (m *histogramMetric) Desc() *prometheus.Desc { return m.desc }

func (m *histogramMetric) Write(out *prometheusgo.Metric) error {
	snap := m.h.Snapshot()
	out.Histogram = snap.ToPrometheusHistogram()
	out.Label = m.labelPairs
	return nil
}

func makeLabelPairs(names, values []string) []*prometheusgo.LabelPair {
	pairs := make([]*prometheusgo.LabelPair, len(names))
	for i := range names {
		n := names[i]
		v := values[i]
		pairs[i] = &prometheusgo.LabelPair{Name: &n, Value: &v}
	}
	return pairs
}
