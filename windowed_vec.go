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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

// WindowedVec is a collection of Windowed histograms partitioned by label
// values. It implements prometheus.Collector so the entire vec can be
// registered with a Prometheus registry. Recording is done on the individual
// *Windowed returned by WithLabelValues.
type WindowedVec struct {
	params     Params
	window     time.Duration
	desc       *prometheus.Desc
	labelNames []string

	mu         sync.RWMutex
	histograms map[string]*labeledWindowed
}

type labeledWindowed struct {
	w          *Windowed
	labelPairs []*prometheusgo.LabelPair
}

// NewWindowedVec creates a new WindowedVec. All child histograms share the
// same Params and window duration.
func NewWindowedVec(
	p Params, window time.Duration, name, help string, labelNames []string,
) *WindowedVec {
	return &WindowedVec{
		params:     p,
		window:     window,
		desc:       prometheus.NewDesc(name, help, labelNames, nil),
		labelNames: labelNames,
		histograms: make(map[string]*labeledWindowed),
	}
}

// WithLabelValues returns the Windowed histogram for the given label values,
// creating it if it doesn't exist. Panics if the number of values doesn't
// match the number of label names.
func (v *WindowedVec) WithLabelValues(lvs ...string) *Windowed {
	if len(lvs) != len(v.labelNames) {
		panic(fmt.Sprintf(
			"goodhistogram: expected %d label values, got %d",
			len(v.labelNames), len(lvs),
		))
	}
	key := strings.Join(lvs, "\x00")

	v.mu.RLock()
	if lw, ok := v.histograms[key]; ok {
		v.mu.RUnlock()
		return lw.w
	}
	v.mu.RUnlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	if lw, ok := v.histograms[key]; ok {
		return lw.w
	}
	w := NewWindowed(v.params, v.window)
	v.histograms[key] = &labeledWindowed{
		w:          w,
		labelPairs: makeLabelPairs(v.labelNames, lvs),
	}
	return w
}

// DeleteLabelValues removes the Windowed histogram for the given label values.
// Returns true if the entry existed.
func (v *WindowedVec) DeleteLabelValues(lvs ...string) bool {
	key := strings.Join(lvs, "\x00")
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.histograms[key]
	delete(v.histograms, key)
	return ok
}

// Reset removes all child histograms.
func (v *WindowedVec) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.histograms = make(map[string]*labeledWindowed)
}

// TickAll manually rotates all child histograms.
func (v *WindowedVec) TickAll() {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, lw := range v.histograms {
		lw.w.Tick()
	}
}

// Describe implements prometheus.Collector.
func (v *WindowedVec) Describe(ch chan<- *prometheus.Desc) {
	ch <- v.desc
}

// Collect implements prometheus.Collector. Exports cumulative data since
// Prometheus expects monotonically increasing counters.
func (v *WindowedVec) Collect(ch chan<- prometheus.Metric) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	for _, lw := range v.histograms {
		ch <- &windowedMetric{
			desc:       v.desc,
			w:          lw.w,
			labelPairs: lw.labelPairs,
		}
	}
}

// windowedMetric implements prometheus.Metric for a single labeled
// windowed histogram.
type windowedMetric struct {
	desc       *prometheus.Desc
	w          *Windowed
	labelPairs []*prometheusgo.LabelPair
}

func (m *windowedMetric) Desc() *prometheus.Desc { return m.desc }

func (m *windowedMetric) Write(out *prometheusgo.Metric) error {
	snap := m.w.Snapshot() // cumulative
	out.Histogram = snap.ToPrometheusHistogram()
	out.Label = m.labelPairs
	return nil
}
