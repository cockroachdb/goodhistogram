// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWindowedRecordAndSnapshot(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, 10*time.Second)

	w.Record(500)
	w.Record(1000)
	w.Record(2000)

	snap := w.Snapshot()
	require.Equal(t, uint64(3), snap.TotalCount)
	require.Equal(t, int64(3500), snap.TotalSum)
}

func TestWindowedSnapshotIsCumulative(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, 10*time.Second)

	w.Record(500)
	w.Record(1000)
	w.Tick()
	w.Record(2000)
	w.Tick()
	w.Record(3000)

	// Cumulative snapshot should have all 4 observations.
	snap := w.Snapshot()
	require.Equal(t, uint64(4), snap.TotalCount)
	require.Equal(t, int64(6500), snap.TotalSum)
}

func TestWindowedWindowedSnapshotBeforeTick(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	w.Record(500)
	w.Record(1000)

	// Before any tick, windowed = cumulative - emptyBaseline = cumulative.
	windowed := w.WindowedSnapshot()
	cum := w.Snapshot()
	require.Equal(t, cum.TotalCount, windowed.TotalCount)
	require.Equal(t, cum.TotalSum, windowed.TotalSum)
}

func TestWindowedTick(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	// Window 1: record 3 values.
	w.Record(100)
	w.Record(200)
	w.Record(300)
	w.Tick()

	// Window 2: record 2 values.
	w.Record(400)
	w.Record(500)

	// Cumulative: all 5.
	snap := w.Snapshot()
	require.Equal(t, uint64(5), snap.TotalCount)
	require.Equal(t, int64(1500), snap.TotalSum)

	// Windowed: cum - prevBaseline (which is the empty initial baseline).
	// Since only one tick happened, prevBaseline is still the initial empty
	// snapshot, so windowed == cumulative.
	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(5), windowed.TotalCount)
	require.Equal(t, int64(1500), windowed.TotalSum)
}

func TestWindowedMultipleTicks(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	// Window 1: 100 observations of 100.
	for i := 0; i < 100; i++ {
		w.Record(100)
	}
	w.Tick()

	// Window 2: 50 observations of 200.
	for i := 0; i < 50; i++ {
		w.Record(200)
	}
	w.Tick()

	// Window 3 (current): 25 observations of 300.
	for i := 0; i < 25; i++ {
		w.Record(300)
	}

	// Cumulative: all 175 observations.
	snap := w.Snapshot()
	require.Equal(t, uint64(175), snap.TotalCount)
	require.Equal(t, int64(100*100+50*200+25*300), snap.TotalSum)

	// Windowed: cum - prevBaseline. After 2 ticks, prevBaseline is the
	// snapshot taken at tick 1 (100 observations). So windowed should
	// have 75 observations (50 + 25).
	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(75), windowed.TotalCount)
	require.Equal(t, int64(50*200+25*300), windowed.TotalSum)
}

func TestWindowedOldDataEvicted(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	// Window 1.
	for i := 0; i < 100; i++ {
		w.Record(100)
	}
	w.Tick()

	// Window 2.
	for i := 0; i < 50; i++ {
		w.Record(200)
	}
	w.Tick()

	// Window 3.
	for i := 0; i < 30; i++ {
		w.Record(300)
	}
	w.Tick()

	// Window 4 (current): nothing recorded yet.

	// After 3 ticks, prevBaseline = snapshot at tick 2 (100 + 50 = 150 obs).
	// Cumulative has 180. Windowed = 180 - 150 = 30.
	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(30), windowed.TotalCount)
	require.Equal(t, int64(30*300), windowed.TotalSum)
}

func TestWindowedLazyTick(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, 10*time.Millisecond)

	w.Record(500)
	time.Sleep(20 * time.Millisecond)
	w.Record(1000)

	// WindowedSnapshot should trigger a lazy tick. After the tick,
	// prevBaseline = initial empty, curBaseline = snapshot taken now.
	// So windowed = cum - empty = cum.
	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(2), windowed.TotalCount)
}

func TestWindowedSnapshotDoesNotTick(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, 10*time.Millisecond)

	w.Record(500)
	time.Sleep(20 * time.Millisecond)

	// Calling Snapshot() (cumulative) should NOT trigger a tick.
	_ = w.Snapshot()

	time.Sleep(20 * time.Millisecond)
	w.Record(1000)

	// WindowedSnapshot will lazy-tick. If Snapshot() had ticked earlier,
	// prevBaseline would include the first observation, and the second
	// sleep would cause another tick advancing prevBaseline further —
	// yielding only 1 observation. With Snapshot() not ticking, only
	// one tick happens here (from the first expired window), so
	// prevBaseline remains the initial empty snapshot and windowed
	// includes both observations.
	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(2), windowed.TotalCount)
}

func TestWindowedReset(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	for i := 0; i < 100; i++ {
		w.Record(500)
	}
	w.Tick()
	for i := 0; i < 50; i++ {
		w.Record(1000)
	}

	w.Reset()

	snap := w.Snapshot()
	require.Equal(t, uint64(0), snap.TotalCount)
	require.Equal(t, int64(0), snap.TotalSum)

	windowed := w.WindowedSnapshot()
	require.Equal(t, uint64(0), windowed.TotalCount)
	require.Equal(t, int64(0), windowed.TotalSum)

	// Recording after reset should work normally.
	w.Record(123)
	snap = w.Snapshot()
	require.Equal(t, uint64(1), snap.TotalCount)
	require.Equal(t, int64(123), snap.TotalSum)
}

func TestWindowedConcurrentRecord(t *testing.T) {
	w := NewWindowed(Params{Lo: 1, Hi: 1e6}, time.Hour)
	const goroutines = 8
	const recordsPerGoroutine = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < recordsPerGoroutine; i++ {
				v := rng.Int63n(1e6) + 1
				w.Record(v)
			}
		}(int64(g))
	}
	wg.Wait()

	snap := w.Snapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), snap.TotalCount)
}

func TestWindowedConcurrentRecordAndTick(t *testing.T) {
	w := NewWindowed(Params{Lo: 1, Hi: 1e6}, time.Hour)
	const goroutines = 8
	const recordsPerGoroutine = 10000

	var wg sync.WaitGroup
	wg.Add(goroutines + 1)

	// Recording goroutines.
	for g := 0; g < goroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < recordsPerGoroutine; i++ {
				v := rng.Int63n(1e6) + 1
				w.Record(v)
			}
		}(int64(g))
	}

	// Ticking goroutine.
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			w.Tick()
			_ = w.WindowedSnapshot()
		}
	}()

	wg.Wait()

	// Cumulative should have all observations.
	snap := w.Snapshot()
	require.Equal(t, uint64(goroutines*recordsPerGoroutine), snap.TotalCount)
}

func TestWindowedSchema(t *testing.T) {
	w := NewWindowed(Params{Lo: 1, Hi: 1e6, ErrorBound: 0.05}, time.Hour)
	require.Equal(t, int32(3), w.Schema())
}

func TestNewWindowedPanicsOnZeroWindow(t *testing.T) {
	require.Panics(t, func() {
		NewWindowed(Params{Lo: 1, Hi: 1000}, 0)
	})
	require.Panics(t, func() {
		NewWindowed(Params{Lo: 1, Hi: 1000}, -time.Second)
	})
}

// WindowedCollector tests.

func TestWindowedCollectorRegistration(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)
	desc := prometheus.NewDesc("test_windowed", "test", nil, nil)
	collector := w.ToPrometheusCollector(desc)

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(collector))

	w.Record(500)
	w.Record(1000)
	w.Record(2000)

	families, err := reg.Gather()
	require.NoError(t, err)
	require.Len(t, families, 1)
	require.Equal(t, "test_windowed", *families[0].Name)

	metrics := families[0].Metric
	require.Len(t, metrics, 1)
	require.Equal(t, uint64(3), *metrics[0].Histogram.SampleCount)
	require.Equal(t, float64(3500), *metrics[0].Histogram.SampleSum)
}

func TestWindowedCollectorExportsCumulative(t *testing.T) {
	w := NewWindowed(Params{Lo: 100, Hi: 1e9}, time.Hour)

	// Record in window 1.
	w.Record(500)
	w.Record(1000)
	w.Tick()

	// Record in window 2.
	w.Record(2000)
	w.Tick()

	// Record in window 3.
	w.Record(3000)

	// Collector should export cumulative data (all 4 observations),
	// not windowed data.
	var collected prometheusgo.Metric
	desc := prometheus.NewDesc("test", "test", nil, nil)
	collector := w.ToPrometheusCollector(desc)
	require.NoError(t, collector.Write(&collected))

	require.Equal(t, uint64(4), collected.Histogram.GetSampleCount())
	require.Equal(t, float64(6500), collected.Histogram.GetSampleSum())
}

// WindowedVec tests.

func TestWindowedVecGather(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
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

	sort.Slice(metrics, func(i, j int) bool {
		return *metrics[i].Label[0].Value < *metrics[j].Label[0].Value
	})

	require.Equal(t, "GET", *metrics[0].Label[0].Value)
	require.Equal(t, uint64(2), *metrics[0].Histogram.SampleCount)
	require.Equal(t, float64(3000), *metrics[0].Histogram.SampleSum)

	require.Equal(t, "POST", *metrics[1].Label[0].Value)
	require.Equal(t, uint64(1), *metrics[1].Histogram.SampleCount)
	require.Equal(t, float64(5000), *metrics[1].Histogram.SampleSum)
}

func TestWindowedVecSamePointer(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
		"test", "test", []string{"x"},
	)
	w1 := vec.WithLabelValues("a")
	w2 := vec.WithLabelValues("a")
	require.True(t, w1 == w2, "WithLabelValues should return the same *Windowed")
}

func TestWindowedVecWrongLabelCountPanics(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
		"test", "test", []string{"method", "path"},
	)
	require.Panics(t, func() { vec.WithLabelValues("GET") })
}

func TestWindowedVecDeleteAndReset(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
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

func TestWindowedVecTickAll(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
		"test", "test", []string{"x"},
	)

	// Record in window 1.
	vec.WithLabelValues("a").Record(100)
	vec.WithLabelValues("b").Record(200)
	vec.TickAll()

	// Record in window 2.
	vec.WithLabelValues("a").Record(300)
	vec.WithLabelValues("b").Record(400)
	vec.TickAll()

	// Record in window 3.
	vec.WithLabelValues("a").Record(500)
	vec.WithLabelValues("b").Record(600)

	// Windowed snapshots should only cover windows 2+3 (after 2 ticks,
	// prevBaseline = snapshot at tick 1).
	wA := vec.WithLabelValues("a").WindowedSnapshot()
	require.Equal(t, uint64(2), wA.TotalCount) // 300 + 500
	require.Equal(t, int64(800), wA.TotalSum)

	wB := vec.WithLabelValues("b").WindowedSnapshot()
	require.Equal(t, uint64(2), wB.TotalCount) // 400 + 600
	require.Equal(t, int64(1000), wB.TotalSum)
}

func TestWindowedVecConcurrent(t *testing.T) {
	vec := NewWindowedVec(
		Params{Lo: 100, Hi: 1e9}, time.Hour,
		"test", "test", []string{"x"},
	)

	var wg sync.WaitGroup
	labels := []string{"a", "b", "c", "d"}
	for _, l := range labels {
		for g := 0; g < 10; g++ {
			wg.Add(1)
			go func(label string) {
				defer wg.Done()
				w := vec.WithLabelValues(label)
				for i := 0; i < 1000; i++ {
					w.Record(500)
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

// Snapshot.Sub tests.

func TestSnapshotSub(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 1e9})

	// Take baseline after 3 observations.
	h.Record(100)
	h.Record(200)
	h.Record(300)
	baseline := h.Snapshot()

	// Record 2 more observations.
	h.Record(400)
	h.Record(500)
	current := h.Snapshot()

	diff := current.Sub(&baseline)
	require.Equal(t, uint64(2), diff.TotalCount)
	require.Equal(t, int64(900), diff.TotalSum) // 400 + 500
}

func TestSnapshotSubIdentity(t *testing.T) {
	h := New(Params{Lo: 100, Hi: 1e9})
	h.Record(500)
	h.Record(1000)

	snap := h.Snapshot()
	diff := snap.Sub(&snap)
	require.Equal(t, uint64(0), diff.TotalCount)
	require.Equal(t, int64(0), diff.TotalSum)
	for _, c := range diff.Counts {
		require.Equal(t, uint64(0), c)
	}
}
