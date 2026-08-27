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
	"math/rand"
	"runtime"
	"testing"
	"time"
)

func BenchmarkWindowedMemory(b *testing.B) {
	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	histograms := make([]*Windowed, b.N)
	for i := range histograms {
		histograms[i] = NewWindowed(
			Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
			10*time.Second,
		)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)

	bytesPerHist := float64(after.TotalAlloc-before.TotalAlloc) / float64(b.N)
	b.ReportMetric(bytesPerHist, "bytes/histogram")
	runtime.KeepAlive(histograms)
}

func BenchmarkWindowedRecordSingleThread(b *testing.B) {
	rng := rand.New(rand.NewSource(42))
	vals := makeInt64Values(rng, 1<<16)

	w := NewWindowed(
		Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
		10*time.Second,
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Record(vals[i&(len(vals)-1)])
	}
}

func BenchmarkWindowedRecordContention(b *testing.B) {
	for _, numGoroutines := range []int{50, 100} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			w := NewWindowed(
				Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
				10*time.Second,
			)
			b.SetParallelism(numGoroutines)
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(rand.Int63()))
				vals := makeInt64Values(rng, 1<<12)
				i := 0
				for pb.Next() {
					w.Record(vals[i&(len(vals)-1)])
					i++
				}
			})
		})
	}
}

func BenchmarkWindowedSnapshot(b *testing.B) {
	w := NewWindowed(
		Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
		10*time.Second,
	)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		w.Record(int64(rng.Float64()*benchRange + benchLo))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.Snapshot()
	}
}

func BenchmarkWindowedWindowedSnapshot(b *testing.B) {
	w := NewWindowed(
		Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
		10*time.Second,
	)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		w.Record(int64(rng.Float64()*benchRange + benchLo))
	}
	// Tick once so prevBaseline is non-empty.
	w.Tick()
	for i := 0; i < 5000; i++ {
		w.Record(int64(rng.Float64()*benchRange + benchLo))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WindowedSnapshot()
	}
}

func BenchmarkWindowedTick(b *testing.B) {
	w := NewWindowed(
		Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound},
		time.Hour, // long interval so maybeTick doesn't interfere
	)
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10000; i++ {
		w.Record(int64(rng.Float64()*benchRange + benchLo))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Tick()
	}
}
