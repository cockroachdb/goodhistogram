// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goodhistogram

import (
	"sync"
	"time"
)

// Windowed is a histogram that supports both cumulative and windowed snapshots.
// It maintains a single cumulative histogram and two baseline snapshots that
// are rotated on a configurable interval. The windowed snapshot is computed by
// subtracting the older baseline from the current cumulative state, providing
// a view of recent activity spanning 1x-2x the window duration.
//
// Recording is identical in cost to a plain Histogram (~20ns): values are
// atomically added to the single underlying histogram with no locking. The
// mutex only protects baseline rotation, which happens lazily on
// WindowedSnapshot() calls or explicitly via Tick().
type Windowed struct {
	h *Histogram // cumulative — the only histogram, never swapped or reset

	mu           sync.Mutex
	prevBaseline Snapshot // baseline from 2 ticks ago (subtracted for windowed view)
	curBaseline  Snapshot // baseline from 1 tick ago (promoted to prev on next tick)
	nextTick     time.Time
	interval     time.Duration
}

// NewWindowed creates a new Windowed histogram. The params configure the
// bucket layout (same as New), and window is the rotation interval. Panics
// if window <= 0.
func NewWindowed(p Params, window time.Duration) *Windowed {
	if window <= 0 {
		panic("goodhistogram: window must be > 0")
	}
	h := New(p)
	empty := h.Snapshot()
	return &Windowed{
		h:            h,
		prevBaseline: empty,
		curBaseline:  empty,
		nextTick:     time.Now().Add(window),
		interval:     window,
	}
}

// Record adds a value to the histogram. This is the hot path: O(1),
// lock-free, no allocations. Identical cost to Histogram.Record.
func (w *Windowed) Record(v int64) {
	w.h.Record(v)
}

// Snapshot returns a cumulative (all-time) snapshot of the histogram.
// This does not trigger a tick rotation.
func (w *Windowed) Snapshot() Snapshot {
	return w.h.Snapshot()
}

// WindowedSnapshot returns a snapshot of recent activity by subtracting
// the older baseline from the current cumulative state. If the tick
// interval has elapsed, a rotation is performed first (lazy ticking).
func (w *Windowed) WindowedSnapshot() Snapshot {
	w.maybeTick()
	cur := w.h.Snapshot()
	w.mu.Lock()
	base := w.prevBaseline
	w.mu.Unlock()
	return cur.Sub(&base)
}

// Tick manually rotates the baselines: curBaseline becomes prevBaseline,
// and a fresh snapshot of the cumulative histogram becomes curBaseline.
// The next automatic tick time is reset to now + interval.
func (w *Windowed) Tick() {
	w.mu.Lock()
	w.tick()
	w.mu.Unlock()
}

// maybeTick checks whether the tick interval has elapsed and rotates if so.
func (w *Windowed) maybeTick() {
	now := time.Now()
	w.mu.Lock()
	if now.Before(w.nextTick) {
		w.mu.Unlock()
		return
	}
	w.tick()
	w.mu.Unlock()
}

// tick performs the actual baseline rotation. Must be called with w.mu held.
func (w *Windowed) tick() {
	w.prevBaseline = w.curBaseline
	w.curBaseline = w.h.Snapshot()
	w.nextTick = time.Now().Add(w.interval)
}

// Schema returns the Prometheus native histogram schema (0-8).
func (w *Windowed) Schema() int32 {
	return w.h.Schema()
}

// Reset zeroes the cumulative histogram, resets both baselines to empty,
// and resets the tick timer.
func (w *Windowed) Reset() {
	w.h.Reset()
	w.mu.Lock()
	empty := w.h.Snapshot()
	w.prevBaseline = empty
	w.curBaseline = empty
	w.nextTick = time.Now().Add(w.interval)
	w.mu.Unlock()
}
