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
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

// --------------------------------------------------------------------------
// Constants — all benchmarks use schema 2, range [500, 6e10].
// --------------------------------------------------------------------------

const (
	benchSchema     int32   = 2
	benchLo         float64 = 500
	benchHi         float64 = 6e10
	benchErrBound           = 0.10 // 10% relative error → schema 2
	promBucketCount         = 60   // CockroachDB's standard bucket count
)

// benchRange is benchHi - benchLo, used by samplers.
const benchRange = benchHi - benchLo

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

// newPromNativeHist creates a Prometheus native histogram at the same schema
// as the goodhistogram. Prometheus picks the tightest schema whose gamma is
// ≤ the factor, so we need to pass a factor slightly above the target schema's
// gamma. For schema 2, gamma=1.1892, so factor=1.2 works.
func newPromNativeHist() prometheus.Histogram {
	// Map from desired schema to the factor that Prometheus needs.
	factorBySchema := map[int32]float64{
		0: 2.1, 1: 1.5, 2: 1.2, 3: 1.1, 4: 1.05,
		5: 1.02, 6: 1.01, 7: 1.005, 8: 1.002,
	}
	factor := factorBySchema[benchSchema]
	buckets := prometheus.ExponentialBucketsRange(benchLo, benchHi, promBucketCount)
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:                          "bench",
		Help:                          "benchmark histogram",
		Buckets:                       buckets,
		NativeHistogramBucketFactor:   factor,
		NativeHistogramMaxBucketNumber: 1000,
	})
}

// newGoodHist creates a goodhistogram with the bench parameters.
func newGoodHist() *Histogram {
	return New(Params{Lo: benchLo, Hi: benchHi, ErrorBound: benchErrBound})
}

// collectPromHist extracts the prometheusgo.Histogram from a prometheus.Histogram.
func collectPromHist(h prometheus.Histogram) *prometheusgo.Histogram {
	ch := make(chan prometheus.Metric, 1)
	h.Collect(ch)
	m := <-ch
	var dto prometheusgo.Metric
	_ = m.Write(&dto)
	return dto.GetHistogram()
}

// nativeHistogramQuantile computes a quantile from a Prometheus native
// histogram by decoding its spans and deltas into bucket boundaries and
// counts, then interpolating in log2 space (matching Prometheus's actual
// exponential interpolation for native histograms).
func nativeHistogramQuantile(h *prometheusgo.Histogram, q float64) float64 {
	schema := h.GetSchema()
	n := float64(h.GetSampleCount())
	if n == 0 {
		return 0
	}
	rank := q * n

	type bucket struct {
		upperBound float64
		count      int64
	}
	var buckets []bucket
	var runningCount int64
	deltaIdx := 0
	bucketKey := 0

	for spanIdx, span := range h.GetPositiveSpan() {
		if spanIdx == 0 {
			bucketKey = int(span.GetOffset())
		} else {
			bucketKey += int(span.GetOffset())
		}
		for i := uint32(0); i < span.GetLength(); i++ {
			if deltaIdx < len(h.GetPositiveDelta()) {
				runningCount += h.GetPositiveDelta()[deltaIdx]
				deltaIdx++
			}
			ub := getLe(bucketKey, schema)
			buckets = append(buckets, bucket{upperBound: ub, count: runningCount})
			bucketKey++
		}
	}

	var cumCount float64
	for i, b := range buckets {
		cumCount += float64(b.count)
		if cumCount >= rank {
			var lo float64
			localCount := float64(b.count)
			localRank := rank - (cumCount - float64(b.count))
			if i > 0 {
				lo = buckets[i-1].upperBound
			}
			hi := b.upperBound
			if localCount == 0 {
				return hi
			}
			fraction := localRank / localCount
			// Exponential (log-linear) interpolation, matching Prometheus's
			// actual implementation for native histogram exponential buckets.
			// Interpolate in log2 space, which assumes observations are
			// uniformly distributed in log-space within each bucket.
			if lo > 0 {
				logLo := math.Log2(lo)
				logHi := math.Log2(hi)
				return math.Exp2(logLo + (logHi-logLo)*fraction)
			}
			// Fallback to linear for zero-boundary buckets.
			return lo + (hi-lo)*fraction
		}
	}
	if len(buckets) > 0 {
		return buckets[len(buckets)-1].upperBound
	}
	return 0
}

// makeInt64Values generates pre-computed log-uniform int64 values for
// benchmarks, avoiding RNG overhead in the timed loop.
func makeInt64Values(rng *rand.Rand, n int) []int64 {
	logLo := math.Log(benchLo)
	logHi := math.Log(benchHi)
	vals := make([]int64, n)
	for i := range vals {
		vals[i] = int64(math.Exp(rng.Float64()*(logHi-logLo) + logLo))
	}
	return vals
}

// makeFloat64Values generates pre-computed log-uniform float64 values.
func makeFloat64Values(rng *rand.Rand, n int) []float64 {
	logLo := math.Log(benchLo)
	logHi := math.Log(benchHi)
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
	}
	return vals
}

// --------------------------------------------------------------------------
// Distributions — each generates sorted, clamped values in [benchLo, benchHi].
// --------------------------------------------------------------------------

type distributionGen struct {
	name  string
	genFn func(rng *rand.Rand, n int) []float64
}

func clampSort(vals []float64) []float64 {
	for i, v := range vals {
		if v < benchLo {
			vals[i] = benchLo
		} else if v > benchHi {
			vals[i] = benchHi
		}
	}
	sort.Float64s(vals)
	return vals
}

var distributions = []distributionGen{
	{
		name: "uniform",
		genFn: func(rng *rand.Rand, n int) []float64 {
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = rng.Float64()*(benchHi-benchLo) + benchLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "log-uniform",
		genFn: func(rng *rand.Rand, n int) []float64 {
			logLo := math.Log(benchLo)
			logHi := math.Log(benchHi)
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
			}
			return clampSort(vals)
		},
	},
	{
		name: "exponential",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Exponential with mean ~ 100ms (1e8 ns). Typical for OLTP latencies.
			mean := 1e8
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = rng.ExpFloat64()*mean + benchLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "lognormal-narrow",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Log-normal with σ=1, centered around 10ms (1e7 ns). Moderate tail.
			mu := math.Log(1e7)
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.NormFloat64()*1.0 + mu)
			}
			return clampSort(vals)
		},
	},
	{
		name: "lognormal-wide",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Log-normal with σ=2, centered around 1ms (1e6 ns). Heavy tail —
			// models workloads where most queries are fast but some hit disk/network.
			mu := math.Log(1e6)
			vals := make([]float64, n)
			for i := range vals {
				vals[i] = math.Exp(rng.NormFloat64()*2.0 + mu)
			}
			return clampSort(vals)
		},
	},
	{
		name: "pareto-alpha1.5",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Pareto (alpha=1.5): heavy tail, finite mean and variance.
			alpha := 1.5
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = benchLo / math.Pow(1-u, 1.0/alpha)
			}
			return clampSort(vals)
		},
	},
	{
		name: "pareto-alpha1.0",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Pareto (alpha=1.0): very heavy tail, infinite mean.
			alpha := 1.0
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = benchLo / math.Pow(1-u, 1.0/alpha)
			}
			return clampSort(vals)
		},
	},
	{
		name: "bimodal",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Two clusters: 80% fast (around 1ms), 20% slow (around 500ms).
			vals := make([]float64, n)
			for i := range vals {
				if rng.Float64() < 0.8 {
					vals[i] = math.Exp(rng.NormFloat64()*0.5 + math.Log(1e6))
				} else {
					vals[i] = math.Exp(rng.NormFloat64()*0.5 + math.Log(500e6))
				}
			}
			return clampSort(vals)
		},
	},
	{
		name: "trimodal",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Three clusters: 60% at ~500µs, 30% at ~50ms, 10% at ~5s.
			vals := make([]float64, n)
			for i := range vals {
				r := rng.Float64()
				switch {
				case r < 0.6:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(500e3))
				case r < 0.9:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(50e6))
				default:
					vals[i] = math.Exp(rng.NormFloat64()*0.3 + math.Log(5e9))
				}
			}
			return clampSort(vals)
		},
	},
	{
		name: "chi-squared-k4",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Chi-squared(k=4), scaled so mean ≈ 50ms. Right-skewed, bounded tail.
			scale := 50e6 / 4.0
			vals := make([]float64, n)
			for i := range vals {
				var sum float64
				for j := 0; j < 4; j++ {
					z := rng.NormFloat64()
					sum += z * z
				}
				vals[i] = sum*scale + benchLo
			}
			return clampSort(vals)
		},
	},
	{
		name: "weibull-k0.5",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// Weibull(k=0.5): decreasing hazard, very heavy tail.
			k := 0.5
			lambda := 10e6
			vals := make([]float64, n)
			for i := range vals {
				u := rng.Float64()
				vals[i] = benchLo + lambda*math.Pow(-math.Log(1-u), 1.0/k)
			}
			return clampSort(vals)
		},
	},
	{
		name: "point-mass-with-tail",
		genFn: func(rng *rand.Rand, n int) []float64 {
			// 90% at exactly ~1ms, 10% log-uniform across range.
			logLo := math.Log(benchLo)
			logHi := math.Log(benchHi)
			vals := make([]float64, n)
			for i := range vals {
				if rng.Float64() < 0.9 {
					vals[i] = 1e6 // 1ms
				} else {
					vals[i] = math.Exp(rng.Float64()*(logHi-logLo) + logLo)
				}
			}
			return clampSort(vals)
		},
	},
}

var quantiles = []float64{0.50, 0.75, 0.90, 0.95, 0.99, 0.999}

// --------------------------------------------------------------------------
// 1. Memory Benchmarks
// --------------------------------------------------------------------------

func BenchmarkMemory(b *testing.B) {
	b.Run("goodhistogram", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		hists := make([]*Histogram, b.N)
		for i := 0; i < b.N; i++ {
			hists[i] = newGoodHist()
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N), "bytes/hist")
		_ = hists
	})

	b.Run("prometheus", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		hists := make([]prometheus.Histogram, b.N)
		for i := 0; i < b.N; i++ {
			hists[i] = newPromNativeHist()
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N), "bytes/hist")
		_ = hists
	})
}

func BenchmarkMemoryPopulated(b *testing.B) {
	const nObs = 100_000
	b.Run("goodhistogram", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		hists := make([]*Histogram, b.N)
		for i := 0; i < b.N; i++ {
			h := newGoodHist()
			rng := rand.New(rand.NewSource(int64(i)))
			for j := 0; j < nObs; j++ {
				h.Record(int64(benchLo + rng.Float64()*benchRange))
			}
			hists[i] = h
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N), "bytes/hist")
		_ = hists
	})

	b.Run("prometheus", func(b *testing.B) {
		b.ReportAllocs()
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		hists := make([]prometheus.Histogram, b.N)
		for i := 0; i < b.N; i++ {
			h := newPromNativeHist()
			rng := rand.New(rand.NewSource(int64(i)))
			for j := 0; j < nObs; j++ {
				h.Observe(benchLo + rng.Float64()*benchRange)
			}
			hists[i] = h
		}

		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(after.TotalAlloc-before.TotalAlloc)/float64(b.N), "bytes/hist")
		_ = hists
	})
}

// --------------------------------------------------------------------------
// 2. Speed Benchmarks — Single-Threaded
//
// Values are pre-generated to isolate histogram recording cost from RNG cost.
// --------------------------------------------------------------------------

func BenchmarkRecordSingleThread(b *testing.B) {
	vals64 := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	valsF := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)

	b.Run("goodhistogram", func(b *testing.B) {
		h := newGoodHist()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Record(vals64[i%len(vals64)])
		}
	})
	b.Run("prometheus", func(b *testing.B) {
		h := newPromNativeHist()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Observe(valsF[i%len(valsF)])
		}
	})
}

// --------------------------------------------------------------------------
// 3. Speed Benchmarks — High Contention (50+ goroutines)
// --------------------------------------------------------------------------

func BenchmarkRecordContention(b *testing.B) {
	vals64 := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	valsF := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)

	for _, numGoroutines := range []int{50, 100} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			b.Run("goodhistogram", func(b *testing.B) {
				h := newGoodHist()
				b.SetParallelism(numGoroutines)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						h.Record(vals64[i%len(vals64)])
						i++
					}
				})
			})
			b.Run("prometheus", func(b *testing.B) {
				h := newPromNativeHist()
				b.SetParallelism(numGoroutines)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						h.Observe(valsF[i%len(valsF)])
						i++
					}
				})
			})
		})
	}
}

// --------------------------------------------------------------------------
// 4. Accuracy Benchmarks — Quantile Estimation
// --------------------------------------------------------------------------

func BenchmarkAccuracy(b *testing.B) {
	const nObs = 500_000

	for _, dist := range distributions {
		b.Run(fmt.Sprintf("dist=%s", dist.name), func(b *testing.B) {
			rng := rand.New(rand.NewSource(42))
			sortedVals := dist.genFn(rng, nObs)

			// Empirical exact quantiles from sorted data.
			trueQ := make(map[float64]float64)
			for _, q := range quantiles {
				idx := int(q * float64(nObs))
				if idx >= nObs {
					idx = nObs - 1
				}
				trueQ[q] = sortedVals[idx]
			}

			b.Run("goodhistogram", func(b *testing.B) {
				for iter := 0; iter < b.N; iter++ {
					h := newGoodHist()
					for _, v := range sortedVals {
						h.Record(int64(v))
					}
					snap := h.Snapshot()

					for _, q := range quantiles {
						got := snap.ValueAtQuantile(q)
						expected := trueQ[q]
						relErr := math.Abs(got-expected) / expected
						b.ReportMetric(relErr, fmt.Sprintf("relErr-p%.1f", q*100))
					}
				}
			})

			b.Run("prometheus", func(b *testing.B) {
				for iter := 0; iter < b.N; iter++ {
					ph := newPromNativeHist()
					for _, v := range sortedVals {
						ph.Observe(v)
					}
					hist := collectPromHist(ph)

					for _, q := range quantiles {
						got := nativeHistogramQuantile(hist, q)
						expected := trueQ[q]
						relErr := math.Abs(got-expected) / expected
						b.ReportMetric(relErr, fmt.Sprintf("relErr-p%.1f", q*100))
					}
				}
			})
		})
	}
}

// --------------------------------------------------------------------------
// 5. Snapshot + Export Speed
// --------------------------------------------------------------------------

func BenchmarkSnapshot(b *testing.B) {
	b.Run("goodhistogram", func(b *testing.B) {
		h := newGoodHist()
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 100_000; i++ {
			h.Record(int64(benchLo + rng.Float64()*benchRange))
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			snap := h.Snapshot()
			_ = snap.ToPrometheusHistogram()
		}
	})
	b.Run("prometheus", func(b *testing.B) {
		h := newPromNativeHist()
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < 100_000; i++ {
			h.Observe(benchLo + rng.Float64()*benchRange)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = collectPromHist(h)
		}
	})
}

// --------------------------------------------------------------------------
// 6. Full comparison report (non-benchmark) for human-readable output
// --------------------------------------------------------------------------

// signedRelError returns (got - expected) / expected, preserving sign.
func signedRelError(got, expected float64) float64 {
	if expected == 0 {
		return 0
	}
	return (got - expected) / expected
}

// fmtDuration formats a nanosecond value as a human-readable duration.
func fmtDuration(ns float64) string {
	switch {
	case ns >= 1e9:
		return fmt.Sprintf("%.2fs", ns/1e9)
	case ns >= 1e6:
		return fmt.Sprintf("%.2fms", ns/1e6)
	case ns >= 1e3:
		return fmt.Sprintf("%.2fµs", ns/1e3)
	default:
		return fmt.Sprintf("%.0fns", ns)
	}
}

func TestFullComparisonReport(t *testing.T) {
	const n = 100_000

	var out strings.Builder

	// --- Header ---
	out.WriteString("Histogram Implementation Comparison Report\n")
	out.WriteString("==========================================\n\n")
	out.WriteString(fmt.Sprintf("Range: [%s, %s], Schema: %d, Error bound: %.2f%%\n",
		fmtDuration(benchLo), fmtDuration(benchHi), benchSchema, benchErrBound*100))
	out.WriteString(fmt.Sprintf("Sample count: %d per distribution\n\n", n))

	// --- Section 1: Memory ---
	ghCfg := newConfig(benchLo, benchHi, benchErrBound)

	// Count Prometheus native buckets after populating.
	phn := newPromNativeHist()
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		phn.Observe(math.Exp(rng.Float64()*(math.Log(benchHi)-math.Log(benchLo)) + math.Log(benchLo)))
	}
	phnMetric := &prometheusgo.Metric{}
	_ = phn.Write(phnMetric)
	var promNativeBucketCount int
	for _, span := range phnMetric.Histogram.GetPositiveSpan() {
		promNativeBucketCount += int(span.GetLength())
	}

	ghMemBytes := ghCfg.numBuckets * 8
	// Prometheus Native: sparse map entries (~85 bytes each, 2 copies) + conventional array.
	phMemBytes := promNativeBucketCount*85*2 + promBucketCount*8*2

	out.WriteString("=== SECTION 1: Bucket Count and Memory ===\n")
	out.WriteString(strings.Repeat("-", 50) + "\n")
	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s\n", "Implementation", "Buckets", "Memory"))
	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s\n", "----------------------", "--------", "--------"))
	out.WriteString(fmt.Sprintf("%-22s  %8d  %7.2fKB\n",
		"GoodHistogram", ghCfg.numBuckets, float64(ghMemBytes)/1024.0))
	out.WriteString(fmt.Sprintf("%-22s  %8d  %7.2fKB\n",
		"Prometheus Native", promNativeBucketCount, float64(phMemBytes)/1024.0))
	out.WriteString("\n")

	// --- Section 2: Speed ---
	vals64 := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	valsF := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)

	bench := func(f func(b *testing.B)) float64 {
		const runs = 5
		results := make([]float64, runs)
		for i := range results {
			r := testing.Benchmark(f)
			results[i] = float64(r.T.Nanoseconds()) / float64(r.N)
		}
		sort.Float64s(results)
		return results[runs/2] // median
	}

	ghSingle := bench(func(b *testing.B) {
		h := newGoodHist()
		for i := 0; i < b.N; i++ {
			h.Record(vals64[i%len(vals64)])
		}
	})
	phSingle := bench(func(b *testing.B) {
		h := newPromNativeHist()
		for i := 0; i < b.N; i++ {
			h.Observe(valsF[i%len(valsF)])
		}
	})
	ghParallel := bench(func(b *testing.B) {
		h := newGoodHist()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Record(vals64[i%len(vals64)])
				i++
			}
		})
	})
	phParallel := bench(func(b *testing.B) {
		h := newPromNativeHist()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				h.Observe(valsF[i%len(valsF)])
				i++
			}
		})
	})

	out.WriteString("=== SECTION 2: Recording Speed ===\n")
	out.WriteString("(median of 5 runs, pre-generated values, no RNG in hot loop)\n\n")
	out.WriteString(fmt.Sprintf("%-22s  %10s  %10s\n", "Implementation", "1-thread", "parallel"))
	out.WriteString(fmt.Sprintf("%-22s  %10s  %10s\n", "----------------------", "----------", "----------"))
	out.WriteString(fmt.Sprintf("%-22s  %8.1fns  %8.1fns\n", "GoodHistogram", ghSingle, ghParallel))
	out.WriteString(fmt.Sprintf("%-22s  %8.1fns  %8.1fns\n", "Prometheus Native", phSingle, phParallel))
	out.WriteString("\n")

	// --- Section 3: Overall Summary ---
	out.WriteString("=== SECTION 3: Overall Summary ===\n")
	out.WriteString(strings.Repeat("-", 95) + "\n")
	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s  %8s  %8s\n",
		"Implementation", "Buckets", "Memory", "1-thread", "parallel"))
	out.WriteString(fmt.Sprintf("%-22s  %8s  %8s  %8s  %8s\n",
		"----------------------", "--------", "--------", "--------", "--------"))
	out.WriteString(fmt.Sprintf("%-22s  %8d  %7.2fKB  %6.1fns  %6.1fns\n",
		"GoodHistogram", ghCfg.numBuckets, float64(ghMemBytes)/1024.0, ghSingle, ghParallel))
	out.WriteString(fmt.Sprintf("%-22s  %8d  %7.2fKB  %6.1fns  %6.1fns\n",
		"Prometheus Native", promNativeBucketCount, float64(phMemBytes)/1024.0, phSingle, phParallel))
	out.WriteString("\n")

	// --- Section 4: Accuracy ---
	out.WriteString("=== SECTION 4: Quantile Accuracy — Per Distribution ===\n")
	out.WriteString("(signed relative error: +overestimate / -underestimate)\n\n")

	var ghErrSum, phErrSum float64
	var errCount int

	for _, dist := range distributions {
		rng := rand.New(rand.NewSource(42))
		sortedVals := dist.genFn(rng, n)

		trueQ := make(map[float64]float64)
		for _, q := range quantiles {
			idx := int(q * float64(n))
			if idx >= n {
				idx = n - 1
			}
			trueQ[q] = sortedVals[idx]
		}

		gh := newGoodHist()
		ph := newPromNativeHist()
		for _, v := range sortedVals {
			gh.Record(int64(v))
			ph.Observe(v)
		}
		ghSnap := gh.Snapshot()
		phHist := collectPromHist(ph)

		out.WriteString(fmt.Sprintf("Distribution: %s\n", dist.name))
		out.WriteString(fmt.Sprintf("%-6s  %12s  %10s  %10s\n", "Quant", "True", "GoodHist", "PromNat"))
		out.WriteString(fmt.Sprintf("%-6s  %12s  %10s  %10s\n", "------", "------------", "----------", "----------"))

		for _, q := range quantiles {
			tv := trueQ[q]
			ghVal := ghSnap.ValueAtQuantile(q)
			phVal := nativeHistogramQuantile(phHist, q)
			ghErr := signedRelError(ghVal, tv)
			phErr := signedRelError(phVal, tv)

			ghErrSum += math.Abs(ghErr)
			phErrSum += math.Abs(phErr)
			errCount++

			out.WriteString(fmt.Sprintf("p%-5.1f  %12s  %+9.2f%%  %+9.2f%%\n",
				q*100, fmtDuration(tv), ghErr*100, phErr*100))
		}
		out.WriteString("\n")
	}

	// --- Section 5: Mean accuracy ---
	out.WriteString("=== SECTION 5: Mean |Relative Error| Across All Distributions ===\n")
	out.WriteString(strings.Repeat("-", 50) + "\n")
	out.WriteString(fmt.Sprintf("%-22s  %10s\n", "Implementation", "Mean |Err|"))
	out.WriteString(fmt.Sprintf("%-22s  %10s\n", "----------------------", "----------"))
	out.WriteString(fmt.Sprintf("%-22s  %9.2f%%\n", "GoodHistogram", ghErrSum/float64(errCount)*100))
	out.WriteString(fmt.Sprintf("%-22s  %9.2f%%\n", "Prometheus Native", phErrSum/float64(errCount)*100))
	out.WriteString("\n")

	t.Log("\n" + out.String())
}

// --------------------------------------------------------------------------
// 7. Contention scaling test (non-benchmark) for human-readable output
// --------------------------------------------------------------------------

func TestContentionScaling(t *testing.T) {
	goroutineCounts := []int{1, 10, 50, 100}

	vals64 := makeInt64Values(rand.New(rand.NewSource(42)), 10000)
	valsF := makeFloat64Values(rand.New(rand.NewSource(42)), 10000)

	t.Logf("%-12s %-18s %-18s %-10s", "goroutines", "goodhist-ns/op", "prom-ns/op", "speedup")

	for _, numG := range goroutineCounts {
		ghResult := testing.Benchmark(func(b *testing.B) {
			h := newGoodHist()
			b.SetParallelism(numG)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					h.Record(vals64[i%len(vals64)])
					i++
				}
			})
		})

		phResult := testing.Benchmark(func(b *testing.B) {
			h := newPromNativeHist()
			b.SetParallelism(numG)
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					h.Observe(valsF[i%len(valsF)])
					i++
				}
			})
		})

		ghNs := float64(ghResult.T.Nanoseconds()) / float64(ghResult.N)
		phNs := float64(phResult.T.Nanoseconds()) / float64(phResult.N)

		t.Logf("%-12d %-18.1f %-18.1f %-10.2fx", numG, ghNs, phNs, phNs/ghNs)
	}
}

// --------------------------------------------------------------------------
// Helper: verify both histograms agree on total count under contention
// --------------------------------------------------------------------------

func TestConcurrentCorrectness(t *testing.T) {
	const numGoroutines = 64
	const opsPerGoroutine = 10_000

	gh := newGoodHist()
	ph := newPromNativeHist()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < opsPerGoroutine; i++ {
				v := benchLo + rng.Float64()*benchRange
				gh.Record(int64(v))
				ph.Observe(v)
			}
		}(int64(g))
	}
	wg.Wait()

	ghSnap := gh.Snapshot()
	phHist := collectPromHist(ph)

	expectedCount := uint64(numGoroutines * opsPerGoroutine)
	if ghSnap.TotalCount != expectedCount {
		t.Errorf("goodhistogram count: got %d, want %d", ghSnap.TotalCount, expectedCount)
	}
	if phHist.GetSampleCount() != expectedCount {
		t.Errorf("prometheus count: got %d, want %d", phHist.GetSampleCount(), expectedCount)
	}
}
