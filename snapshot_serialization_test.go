// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goodhistogram

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotRejectedLayoutDoesNotCacheConfig(t *testing.T) {
	for i := uint64(1); i <= 3; i++ {
		s := Snapshot{
			PrometheusSchema: maxSchema,
			LowestTrackable:  math.Float64frombits(i),
			HighestTrackable: 1.797e308,
			Counts:           []uint64{1},
			TotalCount:       1,
		}
		params := Params{Lo: s.LowestTrackable, Hi: s.HighestTrackable,
			ErrorBound: schemaRelativeError(s.PrometheusSchema)}
		_, cached := configCache.Load(params)
		require.False(t, cached)
		t.Cleanup(func() { configCache.Delete(params) })

		require.ErrorContains(t, s.Validate(), "counts, expected")
		_, cached = configCache.Load(params)
		require.False(t, cached, "rejected layouts must not populate the config cache")

		encoded, err := json.Marshal(s)
		require.NoError(t, err)
		var decoded Snapshot
		require.ErrorContains(t, json.Unmarshal(encoded, &decoded), "counts, expected")
		_, cached = configCache.Load(params)
		require.False(t, cached, "rejected JSON must not populate the config cache")
	}
}

func TestSnapshotLayoutSchemas(t *testing.T) {
	for schema := int32(0); schema <= maxSchema; schema++ {
		for _, bounds := range [][2]float64{{1, 1024}, {1.1, 99.9}, {10, math.Nextafter(10, 11)}} {
			t.Run(fmt.Sprintf("schema=%d/bounds=%v", schema, bounds), func(t *testing.T) {
				h := New(Params{Lo: bounds[0], Hi: bounds[1], ErrorBound: schemaRelativeError(schema)})
				s := h.Snapshot()
				require.Equal(t, schema, s.Schema())
				require.NoError(t, s.Validate())
				encoded, err := json.Marshal(s)
				require.NoError(t, err)
				var decoded Snapshot
				require.NoError(t, json.Unmarshal(encoded, &decoded))
				require.Equal(t, s, decoded)
			})
		}
	}
}

func TestSnapshotSerialization(t *testing.T) {
	h := New(Params{Lo: 10, Hi: 10_000, ErrorBound: 0.2})
	for _, value := range []int64{-1, 0, 5, 10, 25, 100, 1_000, 10_000, 20_000} {
		h.Record(value)
	}
	original := h.Snapshot()

	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	var decoded Snapshot
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, original, decoded)
	require.Equal(t, original.ValuesAtQuantiles([]float64{0, 0.5, 0.9, 1}),
		decoded.ValuesAtQuantiles([]float64{0, 0.5, 0.9, 1}))
	require.Equal(t, original.ToPrometheusHistogram(), decoded.ToPrometheusHistogram())

	restored := histogramFromSnapshotForTest(t, decoded)
	require.Equal(t, decoded, restored.Snapshot())
	for _, value := range []int64{-2, 0, 9, 50, 20_001} {
		h.Record(value)
		restored.Record(value)
	}
	require.Equal(t, h.Snapshot(), restored.Snapshot())
}

func TestSnapshotValidation(t *testing.T) {
	h := New(Params{Lo: 10, Hi: 100, ErrorBound: 0.5})
	h.Record(50)
	valid := h.Snapshot()
	require.NoError(t, valid.Validate())

	testCases := []struct {
		name   string
		mutate func(*Snapshot)
		err    string
	}{
		{"negative schema", func(s *Snapshot) { s.PrometheusSchema = -1 }, "invalid snapshot schema"},
		{"large schema", func(s *Snapshot) { s.PrometheusSchema = maxSchema + 1 }, "invalid snapshot schema"},
		{"NaN lower bound", func(s *Snapshot) { s.LowestTrackable = math.NaN() }, "invalid snapshot bounds"},
		{"infinite upper bound", func(s *Snapshot) { s.HighestTrackable = math.Inf(1) }, "invalid snapshot bounds"},
		{"zero lower bound", func(s *Snapshot) { s.LowestTrackable = 0 }, "invalid snapshot bounds"},
		{"unordered bounds", func(s *Snapshot) { s.HighestTrackable = s.LowestTrackable }, "invalid snapshot bounds"},
		{"short counts", func(s *Snapshot) { s.Counts = s.Counts[:len(s.Counts)-1] }, "counts, expected"},
		{"long counts", func(s *Snapshot) { s.Counts = append(s.Counts, 0) }, "counts, expected"},
		{"incorrect total", func(s *Snapshot) { s.TotalCount++ }, "does not match component count"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid
			s.Counts = append([]uint64(nil), valid.Counts...)
			tc.mutate(&s)
			require.ErrorContains(t, s.Validate(), tc.err)
		})
	}
}

func TestSnapshotUnmarshalRejectsInvalid(t *testing.T) {
	h := New(Params{Lo: 10, Hi: 100, ErrorBound: 0.5})
	h.Record(50)
	valid := h.Snapshot()

	testCases := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{"schema", func(s *Snapshot) { s.PrometheusSchema = maxSchema + 1 }},
		{"bounds", func(s *Snapshot) { s.LowestTrackable = 0 }},
		{"counts", func(s *Snapshot) { s.Counts = s.Counts[:len(s.Counts)-1] }},
		{"total", func(s *Snapshot) { s.TotalCount++ }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid
			s.Counts = append([]uint64(nil), valid.Counts...)
			tc.mutate(&s)
			encoded, err := json.Marshal(s)
			require.NoError(t, err)
			var decoded Snapshot
			require.Error(t, json.Unmarshal(encoded, &decoded))
		})
	}
}

func TestSnapshotZeroValue(t *testing.T) {
	var zero Snapshot
	t.Run("round trip", func(t *testing.T) {
		type envelope struct {
			Snapshot Snapshot
			Exact    ExactSnapshot
		}
		original := envelope{}
		encoded, err := json.Marshal(original)
		require.NoError(t, err)
		var decoded envelope
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Equal(t, original, decoded)
	})
	t.Run("consumers", func(t *testing.T) {
		require.NoError(t, zero.Validate())
		require.Zero(t, zero.ValueAtQuantile(0.5))
		require.Equal(t, []float64{0, 0, 0}, zero.ValuesAtQuantiles([]float64{0, 0.5, 1}))
		require.True(t, math.IsNaN(zero.Mean()))
		count, sum := zero.Total()
		require.Zero(t, count)
		require.Zero(t, sum)
		require.Zero(t, zero.Schema())
		for _, s := range []Snapshot{zero.Merge(&zero), zero.Sub(&zero)} {
			require.NoError(t, s.Validate())
		}
		exported := zero.ToPrometheusHistogram()
		require.Zero(t, exported.GetSampleCount())
		require.Zero(t, exported.GetSampleSum())
		require.Empty(t, exported.Bucket)
		require.Nil(t, exported.Schema, "an unset snapshot has no native bucket layout")
	})
	t.Run("empty counts", func(t *testing.T) {
		var decoded Snapshot
		require.NoError(t, json.Unmarshal([]byte(`{"Counts":[]}`), &decoded))
		require.NoError(t, decoded.Validate())
		require.Equal(t, zero.ToPrometheusHistogram(), decoded.ToPrometheusHistogram())
	})
	t.Run("partial snapshots remain invalid", func(t *testing.T) {
		for _, payload := range []string{
			`{"PrometheusSchema":1}`, `{"LowestTrackable":1}`, `{"HighestTrackable":100}`,
			`{"Counts":[0]}`, `{"ZeroCount":1}`, `{"Underflow":1}`, `{"Overflow":1}`,
			`{"TotalCount":1}`, `{"TotalSum":1}`,
		} {
			t.Run(payload, func(t *testing.T) {
				var decoded Snapshot
				require.Error(t, json.Unmarshal([]byte(payload), &decoded))
			})
		}
	})
}

func TestSnapshotUnmarshalNull(t *testing.T) {
	h := New(Params{Lo: 10, Hi: 100, ErrorBound: 0.5})
	h.Record(50)
	for _, original := range []Snapshot{{}, h.Snapshot()} {
		decoded := original
		require.NoError(t, json.Unmarshal([]byte(" \nnull\t"), &decoded))
		require.Equal(t, original, decoded)
		// The method also accepts whitespace when called directly.
		require.NoError(t, decoded.UnmarshalJSON([]byte(" \nnull\t")))
		require.Equal(t, original, decoded)
	}
	for _, payload := range []string{`{"Min":1,"Max":2}`, `{"Snapshot":null,"Min":1,"Max":2}`} {
		var decoded ExactSnapshot
		require.NoError(t, json.Unmarshal([]byte(payload), &decoded))
		require.Equal(t, ExactSnapshot{Min: 1, Max: 2}, decoded)
	}
}

// histogramFromSnapshotForTest deliberately initializes every mutable field
// in Histogram. This makes the test fail if Snapshot stops carrying enough
// state to restore a histogram and continue recording observations.
func histogramFromSnapshotForTest(t *testing.T, s Snapshot) *Histogram {
	t.Helper()
	h := New(Params{
		Lo:         s.LowestTrackable,
		Hi:         s.HighestTrackable,
		ErrorBound: schemaRelativeError(s.PrometheusSchema),
	})
	require.Len(t, s.Counts, len(h.counts))
	for i, count := range s.Counts {
		h.counts[i].Store(count)
	}
	h.ZeroCount.Store(s.ZeroCount)
	h.Underflow.Store(s.Underflow)
	h.Overflow.Store(s.Overflow)
	h.sum.Store(s.TotalSum)
	require.Equal(t, s.TotalCount, h.Snapshot().TotalCount)
	return h
}
