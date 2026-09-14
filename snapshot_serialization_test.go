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
	"testing"

	"github.com/stretchr/testify/require"
)

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
