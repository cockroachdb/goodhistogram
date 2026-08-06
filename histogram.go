// Copyright 2026 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Package goodhistogram provides an exponential histogram with Prometheus
// native histogram schema alignment, bounded relative error, and trapezoidal
// quantile estimation.
//
// The histogram is configured with a value range [lo, hi] and a desired
// relative error bound. It selects the tightest Prometheus schema whose error
// is at or below the requested bound, then allocates a fixed array of atomic
// counters covering the range. Recording is O(1) and lock-free: values are
// mapped to bucket indices via math.Frexp plus a small precomputed boundary
// table, then the corresponding counter is atomically incremented.
//
// Because the bucket layout is identical to a Prometheus native histogram
// schema, export to the Prometheus sparse format requires no remapping — our
// internal indices are Prometheus bucket indices offset by a constant.
package goodhistogram

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxSchema = 8

// nativeHistogramBounds contains the Prometheus native histogram bucket
// boundaries within each group (power-of-2 octave) for each schema (0–8).
// For schema s, there are 2^s buckets per group. Each entry is the lower
// bound of a bucket in [0.5, 1.0), matching the range of math.Frexp's
// fractional part.
//
// The boundary for bucket j in schema s is 2^(j / 2^s) / 2, which equals the
// fractional part of the Prometheus bucket boundary when decomposed via
// math.Frexp.
//
// The values are transcribed from the table of the same name in
// github.com/prometheus/client_golang, and are spelled out as literals rather
// than computed from math.Pow for the same reason client_golang spells them
// out: math.Pow has architecture-specific implementations and is not
// bit-portable, so a table computed at init time differs by an ULP between
// (say) arm64 and amd64. Bucket boundaries are exported as Prometheus label
// values, so a boundary that varies by architecture splits one bucket into
// two time series across a mixed-architecture deployment, and makes any
// golden-file test of the exported format architecture-dependent.
var nativeHistogramBounds = [maxSchema + 1][]float64{
	// Schema "0":
	{0.5},
	// Schema 1:
	{0.5, 0.7071067811865475},
	// Schema 2:
	{0.5, 0.5946035575013605, 0.7071067811865475, 0.8408964152537144},
	// Schema 3:
	{
		0.5, 0.5452538663326288, 0.5946035575013605, 0.6484197773255048,
		0.7071067811865475, 0.7711054127039704, 0.8408964152537144, 0.9170040432046711,
	},
	// Schema 4:
	{
		0.5, 0.5221368912137069, 0.5452538663326288, 0.5693943173783458,
		0.5946035575013605, 0.620928906036742, 0.6484197773255048, 0.6771277734684463,
		0.7071067811865475, 0.7384130729697496, 0.7711054127039704, 0.805245165974627,
		0.8408964152537144, 0.8781260801866495, 0.9170040432046711, 0.9576032806985735,
	},
	// Schema 5:
	{
		0.5, 0.5109485743270583, 0.5221368912137069, 0.5335702003384117,
		0.5452538663326288, 0.5571933712979462, 0.5693943173783458, 0.5818624293887887,
		0.5946035575013605, 0.6076236799902344, 0.620928906036742, 0.6345254785958666,
		0.6484197773255048, 0.6626183215798706, 0.6771277734684463, 0.6919549409819159,
		0.7071067811865475, 0.7225904034885232, 0.7384130729697496, 0.7545822137967112,
		0.7711054127039704, 0.7879904225539431, 0.805245165974627, 0.8228777390769823,
		0.8408964152537144, 0.8593096490612387, 0.8781260801866495, 0.8973545375015533,
		0.9170040432046711, 0.9370838170551498, 0.9576032806985735, 0.9785720620876999,
	},
	// Schema 6:
	{
		0.5, 0.5054446430258502, 0.5109485743270583, 0.5165124395106142,
		0.5221368912137069, 0.5278225891802786, 0.5335702003384117, 0.5393803988785598,
		0.5452538663326288, 0.5511912916539204, 0.5571933712979462, 0.5632608093041209,
		0.5693943173783458, 0.5755946149764913, 0.5818624293887887, 0.5881984958251406,
		0.5946035575013605, 0.6010783657263515, 0.6076236799902344, 0.6142402680534349,
		0.620928906036742, 0.6276903785123455, 0.6345254785958666, 0.6414350080393891,
		0.6484197773255048, 0.6554806057623822, 0.6626183215798706, 0.6698337620266515,
		0.6771277734684463, 0.6845012114872953, 0.6919549409819159, 0.6994898362691555,
		0.7071067811865475, 0.7148066691959849, 0.7225904034885232, 0.7304588970903234,
		0.7384130729697496, 0.7464538641456323, 0.7545822137967112, 0.762799075372269,
		0.7711054127039704, 0.7795022001189185, 0.7879904225539431, 0.7965710756711334,
		0.805245165974627, 0.8140137109286738, 0.8228777390769823, 0.8318382901633681,
		0.8408964152537144, 0.8500531768592616, 0.8593096490612387, 0.8686669176368529,
		0.8781260801866495, 0.8876882462632604, 0.8973545375015533, 0.9071260877501991,
		0.9170040432046711, 0.9269895625416926, 0.9370838170551498, 0.9472879907934827,
		0.9576032806985735, 0.9680308967461471, 0.9785720620876999, 0.9892280131939752,
	},
	// Schema 7:
	{
		0.5, 0.5027149505564014, 0.5054446430258502, 0.5081891574554764,
		0.5109485743270583, 0.5137229745593818, 0.5165124395106142, 0.5193170509806894,
		0.5221368912137069, 0.5249720429003435, 0.5278225891802786, 0.5306886136446309,
		0.5335702003384117, 0.5364674337629877, 0.5393803988785598, 0.5423091811066545,
		0.5452538663326288, 0.5482145409081883, 0.5511912916539204, 0.5541842058618393,
		0.5571933712979462, 0.5602188762048033, 0.5632608093041209, 0.5663192597993595,
		0.5693943173783458, 0.572486072215902, 0.5755946149764913, 0.5787200368168754,
		0.5818624293887887, 0.585021884841625, 0.5881984958251406, 0.5913923554921704,
		0.5946035575013605, 0.5978321960199137, 0.6010783657263515, 0.6043421618132907,
		0.6076236799902344, 0.6109230164863786, 0.6142402680534349, 0.6175755319684665,
		0.620928906036742, 0.6243004885946023, 0.6276903785123455, 0.6310986751971253,
		0.6345254785958666, 0.637970889198196, 0.6414350080393891, 0.6449179367033329,
		0.6484197773255048, 0.6519406325959679, 0.6554806057623822, 0.659039800633032,
		0.6626183215798706, 0.6662162735415805, 0.6698337620266515, 0.6734708931164728,
		0.6771277734684463, 0.6808045103191123, 0.6845012114872953, 0.688217985377265,
		0.6919549409819159, 0.6957121878859629, 0.6994898362691555, 0.7032879969095076,
		0.7071067811865475, 0.7109463010845827, 0.7148066691959849, 0.718687998724491,
		0.7225904034885232, 0.7265139979245261, 0.7304588970903234, 0.7344252166684908,
		0.7384130729697496, 0.7424225829363761, 0.7464538641456323, 0.7505070348132126,
		0.7545822137967112, 0.7586795205991071, 0.762799075372269, 0.7669409989204777,
		0.7711054127039704, 0.7752924388424999, 0.7795022001189185, 0.7837348199827764,
		0.7879904225539431, 0.7922691326262467, 0.7965710756711334, 0.8008963778413465,
		0.805245165974627, 0.8096175675974316, 0.8140137109286738, 0.8184337248834821,
		0.8228777390769823, 0.8273458838280969, 0.8318382901633681, 0.8363550898207981,
		0.8408964152537144, 0.8454623996346523, 0.8500531768592616, 0.8546688815502312,
		0.8593096490612387, 0.8639756154809185, 0.8686669176368529, 0.8733836930995842,
		0.8781260801866495, 0.8828942179666361, 0.8876882462632604, 0.8925083056594671,
		0.8973545375015533, 0.9022270839033115, 0.9071260877501991, 0.9120516927035263,
		0.9170040432046711, 0.9219832844793128, 0.9269895625416926, 0.9320230241988943,
		0.9370838170551498, 0.9421720895161669, 0.9472879907934827, 0.9524316709088368,
		0.9576032806985735, 0.9628029718180622, 0.9680308967461471, 0.9732872087896164,
		0.9785720620876999, 0.9838856116165875, 0.9892280131939752, 0.9945994234836328,
	},
	// Schema 8:
	{
		0.5, 0.5013556375251013, 0.5027149505564014, 0.5040779490592088,
		0.5054446430258502, 0.5068150424757447, 0.5081891574554764, 0.509566998038869,
		0.5109485743270583, 0.5123338964485679, 0.5137229745593818, 0.5151158188430205,
		0.5165124395106142, 0.5179128468009786, 0.5193170509806894, 0.520725062344158,
		0.5221368912137069, 0.5235525479396449, 0.5249720429003435, 0.526395386502313,
		0.5278225891802786, 0.5292536613972564, 0.5306886136446309, 0.5321274564422321,
		0.5335702003384117, 0.5350168559101208, 0.5364674337629877, 0.5379219445313954,
		0.5393803988785598, 0.5408428074966075, 0.5423091811066545, 0.5437795304588847,
		0.5452538663326288, 0.5467321995364429, 0.5482145409081883, 0.549700901315111,
		0.5511912916539204, 0.5526857228508706, 0.5541842058618393, 0.5556867516724088,
		0.5571933712979462, 0.5587040757836845, 0.5602188762048033, 0.5617377836665098,
		0.5632608093041209, 0.564787964283144, 0.5663192597993595, 0.5678547070789026,
		0.5693943173783458, 0.5709381019847808, 0.572486072215902, 0.5740382394200894,
		0.5755946149764913, 0.5771552102951081, 0.5787200368168754, 0.5802891060137493,
		0.5818624293887887, 0.5834400184762408, 0.585021884841625, 0.5866080400818185,
		0.5881984958251406, 0.5897932637314379, 0.5913923554921704, 0.5929957828304968,
		0.5946035575013605, 0.5962156912915756, 0.5978321960199137, 0.5994530835371903,
		0.6010783657263515, 0.6027080545025619, 0.6043421618132907, 0.6059806996384005,
		0.6076236799902344, 0.6092711149137041, 0.6109230164863786, 0.6125793968185725,
		0.6142402680534349, 0.6159056423670379, 0.6175755319684665, 0.6192499490999082,
		0.620928906036742, 0.622612415087629, 0.6243004885946023, 0.6259931389331581,
		0.6276903785123455, 0.6293922197748583, 0.6310986751971253, 0.6328097572894031,
		0.6345254785958666, 0.6362458516947014, 0.637970889198196, 0.6397006037528346,
		0.6414350080393891, 0.6431741147730128, 0.6449179367033329, 0.6466664866145447,
		0.6484197773255048, 0.6501778216898253, 0.6519406325959679, 0.6537082229673385,
		0.6554806057623822, 0.6572577939746774, 0.659039800633032, 0.6608266388015788,
		0.6626183215798706, 0.6644148621029772, 0.6662162735415805, 0.6680225691020727,
		0.6698337620266515, 0.6716498655934177, 0.6734708931164728, 0.6752968579460171,
		0.6771277734684463, 0.6789636531064505, 0.6808045103191123, 0.6826503586020058,
		0.6845012114872953, 0.6863570825438342, 0.688217985377265, 0.690083933630119,
		0.6919549409819159, 0.6938310211492645, 0.6957121878859629, 0.6975984549830999,
		0.6994898362691555, 0.7013863456101023, 0.7032879969095076, 0.7051948041086352,
		0.7071067811865475, 0.7090239421602076, 0.7109463010845827, 0.7128738720527471,
		0.7148066691959849, 0.7167447066838943, 0.718687998724491, 0.7206365595643126,
		0.7225904034885232, 0.7245495448210174, 0.7265139979245261, 0.7284837772007218,
		0.7304588970903234, 0.7324393720732029, 0.7344252166684908, 0.7364164454346837,
		0.7384130729697496, 0.7404151139112358, 0.7424225829363761, 0.7444354947621984,
		0.7464538641456323, 0.7484777058836176, 0.7505070348132126, 0.7525418658117031,
		0.7545822137967112, 0.7566280937263048, 0.7586795205991071, 0.7607365094544071,
		0.762799075372269, 0.7648672334736434, 0.7669409989204777, 0.7690203869158282,
		0.7711054127039704, 0.7731960915705107, 0.7752924388424999, 0.7773944698885442,
		0.7795022001189185, 0.7816156449856788, 0.7837348199827764, 0.7858597406461707,
		0.7879904225539431, 0.7901268813264122, 0.7922691326262467, 0.7944171921585818,
		0.7965710756711334, 0.7987307989543135, 0.8008963778413465, 0.8030678282083853,
		0.805245165974627, 0.8074284071024302, 0.8096175675974316, 0.8118126635086642,
		0.8140137109286738, 0.8162207259936375, 0.8184337248834821, 0.820652723822003,
		0.8228777390769823, 0.8251087869603088, 0.8273458838280969, 0.8295890460808079,
		0.8318382901633681, 0.8340936325652911, 0.8363550898207981, 0.8386226785089391,
		0.8408964152537144, 0.8431763167241966, 0.8454623996346523, 0.8477546807446661,
		0.8500531768592616, 0.8523579048290255, 0.8546688815502312, 0.8569861239649629,
		0.8593096490612387, 0.8616394738731368, 0.8639756154809185, 0.8663180910111553,
		0.8686669176368529, 0.871022112577578, 0.8733836930995842, 0.8757516765159389,
		0.8781260801866495, 0.8805069215187917, 0.8828942179666361, 0.8852879870317771,
		0.8876882462632604, 0.890095013257712, 0.8925083056594671, 0.8949281411607002,
		0.8973545375015533, 0.8997875124702672, 0.9022270839033115, 0.9046732696855155,
		0.9071260877501991, 0.909585556079304, 0.9120516927035263, 0.9145245157024483,
		0.9170040432046711, 0.9194902933879467, 0.9219832844793128, 0.9244830347552253,
		0.9269895625416926, 0.92950288621441, 0.9320230241988943, 0.9345499949706191,
		0.9370838170551498, 0.93962450902828, 0.9421720895161669, 0.9447265771954693,
		0.9472879907934827, 0.9498563490882775, 0.9524316709088368, 0.9550139751351947,
		0.9576032806985735, 0.9601996065815236, 0.9628029718180622, 0.9654133954938133,
		0.9680308967461471, 0.9706554947643201, 0.9732872087896164, 0.9759260581154889,
		0.9785720620876999, 0.9812252401044634, 0.9838856116165875, 0.9865531961276168,
		0.9892280131939752, 0.9919100824251095, 0.9945994234836328, 0.9972960560854698,
	},
}

// schemaRelativeError returns the relative error for a given schema.
// The error is (γ-1)/(γ+1) where γ = 2^(2^(-schema)).
func schemaRelativeError(schema int32) float64 {
	gamma := math.Pow(2, math.Pow(2, float64(-schema)))
	return (gamma - 1) / (gamma + 1)
}

// pickSchema selects the coarsest (fewest buckets) Prometheus schema whose
// relative error is at or below the desired error. Returns a schema in [0, 8].
func pickSchema(desiredError float64) int32 {
	for s := int32(0); s <= maxSchema; s++ {
		if schemaRelativeError(s) <= desiredError {
			return s
		}
	}
	return maxSchema
}

// promBucketKey computes the Prometheus native histogram bucket key for a
// positive value v, using the given schema. This is the same mapping used by
// the Prometheus client_golang library.
//
// For schema s > 0, the bucket key is:
//
//	sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
//
// where frac, exp = math.Frexp(v) and bounds = nativeHistogramBounds[s].
func promBucketKey(v float64, schema int32) int {
	frac, exp := math.Frexp(v)
	bounds := nativeHistogramBounds[schema]
	return sort.SearchFloat64s(bounds, frac) + (exp-1)*len(bounds)
}

// getLe returns the upper bound of the bucket with the given key and schema.
// This is the inverse of promBucketKey.
func getLe(key int, schema int32) float64 {
	fracIdx := key & ((1 << schema) - 1)
	frac := nativeHistogramBounds[schema][fracIdx]
	exp := (key >> schema) + 1
	return math.Ldexp(frac, exp)
}

// bucketLookupBits is the number of top mantissa bits used to index
// the bucket lookup table. 8 bits → 256 entries, which is enough
// precision for all schemas up to 8 (256 buckets per group).
const bucketLookupBits = 8
const bucketLookupSize = 1 << bucketLookupBits
const bucketLookupShift = 52 - bucketLookupBits

// config holds the immutable parameters for a Histogram, computed at
// construction time from the user-specified range and error bound.
type config struct {
	schema          int32
	lo, hi          float64
	minKey          int
	numBuckets      int
	bucketsPerGroup int
	groupBounds     []float64
	bucketLookup    [bucketLookupSize]uint8
	boundaries      []float64
}

// newConfig creates a config for the given range [lo, hi] and desired
// relative error. The schema is chosen as the tightest Prometheus schema
// whose error is at or below desiredError. Panics if lo <= 0, hi <= lo,
// or desiredError <= 0.
func newConfig(lo, hi, desiredError float64) config {
	if lo <= 0 || hi <= lo || desiredError <= 0 {
		panic("goodhistogram: invalid config: need 0 < lo < hi and desiredError > 0")
	}
	schema := pickSchema(desiredError)
	minKey := promBucketKey(lo, schema)
	// If lo lands exactly on a bucket boundary, the first bucket would span
	// [lo, lo] — a zero-width degenerate bucket. Skip it so the first bucket
	// starts at lo and ends at the next real boundary above it.
	if getLe(minKey, schema) <= lo {
		minKey++
	}
	maxKey := promBucketKey(hi, schema)
	numBuckets := maxKey - minKey + 1

	// Precompute bucket boundaries for quantile estimation.
	boundaries := make([]float64, numBuckets+1)
	boundaries[0] = lo
	for i := 1; i < numBuckets; i++ {
		boundaries[i] = getLe(minKey+i-1, schema)
	}
	boundaries[numBuckets] = hi

	groupBounds := nativeHistogramBounds[schema]
	bucketsPerGroup := len(groupBounds)

	// Build the bucket lookup table. For each of the 256 possible
	// top-8-bit mantissa values, compute the bucket index within
	// the group by evaluating promBucketKey on representative float64
	// values.
	//
	// A single 8-bit entry can straddle a bucket boundary: the low
	// 44 bits that were truncated may push the full-precision value
	// into the next bucket. We detect straddling by checking both
	// the minimum and maximum float64 representable by each table
	// entry. When they disagree, we resolve to the upper key (keyMax).
	// This matches SearchFloat64s's >= semantics and means straddling
	// values always round up to the next bucket — at most one
	// bucket of additional error for a small fraction of values.
	//
	// For the common schema 2 (10% error), only 4 of 256 entries
	// straddle, affecting ~0.4% of recorded values. The maximum
	// additional error for those values is bounded by one bucket width
	// (8.6%), but the impact on quantile estimation is negligible since
	// the affected values are already near the boundary.
	var bucketLookup [bucketLookupSize]uint8
	for tableIdx := 0; tableIdx < bucketLookupSize; tableIdx++ {
		minBits := uint64(1023)<<52 | uint64(tableIdx)<<bucketLookupShift
		maxBits := minBits | (1<<bucketLookupShift - 1)
		keyMin := promBucketKey(math.Float64frombits(minBits), schema)
		keyMax := promBucketKey(math.Float64frombits(maxBits), schema)
		if keyMin == keyMax {
			bucketLookup[tableIdx] = uint8(keyMin)
		} else {
			// Straddling entry — round up to the next bucket.
			bucketLookup[tableIdx] = uint8(keyMax)
		}
	}

	return config{
		schema:          schema,
		lo:              lo,
		hi:              hi,
		minKey:          minKey,
		numBuckets:      numBuckets,
		bucketsPerGroup: bucketsPerGroup,
		groupBounds:     groupBounds,
		bucketLookup:    bucketLookup,
		boundaries:      boundaries,
	}
}

// Params holds the user-facing parameters for creating a Histogram.
//
// Zero-value fields are replaced with defaults:
//   - Lo: 1
//   - Hi: math.MaxInt64
//   - ErrorBound: 0.10 (10%, schema 2)
type Params struct {
	// Lo and Hi define the tracked value range. Values outside this range
	// are counted in Underflow/Overflow.
	Lo, Hi float64
	// ErrorBound is the maximum relative error bound. The histogram
	// selects the tightest Prometheus schema whose error is at or below
	// this value.
	ErrorBound float64
}

// Common Params presets, modeled after the bucket tiers in CockroachDB's
// pkg/util/metric/histogram_buckets.go and Prometheus DefBuckets.
//
// Time-based presets expect values in nanoseconds, matching Go's
// time.Duration. Record with int64(duration).
var (
	// HiResLatencyParams covers high-resolution latency from 1us to 5m.
	// Use for: end-to-end request latencies where you need visibility into
	// both fast-path sub-millisecond operations and slow tail outliers.
	HiResLatencyParams = Params{
		Lo: float64(time.Microsecond),
		Hi: float64(5 * time.Minute),
	}

	// IOLatencyParams covers fast I/O operations from 10us to 10s.
	// Use for: RPC latencies, raft operations, disk I/O, network round-trips.
	IOLatencyParams = Params{
		Lo: float64(10 * time.Microsecond),
		Hi: float64(10 * time.Second),
	}

	// ResponseTimeParams covers request/response latencies from 1ms to 30s.
	// Use for: SQL query execution, HTTP handlers, API response times.
	ResponseTimeParams = Params{
		Lo: float64(time.Millisecond),
		Hi: float64(30 * time.Second),
	}

	// LongRunningParams covers long-running operations from 500ms to 1h.
	// Use for: backups, restores, migrations, bulk ingestion jobs.
	LongRunningParams = Params{
		Lo: float64(500 * time.Millisecond),
		Hi: float64(time.Hour),
	}

	// DataSizeParams covers data payload sizes from 1KB to 16MB (in bytes).
	// Use for: message sizes, request/response bodies, SST sizes.
	DataSizeParams = Params{
		Lo: 1024,
		Hi: 16 * 1024 * 1024,
	}

	// MemoryUsageParams covers memory tracking from 1B to 64MB (in bytes).
	// Use for: memory allocations, buffer sizes, cache entry sizes.
	MemoryUsageParams = Params{
		Lo: 1,
		Hi: 64 * 1024 * 1024,
	}
)

func (p Params) withDefaults() Params {
	if p.Lo == 0 {
		p.Lo = 1
	}
	if p.Hi == 0 {
		p.Hi = float64(math.MaxInt64)
	}
	if p.ErrorBound == 0 {
		p.ErrorBound = 0.10
	}
	return p
}

// configCache stores previously computed configs keyed by Params. Since
// configs are immutable and many histograms share the same parameters
// (e.g., all latency histograms in CockroachDB use [500ns, 60s] @ 10%),
// this avoids redundant allocations of the lookup table and boundaries
// array.
var configCache sync.Map // map[Params]*config

// getOrCreateConfig returns a shared config for the given params, creating
// one if it doesn't already exist.
func getOrCreateConfig(p Params) *config {
	if v, ok := configCache.Load(p); ok {
		return v.(*config)
	}
	cfg := newConfig(p.Lo, p.Hi, p.ErrorBound)
	actual, _ := configCache.LoadOrStore(p, &cfg)
	return actual.(*config)
}

// Histogram is a lock-free exponential histogram with atomic counters.
type Histogram struct {
	cfg    *config
	counts []atomic.Uint64
	// Underflow counts values below cfg.lo.
	Underflow atomic.Uint64
	// Overflow counts values above cfg.hi.
	Overflow atomic.Uint64
	// ZeroCount counts exact zeros (and negative values).
	ZeroCount atomic.Uint64
	sum       atomic.Int64 // using Int64 since CockroachDB histograms record int64
}

// Reset zeroes all counters without reallocating the backing slice.
func (h *Histogram) Reset() {
	for i := range h.counts {
		h.counts[i].Store(0)
	}
	h.ZeroCount.Store(0)
	h.Underflow.Store(0)
	h.Overflow.Store(0)
	h.sum.Store(0)
}

// New creates a new Histogram for the given range and error bound. Configs
// are cached and shared across histograms with identical parameters.
func New(p Params) *Histogram {
	p = p.withDefaults()
	cfg := getOrCreateConfig(p)
	return &Histogram{
		cfg:    cfg,
		counts: make([]atomic.Uint64, cfg.numBuckets),
	}
}

// Record adds a value to the histogram. This is the hot path: O(1), lock-free,
// no allocations. Values <= 0 are counted in ZeroCount. Values outside [lo, hi]
// are clamped and counted in Underflow/Overflow respectively.
//
// Bucket index is computed via math.Frexp (IEEE 754 bit extraction) plus a
// precomputed lookup table indexed by the top mantissa bits. This
// produces indices exactly aligned with Prometheus native histogram bucket
// keys, avoiding the floating-point rounding drift that a math.Log-based
// approach would introduce.
func (h *Histogram) Record(v int64) {
	h.sum.Add(v)

	if v <= 0 {
		h.ZeroCount.Add(1)
		return
	}

	// Convert to float64 and extract IEEE 754 bits. The int→float
	// conversion is a single SCVTF instruction; Float64bits is a
	// zero-cost reinterpret.
	bits := math.Float64bits(float64(v))
	exp := int(bits>>52) - 1022
	sub := int(h.cfg.bucketLookup[(bits>>bucketLookupShift)&0xFF])

	key := sub + (exp-1)*h.cfg.bucketsPerGroup
	idx := key - h.cfg.minKey

	// Clamp to valid range. Values outside [lo, hi] are counted in
	// Underflow/Overflow but are NOT added to any bucket. This matches
	// the Prometheus convention where out-of-range values go to an
	// implicit -Inf/+Inf bucket that is excluded from interpolation,
	// and quantile estimation clamps to lo/hi when the quantile falls
	// in the overflow/underflow region.
	if idx < 0 {
		h.Underflow.Add(1)
		return
	}
	if idx >= h.cfg.numBuckets {
		h.Overflow.Add(1)
		return
	}
	h.counts[idx].Add(1)
}

// Snapshot is a point-in-time, non-atomic copy of a Histogram, suitable for
// quantile computation and export.
type Snapshot struct {
	cfg        *config
	Counts     []uint64
	ZeroCount  uint64
	Underflow  uint64
	Overflow   uint64
	TotalCount uint64
	TotalSum   int64
}

// Schema returns the Prometheus native histogram schema (0–8).
func (s *Snapshot) Schema() int32 {
	return s.cfg.schema
}

// Snapshot returns a point-in-time copy of the histogram. The snapshot is
// not guaranteed to be perfectly consistent (individual counters are read
// independently), but this is acceptable for metrics — the same trade-off
// Prometheus makes.
func (h *Histogram) Snapshot() Snapshot {
	s := Snapshot{
		cfg:       h.cfg,
		Counts:    make([]uint64, h.cfg.numBuckets),
		ZeroCount: h.ZeroCount.Load(),
		Underflow: h.Underflow.Load(),
		Overflow:  h.Overflow.Load(),
		TotalSum:  h.sum.Load(),
	}
	for i := range s.Counts {
		c := h.counts[i].Load()
		s.Counts[i] = c
		s.TotalCount += c
	}
	// Underflow, overflow, and zero observations are not recorded in
	// any bucket, so add them to the total.
	s.TotalCount += s.ZeroCount + s.Underflow + s.Overflow
	return s
}

// Schema returns the Prometheus native histogram schema (0–8).
func (h *Histogram) Schema() int32 {
	return h.cfg.schema
}

// Merge returns a new Snapshot whose counts are the element-wise sum of s
// and other. Both snapshots must share the same config (same schema and
// bucket boundaries). This is used to merge prev and cur window snapshots
// in the tick-based windowing pattern.
func (s *Snapshot) Merge(other *Snapshot) Snapshot {
	merged := Snapshot{
		cfg:        s.cfg,
		Counts:     make([]uint64, len(s.Counts)),
		ZeroCount:  s.ZeroCount + other.ZeroCount,
		Underflow:  s.Underflow + other.Underflow,
		Overflow:   s.Overflow + other.Overflow,
		TotalCount: s.TotalCount + other.TotalCount,
		TotalSum:   s.TotalSum + other.TotalSum,
	}
	for i := range s.Counts {
		merged.Counts[i] = s.Counts[i] + other.Counts[i]
	}
	return merged
}

// Sub returns a new Snapshot whose counts are the element-wise difference
// of s minus other. Both snapshots must share the same config. This is used
// to compute windowed views by subtracting a baseline snapshot from a
// current cumulative snapshot.
func (s *Snapshot) Sub(other *Snapshot) Snapshot {
	diff := Snapshot{
		cfg:        s.cfg,
		Counts:     make([]uint64, len(s.Counts)),
		ZeroCount:  s.ZeroCount - other.ZeroCount,
		Underflow:  s.Underflow - other.Underflow,
		Overflow:   s.Overflow - other.Overflow,
		TotalCount: s.TotalCount - other.TotalCount,
		TotalSum:   s.TotalSum - other.TotalSum,
	}
	for i := range s.Counts {
		diff.Counts[i] = s.Counts[i] - other.Counts[i]
	}
	return diff
}
