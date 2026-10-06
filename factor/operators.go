package factor

import (
	"math"
	"sort"

	"github.com/banbox/banbot/orm"
)

type point struct {
	sid   int32
	value float64
}

func validPoints(reference []int32, values map[int32]Numeric) []point {
	points := make([]point, 0, len(reference))
	for _, sid := range reference {
		if n, ok := values[sid]; ok && n.Validity == Valid {
			points = append(points, point{sid, n.Value})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].value != points[j].value {
			return points[i].value < points[j].value
		}
		return points[i].sid < points[j].sid
	})
	return points
}

func percentile(points []point, q float64) float64 {
	position := q * float64(len(points)-1)
	low := int(position)
	high := int(math.Ceil(position))
	return points[low].value + (points[high].value-points[low].value)*(position-float64(low))
}

func crossSection(node compiledNode, snapshot *Snapshot, columns []map[int32]Numeric) map[int32]Numeric {
	input := columns[node.inputs[0]]
	active := activeFactorSIDs(snapshot.spec)
	result := make(map[int32]Numeric, len(active))
	targets := make([]point, 0, len(active))
	for _, sid := range active {
		if value, ok := input[sid]; ok {
			result[sid] = value
			if value.Validity == Valid {
				targets = append(targets, point{sid, value.Value})
			}
		} else {
			result[sid] = Numeric{math.NaN(), Missing}
		}
	}
	points := validPoints(snapshot.spec.Universe.Reference, input)
	if len(points) == 0 {
		for _, p := range targets {
			result[p.sid] = Numeric{math.NaN(), Missing}
		}
		return result
	}
	switch node.spec.Operator {
	case "rank":
		for _, p := range targets {
			lower := sort.Search(len(points), func(i int) bool { return points[i].value >= p.value })
			upper := sort.Search(len(points), func(i int) bool { return points[i].value > p.value })
			result[p.sid] = Numeric{float64(lower+upper-1) / 2, Valid}
		}
	case "zscore":
		standardize(points, targets, result, true)
	case "robust-zscore", "mad-winsorize":
		median := percentile(points, .5)
		deviations := make([]point, len(points))
		for i, p := range points {
			deviations[i] = point{p.sid, math.Abs(p.value - median)}
		}
		sort.Slice(deviations, func(i, j int) bool { return deviations[i].value < deviations[j].value })
		scale := 1.4826 * percentile(deviations, .5)
		for _, p := range targets {
			value := 0.0
			if node.spec.Operator == "robust-zscore" {
				if scale > 0 {
					value = (p.value - median) / scale
				}
			} else {
				limit := node.spec.Parameters["multiple"] * scale
				value = max(median-limit, min(median+limit, p.value))
			}
			result[p.sid] = numeric(value)
		}
	case "multi-residual", "weighted-residual":
		first := 1
		if node.spec.Operator == "weighted-residual" {
			first = 2
		}
		rowFor := func(sid int32) ([]float64, bool) {
			row := []float64{1}
			for _, id := range node.inputs[first:] {
				x, exists := columns[id][sid]
				if !exists || x.Validity != Valid {
					return nil, false
				}
				row = append(row, x.Value)
			}
			return row, true
		}
		var x [][]float64
		var y, weights []float64
		for _, p := range points {
			row, ok := rowFor(p.sid)
			if !ok {
				continue
			}
			weight := 1.0
			if first == 2 {
				w, exists := columns[node.inputs[1]][p.sid]
				if !exists || w.Validity != Valid || w.Value <= 0 {
					continue
				}
				weight = w.Value
			}
			x, y, weights = append(x, row), append(y, p.value), append(weights, weight)
		}
		coef, _, err := LeastSquares(x, y, weights)
		for _, p := range targets {
			row, ok := rowFor(p.sid)
			if !ok {
				result[p.sid] = Numeric{math.NaN(), Missing}
				continue
			}
			if err != nil || len(x) < len(row) {
				result[p.sid] = Numeric{math.NaN(), Warmup}
				continue
			}
			fit := 0.0
			for i, value := range row {
				fit += coef[i] * value
			}
			result[p.sid] = numeric(p.value - fit)
		}
	case "winsorize":
		low := percentile(points, node.spec.Parameters["tail"])
		high := percentile(points, 1-node.spec.Parameters["tail"])
		for _, p := range targets {
			result[p.sid] = numeric(max(low, min(high, p.value)))
		}
	case "quantile":
		value := percentile(points, node.spec.Parameters["q"])
		for _, p := range targets {
			result[p.sid] = numeric(value)
		}
	case "group-demean", "group-zscore":
		groups := make(map[string][]point)
		groupTargets := make(map[string][]point)
		keys := make(map[int32]string)
		for _, p := range targets {
			row, exists := snapshot.rows[StreamKey{p.sid, node.spec.Source, node.spec.SourceTimeFrame}]
			if !exists {
				result[p.sid] = Numeric{math.NaN(), Missing}
				continue
			}
			raw, exists := row.Series.Values[node.spec.GroupField]
			if !exists {
				result[p.sid] = Numeric{math.NaN(), Missing}
				continue
			}
			if raw == nil {
				result[p.sid] = Numeric{math.NaN(), Null}
				continue
			}
			key, err := contentHash(raw)
			if err != nil {
				result[p.sid] = Numeric{math.NaN(), NotNumeric}
				continue
			}
			keys[p.sid] = key
			groupTargets[key] = append(groupTargets[key], p)
		}
		for _, p := range points {
			if key, ok := keys[p.sid]; ok {
				groups[key] = append(groups[key], p)
			}
		}
		for key, targets := range groupTargets {
			group := groups[key]
			if len(group) == 0 {
				for _, p := range targets {
					result[p.sid] = Numeric{math.NaN(), Missing}
				}
				continue
			}
			standardize(group, targets, result, node.spec.Operator == "group-zscore")
		}
	case "residual":
		xValues := columns[node.inputs[1]]
		paired := make([]point, 0, len(points))
		meanX, meanY := 0.0, 0.0
		for _, p := range points {
			x, ok := xValues[p.sid]
			if !ok || x.Validity != Valid {
				result[p.sid] = Numeric{math.NaN(), Missing}
				continue
			}
			paired = append(paired, p)
			meanX += x.Value
			meanY += p.value
		}
		if len(paired) < 2 {
			for _, p := range targets {
				status := Warmup
				if x, ok := xValues[p.sid]; !ok || x.Validity != Valid {
					status = Missing
				}
				result[p.sid] = Numeric{math.NaN(), status}
			}
			return result
		}
		meanX /= float64(len(paired))
		meanY /= float64(len(paired))
		variance, covariance := 0.0, 0.0
		for _, p := range paired {
			x := xValues[p.sid].Value - meanX
			variance += x * x
			covariance += x * (p.value - meanY)
		}
		beta := 0.0
		if variance != 0 {
			beta = covariance / variance
		}
		for _, p := range targets {
			x, ok := xValues[p.sid]
			if !ok || x.Validity != Valid {
				result[p.sid] = Numeric{math.NaN(), Missing}
				continue
			}
			result[p.sid] = numeric(p.value - meanY - beta*(x.Value-meanX))
		}
	}
	return result
}

func standardize(points, targets []point, result map[int32]Numeric, zscore bool) {
	mean := 0.0
	for _, p := range points {
		mean += p.value
	}
	mean /= float64(len(points))
	variance := 0.0
	for _, p := range points {
		difference := p.value - mean
		variance += difference * difference
	}
	deviation := math.Sqrt(variance / float64(len(points)))
	for _, p := range targets {
		value := p.value - mean
		if zscore {
			if deviation == 0 {
				value = 0
			} else {
				value /= deviation
			}
		}
		result[p.sid] = numeric(value)
	}
}

// Record is a convenience for closed DataSeries inputs with explicit
// visibility. It does not assume every source is published at EndMS.
func Record(series orm.DataSeries, revision uint64, availableAt, ingestedAt int64, sourceVersion string) VersionRecord {
	return VersionRecord{Series: series, EventTime: series.EndMS, Revision: revision, AvailableAt: availableAt, IngestedAt: ingestedAt, SourceVersion: sourceVersion}
}
