package factor

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	SectorNameOther   = "Other"
	SectorNameL1      = "L1"
	SectorNameL2      = "L2"
	SectorNameDeFi    = "DeFi"
	SectorNameAIDePin = "AI_DePin"
	SectorNameMeme    = "Meme"
	SectorNameGaming  = "Gaming"
	SectorNameInfra   = "Infra"
)

const (
	SectorCodeOther   = 0
	SectorCodeL1      = 1
	SectorCodeL2      = 2
	SectorCodeDeFi    = 3
	SectorCodeAIDePin = 4
	SectorCodeMeme    = 5
	SectorCodeGaming  = 6
	SectorCodeInfra   = 7
)

const MaxQuantileBuckets = 1000

var cryptoSectorCodeByName = map[string]int{
	SectorNameOther:   SectorCodeOther,
	SectorNameL1:      SectorCodeL1,
	SectorNameL2:      SectorCodeL2,
	SectorNameDeFi:    SectorCodeDeFi,
	SectorNameAIDePin: SectorCodeAIDePin,
	SectorNameMeme:    SectorCodeMeme,
	SectorNameGaming:  SectorCodeGaming,
	SectorNameInfra:   SectorCodeInfra,
}

var cryptoSectorByBase = map[string]string{
	// L1
	"BTC": SectorNameL1, "ETH": SectorNameL1, "SOL": SectorNameL1, "BNB": SectorNameL1,
	"AVAX": SectorNameL1, "ADA": SectorNameL1, "DOT": SectorNameL1, "NEAR": SectorNameL1,
	"ATOM": SectorNameL1, "SUI": SectorNameL1, "APT": SectorNameL1, "SEI": SectorNameL1,
	"TIA": SectorNameL1, "TON": SectorNameL1, "TRX": SectorNameL1, "XRP": SectorNameL1,
	"LTC": SectorNameL1, "BCH": SectorNameL1, "ETC": SectorNameL1,

	// L2
	"ARB": SectorNameL2, "OP": SectorNameL2, "MATIC": SectorNameL2, "POL": SectorNameL2,
	"STRK": SectorNameL2, "ZK": SectorNameL2, "MNT": SectorNameL2, "IMX": SectorNameL2,
	"METIS": SectorNameL2, "BLAST": SectorNameL2, "STX": SectorNameL2,

	// DeFi
	"UNI": SectorNameDeFi, "AAVE": SectorNameDeFi, "MKR": SectorNameDeFi, "SKY": SectorNameDeFi,
	"LDO": SectorNameDeFi, "CRV": SectorNameDeFi, "SNX": SectorNameDeFi, "COMP": SectorNameDeFi,
	"PENDLE": SectorNameDeFi, "DYDX": SectorNameDeFi, "GMX": SectorNameDeFi, "JUP": SectorNameDeFi,
	"RAY": SectorNameDeFi, "ENA": SectorNameDeFi, "ONDO": SectorNameDeFi, "1INCH": SectorNameDeFi,
	"SUSHI": SectorNameDeFi, "RUNE": SectorNameDeFi, "INJ": SectorNameDeFi,

	// AI_DePin (NEAR retains its primary single label in L1)
	"FET": SectorNameAIDePin, "RNDR": SectorNameAIDePin, "RENDER": SectorNameAIDePin,
	"TAO": SectorNameAIDePin, "WLD": SectorNameAIDePin, "ARKM": SectorNameAIDePin,
	"AGIX": SectorNameAIDePin, "OCEAN": SectorNameAIDePin, "AKT": SectorNameAIDePin,
	"IO": SectorNameAIDePin, "GRT": SectorNameAIDePin, "THETA": SectorNameAIDePin,
	"AR": SectorNameAIDePin, "FIL": SectorNameAIDePin,

	// Meme
	"DOGE": SectorNameMeme, "SHIB": SectorNameMeme, "PEPE": SectorNameMeme, "WIF": SectorNameMeme,
	"BONK": SectorNameMeme, "FLOKI": SectorNameMeme, "MEME": SectorNameMeme, "BOME": SectorNameMeme,
	"TRUMP": SectorNameMeme, "TURBO": SectorNameMeme, "NEIRO": SectorNameMeme, "ORDI": SectorNameMeme,
	"1000SATS": SectorNameMeme, "SATS": SectorNameMeme,

	// Gaming
	"AXS": SectorNameGaming, "SAND": SectorNameGaming, "MANA": SectorNameGaming, "GALA": SectorNameGaming,
	"APE": SectorNameGaming, "GMT": SectorNameGaming, "ILV": SectorNameGaming, "PIXEL": SectorNameGaming,
	"PORTAL": SectorNameGaming, "BEAMX": SectorNameGaming, "BEAM": SectorNameGaming,
	"RONIN": SectorNameGaming, "RON": SectorNameGaming, "NOT": SectorNameGaming, "HMSTR": SectorNameGaming,

	// Infra
	"LINK": SectorNameInfra, "PYTH": SectorNameInfra, "API3": SectorNameInfra, "TRB": SectorNameInfra,
	"BAND": SectorNameInfra, "W": SectorNameInfra, "ZRO": SectorNameInfra, "AXL": SectorNameInfra,
	"ENS": SectorNameInfra, "EIGEN": SectorNameInfra, "ETHFI": SectorNameInfra,
}

// NormalizeCryptoBaseSymbol extracts the canonical uppercase base asset ticker
// from spot or perpetual symbols (e.g. "1000PEPE/USDT:USDT" -> "PEPE",
// "BTC/USDT" -> "BTC", "AAVEUSDT" -> "AAVE").
func NormalizeCryptoBaseSymbol(symbol string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if s == "" {
		return ""
	}
	if idx := strings.IndexByte(s, ':'); idx >= 0 {
		s = s[:idx]
	}
	hasSep := false
	for _, sep := range []byte{'/', '-', '_'} {
		if idx := strings.IndexByte(s, sep); idx >= 0 {
			s = s[:idx]
			hasSep = true
			break
		}
	}
	if !hasSep {
		if _, direct := cryptoSectorByBase[s]; !direct {
			for _, quote := range []string{"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP"} {
				if len(s) > len(quote)+1 && strings.HasSuffix(s, quote) {
					s = strings.TrimSuffix(s, quote)
					break
				}
			}
		}
	}
	for _, mult := range []string{"1000000", "100000", "10000", "1000", "1M"} {
		if len(s) > len(mult) && strings.HasPrefix(s, mult) {
			s = strings.TrimPrefix(s, mult)
			break
		}
	}
	return s
}

// CryptoSector returns the deterministic built-in sector label ("L1", "L2",
// "DeFi", "AI_DePin", "Meme", "Gaming", "Infra", or "Other") for symbol.
func CryptoSector(symbol string) string {
	base := NormalizeCryptoBaseSymbol(symbol)
	if sector, ok := cryptoSectorByBase[base]; ok {
		return sector
	}
	return SectorNameOther
}

// CryptoSectorCode returns the deterministic numeric sector ID (0..7) for symbol.
func CryptoSectorCode(symbol string) int {
	return cryptoSectorCodeByName[CryptoSector(symbol)]
}

// SectorCode declares a built-in per-asset numeric sector ID node derived from
// SnapshotSpec.SIDMap without requiring an external database source.
func SectorCode(timeframe string) *Node {
	n := node("sector-code", TS)
	n.Spec.TimeFrame = timeframe
	return n
}

// SectorDemean demeans input within each built-in crypto sector on the reference pool.
func SectorDemean(input *Node) *Node {
	return node("sector-demean", GROUP, input)
}

// SectorZScore standardizes input within each built-in crypto sector on the reference pool.
func SectorZScore(input *Node) *Node {
	return node("sector-zscore", GROUP, input)
}

// QuantileGroupDemean partitions valid cross-sectional assets at each timestamp into
// nBuckets (>= 2) deterministic rank-based quantile tiers of metric, and demeans x
// within each tier.
func QuantileGroupDemean(x, metric *Node, nBuckets int) *Node {
	n := node("quantile-group-demean", GROUP, x, metric)
	n.Spec.Parameters["buckets"] = float64(nBuckets)
	return n
}

// QuantileGroupZScore partitions valid cross-sectional assets at each timestamp into
// nBuckets (>= 2) deterministic rank-based quantile tiers of metric, and standardizes x
// (population z-score) within each tier.
func QuantileGroupZScore(x, metric *Node, nBuckets int) *Node {
	n := node("quantile-group-zscore", GROUP, x, metric)
	n.Spec.Parameters["buckets"] = float64(nBuckets)
	return n
}

// QuantileGroupRank computes the 0-based ascending average rank of x within each
// dynamic quantile tier of metric.
func QuantileGroupRank(x, metric *Node, nBuckets int) *Node {
	n := node("quantile-group-rank", GROUP, x, metric)
	n.Spec.Parameters["buckets"] = float64(nBuckets)
	return n
}

// QuantileGroupPctRank computes the normalized [0, 1] rank of x (0.5 when bucket size is 1)
// within each dynamic quantile tier of metric.
func QuantileGroupPctRank(x, metric *Node, nBuckets int) *Node {
	n := node("quantile-group-pct-rank", GROUP, x, metric)
	n.Spec.Parameters["buckets"] = float64(nBuckets)
	return n
}

func isSectorOrQuantileOperator(op string) bool {
	switch op {
	case "sector-code", "sector-demean", "sector-zscore",
		"quantile-group-demean", "quantile-group-zscore", "quantile-group-rank", "quantile-group-pct-rank":
		return true
	}
	return false
}

func validateSectorOrQuantileNode(spec NodeSpec, count int) error {
	if spec.Source != "" || spec.Field != "" || spec.GroupField != "" {
		return fmt.Errorf("factor: %s does not accept raw stream fields", spec.Operator)
	}
	switch spec.Operator {
	case "sector-code":
		if count != 0 {
			return errors.New("factor: sector-code takes no dependencies")
		}
		if len(spec.Parameters) != 0 {
			return errors.New("factor: sector-code does not accept scalar parameters")
		}
		return nil
	case "sector-demean", "sector-zscore":
		if count != 1 {
			return fmt.Errorf("factor: %s requires one dependency", spec.Operator)
		}
		if len(spec.Parameters) != 0 {
			return fmt.Errorf("factor: %s does not accept scalar parameters", spec.Operator)
		}
		return nil
	case "quantile-group-demean", "quantile-group-zscore", "quantile-group-rank", "quantile-group-pct-rank":
		if count != 2 {
			return fmt.Errorf("factor: %s requires x and metric dependencies", spec.Operator)
		}
		buckets, ok := spec.Parameters["buckets"]
		if !ok || len(spec.Parameters) != 1 {
			return fmt.Errorf("factor: %s requires buckets parameter", spec.Operator)
		}
		if buckets < 2 || buckets > MaxQuantileBuckets || buckets != float64(int(buckets)) {
			return fmt.Errorf("factor: %s buckets must be an integer in [2,%d]", spec.Operator, MaxQuantileBuckets)
		}
		return nil
	default:
		return fmt.Errorf("factor: unregistered operator %s", spec.Operator)
	}
}

func evaluateSectorCode(snapshot *Snapshot, sid int32) Numeric {
	sym := snapshot.spec.SIDMap[sid]
	if sym == "" {
		return Numeric{math.NaN(), Missing}
	}
	return Numeric{float64(CryptoSectorCode(sym)), Valid}
}

func evaluateSectorOrQuantileCS(
	node compiledNode,
	snapshot *Snapshot,
	columns []map[int32]Numeric,
	active []int32,
	points []point,
	targets []point,
	result map[int32]Numeric,
) (map[int32]Numeric, bool) {
	switch node.spec.Operator {
	case "sector-demean", "sector-zscore":
		groups := make(map[string][]point, 8)
		groupTargets := make(map[string][]point, 8)
		for _, p := range points {
			key := CryptoSector(snapshot.spec.SIDMap[p.sid])
			groups[key] = append(groups[key], p)
		}
		for _, p := range targets {
			key := CryptoSector(snapshot.spec.SIDMap[p.sid])
			groupTargets[key] = append(groupTargets[key], p)
		}
		zscore := node.spec.Operator == "sector-zscore"
		for key, gt := range groupTargets {
			grp := groups[key]
			if len(grp) == 0 {
				for _, p := range gt {
					result[p.sid] = Numeric{math.NaN(), Missing}
				}
				continue
			}
			standardize(grp, gt, result, zscore)
		}
		return result, true

	case "quantile-group-demean", "quantile-group-zscore", "quantile-group-rank", "quantile-group-pct-rank":
		return evaluateQuantileGroupCS(node, snapshot, columns, active), true
	default:
		return nil, false
	}
}

func evaluateQuantileGroupCS(node compiledNode, snapshot *Snapshot, columns []map[int32]Numeric, active []int32) map[int32]Numeric {
	xValues := columns[node.inputs[0]]
	mValues := columns[node.inputs[1]]
	nBuckets := int(node.spec.Parameters["buckets"])
	result := make(map[int32]Numeric, len(active))

	type xmPoint struct {
		sid int32
		x   float64
		m   float64
	}
	validTargets := make([]xmPoint, 0, len(active))
	for _, sid := range active {
		x, okX := xValues[sid]
		if !okX {
			result[sid] = Numeric{math.NaN(), Missing}
			continue
		}
		if x.Validity != Valid {
			result[sid] = Numeric{math.NaN(), x.Validity}
			continue
		}
		m, okM := mValues[sid]
		if !okM {
			result[sid] = Numeric{math.NaN(), Missing}
			continue
		}
		if m.Validity != Valid {
			result[sid] = Numeric{math.NaN(), m.Validity}
			continue
		}
		validTargets = append(validTargets, xmPoint{sid: sid, x: x.Value, m: m.Value})
	}

	refItems := make([]xmPoint, 0, len(snapshot.spec.Universe.Reference))
	refMetrics := make([]float64, 0, len(snapshot.spec.Universe.Reference))
	for _, sid := range snapshot.spec.Universe.Reference {
		x, okX := xValues[sid]
		m, okM := mValues[sid]
		if okX && okM && x.Validity == Valid && m.Validity == Valid {
			refItems = append(refItems, xmPoint{sid: sid, x: x.Value, m: m.Value})
			refMetrics = append(refMetrics, m.Value)
		}
	}
	if len(refItems) == 0 {
		for _, t := range validTargets {
			result[t.sid] = Numeric{math.NaN(), Missing}
		}
		return result
	}

	sort.Float64s(refMetrics)
	sort.Slice(refItems, func(i, j int) bool {
		if refItems[i].x != refItems[j].x {
			return refItems[i].x < refItems[j].x
		}
		return refItems[i].sid < refItems[j].sid
	})

	bucketOf := func(m float64) int {
		lower := sort.Search(len(refMetrics), func(i int) bool { return refMetrics[i] >= m })
		upper := sort.Search(len(refMetrics), func(i int) bool { return refMetrics[i] > m })
		var r float64
		if lower == len(refMetrics) {
			r = float64(len(refMetrics) - 1)
		} else if upper == 0 {
			r = 0
		} else {
			r = float64(lower+upper-1) / 2.0
		}
		b := int(r * float64(nBuckets) / float64(len(refMetrics)))
		if b < 0 {
			b = 0
		} else if b >= nBuckets {
			b = nBuckets - 1
		}
		return b
	}

	bucketPoints := make([][]point, nBuckets)
	bucketTargets := make([][]point, nBuckets)
	for _, item := range refItems {
		b := bucketOf(item.m)
		bucketPoints[b] = append(bucketPoints[b], point{sid: item.sid, value: item.x})
	}
	for _, item := range validTargets {
		b := bucketOf(item.m)
		bucketTargets[b] = append(bucketTargets[b], point{sid: item.sid, value: item.x})
	}

	for b := 0; b < nBuckets; b++ {
		gt := bucketTargets[b]
		if len(gt) == 0 {
			continue
		}
		pts := bucketPoints[b]
		if len(pts) == 0 {
			for _, p := range gt {
				result[p.sid] = Numeric{math.NaN(), Missing}
			}
			continue
		}
		switch node.spec.Operator {
		case "quantile-group-demean":
			standardize(pts, gt, result, false)
		case "quantile-group-zscore":
			standardize(pts, gt, result, true)
		case "quantile-group-rank", "quantile-group-pct-rank":
			denom := float64(len(pts) - 1)
			for _, p := range gt {
				lower := sort.Search(len(pts), func(i int) bool { return pts[i].value >= p.value })
				upper := sort.Search(len(pts), func(i int) bool { return pts[i].value > p.value })
				r := float64(lower+upper-1) / 2.0
				if node.spec.Operator == "quantile-group-pct-rank" {
					if len(pts) == 1 {
						if p.value < pts[0].value {
							r = 0
						} else if p.value > pts[0].value {
							r = 1
						} else {
							r = 0.5
						}
					} else {
						r = max(0.0, min(1.0, r/denom))
					}
				}
				result[p.sid] = Numeric{r, Valid}
			}
		}
	}
	return result
}
