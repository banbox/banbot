package research

import (
	"errors"
	"maps"
	"math"
	"slices"
	"sort"

	"github.com/banbox/banbot/factor"
)

type ComboMethod string

const (
	Equal           ComboMethod = "equal"
	Fixed           ComboMethod = "fixed"
	HistoryIC       ComboMethod = "history-ic"
	HistoryRankIC   ComboMethod = "history-rank-ic"
	HistoryICIR     ComboMethod = "history-icir"
	HistoryRankICIR ComboMethod = "history-rank-icir"
	HistoryEWMA     ComboMethod = "history-ewma"
)

type ComboSpec struct {
	Method        ComboMethod        `yaml:"method"`
	Columns       []string           `yaml:"columns"`
	Weights       map[string]float64 `yaml:"weights,omitempty"`
	Label         string             `json:",omitempty" yaml:"label,omitempty"`
	MinSamples    int                `json:",omitempty" yaml:"min_samples,omitempty"`
	MinPairs      int                `json:",omitempty" yaml:"min_pairs,omitempty"`
	MinConfidence float64            `json:",omitempty" yaml:"min_confidence,omitempty"`
	Decay         float64            `json:",omitempty" yaml:"decay,omitempty"`
	Direction     string             `json:",omitempty" yaml:"direction,omitempty"`
	Fallback      string             `json:",omitempty" yaml:"fallback,omitempty"`
}
type ICSample struct {
	Column                              string
	Label                               string
	DecisionTime, MatureAt, AvailableAt int64
	IC                                  factor.Numeric
	RankIC                              factor.Numeric
	Samples                             int
}

// ICHistory retains only a configured number of cross sections per column.
// Availability is checked when weights are selected, even for preloaded data.
type ICHistory struct {
	window   int
	label    string
	samples  map[string][]ICSample
	lastAsOf int64
}

func NewICHistory(window int, label string, columns []string) (*ICHistory, error) {
	if window <= 0 || label == "" || len(columns) == 0 {
		return nil, errors.New("research: IC history needs a window and label")
	}
	samples := make(map[string][]ICSample)
	for _, name := range columns {
		if name == "" {
			return nil, errors.New("research: empty IC column")
		}
		samples[name] = nil
	}
	return &ICHistory{window: window, label: label, samples: samples}, nil
}
func (h *ICHistory) Add(asof int64, sample ICSample) error {
	if asof < h.lastAsOf {
		return errors.New("research: IC history time moved backwards; replay with a new history")
	}
	if _, declared := h.samples[sample.Column]; !declared {
		return errors.New("research: undeclared IC column")
	}
	if sample.Column == "" || sample.Label != h.label || sample.DecisionTime <= 0 || sample.MatureAt <= sample.DecisionTime || sample.AvailableAt <= 0 || sample.Samples < 2 {
		return errors.New("research: invalid IC sample identity/maturity")
	}
	if sample.MatureAt > asof || sample.AvailableAt > asof || sample.DecisionTime >= asof {
		return errors.New("research: IC sample is not matured and visible at decision")
	}
	h.lastAsOf = asof
	if !valid(sample.IC) && !valid(sample.RankIC) {
		return nil
	}
	rows := h.samples[sample.Column]
	for _, old := range rows {
		if old.DecisionTime == sample.DecisionTime {
			if old != sample {
				return errors.New("research: conflicting IC cross section")
			}
			return nil
		}
	}
	rows = append(rows, sample)
	sort.Slice(rows, func(i, j int) bool { return rows[i].DecisionTime < rows[j].DecisionTime })
	if len(rows) > h.window {
		rows = slices.Clone(rows[len(rows)-h.window:])
	}
	h.samples[sample.Column] = rows
	return nil
}
func IsHistoryMethod(method ComboMethod) bool {
	return method == HistoryIC || method == HistoryRankIC || method == HistoryICIR || method == HistoryRankICIR || method == HistoryEWMA
}

// ValidateComboSpec validates quality/fallback rules before any history exists,
// so an invalid rule cannot become silently effective only after warmup.
func ValidateComboSpec(spec ComboSpec) error {
	if spec.Method != Equal && spec.Method != Fixed && !IsHistoryMethod(spec.Method) {
		return errors.New("research: unsupported combination method")
	}
	if len(spec.Columns) == 0 {
		return errors.New("research: combination requires columns")
	}
	for _, column := range spec.Columns {
		if column == "" {
			return errors.New("research: empty combination column")
		}
	}
	if spec.MinSamples < 0 || spec.MinPairs < 0 || !finiteValues(spec.MinConfidence, spec.Decay) || spec.MinConfidence < 0 || spec.Decay < 0 || spec.Decay > 1 || (spec.Direction != "" && spec.Direction != "signed" && spec.Direction != "positive") || (spec.Fallback != "" && spec.Fallback != "equal" && spec.Fallback != "fixed" && spec.Fallback != "error") {
		return errors.New("research: invalid IC quality/fallback rules")
	}
	if spec.Method == Fixed || (IsHistoryMethod(spec.Method) && spec.Fallback == "fixed") {
		for _, column := range spec.Columns {
			value, exists := spec.Weights[column]
			if !exists || !finiteValues(value) {
				return errors.New("research: fixed weight missing/nonfinite")
			}
		}
	}
	return nil
}

// QualityWeights selects only matured, published observations. Confidence is
// an unadjusted mean/standard-error threshold; overlapping sections are not
// claimed to be independent statistical evidence.
func (h *ICHistory) QualityWeights(asof int64, spec ComboSpec) (map[string]float64, bool, error) {
	if err := ValidateComboSpec(spec); err != nil {
		return nil, false, err
	}
	if asof < h.lastAsOf {
		return nil, false, errors.New("research: IC history time moved backwards")
	}
	if spec.MinSamples < 0 || spec.MinPairs < 0 || spec.MinConfidence < 0 || math.IsNaN(spec.MinConfidence) || math.IsInf(spec.MinConfidence, 0) || spec.Decay < 0 || spec.Decay > 1 || math.IsNaN(spec.Decay) || (spec.Direction != "" && spec.Direction != "signed" && spec.Direction != "positive") {
		return nil, false, errors.New("research: invalid IC quality rules")
	}
	h.lastAsOf = asof
	weights := make(map[string]float64)
	total := 0.0
	minimum := max(1, spec.MinSamples)
	alpha := spec.Decay
	if alpha == 0 {
		alpha = .2
	}
	for _, name := range spec.Columns {
		var stats running
		var ewma float64
		for _, sample := range h.samples[name] {
			if sample.DecisionTime >= asof || sample.MatureAt > asof || sample.AvailableAt > asof || sample.Samples < max(2, spec.MinPairs) {
				continue
			}
			v := sample.IC
			if spec.Method == HistoryRankIC || spec.Method == HistoryRankICIR {
				v = sample.RankIC
			}
			if !valid(v) {
				continue
			}
			if stats.Count == 0 {
				ewma = v.Value
			} else {
				ewma = alpha*v.Value + (1-alpha)*ewma
			}
			stats.add(v.Value)
		}
		if stats.Count < minimum {
			continue
		}
		if spec.MinConfidence > 0 {
			if stats.Count < 2 {
				continue
			}
			se := math.Sqrt(stats.M2 / float64(stats.Count-1) / float64(stats.Count))
			if stats.Mean == 0 || (se > 0 && math.Abs(stats.Mean)/se < spec.MinConfidence) {
				continue
			}
		}
		weight := stats.Mean
		if spec.Method == HistoryEWMA {
			weight = ewma
		}
		if spec.Method == HistoryICIR || spec.Method == HistoryRankICIR {
			ir := stats.ir()
			if !valid(ir) {
				continue
			}
			weight = ir.Value
		}
		if spec.Direction == "positive" {
			weight = max(0, weight)
		}
		if math.IsNaN(weight) || math.IsInf(weight, 0) {
			continue
		}
		weights[name] = weight
		total += math.Abs(weight)
	}
	if total == 0 {
		return nil, false, nil
	}
	for name, weight := range weights {
		weights[name] = weight / total
	}
	return weights, true, nil
}
func (h *ICHistory) Retained() int {
	n := 0
	for _, rows := range h.samples {
		n += len(rows)
	}
	return n
}
func (h *ICHistory) Weights(asof int64, columns []string) (map[string]float64, bool, error) {
	if asof < h.lastAsOf {
		return nil, false, errors.New("research: IC history time moved backwards; replay with a new history")
	}
	h.lastAsOf = asof
	weights := make(map[string]float64)
	total := 0.0
	for _, name := range columns {
		sum := 0.0
		n := 0
		for _, sample := range h.samples[name] {
			if sample.DecisionTime < asof && sample.MatureAt <= asof && sample.AvailableAt <= asof && valid(sample.IC) {
				sum += sample.IC.Value
				n++
			}
		}
		if n > 0 {
			weights[name] = sum / float64(n)
			total += math.Abs(weights[name])
		}
	}
	if total == 0 {
		return nil, false, nil
	}
	for name, w := range weights {
		weights[name] = w / total
	}
	return weights, true, nil
}
func valid(n factor.Numeric) bool {
	return n.Validity == factor.Valid && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0)
}

// Combine uses inference columns only. Labels never select its pool. Missing
// nonzero-weight inputs invalidate scores rather than renormalizing per asset.
func Combine(frame factor.Frame, universe factor.Universe, spec ComboSpec, history *ICHistory) (map[int32]factor.Numeric, []factor.Diagnostic, error) {
	if err := ValidateComboSpec(spec); err != nil {
		return nil, nil, err
	}
	columns := slices.Clone(spec.Columns)
	slices.Sort(columns)
	columns = slices.Compact(columns)
	if len(columns) == 0 {
		return nil, nil, errors.New("research: combination requires columns")
	}
	weights := make(map[string]float64)
	var diagnostics []factor.Diagnostic
	switch spec.Method {
	case Equal:
		for _, name := range columns {
			weights[name] = 1 / float64(len(columns))
		}
	case Fixed:
		for _, name := range columns {
			w, ok := spec.Weights[name]
			if !ok || math.IsNaN(w) || math.IsInf(w, 0) {
				return nil, nil, errors.New("research: fixed weight missing/nonfinite")
			}
			weights[name] = w
		}
	case HistoryIC, HistoryRankIC, HistoryICIR, HistoryRankICIR, HistoryEWMA:
		var ok bool
		if history != nil {
			var err error
			quality := spec
			quality.Columns = columns
			weights, ok, err = history.QualityWeights(frame.DecisionTime, quality)
			if err != nil {
				return nil, nil, err
			}
		}
		if !ok {
			if spec.Fallback == "error" {
				return nil, nil, errors.New("research: insufficient qualified historical IC")
			}
			if spec.Fallback != "" && spec.Fallback != "equal" && spec.Fallback != "fixed" {
				return nil, nil, errors.New("research: invalid historical fallback")
			}
			weights = make(map[string]float64)
			for _, name := range columns {
				weights[name] = 1 / float64(len(columns))
				if spec.Fallback == "fixed" {
					w, exists := spec.Weights[name]
					if !exists || math.IsNaN(w) || math.IsInf(w, 0) {
						return nil, nil, errors.New("research: fixed fallback weight missing/nonfinite")
					}
					weights[name] = w
				}
			}
			diagnostics = append(diagnostics, factor.Diagnostic{Code: "ic-equal-fallback", Detail: "no matured visible nonzero historical IC"})
		}
	default:
		return nil, nil, errors.New("research: unsupported combination method")
	}
	result := make(map[int32]factor.Numeric)
	for _, sid := range uniqueSIDs(universe.Investable) {
		sum := 0.0
		status := factor.Valid
		for _, name := range columns {
			w := weights[name]
			if w == 0 {
				continue
			}
			value, exists := frame.Values[name][sid]
			if !exists {
				status = factor.Missing
				break
			}
			if !valid(value) {
				status = value.Validity
				if status == factor.Valid {
					status = factor.NonFinite
				}
				break
			}
			sum += w * value.Value
		}
		if status == factor.Valid && (math.IsNaN(sum) || math.IsInf(sum, 0)) {
			status = factor.NonFinite
		}
		if status != factor.Valid {
			sum = math.NaN()
		}
		result[sid] = factor.Numeric{Value: sum, Validity: status}
	}
	return result, diagnostics, nil
}
func uniqueSIDs(sids []int32) []int32 {
	out := slices.Clone(sids)
	slices.Sort(out)
	return slices.Compact(out)
}

type MomentumVolConfig struct {
	Source, Field, TimeFrame string
	Window, DDOF             int
	WinsorTail               float64
	Standardize              bool
}

func DefaultMomentumVolConfig() MomentumVolConfig {
	return MomentumVolConfig{"kline", "close", "1h", 24, 1, 0.01, true}
}

// MomentumVolPlan builds existing banta-backed nodes and CS transforms.
func MomentumVolPlan(cfg MomentumVolConfig) (*factor.Plan, ComboSpec, error) {
	if cfg.Window < 2 || cfg.DDOF < 0 || cfg.DDOF >= cfg.Window || cfg.WinsorTail < 0 || cfg.WinsorTail >= 0.5 {
		return nil, ComboSpec{}, errors.New("research: invalid momentum/volatility parameters")
	}
	close := factor.Field(cfg.Source, cfg.Field, cfg.TimeFrame)
	momentum := factor.Return(close, cfg.Window)
	volatility := factor.StdDev(factor.Return(close, 1), cfg.Window, cfg.DDOF)
	if cfg.WinsorTail > 0 {
		momentum = factor.Winsorize(momentum, cfg.WinsorTail)
		volatility = factor.Winsorize(volatility, cfg.WinsorTail)
	}
	if cfg.Standardize {
		momentum = factor.ZScore(momentum)
		volatility = factor.ZScore(volatility)
	}
	plan, err := factor.New().Add("momentum", momentum).Add("volatility", volatility).Compile()
	return plan, ComboSpec{Method: Fixed, Columns: []string{"momentum", "volatility"}, Weights: map[string]float64{"momentum": 1, "volatility": -1}}, err
}
func DefaultMomentumVolPlan() (*factor.Plan, ComboSpec, error) {
	return MomentumVolPlan(DefaultMomentumVolConfig())
}

// CloneComboSpec preserves column order and nil/empty containers.
func CloneComboSpec(spec ComboSpec) ComboSpec {
	spec.Columns = slices.Clone(spec.Columns)
	spec.Weights = maps.Clone(spec.Weights)
	return spec
}
