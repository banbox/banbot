package runner

import (
	"errors"
	"fmt"
	"strings"

	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/research"
)

// FactorSpec is everything a plain cross-section strategy has to say: named
// factor outputs. Combine is optional; the default is an equal-weight mix of
// all outputs, exactly what the expression path does.
type FactorSpec struct {
	Outputs map[string]*factor.Node
	Combine research.ComboSpec
}

// FactorContext carries the decision timeframe and numeric params of one run.
type FactorContext struct {
	TimeFrame string
	Params    *Params
}

// Open returns the kline open price node on the decision timeframe.
func (f FactorContext) Open() *factor.Node { return f.Field("open") }

// High returns the kline high price node on the decision timeframe.
func (f FactorContext) High() *factor.Node { return f.Field("high") }

// Low returns the kline low price node on the decision timeframe.
func (f FactorContext) Low() *factor.Node { return f.Field("low") }

// Close is the usual starting node: the closed `field` of the default kline source.
func (f FactorContext) Close() *factor.Node { return f.Field("close") }

// Volume returns the kline base volume node on the decision timeframe.
func (f FactorContext) Volume() *factor.Node { return f.Field("volume") }

// Quote returns the kline quote turnover node on the decision timeframe.
func (f FactorContext) Quote() *factor.Node { return f.Field("quote") }

// BuyVolume returns the kline taker buy base volume node on the decision timeframe.
func (f FactorContext) BuyVolume() *factor.Node { return f.Field("buy_volume") }

// Field is a kline field on the decision timeframe.
func (f FactorContext) Field(name string) *factor.Node {
	return factor.Field("kline", name, f.TimeFrame)
}

// SourceField reads a field from a non-kline data source (such as "funding_rate",
// "open_interest", "premium_kline", "index_kline", or "mark_kline") on f.TimeFrame,
// automatically applying causal AsOf alignment when a built-in source is stored on
// a different cadence (for example "funding_rate" at "8h").
func (f FactorContext) SourceField(source, name string) *factor.Node {
	src, fld, srcTF, maxAge := resolveContextSourceSpec(source, name, f.TimeFrame)
	if srcTF != "" && srcTF != strings.TrimSpace(f.TimeFrame) {
		return factor.AsOfField(src, fld, srcTF, f.TimeFrame, maxAge)
	}
	return factor.Field(src, fld, f.TimeFrame)
}

// RefField reads a kline field of a specific benchmark symbol on f.TimeFrame.
func (f FactorContext) RefField(symbol, name string) *factor.Node {
	return factor.RefAssetField(symbol, "kline", name, f.TimeFrame)
}

// RefSourceField reads any source field of a specific benchmark symbol on f.TimeFrame,
// applying causal AsOf alignment when a built-in source has a distinct native timeframe.
func (f FactorContext) RefSourceField(symbol, source, name string) *factor.Node {
	src, fld, srcTF, maxAge := resolveContextSourceSpec(source, name, f.TimeFrame)
	if srcTF != "" && srcTF != strings.TrimSpace(f.TimeFrame) {
		return factor.AsOfRefAssetField(symbol, src, fld, srcTF, f.TimeFrame, maxAge)
	}
	return factor.RefAssetField(symbol, src, fld, f.TimeFrame)
}

func resolveContextSourceSpec(source, name, decisionTF string) (src, fld, sourceTF string, maxAge int64) {
	src = strings.TrimSpace(source)
	fld = strings.TrimSpace(name)
	tf := strings.TrimSpace(decisionTF)
	tfMS, hasTF := factor.ParseTimeFrameMS(tf)
	switch src {
	case "funding_rate":
		if fld == "" {
			fld = "rate"
		}
		if tf != "" && tf != "event" && tf != "8h" {
			age := int64(24 * 3600 * 1000)
			if hasTF && 2*tfMS > age {
				age = 2 * tfMS
			}
			return src, fld, "8h", age
		}
	case "open_interest":
		if fld == "" || fld == "open_interest" {
			fld = "oi"
		}
		if tf != "" && tf != "event" && !isBuiltinOpenInterestTF(tf) {
			switch {
			case hasTF && tfMS < 5*60*1000:
				return src, fld, "5m", 30 * 60 * 1000
			case tf == "8h":
				return src, fld, "4h", 24 * 3600 * 1000
			case hasTF && tfMS > 24*3600*1000:
				age := int64(7 * 24 * 3600 * 1000)
				if 2*tfMS > age {
					age = 2 * tfMS
				}
				return src, fld, "1d", age
			default:
				age := int64(24 * 3600 * 1000)
				if hasTF && 2*tfMS > age {
					age = 2 * tfMS
				}
				return src, fld, "1h", age
			}
		}
	}
	return src, fld, "", 0
}

func isBuiltinOpenInterestTF(tf string) bool {
	switch tf {
	case "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d":
		return true
	default:
		return false
	}
}

// SubTF evaluates a source-timeframe subgraph and causally aligns it onto f.TimeFrame.
func (f FactorContext) SubTF(sourceTimeFrame string, build func(FactorContext) *factor.Node, maxAge ...int64) *factor.Node {
	var age int64
	if len(maxAge) > 0 {
		age = maxAge[0]
	}
	var inner *factor.Node
	if build != nil {
		inner = build(FactorContext{TimeFrame: sourceTimeFrame, Params: f.Params})
	}
	return factor.AsOfNode(inner, f.TimeFrame, age)
}

// SectorCode returns the built-in per-asset numeric crypto sector ID node on the decision timeframe.
func (f FactorContext) SectorCode() *factor.Node {
	return factor.SectorCode(f.TimeFrame)
}

// RegisterFactor registers a cross-section strategy in one call:
//
//	runner.RegisterFactor("my-mom", func(c runner.FactorContext) (runner.FactorSpec, error) {
//	    w := c.Params.Int("window", 24)
//	    return runner.FactorSpec{Outputs: map[string]*factor.Node{
//	        "mom": factor.ZScore(factor.Return(c.Close(), w)),
//	    }}, c.Params.Err()
//	})
//
// Then select it with `run_policy: [{name: my-mom, engine: factor, ...}]`; the
// default top/bottom-k portfolio (`params.k`) applies unless the strategy has
// its own bound builder. It is a thin layer over RegisterDefinition.
func RegisterFactor(name string, build func(FactorContext) (FactorSpec, error)) error {
	if name == "" || build == nil {
		return errors.New("runner: factor needs name and build function")
	}
	return RegisterDefinition(name, func(c Config) (*factor.Plan, research.ComboSpec, error) {
		params := c.Params()
		spec, err := build(FactorContext{TimeFrame: c.Factor.TimeFrame, Params: params})
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		// A builder that forgets to return Params.Err() must not hide bad values.
		if err = params.Err(); err != nil {
			return nil, research.ComboSpec{}, err
		}
		if len(spec.Outputs) == 0 {
			return nil, research.ComboSpec{}, fmt.Errorf("runner: factor %q declares no outputs", name)
		}
		plan, err := factor.Compile(spec.Outputs)
		if err != nil {
			return nil, research.ComboSpec{}, err
		}
		combo := research.CloneComboSpec(spec.Combine)
		if combo.Method == "" {
			combo.Method = research.Equal
		}
		if len(combo.Columns) == 0 {
			combo.Columns = defaultComboColumns(plan.Outputs(), allocationReservedColumns(c)...)
			if len(combo.Columns) == 0 {
				return nil, research.ComboSpec{}, fmt.Errorf("runner: factor %q declares no score outputs", name)
			}
		}
		return plan, combo, nil
	})
}
