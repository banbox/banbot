package runner

import (
	"errors"
	"fmt"

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

// Close is the usual starting node: the closed `field` of the default kline source.
func (f FactorContext) Close() *factor.Node { return f.Field("close") }

// Field is a kline field on the decision timeframe.
func (f FactorContext) Field(name string) *factor.Node {
	return factor.Field("kline", name, f.TimeFrame)
}

// RefField reads a kline field of a specific benchmark symbol on f.TimeFrame.
func (f FactorContext) RefField(symbol, name string) *factor.Node {
	return factor.RefAssetField(symbol, "kline", name, f.TimeFrame)
}

// RefSourceField reads any source field of a specific benchmark symbol on f.TimeFrame.
func (f FactorContext) RefSourceField(symbol, source, name string) *factor.Node {
	return factor.RefAssetField(symbol, source, name, f.TimeFrame)
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
