package runner

import (
	"encoding/json"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"github.com/banbox/banbot/factor/research"
	"io"
	"sort"
)

// JSONOutput streams panels and scalar diagnostics; it never builds a cube.
type JSONOutput struct {
	Writer io.Writer
	SIDs   []int32
}

func (o *JSONOutput) Decision(f factor.Frame, p *factor.TargetPortfolio, d []factor.Diagnostic) error {
	columns := make([]string, 0, len(f.Values))
	for name := range f.Values {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	if err := research.WritePanel(o.Writer, f, columns, o.SIDs); err != nil {
		return err
	}
	event := struct {
		Kind         string
		DecisionTime int64
		Diagnostics  []factor.Diagnostic
		PortfolioID  string
		Spec         *factor.PortfolioSpec
		Targets      map[int32]float64
	}{Kind: "decision", DecisionTime: f.DecisionTime, Diagnostics: d}
	if p != nil {
		s := p.Spec()
		event.Spec = &s
		event.PortfolioID = p.ID()
		event.Targets = p.Targets()
	}
	return json.NewEncoder(o.Writer).Encode(event)
}
func (o *JSONOutput) Evaluation(r research.Report) error {
	return json.NewEncoder(o.Writer).Encode(struct {
		Kind   string
		Report research.Report
	}{"evaluation", r})
}
func (o *JSONOutput) Executed(p *factor.TargetPortfolio, s backtest.State, at int64) error {
	return o.TargetAccepted(p, s, at)
}
func (o *JSONOutput) TargetAccepted(p *factor.TargetPortfolio, s backtest.State, at int64) error {
	return json.NewEncoder(o.Writer).Encode(struct {
		Kind, PortfolioID string
		AtMS              int64
		Book              backtest.State
	}{"target-accepted", p.ID(), at, s})
}
