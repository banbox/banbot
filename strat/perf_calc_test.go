package strat

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
)

func TestCalcJobPerfsFewJobs(t *testing.T) {
	cfg := &config.StratPerfConfig{BadWeight: 0.1, MidWeight: 0.5}
	p := &core.PerfSta{}
	perfs := []*core.JobPerf{
		{TotProfit: 1.5},
		{TotProfit: -0.5},
	}
	CalcJobPerfs(cfg, p, perfs)
	if p.Splits != nil {
		t.Fatalf("expected Splits to stay nil when clusters < 4, got %v", *p.Splits)
	}
}
