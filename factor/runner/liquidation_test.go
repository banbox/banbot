package runner

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/banbox/banbot/core"
)

// A weights-mode run whose strategy NAV reaches <= 0 (here: costs eat the account at every full rebalance) ends as a
// bankruptcy: positions closed at the marks, NAV 0, no further decisions, run.json liquidated fields, no error.
func TestWeightsRunNavDepletedIsLiquidation(t *testing.T) {
	cfg := archiveConfig(t, false)
	cfg.Manifest.Costs.FeeRate = 0.45
	var buf bytes.Buffer
	out := &JSONOutput{Writer: &buf, SIDs: cfg.Snapshot.Universe.Evaluation}
	res, err := Run(context.Background(), cfg, nil, out)
	if err != nil {
		t.Fatalf("a bankrupt strategy is a result, not an error: %v", err)
	}
	if !res.Liquidated || res.LiquidatedCause != core.LiquidationEquityDepleted || res.LiquidatedAtMS <= 0 || res.LiquidatedNAV > 0 {
		t.Fatalf("liquidation fields: %+v", res)
	}
	if res.Book.NAV != 0 || res.Book.Cash != 0 || len(res.Book.Quantities) != 0 {
		t.Fatalf("book after liquidation: %+v", res.Book)
	}
	var lastNav float64
	var lastAt int64
	sc := bufio.NewScanner(&buf)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var ev struct {
			Kind string
			AtMS int64
			NAV  float64
		}
		if json.Unmarshal(sc.Bytes(), &ev) == nil && ev.Kind == "nav" {
			lastNav, lastAt = ev.NAV, ev.AtMS
		}
	}
	if lastNav != 0 || lastAt != res.LiquidatedAtMS {
		t.Fatalf("last nav event %v at %d, want 0 at %d", lastNav, lastAt, res.LiquidatedAtMS)
	}
	// a healthy run is unchanged
	healthy, err := Run(context.Background(), archiveConfig(t, false), nil, &JSONOutput{Writer: &bytes.Buffer{}})
	if err != nil || healthy.Liquidated || healthy.Book.NAV <= 0 {
		t.Fatalf("healthy run: %v %+v", err, healthy.Book)
	}
}
