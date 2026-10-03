package runtime

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
)

func TestSharedLegacyCheckpointAbsentInitialStepsReadsGenesis(t *testing.T) {
	for _, short := range []bool{false, true} {
		name := "long"
		if short {
			name = "short"
		}
		t.Run(name, func(t *testing.T) {
			row := semanticLegacyRow(.5)
			row.Short = short
			if short {
				row.Enter.Side = "sell"
			}
			f := migratedSemanticFixture(t, row)
			if err := f.rt.SharedExecution().WithState(func(s *execution.SharedAccount) error {
				body, err := s.Store().StrategyCheckpoint(context.Background(), "__legacy_bridge", "legacy-ts")
				if err != nil {
					return err
				}
				var main map[string]json.RawMessage
				if err := json.Unmarshal(body, &main); err != nil {
					return err
				}
				var orders map[string]map[string]json.RawMessage
				if err := json.Unmarshal(main["Orders"], &orders); err != nil {
					return err
				}
				for _, record := range orders {
					delete(record, "SourceEntrySteps")
				}
				main["Orders"], err = json.Marshal(orders)
				if err != nil {
					return err
				}
				body, err = json.Marshal(main)
				if err != nil {
					return err
				}
				return s.Store().SaveStrategyCheckpoint(context.Background(), "__legacy_bridge", "legacy-ts", body)
			}); err != nil {
				t.Fatal(err)
			}
			price := float64(90)
			wantHeld := int64(10)
			if short {
				price, wantHeld = 100, -10
			}
			// The remaining half creates typed postings. The pre-import half is
			// source genesis and must be added exactly once after the old field
			// is absent, rather than replaced by the post-import quantity.
			f.observe(t, price, 2)
			if f.steps(t) != wantHeld {
				t.Fatal("pending imported entry did not finish", f.steps(t))
			}
			open, lock := f.rt.Orders.GetOpenODs("default")
			lock.Lock()
			view := open[42].Clone()
			lock.Unlock()
			if view == nil || view.Enter.Filled != 1 {
				t.Fatalf("genesis plus new entry quantity lost: %+v", view)
			}
			closed, err := f.manager.ExitOrder(view, &strat.ExitReq{StratName: "legacy", CommandID: "close-imported", Force: true})
			if err != nil {
				t.Fatal(err)
			}
			if closed.Status != ormo.InOutStatusFullExit || closed.Enter.Filled != 1 || closed.Exit == nil || closed.Exit.Filled != 1 {
				t.Fatalf("closed legacy projection lost initial entry evidence: %+v", closed)
			}
		})
	}
}
