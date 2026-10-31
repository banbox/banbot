package biz

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/banbox/banbot/execution"
)

func TestTimeSeriesProjectionAcceptanceUsesEventTimeMetadata(t *testing.T) {
	body, err := json.Marshal(execution.StrategyAcceptedEvent{Strategy: "s", Lot: "lot", Kind: execution.ExitIntent, Metadata: &execution.TimeSeriesMetadata{ExitTag: "original"}})
	if err != nil {
		t.Fatal(err)
	}
	page := projectionTestPage()
	page.Records[0].Metadata.ExitTag = "newer-command"
	page.Events = []execution.CommittedEvent{{ID: "accepted", Kind: "StrategyAccepted", Checkpoint: 2, Payload: body}}
	port := &projectionTestPort{page: page}
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"},
		Hydrate: func(page execution.TimeSeriesProjectionPage) error {
			if page.Records[0].Metadata.ExitTag != "newer-command" {
				t.Fatal("latest hydration must not be rewound")
			}
			return nil
		},
		Dispatch: func(event TimeSeriesOrderEvent) error {
			if event.Record.Metadata.ExitTag != "original" {
				t.Fatal("historical callback used newer exit metadata", event.Record)
			}
			return nil
		},
	}
	if n, err := consumer.Poll(context.Background(), false, nil); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}

type projectionTestPort struct {
	page    execution.TimeSeriesProjectionPage
	through int64
	ackErr  error
	request execution.TimeSeriesProjectionRequest
}

func (p *projectionTestPort) TimeSeriesProjection(_ context.Context, request execution.TimeSeriesProjectionRequest) (execution.TimeSeriesProjectionPage, error) {
	p.request = request
	result := p.page
	result.Cursor = p.through
	result.Events = nil
	for _, event := range p.page.Events {
		if event.Checkpoint > p.through {
			result.Events = append(result.Events, event)
		}
	}
	return result, nil
}
func (p *projectionTestPort) AdvanceProjection(_ context.Context, _ string, through int64) error {
	if p.ackErr != nil {
		return p.ackErr
	}
	p.through = through
	return nil
}
func projectionTestPage() execution.TimeSeriesProjectionPage {
	return execution.TimeSeriesProjectionPage{
		Snapshot: execution.AccountSnapshot{Checkpoint: 3},
		Records:  []execution.TimeSeriesProjectionRecord{{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "s", Lot: "lot"}}},
		Events:   []execution.CommittedEvent{{ID: "fill", Kind: "ExchangeFill", Checkpoint: 2, Ledger: []execution.LedgerEntry{{Strategy: "s", Lot: "lot", QuantityDelta: 2}, {Strategy: "factor", Lot: "other", QuantityDelta: -2}}}, {ID: "status", Kind: "Other", Checkpoint: 3}},
	}
}
func TestTimeSeriesProjectionReplayAfterLostAckPreservesCallbackIdentity(t *testing.T) {
	ctx := context.Background()
	port := &projectionTestPort{page: projectionTestPage(), ackErr: errors.New("lost ack")}
	var ids []string
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { return nil }, Dispatch: func(event TimeSeriesOrderEvent) error {
		ids = append(ids, event.ID)
		if len(event.Event.Ledger) != 1 || event.Event.Ledger[0].QuantityDelta != 2 {
			t.Fatal("cross-lot postings leaked", event)
		}
		return nil
	}}
	if n, err := consumer.Poll(ctx, false, nil); err == nil || n != 1 || port.through != 0 {
		t.Fatalf("lost ack: %d %v %+v", n, err, port)
	}
	port.ackErr = nil
	if n, err := consumer.Poll(ctx, false, nil); err != nil || n != 1 || port.through != 3 {
		t.Fatalf("replay: %d %v %+v", n, err, port)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("unstable callback IDs: %v", ids)
	}
	if n, err := consumer.Poll(ctx, false, nil); err != nil || n != 0 || len(ids) != 2 {
		t.Fatalf("acknowledged callback replayed: %d %v", n, err)
	}
}
func TestTimeSeriesProjectionHydrationAndDispatchFailuresDoNotAcknowledge(t *testing.T) {
	for _, hydrateFailure := range []bool{true, false} {
		t.Run(map[bool]string{true: "hydrate", false: "callback"}[hydrateFailure], func(t *testing.T) {
			port := &projectionTestPort{page: projectionTestPage()}
			failure := errors.New("failed")
			consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error {
				if hydrateFailure {
					return failure
				}
				return nil
			}, Dispatch: func(TimeSeriesOrderEvent) error { return failure }}
			if _, err := consumer.Poll(context.Background(), false, nil); !errors.Is(err, failure) || port.through != 0 {
				t.Fatalf("acknowledged failed apply: %v %d", err, port.through)
			}
		})
	}
}
func TestTimeSeriesProjectionRestoreSuppressesCallbacksAndForwardsTerminalKeys(t *testing.T) {
	port := &projectionTestPort{page: projectionTestPage()}
	hydrated := false
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { hydrated = true; return nil }, Dispatch: func(TimeSeriesOrderEvent) error { t.Fatal("restore dispatched historical callback"); return nil }}
	keys := []execution.TimeSeriesLotKey{{Strategy: "s", Lot: "closed"}}
	if n, err := consumer.Poll(context.Background(), true, keys); err != nil || n != 0 || !hydrated || port.through != 3 {
		t.Fatalf("restore: %d %v %+v", n, err, port)
	}
	if len(port.request.Lots) != 1 || port.request.Lots[0] != keys[0] {
		t.Fatal("terminal keys lost")
	}
}
func TestTimeSeriesProjectionRejectsFutureOrUnorderedEventsBeforeHydration(t *testing.T) {
	port := &projectionTestPort{page: projectionTestPage()}
	port.page.Events[0].Checkpoint = 4
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { t.Fatal("hydrated invalid page"); return nil }, Dispatch: func(TimeSeriesOrderEvent) error { return nil }}
	if _, err := consumer.Poll(context.Background(), false, nil); err == nil {
		t.Fatal("future event accepted")
	}
}
func TestTimeSeriesProjectionCallbackCanReenterWithoutDeadlock(t *testing.T) {
	port := &projectionTestPort{page: projectionTestPage()}
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { return nil }}
	consumer.Dispatch = func(TimeSeriesOrderEvent) error {
		if _, err := consumer.Poll(context.Background(), false, nil); err == nil {
			t.Fatal("recursive poll accepted")
		}
		return nil
	}
	if _, err := consumer.Poll(context.Background(), false, nil); err != nil {
		t.Fatal(err)
	}
}

func TestTimeSeriesProjectionStrategyCashWithoutLotIsNotAnOrderEvent(t *testing.T) {
	port := &projectionTestPort{page: execution.TimeSeriesProjectionPage{
		Snapshot: execution.AccountSnapshot{Checkpoint: 1},
		Events: []execution.CommittedEvent{{ID: "capital", Kind: "Capital", Checkpoint: 1,
			Ledger: []execution.LedgerEntry{{Strategy: "s"}},
		}},
	}}
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"},
		Hydrate: func(execution.TimeSeriesProjectionPage) error { return nil },
		Dispatch: func(TimeSeriesOrderEvent) error {
			t.Fatal("strategy cash was treated as a lot event")
			return nil
		},
	}
	if n, err := consumer.Poll(context.Background(), false, nil); err != nil || n != 0 || port.through != 1 {
		t.Fatal("cash event blocked order projection", n, err, port.through)
	}
}

func TestTimeSeriesProjectionMissingTSMetadataFailsWithoutAcknowledgement(t *testing.T) {
	port := &projectionTestPort{page: projectionTestPage()}
	port.page.Records = nil
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { t.Fatal("hydrated incomplete TS metadata"); return nil }, Dispatch: func(TimeSeriesOrderEvent) error { t.Fatal("dispatched incomplete TS metadata"); return nil }}
	if _, err := consumer.Poll(context.Background(), false, nil); err == nil || port.through != 0 {
		t.Fatalf("missing TS metadata acknowledged: %v %d", err, port.through)
	}
}

func TestTimeSeriesProjectionMultiLotCallbackFailureReplaysWholeEvent(t *testing.T) {
	port := &projectionTestPort{page: projectionTestPage()}
	port.page.Records = append(port.page.Records, execution.TimeSeriesProjectionRecord{TimeSeriesLotKey: execution.TimeSeriesLotKey{Strategy: "s", Lot: "second"}})
	port.page.Events[0].Ledger = append(port.page.Events[0].Ledger, execution.LedgerEntry{Strategy: "s", Lot: "second", QuantityDelta: 3})
	var ids []string
	fail := true
	consumer := &TimeSeriesProjectionConsumer{Port: port, Name: "robot", Strategies: []execution.StrategyID{"s"}, Hydrate: func(execution.TimeSeriesProjectionPage) error { return nil }, Dispatch: func(event TimeSeriesOrderEvent) error {
		ids = append(ids, event.ID)
		if event.Record.Lot == "second" && fail {
			return errors.New("second callback failed")
		}
		return nil
	}}
	if n, err := consumer.Poll(context.Background(), false, nil); err == nil || n != 1 || port.through != 0 {
		t.Fatalf("partial account event acknowledged: %d %v %d", n, err, port.through)
	}
	fail = false
	if n, err := consumer.Poll(context.Background(), false, nil); err != nil || n != 2 || port.through != 3 {
		t.Fatalf("event replay failed: %d %v %d", n, err, port.through)
	}
	if len(ids) != 4 || ids[0] != ids[2] || ids[1] != ids[3] {
		t.Fatal("per-lot replay identities changed", ids)
	}
}
