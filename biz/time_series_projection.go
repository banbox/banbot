package biz

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/banbox/banbot/execution"
)

// TimeSeriesOrderEvent preserves the committed event identity and its exact
// per-lot postings. Callbacks must deduplicate ID across a process crash or a
// lost acknowledgement; acknowledgement follows successful callback delivery.
type TimeSeriesOrderEvent struct {
	ID     string
	Record execution.TimeSeriesProjectionRecord
	Event  execution.CommittedEvent
}

// TimeSeriesProjectionConsumer consumes either a local borrow or a bound Hub
// port. Hydrate must not submit orders. Dispatch runs outside account admission
// and may submit another command; that command is consumed on the next poll.
// Hydration uses the latest snapshot, NOT historical callback state. Dispatch
// must use Event's postings/payload to reconstruct event-time order fields. This
// delivery seam is not itself the legacy ormo projection/callback converter.
type TimeSeriesProjectionConsumer struct {
	Port       execution.TimeSeriesProjectionPort
	Name       string
	Strategies []execution.StrategyID // Explicit TS scope; other contributors are ignored.
	Limit      int
	Hydrate    func(execution.TimeSeriesProjectionPage) error
	Dispatch   func(TimeSeriesOrderEvent) error
	mu         sync.Mutex
	polling    bool
}

// Poll restores presentation state and delivers a bounded committed event page.
// Restore suppresses historical callbacks and acknowledges only after hydration.
// Callers retain terminal lot keys until their final projection has been applied.
func (c *TimeSeriesProjectionConsumer) Poll(ctx context.Context, restore bool, lots []execution.TimeSeriesLotKey) (int, error) {
	c.mu.Lock()
	if c.polling {
		c.mu.Unlock()
		return 0, errors.New("biz: time-series projection is already polling")
	}
	c.polling = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.polling = false; c.mu.Unlock() }()
	if c.Port == nil || c.Name == "" || len(c.Strategies) == 0 || c.Hydrate == nil || c.Dispatch == nil {
		return 0, errors.New("biz: incomplete time-series projection consumer")
	}
	strategies := map[execution.StrategyID]bool{}
	for _, strategy := range c.Strategies {
		if strategy == "" || strategies[strategy] {
			return 0, errors.New("biz: invalid time-series projection strategy scope")
		}
		strategies[strategy] = true
	}
	limit := c.Limit
	if limit == 0 {
		limit = 512
	}
	page, err := c.Port.TimeSeriesProjection(ctx, execution.TimeSeriesProjectionRequest{Name: c.Name, Limit: limit, Lots: lots})
	if err != nil {
		return 0, err
	}
	if page.Cursor < 0 || page.Cursor > page.Snapshot.Checkpoint {
		return 0, errors.New("biz: invalid time-series projection boundary")
	}
	previous := page.Cursor
	for _, event := range page.Events {
		if event.ID == "" || event.Checkpoint <= previous || event.Checkpoint > page.Snapshot.Checkpoint {
			return 0, errors.New("biz: invalid time-series event sequence")
		}
		previous = event.Checkpoint
	}
	records := map[execution.TimeSeriesLotKey]execution.TimeSeriesProjectionRecord{}
	for _, record := range page.Records {
		if _, exists := records[record.TimeSeriesLotKey]; exists {
			return 0, errors.New("biz: duplicate time-series projection record")
		}
		records[record.TimeSeriesLotKey] = record
	}
	eventLots := make([]map[execution.TimeSeriesLotKey]bool, len(page.Events))
	for i, event := range page.Events {
		keys, err := timeSeriesEventLots(event)
		if err != nil {
			return 0, err
		}
		for key := range keys {
			if _, exists := records[key]; strategies[key.Strategy] && !exists {
				return 0, errors.New("biz: committed TS event is missing projection metadata")
			}
		}
		eventLots[i] = keys
	}
	if err := c.Hydrate(page); err != nil {
		return 0, err
	}
	if restore {
		return 0, c.Port.AdvanceProjection(ctx, c.Name, page.Snapshot.Checkpoint)
	}
	delivered := 0
	for i, event := range page.Events {
		keys := eventLots[i]
		// Records arrive deterministically sorted by the owner. Preserve that order
		// when one account event affects multiple TS contributors.
		for _, record := range page.Records {
			if !strategies[record.Strategy] || !keys[record.TimeSeriesLotKey] {
				continue
			}
			if event.Kind == "StrategyAccepted" {
				var accepted execution.StrategyAcceptedEvent
				if err := json.Unmarshal(event.Payload, &accepted); err != nil {
					return delivered, err
				}
				// Later commands may have replaced presentation fields already.
				// New TS acceptance facts retain their own immutable metadata.
				if accepted.Metadata != nil {
					record.Metadata = *accepted.Metadata
				}
			}
			selected := event
			selected.Ledger = nil
			for _, entry := range event.Ledger {
				if entry.Strategy == record.Strategy && entry.Lot == record.Lot {
					selected.Ledger = append(selected.Ledger, entry)
				}
			}
			identity, _ := json.Marshal([3]string{event.ID, string(record.Strategy), string(record.Lot)})
			if err := c.Dispatch(TimeSeriesOrderEvent{ID: string(identity), Record: record, Event: selected}); err != nil {
				return delivered, err
			}
			delivered++
		}
		if err := c.Port.AdvanceProjection(ctx, c.Name, event.Checkpoint); err != nil {
			return delivered, err
		}
	}
	return delivered, nil
}

func timeSeriesEventLots(event execution.CommittedEvent) (map[execution.TimeSeriesLotKey]bool, error) {
	keys := map[execution.TimeSeriesLotKey]bool{}
	add := func(strategy execution.StrategyID, lot execution.VirtualLotID) {
		// Capital and account cash postings can name a strategy without a lot.
		// They change the account snapshot, not an individual order callback.
		if strategy != "" && lot != "" {
			keys[execution.TimeSeriesLotKey{Strategy: strategy, Lot: lot}] = true
		}
	}
	for _, entry := range event.Ledger {
		add(entry.Strategy, entry.Lot)
	}
	switch event.Kind {
	case "StrategyAccepted":
		var accepted execution.StrategyAcceptedEvent
		if err := json.Unmarshal(event.Payload, &accepted); err != nil {
			return nil, err
		}
		add(accepted.Strategy, accepted.Lot)
	case "OrderState":
		var changed execution.OrderStateEvent
		if err := json.Unmarshal(event.Payload, &changed); err != nil {
			return nil, err
		}
		for _, allocation := range changed.Allocations {
			add(allocation.Strategy, allocation.Lot)
		}
	}
	return keys, nil
}
