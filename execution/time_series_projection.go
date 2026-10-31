package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
)

type TimeSeriesLotKey struct {
	Strategy StrategyID
	Lot      VirtualLotID
}

// Lots includes already projected terminal lots which no longer occur in the
// active snapshot. The owner also discovers keys from pending orders and events.
type TimeSeriesProjectionRequest struct {
	Name  string
	Limit int
	Lots  []TimeSeriesLotKey
}

type TimeSeriesProjectionRecord struct {
	TimeSeriesLotKey
	Metadata TimeSeriesMetadata
}

// TimeSeriesProjectionPage is captured under account admission. Events are a
// bounded prefix after Cursor; Snapshot may be newer than that prefix. Consumers
// must derive callback deltas from Events, not from snapshot differences.
type TimeSeriesProjectionPage struct {
	Cursor     int64
	Snapshot   AccountSnapshot
	Pending    []PendingDefinition
	Records    []TimeSeriesProjectionRecord
	Events     []CommittedEvent
	Reconciled bool
}

// TimeSeriesProjectionPort is the robot-facing seam. Implementations never
// expose a Store or accept transaction closures from a robot.
type TimeSeriesProjectionPort interface {
	TimeSeriesProjection(context.Context, TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error)
	AdvanceProjection(context.Context, string, int64) error
}

func (b *SharedAccountBorrow) TimeSeriesProjection(ctx context.Context, request TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error) {
	var page TimeSeriesProjectionPage
	if !canonicalID(request.Name) || request.Limit < 1 || request.Limit > 10000 || len(request.Lots) > 10000 {
		return page, errors.New("execution: invalid time-series projection request")
	}
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			scope, done := joinedOperationContext(ownerCtx, ctx)
			defer done()
			var err error
			page.Cursor, err = s.store.ProjectionCursor(scope, request.Name)
			if err != nil {
				return err
			}
			page.Snapshot, err = s.store.Snapshot(scope)
			if err != nil {
				return err
			}
			page.Events, err = s.store.EventsAfter(scope, page.Cursor, request.Limit)
			if err != nil {
				return err
			}
			page.Pending, err = s.store.LoadPendingBook(scope)
			if err != nil {
				return err
			}
			page.Reconciled = s.ready
			keys := map[TimeSeriesLotKey]bool{}
			add := func(strategy StrategyID, lot VirtualLotID) {
				if strategy != "" && lot != "" {
					keys[TimeSeriesLotKey{strategy, lot}] = true
				}
			}
			for _, key := range request.Lots {
				if !canonicalID(string(key.Strategy)) || !canonicalID(string(key.Lot)) {
					return errors.New("execution: invalid projection lot")
				}
				add(key.Strategy, key.Lot)
			}
			for _, lot := range page.Snapshot.Lots {
				add(lot.Strategy, lot.ID)
			}
			for _, definition := range page.Pending {
				add(definition.Order.Strategy, definition.Order.Lot)
			}
			for _, event := range page.Events {
				for _, entry := range event.Ledger {
					add(entry.Strategy, entry.Lot)
				}
				switch event.Kind {
				case "StrategyAccepted":
					var accepted StrategyAcceptedEvent
					if err := json.Unmarshal(event.Payload, &accepted); err != nil {
						return err
					}
					add(accepted.Strategy, accepted.Lot)
				case "OrderState":
					var changed OrderStateEvent
					if err := json.Unmarshal(event.Payload, &changed); err != nil {
						return err
					}
					for _, allocation := range changed.Allocations {
						add(allocation.Strategy, allocation.Lot)
					}
				}
			}
			ordered := make([]TimeSeriesLotKey, 0, len(keys))
			for key := range keys {
				ordered = append(ordered, key)
			}
			sort.Slice(ordered, func(i, j int) bool {
				if ordered[i].Strategy != ordered[j].Strategy {
					return ordered[i].Strategy < ordered[j].Strategy
				}
				return ordered[i].Lot < ordered[j].Lot
			})
			for _, key := range ordered {
				body, err := s.store.StrategyCheckpoint(scope, key.Strategy, TimeSeriesMetadataName(key.Lot))
				if errors.Is(err, sql.ErrNoRows) {
					continue
				} // Factor lots have no TS metadata.
				if err != nil {
					return err
				}
				record := TimeSeriesProjectionRecord{TimeSeriesLotKey: key}
				if err := json.Unmarshal(body, &record.Metadata); err != nil {
					return err
				}
				page.Records = append(page.Records, record)
			}
			return nil
		})
	})
	return page, err
}

func (h *Hub) TimeSeriesProjection(ctx context.Context, key AccountKey, request TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error) {
	var page TimeSeriesProjectionPage
	err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
		var err error
		page, err = account.TimeSeriesProjection(ctx, request)
		return err
	})
	return page, err
}

func (c *HubClient) TimeSeriesProjection(ctx context.Context, account AccountKey, request TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error) {
	result, err := c.call(ctx, "ts-projection", hubRPCRequest{Account: account, TSProjection: &request})
	if err != nil {
		return TimeSeriesProjectionPage{}, err
	}
	if result.TSProjection == nil {
		return TimeSeriesProjectionPage{}, errors.New("execution: hub returned no time-series projection")
	}
	return *result.TSProjection, nil
}

// HubTimeSeriesProjection binds a remote account to the same port as a local
// borrow. Closing a client does not release the hub's account owner.
type HubTimeSeriesProjection struct {
	Client  *HubClient
	Account AccountKey
}

func (p HubTimeSeriesProjection) TimeSeriesProjection(ctx context.Context, request TimeSeriesProjectionRequest) (TimeSeriesProjectionPage, error) {
	if p.Client == nil {
		return TimeSeriesProjectionPage{}, errors.New("execution: projection client missing")
	}
	return p.Client.TimeSeriesProjection(ctx, p.Account, request)
}
func (p HubTimeSeriesProjection) AdvanceProjection(ctx context.Context, name string, through int64) error {
	if p.Client == nil {
		return errors.New("execution: projection client missing")
	}
	return p.Client.AdvanceProjection(ctx, p.Account, name, through)
}

var _ TimeSeriesProjectionPort = (*SharedAccountBorrow)(nil)
var _ TimeSeriesProjectionPort = HubTimeSeriesProjection{}
