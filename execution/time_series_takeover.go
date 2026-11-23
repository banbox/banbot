package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
)

// TimeSeriesTakeoverBinding gives an automatically adopted lot the same
// presentation metadata a robot's create command would have committed, so the
// lot is selectable by the regular time-series exit and projected like any
// other robot lot. It carries no authority: the lot must already be a recorded
// adoption of Strategy, and ownership, quantity and cost stay untouched.
type TimeSeriesTakeoverBinding struct {
	Strategy StrategyID
	Lot      VirtualLotID
	Metadata TimeSeriesMetadata
}

// takeoverRecordsFor returns the adoption records of the listed strategies.
func takeoverRecordsFor(ctx context.Context, s *Store, strategies []StrategyID) ([]AutoTakeoverRecord, error) {
	if len(strategies) == 0 {
		return nil, nil
	}
	wanted := map[StrategyID]bool{}
	for _, strategy := range strategies {
		if !canonicalID(string(strategy)) {
			return nil, errors.New("execution: invalid takeover strategy")
		}
		wanted[strategy] = true
	}
	records, err := s.AutoTakeoverRecords(ctx)
	if err != nil {
		return nil, err
	}
	var scoped []AutoTakeoverRecord
	for _, record := range records {
		if wanted[record.Attribution.Strategy] {
			scoped = append(scoped, record)
		}
	}
	return scoped, nil
}

// BindTimeSeriesTakeover is idempotent: an identical retry is a no-op, a
// different identity for an already bound lot is refused.
func (b *SharedAccountBorrow) BindTimeSeriesTakeover(ctx context.Context, binding TimeSeriesTakeoverBinding) error {
	if !canonicalID(string(binding.Strategy)) || !canonicalID(string(binding.Lot)) {
		return errors.New("execution: invalid takeover binding")
	}
	if err := binding.Metadata.validate(); err != nil {
		return err
	}
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			scope, done := joinedOperationContext(ownerCtx, ctx)
			defer done()
			records, err := takeoverRecordsFor(scope, s.store, []StrategyID{binding.Strategy})
			if err != nil {
				return err
			}
			var adopted *AutoTakeoverRecord
			for i := range records {
				if records[i].Attribution.Lot == binding.Lot {
					adopted = &records[i]
				}
			}
			if adopted == nil {
				return errors.New("execution: lot is not an adopted position of the strategy")
			}
			if binding.Metadata.Short != (adopted.Attribution.ExpectedSignedSteps < 0) {
				return errors.New("execution: takeover metadata direction differs from the adopted position")
			}
			body, err := s.store.StrategyCheckpoint(scope, binding.Strategy, TimeSeriesMetadataName(binding.Lot))
			if err == nil {
				var old TimeSeriesMetadata
				if err = json.Unmarshal(body, &old); err != nil {
					return err
				}
				if !old.sameIdentity(binding.Metadata) {
					return errors.New("execution: time-series metadata identity changed")
				}
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			checkpoint, err := timeSeriesMetadataCheckpoint(binding.Strategy, binding.Lot, binding.Metadata)
			if err != nil {
				return err
			}
			return s.store.SaveProjectionMetadata(scope, checkpoint)
		})
	})
}

func (h *Hub) BindTimeSeriesTakeover(ctx context.Context, key AccountKey, binding TimeSeriesTakeoverBinding) error {
	return h.use(ctx, key, func(account *SharedAccountBorrow) error {
		return account.BindTimeSeriesTakeover(ctx, binding)
	})
}

func (c *HubClient) BindTimeSeriesTakeover(ctx context.Context, account AccountKey, binding TimeSeriesTakeoverBinding) error {
	_, err := c.call(ctx, "ts-takeover-bind", hubRPCRequest{Account: account, TSTakeover: &binding})
	return err
}
