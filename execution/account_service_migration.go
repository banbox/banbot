package execution

import (
	"context"
	"errors"
	"time"
)

func (b *SharedAccountBorrow) ImportLegacyMigration(request LegacyMigration, checkStopped func() error) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		s.ready = false
		if checkStopped == nil || !s.opts.AuthoritativeSnapshot {
			return errors.New("execution: legacy migration requires joined owner and complete venue inventory")
		}
		var ownerContext context.Context
		request.Preflight = func() error {
			if err := checkStopped(); err != nil {
				return err
			}
			ctx, cancel := context.WithTimeout(ownerContext, 15*time.Second)
			defer cancel()
			source, ok := s.opts.Adapter.(interface {
				MigrationSnapshot(context.Context) (LegacyVenueSnapshot, error)
			})
			if !ok {
				return errors.New("execution: exact migration snapshot capability unavailable")
			}
			venue, err := source.MigrationSnapshot(ctx)
			if err != nil {
				return err
			}
			if !venue.Complete || !venue.AccountCash.Equal(request.VenueSnapshot.AccountCash) {
				return errors.New("execution: migration venue cash/inventory changed")
			}
			expected := map[string]VirtualLot{}
			for _, p := range request.VenueSnapshot.Positions {
				expected[p.Instrument.ID] = p
			}
			for _, p := range venue.Positions {
				old, ok := expected[p.Instrument.ID]
				if !ok || !sameAccountInstrument(old.Instrument, p.Instrument) || old.SignedSteps != p.SignedSteps || !old.CostBasis.Equal(p.CostBasis) {
					return errors.New("execution: migration venue position or cost basis changed")
				}
				delete(expected, p.Instrument.ID)
			}
			if len(expected) != 0 {
				return errors.New("execution: migration venue position missing")
			}
			orders := map[string]LegacyVenueOrder{}
			for _, o := range request.VenueSnapshot.OpenOrders {
				orders[o.ExchangeID] = o
			}
			for _, o := range venue.OpenOrders {
				old, ok := orders[o.ExchangeID]
				if !ok || old.ClientID != o.ClientID || old.Instrument != o.Instrument || old.Side != o.Side || old.Steps != o.Steps || old.FilledSteps != o.FilledSteps || !old.Cost.Equal(o.Cost) || !old.Fee.Equal(o.Fee) {
					return errors.New("execution: migration order identity/quantity/cost/fee changed")
				}
				delete(orders, o.ExchangeID)
			}
			if len(orders) != 0 {
				return errors.New("execution: migration venue order disappeared; rebuild snapshot")
			}
			return nil
		}
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			ownerContext = ctx
			var err error
			applied, err = s.store.ImportMigration(ctx, request)
			return err
		})
	})
	return applied, err
}
