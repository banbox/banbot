package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

// TimeSeriesLegacyBinding gives a lot created by the embedded SharedOrderMgr
// (which writes the pending book directly, without create commands) the
// time-series metadata a robot's create command would have committed, so a
// Hub robot can select, project and exit it. It is the one-time migration
// counterpart of TimeSeriesTakeoverBinding, with the same non-authority: the
// lot must already exist (a pending definition or a held lot of Strategy) and
// ownership, quantity, cost and protections stay untouched.
type TimeSeriesLegacyBinding struct {
	Strategy StrategyID
	Lot      VirtualLotID
	Metadata TimeSeriesMetadata
	// OCOGroup/OCOSeq join a still-open entry to the owner's OCO enforcement
	// exactly like PendingCreateSpec.OCOGroup (same validation). OCOSeq must be
	// positive with a group and orders members by creation; the migration uses
	// the legacy order ID, which SharedOrderMgr allocates in creation order and
	// which stays below every sequence nextOCOSeq hands out afterwards.
	OCOGroup string
	OCOSeq   int64
	// Entry rebinds a still-open entry whose StopBars lifetime runs on a quote
	// bar clock (Conditions.StopBars with CreatedBar) to the feed clock the hub
	// path uses. It is required for such an entry, ignored otherwise.
	Entry *TimeSeriesLegacyEntryClock
}

// TimeSeriesLegacyEntryClock carries the legacy StopBars lifetime that is left
// (StopBars minus the bars already consumed) as that many further closed bars
// of Feed. Zero remaining bars expire the entry on the next tick.
type TimeSeriesLegacyEntryClock struct {
	Feed          PendingFeedID
	RemainingBars int64
}

// BindTimeSeriesLegacy is idempotent: an identical retry is a no-op. It
// refuses a lot that is unknown to the owner, a direction that differs from
// the entry, a lot that already carries metadata of a different identity
// (for instance committed by a command), and an order ID already used by
// another lot of the strategy.
func (b *SharedAccountBorrow) BindTimeSeriesLegacy(ctx context.Context, binding TimeSeriesLegacyBinding) error {
	if !canonicalID(string(binding.Strategy)) || !canonicalID(string(binding.Lot)) {
		return errors.New("execution: invalid legacy binding")
	}
	if err := binding.Metadata.validate(); err != nil {
		return err
	}
	if binding.OCOGroup != "" && !validOCOGroup(binding.OCOGroup) || binding.OCOSeq < 0 || binding.OCOGroup == "" && binding.OCOSeq != 0 || binding.OCOGroup != "" && binding.OCOSeq == 0 {
		return errors.New("execution: invalid legacy OCO group")
	}
	if e := binding.Entry; e != nil && (!validPendingFeed(e.Feed) || e.RemainingBars < 0) {
		return errors.New("execution: invalid legacy entry clock")
	}
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			scope, done := joinedOperationContext(ownerCtx, ctx)
			defer done()
			definitions, err := s.store.LoadPendingBook(scope)
			if err != nil {
				return err
			}
			snapshot, err := s.store.Snapshot(scope)
			if err != nil {
				return err
			}
			known, signed := false, int64(0)
			short := binding.Metadata.Short
			for _, d := range definitions {
				if d.Order.Strategy == binding.Strategy && d.Order.Lot == binding.Lot {
					known, short = true, d.Order.Entry.Side == Sell
				}
			}
			for _, lot := range snapshot.Lots {
				if lot.Strategy == binding.Strategy && lot.ID == binding.Lot && lot.SignedSteps != 0 {
					signed = lot.SignedSteps
				}
			}
			if !known && signed == 0 {
				return errors.New("execution: legacy lot has neither a pending definition nor a held position")
			}
			if !known {
				short = signed < 0
			}
			if binding.Metadata.Short != short {
				return errors.New("execution: legacy metadata direction differs from the lot")
			}
			return s.store.atomically(scope, func(scope context.Context) error {
				if err := s.bindLegacyMetadata(scope, binding, definitions); err != nil {
					return err
				}
				return s.bindLegacyPending(scope, binding, definitions)
			})
		})
	})
}

func (s *SharedAccount) bindLegacyMetadata(scope context.Context, binding TimeSeriesLegacyBinding, definitions []PendingDefinition) error {
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
	for _, d := range definitions {
		if d.Order.Strategy != binding.Strategy || d.Order.Lot == binding.Lot {
			continue
		}
		other, err := s.store.StrategyCheckpoint(scope, binding.Strategy, TimeSeriesMetadataName(d.Order.Lot))
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		var m TimeSeriesMetadata
		if err = json.Unmarshal(other, &m); err != nil {
			return err
		}
		if m.OrderID == binding.Metadata.OrderID {
			return fmt.Errorf("execution: legacy order ID %d already used by lot %s", m.OrderID, d.Order.Lot)
		}
	}
	checkpoint, err := timeSeriesMetadataCheckpoint(binding.Strategy, binding.Lot, binding.Metadata)
	if err != nil {
		return err
	}
	return s.store.SaveProjectionMetadata(scope, checkpoint)
}

// bindLegacyPending completes the owner-side state of a migrated lot. It is
// idempotent and never touches quantity, cost or the cash ledger.
//
//   - Every intent of the lot loses its quote-bar CreatedBar: that value only
//     guards against a quote Bar below it, and the hub path's quotes carry Bar 0,
//     which would otherwise fail every owner tick with "invalid execution
//     observation" (freezing the whole account's pending evaluation). It has no
//     other effect without StopBars.
//   - A still-open entry with a quote-bar StopBars lifetime is rebound to the
//     feed clock (binding.Entry), exactly like a hub-created entry; without the
//     remaining lifetime the bind is refused instead of guessing.
//   - A still-open entry joins its OCO group; a conflicting group is refused.
func (s *SharedAccount) bindLegacyPending(ctx context.Context, binding TimeSeriesLegacyBinding, definitions []PendingDefinition) error {
	n := -1
	for i, d := range definitions {
		if d.Order.Strategy == binding.Strategy && d.Order.Lot == binding.Lot {
			n = i
		}
	}
	if n < 0 {
		return nil // a held lot without a pending definition has no entry to adjust
	}
	o := &definitions[n].Order
	changed := false
	clear := func(i *EligibleIntent) {
		if i.Conditions.CreatedBar != 0 {
			i.Conditions.CreatedBar, changed = 0, true
		}
	}
	if o.Entry.Conditions.StopBars > 0 && ocoEntryOpen(o) {
		if binding.Entry == nil {
			return errors.New("execution: legacy entry has a quote-bar StopBars lifetime but no remaining lifetime was given")
		}
		feeds, err := s.store.LoadPendingFeeds(ctx)
		if err != nil {
			return err
		}
		if feeds[binding.Entry.Feed].Bars > math.MaxInt64-binding.Entry.RemainingBars {
			return errors.New("execution: pending feed deadline overflow")
		}
		o.EntryFeedExpiry = &PendingFeedExpiry{Feed: binding.Entry.Feed, ExpiresAtBar: feeds[binding.Entry.Feed].Bars + binding.Entry.RemainingBars}
		o.Entry.Conditions.StopBars, changed = 0, true
	}
	clear(&o.Entry)
	if o.Exit != nil {
		clear(o.Exit)
	}
	for k, p := range o.Protections {
		clear(&p)
		o.Protections[k] = p
	}
	if binding.OCOGroup != "" && ocoEntryOpen(o) {
		switch {
		case o.OCOGroup == "":
			o.OCOGroup, o.OCOSeq, changed = binding.OCOGroup, binding.OCOSeq, true
		case o.OCOGroup != binding.OCOGroup || o.OCOSeq != binding.OCOSeq:
			return errors.New("execution: legacy OCO group conflicts with the pending entry")
		}
	}
	if !changed {
		return nil
	}
	checkpoint, err := PendingBookCheckpoint(definitions)
	if err != nil {
		return err
	}
	return s.store.saveAcceptedCheckpoint(ctx, checkpoint)
}
