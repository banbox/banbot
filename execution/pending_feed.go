package execution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
)

const PendingFeedsName = "feeds-v1"

type PendingFeedID string

type PendingFeedObservation struct {
	Feed   PendingFeedID
	EndMS  int64
	Closed bool
	WarmUp bool
}

type PendingFeedProgress struct {
	EndMS int64
	Bars  int64
}

type PendingFeedExpiry struct {
	Feed         PendingFeedID
	ExpiresAtBar int64
	// A newly accepted order cannot spend its lifetime on closed facts still
	// queued before its acceptance. Zero preserves legacy feed-counter semantics.
	NotBeforeEndMS int64 `json:",omitempty"`
}

// LegacyPendingFeedBinding carries a deadline expressed in the legacy feed
// counter, not an already execution-owned PendingOrder deadline.
type LegacyPendingFeedBinding struct {
	Strategy StrategyID
	Lot      VirtualLotID
	Expiry   PendingFeedExpiry
}

func validPendingFeed(feed PendingFeedID) bool {
	return canonicalID(string(feed)) && len(feed) <= 512
}

func (e PendingFeedExpiry) validate() error {
	if !validPendingFeed(e.Feed) || e.ExpiresAtBar < 0 || e.NotBeforeEndMS < 0 {
		return errors.New("execution: invalid pending feed expiry")
	}
	return nil
}

func validatePendingFeeds(feeds map[PendingFeedID]PendingFeedProgress) error {
	for feed, progress := range feeds {
		if !validPendingFeed(feed) || progress.Bars < 0 || progress.EndMS < 0 || (progress.Bars == 0) != (progress.EndMS == 0) {
			return errors.New("execution: invalid pending feed progress")
		}
	}
	return nil
}

func (s *Store) LoadPendingFeeds(ctx context.Context) (map[PendingFeedID]PendingFeedProgress, error) {
	body, err := s.StrategyCheckpoint(ctx, PendingBookStrategy, PendingFeedsName)
	if errors.Is(err, sql.ErrNoRows) {
		return map[PendingFeedID]PendingFeedProgress{}, nil
	}
	if err != nil {
		return nil, err
	}
	feeds := map[PendingFeedID]PendingFeedProgress{}
	if err := json.Unmarshal(body, &feeds); err != nil {
		return nil, err
	}
	if err := validatePendingFeeds(feeds); err != nil {
		return nil, err
	}
	if feeds == nil {
		feeds = map[PendingFeedID]PendingFeedProgress{}
	}
	return feeds, nil
}

func (s *Store) savePendingFeeds(ctx context.Context, feeds map[PendingFeedID]PendingFeedProgress) error {
	if err := validatePendingFeeds(feeds); err != nil {
		return err
	}
	body, err := json.Marshal(feeds)
	if err != nil {
		return err
	}
	return s.SaveStrategyCheckpoint(ctx, PendingBookStrategy, PendingFeedsName, body)
}

// ObservePendingFeed persists a feed fact independently of quote availability,
// account readiness and any subsequent cancellation or rebalance failure.
func (b *SharedAccountBorrow) ObservePendingFeed(callerCtx context.Context, observation PendingFeedObservation) (bool, error) {
	accepted := false
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		return s.executorFor(ctx).local(func(ctx context.Context) error {
			if !validPendingFeed(observation.Feed) || observation.EndMS < 0 {
				return errors.New("execution: invalid pending feed observation")
			}
			if !observation.Closed || observation.WarmUp {
				return nil
			}
			if observation.EndMS == 0 {
				return errors.New("execution: closed pending feed requires an end timestamp")
			}
			return s.store.atomically(ctx, func(ctx context.Context) error {
				feeds, err := s.store.LoadPendingFeeds(ctx)
				if err != nil {
					return err
				}
				progress := feeds[observation.Feed]
				if observation.EndMS <= progress.EndMS {
					return nil
				}
				if progress.Bars == math.MaxInt64 {
					return errors.New("execution: pending feed counter overflow")
				}
				definitions, err := s.store.LoadPendingBook(ctx)
				if err != nil {
					return err
				}
				changed := false
				for n := range definitions {
					order := &definitions[n].Order
					expiry := order.EntryFeedExpiry
					if expiry == nil || expiry.Feed != observation.Feed || expiry.NotBeforeEndMS == 0 || observation.EndMS > expiry.NotBeforeEndMS || order.Canceled || order.Entry.State == Filled || order.Entry.State == Canceled || order.Entry.State == Expired {
						continue
					}
					if expiry.ExpiresAtBar == math.MaxInt64 {
						return errors.New("execution: pending feed deadline overflow")
					}
					// Offset the shared feed increment for this order only. Older
					// orders may legitimately consume the same observation.
					expiry.ExpiresAtBar++
					changed = true
				}
				feeds[observation.Feed] = PendingFeedProgress{EndMS: observation.EndMS, Bars: progress.Bars + 1}
				if err := s.store.savePendingFeeds(ctx, feeds); err != nil {
					return err
				}
				if changed {
					checkpoint, err := PendingBookCheckpoint(definitions)
					if err != nil {
						return err
					}
					if err := s.store.SaveStrategyCheckpoint(ctx, checkpoint.Strategy, checkpoint.Name, checkpoint.Payload); err != nil {
						return err
					}
				}
				accepted = true
				return nil
			})
		})
	})
	return accepted && err == nil, err
}

// BindPendingFeedExpiry runs under WithState, alongside acceptance of the entry.
// Feed clocks and quote bar clocks must never be mixed for the same request.
func (s *SharedAccount) BindPendingFeedExpiry(ctx context.Context, order *PendingOrder, feed PendingFeedID, bars int64) error {
	return s.BindPendingFeedExpiryAt(ctx, order, feed, bars, 0)
}

// BindPendingFeedExpiryAt binds new commands to the owner's acceptance clock,
// not to delayed consumption of a public fact generated before acceptance.
func (s *SharedAccount) BindPendingFeedExpiryAt(ctx context.Context, order *PendingOrder, feed PendingFeedID, bars, acceptedMS int64) error {
	if order == nil || !validPendingFeed(feed) || bars <= 0 || acceptedMS < 0 {
		return errors.New("execution: invalid pending feed binding")
	}
	feeds, err := s.store.LoadPendingFeeds(ctx)
	if err != nil {
		return err
	}
	if feeds[feed].Bars > math.MaxInt64-bars {
		return errors.New("execution: pending feed deadline overflow")
	}
	order.EntryFeedExpiry = &PendingFeedExpiry{Feed: feed, ExpiresAtBar: feeds[feed].Bars + bars, NotBeforeEndMS: acceptedMS}
	order.Entry.Conditions.StopBars = 0
	return nil
}

func (s *SharedAccount) expirePendingFeeds(ctx context.Context, definitions []PendingDefinition) error {
	feeds, err := s.store.LoadPendingFeeds(ctx)
	if err != nil {
		return err
	}
	for n := range definitions {
		order := &definitions[n].Order
		if order.EntryFeedExpiry == nil || order.Canceled || order.Entry.State == Filled || order.Entry.State == Canceled {
			continue
		}
		if err := order.EntryFeedExpiry.validate(); err != nil {
			return err
		}
		if feeds[order.EntryFeedExpiry.Feed].Bars >= order.EntryFeedExpiry.ExpiresAtBar {
			order.Entry.State, order.Entry.ReservedSteps = Expired, 0
		}
	}
	return nil
}

// SeedPendingBookWithFeeds upgrades legacy metadata without trading or replacing
// authoritative execution state. Feed counters and amended deadlines commit in
// one transaction, including when an older execution book already exists.
func (s *SharedAccount) SeedPendingBookWithFeeds(ctx context.Context, definitions []PendingDefinition, legacyFeeds map[PendingFeedID]PendingFeedProgress, bindings []LegacyPendingFeedBinding) error {
	if err := validatePendingBook(definitions); err != nil {
		return err
	}
	if err := validatePendingFeeds(legacyFeeds); err != nil {
		return err
	}
	type lotKey struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	legacy := make(map[lotKey]PendingFeedExpiry, len(bindings))
	for _, binding := range bindings {
		key := lotKey{binding.Strategy, binding.Lot}
		if !canonicalID(string(binding.Strategy)) || !canonicalID(string(binding.Lot)) {
			return errors.New("execution: invalid legacy pending feed owner")
		}
		if err := binding.Expiry.validate(); err != nil {
			return err
		}
		if _, duplicate := legacy[key]; duplicate {
			return errors.New("execution: duplicate legacy pending feed binding")
		}
		legacy[key] = binding.Expiry
	}
	return s.executorFor(ctx).local(func(ctx context.Context) error {
		return s.store.atomically(ctx, func(ctx context.Context) error {
			_, bookErr := s.store.StrategyCheckpoint(ctx, PendingBookStrategy, PendingBookName)
			if bookErr != nil && !errors.Is(bookErr, sql.ErrNoRows) {
				return bookErr
			}
			book, err := s.store.LoadPendingBook(ctx)
			if err != nil {
				return err
			}
			bookMissing := errors.Is(bookErr, sql.ErrNoRows)
			if bookMissing {
				book = append([]PendingDefinition(nil), definitions...)
			}
			_, feedErr := s.store.StrategyCheckpoint(ctx, PendingBookStrategy, PendingFeedsName)
			if feedErr != nil && !errors.Is(feedErr, sql.ErrNoRows) {
				return feedErr
			}
			feeds, err := s.store.LoadPendingFeeds(ctx)
			if err != nil {
				return err
			}
			if errors.Is(feedErr, sql.ErrNoRows) {
				feeds = legacyFeeds
				if feeds == nil {
					feeds = map[PendingFeedID]PendingFeedProgress{}
				}
				if err := s.store.savePendingFeeds(ctx, feeds); err != nil {
					return err
				}
			}
			changed := bookMissing
			for n := range book {
				order := &book[n].Order
				deadline, hasLegacy := legacy[lotKey{order.Strategy, order.Lot}]
				if !hasLegacy || order.EntryFeedExpiry != nil {
					continue
				}
				// If execution has already observed newer feed facts, preserve the
				// legacy remaining lifetime, not its unrelated absolute counter.
				remaining := max(int64(0), deadline.ExpiresAtBar-legacyFeeds[deadline.Feed].Bars)
				if feeds[deadline.Feed].Bars > math.MaxInt64-remaining {
					return errors.New("execution: migrated pending feed deadline overflow")
				}
				order.EntryFeedExpiry = &PendingFeedExpiry{Feed: deadline.Feed, ExpiresAtBar: feeds[deadline.Feed].Bars + remaining}
				order.Entry.Conditions.StopBars = 0
				changed = true
			}
			if !changed {
				return nil
			}
			checkpoint, err := PendingBookCheckpoint(book)
			if err != nil {
				return err
			}
			return s.store.saveAcceptedCheckpoint(ctx, checkpoint)
		})
	})
}
