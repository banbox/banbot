package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// HubPendingFeed identifies an accepted clock whose public subscription must
// outlive its robot. Instrument resolves through the request-owned registration.
type HubPendingFeed struct {
	Account    AccountKey
	Instrument string
	Feed       PendingFeedID
}

func (h *Hub) feedAccounts() []AccountKey {
	h.mu.Lock()
	defer h.mu.Unlock()
	keys := make([]AccountKey, 0, len(h.accounts))
	for key, account := range h.accounts {
		select {
		case <-account.done:
			if account.err == nil {
				keys = append(keys, key)
			}
		default:
		}
	}
	return keys
}

// PendingFeeds returns only clocks referenced by live accepted orders. It does
// not make arbitrary public observations create account-owned feed histories.
func (h *Hub) PendingFeeds(ctx context.Context) ([]HubPendingFeed, error) {
	var feeds []HubPendingFeed
	var failures []error
	for _, key := range h.feedAccounts() {
		err := h.use(ctx, key, func(account *SharedAccountBorrow) error {
			return account.WithState(func(service *SharedAccount) error {
				definitions, err := service.store.LoadPendingBook(ctx)
				if err != nil {
					return err
				}
				seen := map[PendingFeedID]string{}
				var accountFeeds []HubPendingFeed
				for _, definition := range definitions {
					order := definition.Order
					if order.EntryFeedExpiry == nil || order.Canceled || order.Entry.State == Filled || order.Entry.State == Canceled || order.Entry.State == Expired {
						continue
					}
					feed := order.EntryFeedExpiry.Feed
					if previous := seen[feed]; previous != "" && previous != definition.Instrument.ID {
						return errors.New("execution: pending feed clock is bound to conflicting account instruments")
					}
					if seen[feed] == "" {
						seen[feed] = definition.Instrument.ID
						accountFeeds = append(accountFeeds, HubPendingFeed{Account: key, Instrument: definition.Instrument.ID, Feed: feed})
					}
				}
				feeds = append(feeds, accountFeeds...)
				return nil
			})
		})
		if err != nil {
			failures = append(failures, err)
		}
	}
	sort.Slice(feeds, func(i, j int) bool {
		if fmt.Sprint(feeds[i].Account) != fmt.Sprint(feeds[j].Account) {
			return fmt.Sprint(feeds[i].Account) < fmt.Sprint(feeds[j].Account)
		}
		return feeds[i].Feed < feeds[j].Feed
	})
	return feeds, errors.Join(failures...)
}

// ObservePublicFeed persists one public fact for every interested account,
// then advances expiry without requiring a robot callback. Per-account failures
// are joined only after the other owners have had a chance to advance.
func (h *Hub) ObservePublicFeed(ctx context.Context, observation PendingFeedObservation) error {
	feeds, err := h.PendingFeeds(ctx)
	var failures []error
	if err != nil {
		failures = append(failures, err)
	}
	for _, feed := range feeds {
		h.mu.Lock()
		authorized := h.publicFeeds[feed]
		h.mu.Unlock()
		if feed.Feed != observation.Feed || !authorized {
			continue
		}
		err := h.use(ctx, feed.Account, func(account *SharedAccountBorrow) error {
			_, err := account.ObservePendingFeed(ctx, observation)
			if err != nil {
				return err
			}
			if !observation.Closed || observation.WarmUp {
				return nil
			}
			// A lost/failed advancement must be retried even when its durable fact
			// was already accepted. Quote and send gates still apply on every tick.
			return account.AdvancePendingExecution(ctx, observation.EndMS)
		})
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// AuthorizePublicFeed is called only by host composition after the public
// symbol authority verifies account/instrument/SID identity, never over RPC.
func (h *Hub) AuthorizePublicFeed(binding HubPendingFeed) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrOwnerStopped
	}
	if h.accounts[binding.Account] == nil || !validPendingFeed(binding.Feed) || binding.Instrument == "" {
		return errors.New("execution: invalid public feed binding")
	}
	if h.publicFeeds == nil {
		h.publicFeeds = map[HubPendingFeed]bool{}
	}
	h.publicFeeds[binding] = true
	return nil
}

// PendingFeedAdmissionValidator belongs to host composition. In-process custom
// clocks retain their existing application contract; remote clocks must prove
// that their host can observe and retain the requested public feed.
type PendingFeedAdmissionValidator func(context.Context, HubPendingFeed) error

func (h *Hub) SetPendingFeedAdmission(validator PendingFeedAdmissionValidator) {
	h.mu.Lock()
	h.feedAdmission = validator
	h.mu.Unlock()
}

func (h *Hub) applyPendingWithFeedAdmission(caller context.Context, account *SharedAccountBorrow, command PendingCommand) (int64, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return 0, err
	}
	var owned PendingCommand
	if err := json.Unmarshal(body, &owned); err != nil {
		return 0, err
	}
	if err := owned.validate(); err != nil {
		return 0, err
	}
	hasReceipt := func(ctx context.Context, service *SharedAccount) (bool, error) {
		_, err := service.store.StrategyCheckpoint(ctx, owned.Strategy, "pending-command:"+owned.ID)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return err == nil, err
	}
	var revision int64
	execute := func(admissionFailure error) error {
		return account.use(func(service *SharedAccount) error {
			ctx, done := joinedOperationContext(account.ctx, caller)
			defer done()
			if admissionFailure != nil {
				accepted, err := hasReceipt(ctx, service)
				if err != nil {
					return err
				}
				if !accepted {
					return admissionFailure
				}
			}
			var err error
			revision, err = service.applyPendingCommand(ctx, owned, sha256.Sum256(body), time.Now().UnixMilli())
			return err
		})
	}
	if owned.Action == PendingCreate && owned.Create.StopBars > 0 {
		accepted := false
		err = account.use(func(service *SharedAccount) error {
			ctx, done := joinedOperationContext(account.ctx, caller)
			defer done()
			var err error
			accepted, err = hasReceipt(ctx, service)
			return err
		})
		if err != nil {
			return 0, err
		}
		if !accepted {
			h.mu.Lock()
			validator := h.feedAdmission
			h.mu.Unlock()
			admissionErr := errors.New("execution: hub pending feed admission is not ready")
			if validator != nil {
				ctx, done := joinedOperationContext(account.ctx, caller)
				admissionErr = validator(ctx, HubPendingFeed{Account: account.AccountKey(), Instrument: owned.Create.Instrument, Feed: owned.Create.Feed})
				done()
			}
			if admissionErr != nil {
				// Admission is outside the account lock: public subscription startup must
				// not stall protections. Recheck a concurrently committed receipt before
				// returning failure so a lost ACK still resumes the same accepted fact.
				err = execute(admissionErr)
				account.service.notifyCommitted()
				return revision, err
			}
		}
	}
	err = execute(nil)
	account.service.notifyCommitted()
	return revision, err
}
