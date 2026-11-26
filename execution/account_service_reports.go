package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/banbox/banexg/log"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"time"
)

const privateReportJoinTimeout = 15 * time.Second

// reconcileRaceAttempts bounds refresh+reconcile runs per tick when the venue
// state moved between the two steps; reconcileRacePause separates them.
const reconcileRaceAttempts = 3

var reconcileRacePause = 500 * time.Millisecond

// privateReportTick is the worker's tick period; tests shorten it.
var privateReportTick = 5 * time.Second

// reconcileRaceError excludes the cash mismatch, which settleAndReconcile
// already retries after an on-demand funding fetch.
func reconcileRaceError(err error) bool {
	return errors.Is(err, ErrReconciliationOrderAbsent) || errors.Is(err, ErrReconciliationPositionMismatch)
}

// Serialize checkpoint capture and commit under the service's exclusive store
// lease. Each committed failure advances the durable checkpoint; a later
// service lifecycle therefore cannot reuse an earlier immutable event ID.
// Close joins each drain once and retains its result, including failed commits.
func (s *SharedAccount) freezeFailure(ctx context.Context, reason string, now int64) error {
	s.failureFreezeMu.Lock()
	defer s.failureFreezeMu.Unlock()
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	event := CashEvent{ID: fmt.Sprintf("%s/%d", reason, snapshot.Checkpoint), Kind: ExternalCashChange, Postings: []CashPosting{{Amount: decimal.Zero}}, AtMS: now}
	_, err = s.store.ApplyCashEvent(ctx, event)
	return err
}

// The source must close its report channel after cancellation and join its
// transport in Adapter.Close. A broken source cannot hold the core Join forever.
func (s *SharedAccount) reportShutdownFailure(cause error) {
	// Process.Close has already stopped owner admission. The service still owns
	// the open store and joins this worker before Store.Close, so commit only the
	// idempotent risk freeze directly; never resume exchange work after stop.
	freezeErr := s.freezeFailure(context.Background(), "private-stream-shutdown", time.Now().UnixMilli())
	err := errors.Join(cause, freezeErr)
	s.mu.Lock()
	s.ready = false
	s.reportShutdownErr = errors.Join(s.reportShutdownErr, err)
	s.mu.Unlock()
	select {
	case s.reportErrors <- err:
	default:
	}
}

func drainReportSource(stream <-chan BanexgStreamReport) error {
	timer := time.NewTimer(privateReportJoinTimeout)
	defer timer.Stop()
	queued := 0
	for {
		select {
		case _, ok := <-stream:
			if !ok {
				if queued > 0 {
					return fmt.Errorf("execution: %d private recovery hints remain unsettled after cancellation; durable orders require startup recovery", queued)
				}
				return nil
			}
			queued++
		case <-timer.C:
			return errors.Join(context.DeadlineExceeded, fmt.Errorf("execution: private report source did not close after cancellation; %d recovery hints require startup recovery", queued))
		}
	}
}

func (s *SharedAccount) joinReportSource(stream <-chan BanexgStreamReport) {
	if err := drainReportSource(stream); err != nil {
		s.reportShutdownFailure(err)
	}
}

// StartReports starts exactly one process-owned private account stream. The
// adapter stream provides identity wake-ups; only authoritative recovery queries
// mutate fills. Stream failure/unknown identity persists a risk freeze.
func (b *SharedAccountBorrow) StartReports() error {
	return b.StartReportsContext(b.ctx)
}

// Startup cancellation applies only until registration completes. The accepted
// stream then belongs to the account service, independently of its borrower.
func (b *SharedAccountBorrow) StartReportsContext(callerCtx context.Context) error {
	return b.use(func(s *SharedAccount) error {
		if s.reportDone != nil {
			select {
			case <-s.reportDone:
				s.ready = false
				return errors.New("execution: private account report worker stopped; recovery and a new account session are required")
			default:
				return nil
			}
		}
		source, ok := s.opts.Adapter.(interface {
			Reports(context.Context) (<-chan BanexgStreamReport, error)
		})
		if !ok {
			s.ready = false
			return errors.New("execution: shared adapter has no private report stream")
		}
		var ctx context.Context
		var cancel context.CancelFunc
		var stream <-chan BanexgStreamReport
		err := s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ctx, cancel = context.WithCancel(ownerCtx)
			stopService := context.AfterFunc(s.ctx, cancel)
			streamCancel := cancel
			cancel = func() { stopService(); streamCancel() }
			setupCtx, setupDone := joinedOperationContext(ownerCtx, b.ctx)
			defer setupDone()
			setupCtx, callerDone := joinedOperationContext(setupCtx, callerCtx)
			defer callerDone()
			stopSetup := context.AfterFunc(setupCtx, streamCancel)
			var err error
			stream, err = source.Reports(ctx)
			stopSetup()
			if setupErr := setupCtx.Err(); err != nil || setupErr != nil {
				streamCancel()
				s.ready = false
				var joinErr error
				if stream != nil {
					joinErr = drainReportSource(stream)
					if joinErr != nil {
						s.ready = false
						freezeErr := s.freezeFailure(context.Background(), "private-stream-setup-shutdown", time.Now().UnixMilli())
						s.reportShutdownErr = errors.Join(s.reportShutdownErr, joinErr, freezeErr)
						joinErr = s.reportShutdownErr
					}
				}
				return errors.Join(err, setupErr, joinErr)
			}
			return err
		})
		if err != nil {
			if cancel != nil {
				cancel()
			}
			return err
		}
		if stream == nil {
			cancel()
			s.ready = false
			return errors.New("execution: nil account report stream")
		}
		s.reportCancel = cancel
		s.reportDone = make(chan struct{})
		s.reportErrors = make(chan error, 1)
		service := s
		done := s.reportDone
		tick := privateReportTick
		go func() {
			defer close(done)
			borrow := service.Borrow()
			defer borrow.Release()
			fail := func(id string, cause error, now int64) {
				// A step interrupted by this worker's own shutdown says nothing about
				// the ledger; do not turn a stop into a persisted freeze.
				if ctx.Err() != nil && (errors.Is(cause, context.Canceled) || errors.Is(cause, ErrReleased) || errors.Is(cause, ErrOwnerStopped)) {
					return
				}
				// The cause otherwise only reaches reportErrors, which nobody may read.
				log.Error("account frozen by the private report worker", zap.String("reason", id), zap.Error(cause))
				freezeErr := borrow.FreezeFailure(id, now)
				select {
				case service.reportErrors <- errors.Join(cause, freezeErr):
				default:
				}
			}
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			var lastFundingMS, lastReconcileMS int64
			var outage transientOutage
			// recoverCash fetches funding settlements. It is not run on every tick:
			// a reconciliation cash mismatch fetches on demand (settleAndReconcile),
			// and fundingRecoveryInterval keeps the watermark advancing. Polling it
			// every tick exceeded the venue's income rate limit within minutes.
			recoverCash := func() error {
				recovery, ok := service.opts.Adapter.(interface {
					RecoverCash(context.Context, *SharedAccountBorrow) error
				})
				if !ok {
					return nil
				}
				if err := recovery.RecoverCash(ctx, borrow); err != nil {
					return err
				}
				lastFundingMS = time.Now().UnixMilli()
				return nil
			}
			// settleAndReconcile returns (skipped, err). skipped means a transient
			// venue read failed: the account stays unready until a later attempt
			// succeeds, nothing is frozen and the worker keeps running.
			settleAndReconcile := func(id string) (bool, error) {
				if time.Now().UnixMilli()-lastFundingMS >= fundingRecoveryInterval.Milliseconds() {
					if err := recoverCash(); err != nil {
						if !transientAccountReadError(err) {
							return false, err
						}
						// Back off instead of hitting a rate-limited endpoint every tick.
						// Reconcile still decides: without a new settlement it succeeds.
						lastFundingMS = time.Now().UnixMilli() - fundingRecoveryInterval.Milliseconds() + fundingRetryBackoff.Milliseconds()
						outage.noteRateLimit(id+"-funding", err)
						log.Warn("scheduled funding fetch failed transiently; retrying later", zap.String("stage", id), zap.Error(err))
					}
				}
				now := time.Now().UnixMilli()
				err := borrow.Reconcile(fmt.Sprintf("%s/%d", id, now), now)
				if errors.Is(err, ErrReconciliationCashMismatch) {
					// A settlement since the last fetch explains the difference.
					// A genuine external change still mismatches after the fetch.
					if ferr := recoverCash(); ferr != nil {
						if outage.transient(id+"-funding", ferr) {
							return true, nil
						}
						return false, ferr
					}
					now = time.Now().UnixMilli()
					err = borrow.Reconcile(fmt.Sprintf("%s/%d", id, now), now)
				}
				if err != nil && outage.transient(id, err) {
					return true, nil
				}
				return false, err
			}
			reconcile := func(reason string) bool {
				if err := borrow.RefreshWorkingOrders(ctx, time.Now().UnixMilli()); err != nil {
					if outage.transient(reason+"-recovery", err) {
						return true
					}
					fail(reason+"-recovery", err, time.Now().UnixMilli())
					return false
				}
				skipped, err := settleAndReconcile(reason + "-reconcile")
				// A resting order that filled or was cancelled between the refresh
				// above and the venue snapshot disagrees once; refresh again so the
				// fill is processed before any freeze. Persistent disagreement still
				// freezes after the bounded retries.
				for attempt := 1; attempt < reconcileRaceAttempts && err != nil && reconcileRaceError(err); attempt++ {
					select {
					case <-ctx.Done():
						attempt = reconcileRaceAttempts
						continue
					case <-time.After(reconcileRacePause):
					}
					if rerr := borrow.RefreshWorkingOrders(ctx, time.Now().UnixMilli()); rerr != nil {
						if outage.transient(reason+"-recovery", rerr) {
							return true
						}
						fail(reason+"-recovery", rerr, time.Now().UnixMilli())
						return false
					}
					skipped, err = settleAndReconcile(reason + "-reconcile")
				}
				if err != nil {
					fail(reason+"-reconcile", err, time.Now().UnixMilli())
					return false
				}
				if skipped {
					return true
				}
				lastReconcileMS = time.Now().UnixMilli()
				outage.recovered()
				if err := borrow.AdvancePendingExecution(ctx, time.Now().UnixMilli()); err != nil {
					if outage.transient(reason+"-pending-execution", err) {
						return true
					}
					fail(reason+"-pending-execution", err, time.Now().UnixMilli())
					return false
				}
				// Keeps the loss-stop equity history current while no robot asks.
				// It is advisory sampling: a failure must not freeze the account.
				_, _ = borrow.LossStopStatus(ctx, time.Now().UnixMilli())
				service.notifyCommitted()
				return true
			}
			for {
				select {
				case <-ctx.Done():
					// The source closes its channel after its accepted translation work
					// joins. Discard queued hints after stop, before closing the store.
					service.joinReportSource(stream)
					return
				case <-ticker.C:
					if outage.paused() {
						continue
					}
					// A full venue snapshot costs request weight shared by every
					// process on the host IP. The private stream wakes reconciliation
					// on account changes; between periodic snapshots a reconciled
					// account only advances pending execution on visible quotes.
					if outage.sinceMS == 0 && time.Now().UnixMilli()-lastReconcileMS < privateReconcileInterval.Milliseconds() {
						reconciled := false
						err := borrow.WithState(func(s *SharedAccount) error {
							if reconciled = s.Reconciled(); !reconciled {
								return nil
							}
							return s.AdvancePendingExecution(ctx, time.Now().UnixMilli())
						})
						if reconciled && (err == nil || outage.transient("private-periodic-pending-execution", err)) {
							continue
						}
						if reconciled {
							fail("private-periodic-pending-execution", err, time.Now().UnixMilli())
							cancel()
							service.joinReportSource(stream)
							return
						}
					}
					if !reconcile("private-periodic") {
						cancel()
						service.joinReportSource(stream)
						return
					}
				case report, ok := <-stream:
					if !ok {
						if ctx.Err() == nil {
							fail("private-stream-closed", errors.New("execution: private account stream closed"), time.Now().UnixMilli())
						}
						return
					}
					now := time.Now().UnixMilli()
					if report.ExternalOrder != nil {
						if err := borrow.RecoverExternalReport(ctx, report); err != nil {
							fail("private-external/"+report.UnassignedExchangeID, err, now)
						} else if err := borrow.Reconcile(fmt.Sprintf("private-external-reconcile/%s/%d", report.UnassignedExchangeID, now), now); err != nil {
							fail("private-external-reconcile/"+report.UnassignedExchangeID, err, now)
						} else if err := borrow.AdvancePendingExecution(ctx, time.Now().UnixMilli()); err != nil && !outage.transient("private-external-pending/"+report.UnassignedExchangeID, err) {
							fail("private-external-pending/"+report.UnassignedExchangeID, err, time.Now().UnixMilli())
						}
					} else if report.AccountChanged && report.Err == nil && report.UnassignedExchangeID == "" && report.OrderID == "" {
						// A wake-up only: the first periodic tick after a rate-limit
						// pause reconciles the same change.
						if outage.paused() {
							continue
						}
						if !reconcile("private-account") {
							cancel()
							service.joinReportSource(stream)
							return
						}
					} else if report.Err != nil || report.UnassignedExchangeID != "" || report.OrderID == "" {
						fail("private-report/"+report.UnassignedExchangeID, errors.Join(report.Err, errors.New("execution: unassigned private account report")), now)
					} else {
						if err := borrow.Recover(report.OrderID); err != nil {
							fail("private-recovery/"+report.OrderID, err, now)
						} else {
							// A transient read failure leaves the account unready; the
							// next periodic tick reconciles again.
							// The recovered fill is committed either way: listeners are
							// notified below even when reconciliation was skipped.
							if skipped, err := settleAndReconcile("private-reconcile/" + report.OrderID); err != nil {
								fail("private-reconcile-error/"+report.OrderID, err, now)
							} else if !skipped {
								lastReconcileMS = time.Now().UnixMilli()
								outage.recovered()
								if err := borrow.AdvancePendingExecution(ctx, time.Now().UnixMilli()); err != nil && !outage.transient("private-pending-execution/"+report.OrderID, err) {
									fail("private-pending-execution/"+report.OrderID, err, time.Now().UnixMilli())
								}
							}
						}
					}
					service.notifyCommitted()
				}
			}
		}()
		return nil
	})
}

func (b *SharedAccountBorrow) ReportErrors() (<-chan error, error) {
	var result <-chan error
	err := b.use(func(s *SharedAccount) error {
		if s.reportErrors == nil {
			return errors.New("execution: account reports not started")
		}
		result = s.reportErrors
		return nil
	})
	return result, err
}

// SubscribeCommitted projects already committed events outside the owner lock.
// Runtime callers must wrap callback admission and unregister at Stop.
func (b *SharedAccountBorrow) SubscribeCommitted(callback func()) (func(), error) {
	if callback == nil {
		return nil, errors.New("execution: committed callback required")
	}
	var id uint64
	err := b.use(func(s *SharedAccount) error {
		if s.listeners == nil {
			s.listeners = map[uint64]func(){}
		}
		s.listenerSerial++
		id = s.listenerSerial
		s.listeners[id] = callback
		return nil
	})
	if err != nil {
		return nil, err
	}
	service := b.service
	return func() { service.mu.Lock(); delete(service.listeners, id); service.mu.Unlock() }, nil
}
func (s *SharedAccount) notifyCommitted() {
	s.mu.Lock()
	listeners := make([]func(), 0, len(s.listeners))
	for _, callback := range s.listeners {
		listeners = append(listeners, callback)
	}
	s.mu.Unlock()
	for _, callback := range listeners {
		callback()
	}
}

// FreezeFailure records a distinct internally detected failure occurrence, even
// when a prior occurrence was reconciled at the same clock timestamp. Explicit
// externally supplied immutable events continue to use Freeze or CashEvent.
func (b *SharedAccountBorrow) FreezeFailure(reason string, now int64) error {
	return b.use(func(s *SharedAccount) error {
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error { return s.freezeFailure(ctx, reason, now) })
	})
}

func (b *SharedAccountBorrow) Freeze(id string, now int64) error {
	return b.use(func(s *SharedAccount) error {
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			_, err := s.store.ApplyCashEvent(ctx, CashEvent{ID: id, Kind: ExternalCashChange, Postings: []CashPosting{{Amount: decimal.Zero}}, AtMS: now})
			return err
		})
	})
}

// ApplyExternalPosition is the explicit manual/forced trade path. Attribution
// remains unassigned unless configured takeover proves its opening history;
// liquidation never guesses an affected strategy by symbol.
func (b *SharedAccountBorrow) ApplyExternalPosition(event ExternalPositionEvent) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			var err error
			applied, err = s.store.ApplyExternalPosition(ctx, event)
			if err == nil && s.opts.TakeoverStrategy != "" {
				_, err = s.store.AutoTakeoverExternal(ctx, s.opts.TakeoverStrategy, event.AtMS)
			}
			return err
		})
	})
	if err == nil {
		b.service.notifyCommitted()
	}
	return applied, err
}

// Committed quantity activity identifies its instrument through its source
// type. RealAccountFill has no virtual lot, but its immutable order does.
func fundingActivityInstrument(ctx context.Context, store *Store, event CommittedEvent) (Instrument, error) {
	var instrument Instrument
	switch event.Kind {
	case "ExchangeFill":
		var report FillReport
		if err := json.Unmarshal(event.Payload, &report); err != nil {
			return instrument, err
		}
		if report.OrderID == "" {
			return instrument, errors.New("execution: funding history fill lacks order identity")
		}
		order, err := store.Order(ctx, report.OrderID)
		if err != nil {
			return instrument, err
		}
		instrument = order.Intent.Instrument
	case "InternalFill":
		var match InternalMatch
		if err := json.Unmarshal(event.Payload, &match); err != nil {
			return instrument, err
		}
		instrument = match.Instrument
	case "TerminalSettlement":
		var terminal TerminalSettlement
		if err := json.Unmarshal(event.Payload, &terminal); err != nil {
			return instrument, err
		}
		instrument = terminal.Instrument
	case "OptionExpiry":
		var expiry OptionExpiry
		if err := json.Unmarshal(event.Payload, &expiry); err != nil {
			return instrument, err
		}
		instrument = expiry.Instrument
	case "ExternalAttribution":
		var attribution ExternalAttribution
		if err := json.Unmarshal(event.Payload, &attribution); err != nil {
			return instrument, err
		}
		instrument = attribution.Instrument
	case string(ExternalCashChange), string(Liquidation):
		var external ExternalPositionEvent
		if err := json.Unmarshal(event.Payload, &external); err != nil {
			return instrument, err
		}
		instrument = external.Instrument
	default:
		return instrument, fmt.Errorf("execution: unsupported funding history activity %s", event.Kind)
	}
	if err := instrument.Validate(); err != nil {
		return instrument, err
	}
	return instrument, nil
}

// ApplyFunding receives an authoritative signed account settlement. Late
// delivery is safe only while the affected virtual lots have not changed after
// settlement; otherwise admission is frozen until an attributed recovery is
// supplied. Historical closed lots participate in this check.
func (b *SharedAccountBorrow) ApplyFunding(event FundingSettlement) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			var cursor int64
			late := false
			duplicate := false
			for {
				page, err := s.store.EventsAfter(ctx, cursor, 512)
				if err != nil {
					return err
				}
				if len(page) == 0 {
					break
				}
				for _, committed := range page {
					if committed.ID == event.ID {
						duplicate = true
					}
					activityAfterSettlement := false
					for _, entry := range committed.Ledger {
						if entry.QuantityDelta == 0 || entry.AtMS <= event.AtMS {
							continue
						}
						activityAfterSettlement = true
						break
					}
					if !activityAfterSettlement {
						continue
					}
					instrument, err := fundingActivityInstrument(ctx, s.store, committed)
					if err != nil {
						s.ready = false
						freezeErr := s.freezeFailure(ctx, "funding-history/"+event.ID, event.AtMS)
						return errors.Join(fmt.Errorf("execution: cannot classify funding history event %s: %w", committed.ID, err), freezeErr)
					}
					if instrument.ID == event.Instrument.ID {
						late = true
					}
				}
				cursor = page[len(page)-1].Checkpoint
			}
			if late && !duplicate {
				s.ready = false
				freezeErr := s.freezeFailure(ctx, "late-funding/"+event.ID, event.AtMS)
				return errors.Join(errors.New("execution: late funding settlement cannot use changed virtual lots"), freezeErr)
			}
			var err error
			applied, err = s.store.ApplyFunding(ctx, event)
			return err
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied, err
}
