package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"time"
)

const privateReportJoinTimeout = 15 * time.Second

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
		go func() {
			defer close(done)
			borrow := service.Borrow()
			defer borrow.Release()
			fail := func(id string, cause error, now int64) {
				freezeErr := borrow.FreezeFailure(id, now)
				select {
				case service.reportErrors <- errors.Join(cause, freezeErr):
				default:
				}
			}
			for {
				select {
				case <-ctx.Done():
					// The source closes its channel after its accepted translation work
					// joins. Discard queued hints after stop, before closing the store.
					service.joinReportSource(stream)
					return
				case report, ok := <-stream:
					if !ok {
						if ctx.Err() == nil {
							fail("private-stream-closed", errors.New("execution: private account stream closed"), time.Now().UnixMilli())
						}
						return
					}
					now := time.Now().UnixMilli()
					if report.Err != nil || report.UnassignedExchangeID != "" || report.OrderID == "" {
						fail("private-report/"+report.UnassignedExchangeID, errors.Join(report.Err, errors.New("execution: unassigned private account report")), now)
					} else {
						if err := borrow.Recover(report.OrderID); err != nil {
							fail("private-recovery/"+report.OrderID, err, now)
						} else {
							if err := borrow.Reconcile(fmt.Sprintf("private-reconcile/%s/%d", report.OrderID, now), now); err != nil {
								fail("private-reconcile-error/"+report.OrderID, err, now)
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
// remains unassigned; liquidation never guesses an affected strategy by symbol.
func (b *SharedAccountBorrow) ApplyExternalPosition(event ExternalPositionEvent) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			var err error
			applied, err = s.store.ApplyExternalPosition(ctx, event)
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
