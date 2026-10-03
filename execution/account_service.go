package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// SharedExecutionOptions describes a process-owned service, never a second
// SQLite view or a borrower-owned adapter. Adapter identity must match exactly
// across borrowers. AuthoritativeSnapshot is an explicit composition proof
// that Snapshot includes all open orders, including native conditional orders.
type SharedExecutionOptions struct {
	Memory                bool
	HistoryPath           string
	StorePath             string
	SenderLeaseDir        string
	Adapter               ExecutionAdapter
	AuthoritativeSnapshot bool
}

func (o SharedExecutionOptions) Validate() error {
	if o.Memory {
		if paper, simulated := o.Adapter.(*PaperAdapter); !simulated || paper == nil || o.StorePath != "" || o.SenderLeaseDir != "" {
			return errors.New("execution: memory account requires a simulated venue and no store or sender lease paths")
		}
		if o.HistoryPath != "" && !filepath.IsAbs(o.HistoryPath) {
			return errors.New("execution: optional memory history needs an absolute path")
		}
		return nil
	}
	if o.HistoryPath != "" {
		return errors.New("execution: cold memory history is only available for simulated memory accounts")
	}
	if !filepath.IsAbs(o.StorePath) || !filepath.IsAbs(o.SenderLeaseDir) || o.Adapter == nil || reflect.ValueOf(o.Adapter).Kind() != reflect.Pointer || reflect.ValueOf(o.Adapter).IsNil() {
		return errors.New("execution: shared execution needs absolute store/lease paths and one pointer-owned adapter")
	}
	return nil
}

func (o SharedExecutionOptions) Same(other SharedExecutionOptions) bool {
	return o.Memory == other.Memory && filepath.Clean(o.HistoryPath) == filepath.Clean(other.HistoryPath) && filepath.Clean(o.StorePath) == filepath.Clean(other.StorePath) && filepath.Clean(o.SenderLeaseDir) == filepath.Clean(other.SenderLeaseDir) && o.Adapter == other.Adapter && o.AuthoritativeSnapshot == other.AuthoritativeSnapshot
}

// SharedAccount is owned by Process. Its keeper is independent of Runtime
// borrowers, so releasing the final Runtime does not invalidate the executor.
type SharedAccount struct {
	mu                sync.Mutex
	ctx               context.Context
	cancel            context.CancelFunc
	owner             *AccountHandle
	store             *Store
	executor          *OwnerExecutor
	opts              SharedExecutionOptions
	ready             bool
	closed            bool
	closeErr          error
	reportCancel      context.CancelFunc
	reportDone        chan struct{}
	reportErrors      chan error
	reportShutdownErr error
	failureFreezeMu   sync.Mutex
	listeners         map[uint64]func()
	listenerSerial    uint64
	markets           map[string]accountMarket
}

func NewSharedAccount(owner *AccountHandle, opts SharedExecutionOptions) (*SharedAccount, error) {
	if owner == nil {
		return nil, ErrOwnerToken
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	var store *Store
	var err error
	if opts.Memory {
		store, err = NewMemoryStoreWithHistory(owner.Token().Key, opts.HistoryPath)
	} else {
		store, err = OpenStoreWithLeaseDir(opts.StorePath, owner.Token().Key, opts.SenderLeaseDir)
	}
	if err != nil {
		return nil, err
	}
	if binder, ok := opts.Adapter.(interface{ BindStore(*Store) error }); ok {
		if err := binder.BindStore(store); err != nil {
			_ = store.Close()
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SharedAccount{ctx: ctx, cancel: cancel, owner: owner, store: store, executor: &OwnerExecutor{Handle: owner, Token: owner.Token(), Store: store, Adapter: opts.Adapter}, opts: opts}, nil
}

func (s *SharedAccount) Matches(opts SharedExecutionOptions) bool { return s.opts.Same(opts) }

// Borrowers share the same service/executor but retain independent admission.
type SharedAccountBorrow struct {
	mu       sync.Mutex
	service  *SharedAccount
	released bool
	ctx      context.Context
	cancel   context.CancelFunc
}

var ErrSharedCheckpoint = errors.New("execution: shared account checkpoint changed; rebuild combined targets")

func (b *SharedAccountBorrow) LatestPlan(ctx context.Context) (Plan, error) {
	var result Plan
	err := b.use(func(s *SharedAccount) error { var err error; result, err = s.store.LatestPlan(ctx); return err })
	return result, err
}

func (s *SharedAccount) Borrow() *SharedAccountBorrow {
	ctx, cancel := context.WithCancel(s.ctx)
	return &SharedAccountBorrow{service: s, ctx: ctx, cancel: cancel}
}
func (b *SharedAccountBorrow) Service() *SharedAccount { return b.service }
func (b *SharedAccountBorrow) Stop() {
	if b != nil && b.cancel != nil {
		b.cancel()
	}
}
func (b *SharedAccountBorrow) Release() { b.Stop(); b.mu.Lock(); b.released = true; b.mu.Unlock() }
func (b *SharedAccountBorrow) use(call func(*SharedAccount) error) error {
	if b == nil || b.service == nil {
		return ErrReleased
	}
	b.mu.Lock()
	released := b.released
	b.mu.Unlock()
	if released || b.ctx.Err() != nil {
		return ErrReleased
	}
	s := b.service
	s.mu.Lock()
	defer s.mu.Unlock()
	if b.ctx.Err() != nil {
		return ErrReleased
	}
	if s.closed {
		return ErrOwnerStopped
	}
	return call(s)
}

func (b *SharedAccountBorrow) Snapshot(ctx context.Context) (AccountSnapshot, error) {
	var result AccountSnapshot
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var err error
			result, err = s.store.Snapshot(ownerCtx)
			return err
		})
	})
	return result, err
}

// AtSnapshot runs a small, synchronous boundary change while the account owner
// holds the same serialized revision that produced the snapshot. The callback
// must not perform I/O, invoke account methods, or wait on execution work.
func (b *SharedAccountBorrow) AtSnapshot(ctx context.Context, change func(AccountSnapshot) error) error {
	if change == nil {
		return errors.New("execution: snapshot boundary needs a callback")
	}
	return b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			snapshot, err := s.store.Snapshot(ownerCtx)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return change(snapshot)
		})
	})
}

// CashEvent is explicit funding/allocation, never an inferred strategy deposit.
func (b *SharedAccountBorrow) CashEvent(event CashEvent) error {
	_, err := b.CashEventApplied(event)
	return err
}

func (b *SharedAccountBorrow) CashEventApplied(event CashEvent) (bool, error) {
	var applied bool
	err := b.use(func(s *SharedAccount) error {
		return s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
			var err error
			applied, err = s.store.ApplyCashEvent(ctx, event)
			return err
		})
	})
	if err == nil && applied {
		b.service.notifyCommitted()
	}
	return applied, err
}

func (b *SharedAccountBorrow) StrategyTotals(ctx context.Context, strategy StrategyID) (StrategyTotals, error) {
	var result StrategyTotals
	err := b.use(func(s *SharedAccount) error {
		var err error
		result, err = s.store.StrategyTotals(ctx, strategy)
		return err
	})
	return result, err
}

// Reconcile checks open-order identity in addition to cash and positions. Any
// unmatched/manual order persists a risk freeze before returning. A failed or
// incomplete snapshot never publishes ready and never clears a prior freeze.
func (b *SharedAccountBorrow) Reconcile(id string, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		s.ready = false
		return s.owner.DoLocal(s.owner.Token(), func(ownerCtx context.Context) error {
			ownerCtx, done := joinedOperationContext(ownerCtx, b.ctx)
			defer done()
			if !s.opts.AuthoritativeSnapshot {
				return errors.New("execution: complete open/conditional order snapshot capability is unproven")
			}
			ctx, cancel := context.WithTimeout(ownerCtx, 15*time.Second)
			defer cancel()
			venue, err := s.opts.Adapter.Snapshot(ctx)
			if err != nil {
				return err
			}
			local, err := s.store.Snapshot(ctx)
			if err != nil {
				return err
			}
			known := make(map[string]bool)
			for _, o := range local.Orders {
				if o.State == OrderAcknowledged || o.State == OrderPartial {
					known[o.ExchangeID] = true
				}
			}
			seen := make(map[string]bool)
			for _, o := range venue.OpenOrders {
				if !o.Found || !o.Authoritative || !o.Complete || o.Canceled || o.Receipt.ExchangeID == "" || !known[o.Receipt.ExchangeID] || seen[o.Receipt.ExchangeID] {
					freezeErr := s.freezeFailure(context.Background(), id+"/unmatched-order", nowMS)
					return errors.Join(errors.New("execution: unmatched or incomplete venue open order freezes shared account"), freezeErr)
				}
				seen[o.Receipt.ExchangeID] = true
			}
			for exID := range known {
				if !seen[exID] {
					return fmt.Errorf("execution: local open order %s absent from authoritative snapshot; recover first", exID)
				}
			}
			cash, err := decimal.NewFromString(venue.Cash)
			if err != nil {
				return err
			}
			// The prefix labels the caller, while the timestamp and full snapshot
			// identify its revision. Repeated startup prefixes never alias a new
			// authoritative snapshot; identical revisions remain idempotent.
			revision, _ := json.Marshal(struct {
				Cash      string
				Positions map[string]int64
				AtMS      int64
			}{cash.String(), venue.Positions, nowMS})
			snapshotID := fmt.Sprintf("%s/%x", id, sha256.Sum256(revision))
			_, err = s.store.Reconcile(ctx, AccountReconciliation{ID: snapshotID, AccountCash: cash, Positions: venue.Positions, AtMS: nowMS})
			if err == nil {
				s.ready = true
			}
			return err
		})
	})
}

// RecoverPersisted refreshes all transmitted durable orders before startup
// reconciliation. It never resends Prepared or uncertain orders and preserves
// frozen attribution/highwater dedup while admitting no new trading. Stable
// bounded pages include terminal history for offline fills/fees. Missing or
// incomplete historical queries fail startup rather than assuming finality.
func (b *SharedAccountBorrow) RecoverPersisted(ctx context.Context) error {
	return b.use(func(s *SharedAccount) error {
		s.ready = false
		operationCtx, cancel := joinedOperationContext(b.ctx, ctx)
		defer cancel()
		for after := ""; ; {
			ids, err := s.store.recoveryOrdersAfter(operationCtx, after)
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				return nil
			}
			for _, id := range ids {
				if err := s.executorFor(operationCtx).Recover(id); err != nil {
					return fmt.Errorf("execution: startup recover %s: %w", id, err)
				}
			}
			after = ids[len(ids)-1]
		}
	})
}

func (b *SharedAccountBorrow) Submit(plan Plan, order OrderIntent, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		if !s.ready {
			return errors.New("execution: shared account is not reconciled")
		}
		return s.executorFor(b.ctx).SubmitEligible(plan, order, s.sendTime(nowMS))
	})
}

// PrepareRebalance uses the same durable coordinator as TS requests. Internal
// crossing and real residual allocations are committed before any network send.
func (b *SharedAccountBorrow) PrepareRebalance(request CombinedRebalance) (PreparedRebalance, error) {
	var result PreparedRebalance
	err := b.use(func(s *SharedAccount) error {
		if !s.ready {
			return errors.New("execution: shared account is not reconciled")
		}
		var err error
		result, err = s.prepareRebalance(request, b.ctx, nil)
		return err
	})
	return result, err
}

// Account sequence belongs to this shared service, whereas incoming portfolio
// sequences belong to individual strategies. Persisted plan IDs reuse their
// sequence on retry so request-hash validation still detects changed contents.
func (s *SharedAccount) prepareRebalance(request CombinedRebalance, operationCtx context.Context, checkpoint *StrategyCheckpoint) (PreparedRebalance, error) {
	var checkpoints []StrategyCheckpoint
	if checkpoint != nil {
		checkpoints = []StrategyCheckpoint{*checkpoint}
	}
	return s.prepareRebalanceWithCheckpoints(request, operationCtx, checkpoints)
}

func (s *SharedAccount) prepareRebalanceWithCheckpoints(request CombinedRebalance, operationCtx context.Context, checkpoints []StrategyCheckpoint) (PreparedRebalance, error) {
	prepare := func(request CombinedRebalance) (PreparedRebalance, error) {
		executor := s.executorFor(operationCtx)
		if len(checkpoints) > 0 {
			return executor.PrepareRebalanceWithCheckpoints(request, checkpoints)
		}
		return executor.PrepareRebalance(request)
	}
	var risk PortfolioRisk
	var validUntil int64
	riskErr := s.owner.DoLocal(s.owner.Token(), func(ctx context.Context) error {
		var err error
		ctx, cancel := joinedOperationContext(ctx, operationCtx)
		defer cancel()
		risk, validUntil, err = s.accountRisk(ctx, &request)
		return err
	})
	if riskErr != nil {
		return PreparedRebalance{}, riskErr
	}
	request.Risk = risk
	for index := range request.Requests {
		request.Requests[index].Quote.ValidUntilMS = min(request.Requests[index].Quote.ValidUntilMS, validUntil)
	}
	plan, err := s.store.Plan(context.Background(), request.PlanID)
	if err == nil {
		request.Sequence = plan.Sequence
	} else if errors.Is(err, sql.ErrNoRows) {
		latest, readErr := s.store.LatestPlanSequence(context.Background())
		if readErr != nil {
			return PreparedRebalance{}, readErr
		}
		if latest == math.MaxInt64 {
			return PreparedRebalance{}, errors.New("execution: account plan sequence exhausted")
		}
		request.Sequence = latest + 1
	} else {
		return PreparedRebalance{}, err
	}
	// Other strategy pending definitions remain in the combined membership.
	// Runtime trigger state is retained by Store; stable definitions are carried
	// without replacing their original IDs or visibility conditions.
	latestPlan, latestErr := s.store.LatestPlan(context.Background())
	if latestErr != nil && !errors.Is(latestErr, sql.ErrNoRows) {
		return PreparedRebalance{}, latestErr
	}
	if err == nil {
		request.CarryIntents = plan.CarriedIntents
		request.CarryTargets = plan.CarriedTargets
		for index := range request.Requests {
			r := &request.Requests[index]
			if len(r.IntentConstraints) == 0 {
				for _, constraint := range plan.ContributorConstraints {
					if constraint.Instrument == r.Instrument.ID {
						r.IntentConstraints = append(r.IntentConstraints, constraint)
					}
				}
			}
		}
		return prepare(request)
	}
	// A touched instrument may contain another strategy's still-pending target.
	// Preserve its contributor definition; plan membership alone cannot prevent
	// a replacement delta from losing its limit or renewing its expiry.
	for index := range request.Requests {
		r := &request.Requests[index]
		for _, target := range r.Targets {
			carried := false
			for _, old := range latestPlan.Targets {
				if old.Strategy == target.Strategy && old.Lot == target.Lot && old.Instrument == r.Instrument.ID && old.SignedSteps == target.SignedSteps {
					carried = true
					break
				}
			}
			if !carried {
				continue
			}
			for _, original := range latestPlan.Intents {
				if original.Strategy != target.Strategy || original.Lot != target.Lot || original.Instrument != r.Instrument.ID {
					continue
				}
				explicit := false
				for _, existing := range r.IntentConstraints {
					if existing.Strategy == original.Strategy && existing.Lot == original.Lot && existing.Side == original.Side && existing.Kind == original.Kind {
						explicit = true
						break
					}
				}
				if explicit {
					continue
				}
				current, readErr := s.store.Intent(context.Background(), original.ID)
				if readErr != nil {
					return PreparedRebalance{}, readErr
				}
				if current.State == Filled {
					continue
				}
				current.ReservedSteps = 0
				if current.Conditions.ExpiresAtMS == 0 || latestPlan.ExpiresMS < current.Conditions.ExpiresAtMS {
					current.Conditions.ExpiresAtMS = latestPlan.ExpiresMS
				}
				r.IntentConstraints = append(r.IntentConstraints, current)
			}
		}
	}
	seen := make(map[VirtualIntentID]bool)
	for _, intent := range request.CarryIntents {
		seen[intent.ID] = true
	}
	for _, intent := range latestPlan.Intents {
		current, readErr := s.store.Intent(context.Background(), intent.ID)
		if readErr != nil {
			return PreparedRebalance{}, readErr
		}
		intent = current
		if !seen[intent.ID] && intent.State != Filled && intent.State != Canceled && intent.State != Expired {
			request.CarryIntents = append(request.CarryIntents, intent)
		}
	}
	requested := make(map[string]bool)
	for _, r := range request.Requests {
		requested[r.Instrument.ID] = true
	}
	request.CarryTargets = nil
	for _, target := range latestPlan.Targets {
		if target.SignedSteps == 0 && !requested[target.Instrument] {
			snapshot, err := s.store.Snapshot(operationCtx)
			if err != nil {
				return PreparedRebalance{}, err
			}
			latestPlan.Targets, err = s.retainedPlanTargets(operationCtx, latestPlan, snapshot)
			if err != nil {
				return PreparedRebalance{}, err
			}
			break
		}
	}
	for _, target := range latestPlan.Targets {
		if !requested[target.Instrument] {
			request.CarryTargets = append(request.CarryTargets, target)
		}
	}
	return prepare(request)
}

func (b *SharedAccountBorrow) Send(id string, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		if !s.ready {
			return errors.New("execution: shared account is not reconciled")
		}
		return s.executorFor(b.ctx).Send(id, s.sendTime(nowMS))
	})
}

// Rebalance sends in the coordinator's decrease-first order. Each increase
// remains protected by its persisted RequiresFilled check; an asynchronous
// decrease returns an error rather than bypassing the dependency.
func (b *SharedAccountBorrow) Rebalance(request CombinedRebalance, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		return s.rebalance(request, nowMS, b.ctx)
	})
}

func (b *SharedAccountBorrow) RebalanceAtCheckpoint(request CombinedRebalance, checkpoint, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		current, err := s.store.Snapshot(context.Background())
		if err != nil {
			return err
		}
		if current.Checkpoint != checkpoint {
			return ErrSharedCheckpoint
		}
		return s.rebalance(request, nowMS, b.ctx)
	})
}

// RebalanceAtVersion also fences target-only plan changes, which need not
// change a cash/fill checkpoint. Callers rebuild on ErrSharedCheckpoint.
func (b *SharedAccountBorrow) RebalanceAtVersion(request CombinedRebalance, checkpoint int64, expectedPlanID string, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		current, err := s.store.Snapshot(context.Background())
		if err != nil {
			return err
		}
		latest, err := s.store.LatestPlan(context.Background())
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if current.Checkpoint != checkpoint || latest.ID != expectedPlanID {
			return ErrSharedCheckpoint
		}
		return s.rebalance(request, nowMS, b.ctx)
	})
}

func (s *SharedAccount) executorFor(ctx context.Context) *OwnerExecutor {
	executor := *s.executor
	executor.Context = ctx
	return &executor
}

func (s *SharedAccount) rebalance(request CombinedRebalance, nowMS int64, ctx context.Context) error {
	if !s.ready {
		return errors.New("execution: shared account is not reconciled")
	}
	prepared, err := s.prepareRebalance(request, ctx, nil)
	if err != nil {
		return err
	}
	return s.sendPreparedRebalance(prepared, nowMS, ctx)
}

func (s *SharedAccount) sendPreparedRebalance(prepared PreparedRebalance, nowMS int64, ctx context.Context) error {
	for _, id := range prepared.OrderIDs {
		order, err := s.store.Order(context.Background(), id)
		if err != nil {
			return err
		}
		if order.State == OrderUnknown || order.State == OrderSending || order.State == OrderCancelPending || order.State == OrderRejected || order.State == OrderCanceled {
			return fmt.Errorf("execution: rebalance order %s requires recovery or replacement: %s", id, order.State)
		}
		if order.State != OrderPrepared {
			continue
		}
		if err := s.executorFor(ctx).Send(id, s.sendTime(max(nowMS, prepared.Plan.DecisionMS))); err != nil {
			return err
		}
	}
	return nil
}
func (b *SharedAccountBorrow) Cancel(id string, nowMS int64) error {
	return b.use(func(s *SharedAccount) error { return s.executorFor(b.ctx).Cancel(id, nowMS) })
}
func (b *SharedAccountBorrow) Recover(id string) error {
	return b.use(func(s *SharedAccount) error { s.ready = false; return s.executorFor(b.ctx).Recover(id) })
}
func (b *SharedAccountBorrow) ApplyTrade(report FillReport) error {
	return b.use(func(s *SharedAccount) error { return s.executor.ApplyTrade(report) })
}

// ProjectionEvents returns committed state only. Callbacks run outside owner
// admission and must deduplicate the stable event ID across crash replay.
func (b *SharedAccountBorrow) ProjectionEvents(ctx context.Context, name string, limit int) ([]CommittedEvent, error) {
	var result []CommittedEvent
	err := b.use(func(s *SharedAccount) error {
		cursor, err := s.store.ProjectionCursor(ctx, name)
		if err != nil {
			return err
		}
		result, err = s.store.EventsAfter(ctx, cursor, limit)
		return err
	})
	return result, err
}

func (b *SharedAccountBorrow) AdvanceProjection(ctx context.Context, name string, through int64) error {
	return b.use(func(s *SharedAccount) error { return s.store.AdvanceProjection(ctx, name, through) })
}

// Close follows Process account-owner Stop/Join. Adapters with a Close method
// are closed once here, after admitted network/report work has joined.
func (s *SharedAccount) Close() error {
	s.cancel()
	s.mu.Lock()
	if s.reportCancel != nil {
		s.reportCancel()
	}
	done := s.reportDone
	s.mu.Unlock()
	if done != nil {
		<-done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.ready = false
	var adapterErr error
	if closer, ok := s.opts.Adapter.(interface{ Close() error }); ok {
		adapterErr = closer.Close()
	}
	s.owner.Release()
	s.closeErr = errors.Join(s.reportShutdownErr, adapterErr, s.store.Close())
	return s.closeErr
}
