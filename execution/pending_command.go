package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/shopspring/decimal"
)

type PendingCommandAction string

const (
	PendingCreate         PendingCommandAction = "create"
	PendingEditLimit      PendingCommandAction = "edit_limit"
	PendingCancelEntry    PendingCommandAction = "cancel_entry"
	PendingSetProtections PendingCommandAction = "set_protections"
	PendingSetExit        PendingCommandAction = "set_exit"
	PendingEditExitLimit  PendingCommandAction = "edit_exit_limit"
	PendingCancelExit     PendingCommandAction = "cancel_exit"
)

// PendingExitSpec requests a reduction, never a reversal. Zero means close all
// inventory remaining after cancellation final fills; positive sizes are capped
// at that inventory. Explicit exits also cancel the remaining entry.
type PendingExitSpec struct {
	QuantitySteps int64
	Conditions    IntentConditions
}

// PendingCreateSpec deliberately excludes runtime trigger/fill state and venue
// identities. Only the account owner may construct those fields.
type PendingCreateSpec struct {
	Instrument    string
	Side          OrderSide
	QuantitySteps int64
	Conditions    IntentConditions
	Policy        OrderPolicy
	Protections   []PositionProtection
	Feed          PendingFeedID
	StopBars      int64
}

// PendingCommand is a strategy-scoped optimistic revision, not a replacement of
// the account book. Retrying an accepted ID returns its original revision even
// after subsequent commands. CancelEntry retains holdings and their protection.
type PendingCommand struct {
	ID               string
	Strategy         StrategyID
	StrategyEpoch    uint64 `json:",omitempty"`
	StrategySequence uint64 `json:",omitempty"`
	Lot              VirtualLotID
	ExpectedRevision int64
	Action           PendingCommandAction
	Create           *PendingCreateSpec `json:",omitempty"`
	Exit             *PendingExitSpec   `json:",omitempty"`
	Limit            decimal.Decimal
	Protections      []PositionProtection
	Metadata         *TimeSeriesMetadata `json:",omitempty"`
}

type pendingCommandReceipt struct {
	Hash     [32]byte
	Revision int64
	PlanID   string
}

func (b *SharedAccountBorrow) PendingOrders(caller context.Context) ([]PendingDefinition, error) {
	var book []PendingDefinition
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		var err error
		book, err = s.store.LoadPendingBook(ctx)
		return err
	})
	return book, err
}

func (b *SharedAccountBorrow) ApplyPendingCommand(caller context.Context, command PendingCommand, now int64) (int64, error) {
	// Isolate caller-owned slices/pointers before passing through serialization.
	body, err := json.Marshal(command)
	if err != nil {
		return 0, err
	}
	var owned PendingCommand
	if err := json.Unmarshal(body, &owned); err != nil {
		return 0, err
	}
	var revision int64
	err = b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		var err error
		revision, err = s.applyPendingCommand(ctx, owned, sha256.Sum256(body), now)
		return err
	})
	// An uncertain send may have committed the command and projection already.
	b.service.notifyCommitted()
	return revision, err
}

func validateCommandProtections(specs []PositionProtection) error {
	seen := map[ProtectionKind]bool{}
	for _, spec := range specs {
		if seen[spec.Kind] {
			return errors.New("execution: duplicate command protection")
		}
		seen[spec.Kind] = true
		template := EligibleIntent{ID: "validate-protection", Account: AccountKey{"validate", "validate", "validate"}, Strategy: "validate", Lot: "validate", Instrument: "validate", Kind: ExitIntent, Side: Sell}
		if _, _, _, err := PreparePositionProtection(template, spec, 1, spec.Distance.Add(decimal.NewFromInt(1)), 0); err != nil {
			return err
		}
	}
	return nil
}

func (c PendingCommand) validate() error {
	if !canonicalID(c.ID) || len(c.ID) > 512 || !canonicalID(string(c.Strategy)) || !canonicalID(string(c.Lot)) || len(c.Lot) > 512 || c.ExpectedRevision < 0 || c.ExpectedRevision == math.MaxInt64 {
		return errors.New("execution: invalid pending command identity or revision")
	}
	if c.Metadata != nil {
		if err := c.Metadata.validate(); err != nil {
			return err
		}
	}
	if c.Action != PendingCreate && c.Create != nil || c.Action != PendingSetExit && c.Exit != nil || c.Action != PendingEditLimit && c.Action != PendingEditExitLimit && !c.Limit.IsZero() || c.Action != PendingSetProtections && len(c.Protections) > 0 {
		return errors.New("execution: unrelated pending command fields")
	}
	switch c.Action {
	case PendingCreate:
		if c.Create == nil || c.ExpectedRevision != 0 {
			return errors.New("execution: create requires a new lot")
		}
		if c.Create.StopBars < 0 || c.Create.StopBars > 0 && !validPendingFeed(c.Create.Feed) || c.Create.StopBars == 0 && c.Create.Feed != "" || c.Create.Conditions.StopBars != 0 {
			return errors.New("execution: command bar expiry requires an explicit feed")
		}
		if err := c.Create.Policy.Validate(); err != nil {
			return err
		}
		return validateCommandProtections(c.Create.Protections)
	case PendingEditLimit, PendingEditExitLimit:
		if !c.Limit.IsPositive() {
			return errors.New("execution: edited limit must be positive")
		}
	case PendingCancelEntry:
	case PendingCancelExit:
	case PendingSetExit:
		if c.Exit == nil || c.Exit.QuantitySteps < 0 || c.Exit.Conditions.StopBars != 0 {
			return errors.New("execution: exit requires a nonnegative reduction and no quote-bar expiry")
		}
		intent := EligibleIntent{ID: "validate-exit", Account: AccountKey{"validate", "validate", "validate"}, Strategy: c.Strategy, Lot: c.Lot, Instrument: "validate", Kind: ExitIntent, Side: Sell, QuantitySteps: max(int64(1), c.Exit.QuantitySteps), Conditions: c.Exit.Conditions}
		return intent.Validate()
	case PendingSetProtections:
		return validateCommandProtections(c.Protections)
	default:
		return errors.New("execution: unknown pending command action")
	}
	return nil
}

func (s *SharedAccount) applyPendingCommand(ctx context.Context, c PendingCommand, hash [32]byte, now int64) (int64, error) {
	if err := c.validate(); err != nil {
		return 0, err
	}
	if c.Create != nil {
		if err := s.validateContributorInstruments(ctx, c.Strategy, []string{c.Create.Instrument}); err != nil {
			return 0, err
		}
	}
	receiptName := "pending-command:" + c.ID
	body, err := s.store.StrategyCheckpoint(ctx, c.Strategy, receiptName)
	if err == nil {
		var receipt pendingCommandReceipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			return 0, err
		}
		if receipt.Hash != hash {
			return 0, errors.New("execution: pending command identity reused")
		}
		// Only the latest immutable plan may resume sends. Newer autonomous
		// ticks or revisions have already carried/replaced the old allocation.
		latest, err := s.store.LatestPlan(ctx)
		if err != nil {
			return 0, err
		}
		if latest.ID != receipt.PlanID {
			return receipt.Revision, nil
		}
		if !s.ready {
			return 0, errors.New("execution: pending replay requires reconciliation")
		}
		prepared, err := s.restorePreparedRebalance(ctx, receipt.PlanID)
		if err != nil {
			return 0, err
		}
		return receipt.Revision, s.SendPrepared(prepared, s.SendTime(now), ctx)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	ordering, err := s.strategyOrderingCheckpoints(ctx, []StrategyRebalance{{Strategy: c.Strategy, StrategyEpoch: c.StrategyEpoch, StrategySequence: c.StrategySequence}})
	if err != nil {
		return 0, err
	}
	policy, err := s.store.AccountPolicy(ctx)
	if err != nil {
		return 0, err
	}
	if !s.ready || !policy.StrategyGrossLimits[c.Strategy].IsPositive() {
		return 0, errors.New("execution: pending command requires a reconciled, declared strategy")
	}
	revisionName := "pending-revision:" + string(c.Lot)
	body, err = s.store.StrategyCheckpoint(ctx, c.Strategy, revisionName)
	var revision int64
	if err == nil {
		if err := json.Unmarshal(body, &revision); err != nil {
			return 0, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	if revision != c.ExpectedRevision {
		return 0, errors.New("execution: stale pending command revision")
	}
	definitions, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return 0, err
	}
	index := -1
	for n, d := range definitions {
		if d.Order.Strategy == c.Strategy && d.Order.Lot == c.Lot {
			index = n
		}
	}
	now = s.SendTime(now)
	if c.Metadata != nil {
		entrySide := OrderSide("")
		if c.Create != nil {
			entrySide = c.Create.Side
		} else if index >= 0 {
			entrySide = definitions[index].Order.Entry.Side
		}
		if c.Metadata.Short != (entrySide == Sell) {
			return 0, errors.New("execution: time-series metadata direction differs from entry")
		}
		if c.Action != PendingCreate {
			body, err := s.store.StrategyCheckpoint(ctx, c.Strategy, TimeSeriesMetadataName(c.Lot))
			if err != nil {
				return 0, err
			}
			var old TimeSeriesMetadata
			if err := json.Unmarshal(body, &old); err != nil {
				return 0, err
			}
			if !old.sameIdentity(*c.Metadata) {
				return 0, errors.New("execution: time-series metadata identity changed")
			}
		}
	}
	if c.Action == PendingCreate {
		if index >= 0 || revision != 0 {
			return 0, errors.New("execution: pending lot already exists")
		}
		market, ok := s.markets[c.Create.Instrument]
		if !ok {
			return 0, errors.New("execution: pending instrument not registered")
		}
		snapshot, err := s.store.Snapshot(ctx)
		if err != nil {
			return 0, err
		}
		for _, lot := range snapshot.Lots {
			if lot.Strategy == c.Strategy && lot.ID == c.Lot {
				return 0, errors.New("execution: pending lot already has attributed inventory")
			}
		}
		if plan, err := s.store.LatestPlan(ctx); err == nil {
			for _, target := range plan.Targets {
				if target.Strategy == c.Strategy && target.Lot == c.Lot {
					return 0, errors.New("execution: pending lot already has a target")
				}
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		definition := PendingDefinition{Instrument: market.instrument.Clone(), Policy: c.Create.Policy, Specs: c.Create.Protections, Order: PendingOrder{Strategy: c.Strategy, Lot: c.Lot}}
		definition.Order.Entry = EligibleIntent{ID: VirtualIntentID(fmt.Sprintf("pending-entry/%x", hash)), Account: s.AccountKey(), Strategy: c.Strategy, Lot: c.Lot, Instrument: c.Create.Instrument, Kind: EntryIntent, Side: c.Create.Side, QuantitySteps: c.Create.QuantitySteps, Conditions: c.Create.Conditions, State: PendingCondition}
		if err := definition.Order.Entry.Validate(); err != nil {
			return 0, err
		}
		if c.Create.StopBars > 0 {
			if err := s.BindPendingFeedExpiryAt(ctx, &definition.Order, c.Create.Feed, c.Create.StopBars, now); err != nil {
				return 0, err
			}
		}
		definitions = append(definitions, definition)
	} else {
		if index < 0 {
			return 0, errors.New("execution: pending command lot not found")
		}
		d := &definitions[index]
		if c.Action == PendingSetExit || c.Action == PendingEditExitLimit || c.Action == PendingCancelExit {
			if err := s.applyPendingExit(ctx, d, c, hash, now); err != nil {
				return 0, err
			}
		} else if c.Action == PendingSetProtections {
			if err := s.cancelNativeForLot(ctx, &d.Order, now); err != nil {
				return 0, err
			}
			d.Specs = c.Protections
			d.Order.Protections = nil
			d.Order.ProtectionLevels = nil
			d.Order.ProtectionDone = nil
		} else {
			if d.Order.Exit != nil {
				return 0, errors.New("execution: entry command after an exit is not supported")
			}
			if c.Action == PendingEditLimit && (d.Order.Canceled || d.Order.Entry.State == Filled || d.Order.Entry.State == Expired || d.Order.Entry.State == Canceled || !d.Order.Entry.Conditions.Limit.IsPositive()) {
				return 0, errors.New("execution: only an active limit entry can be edited")
			}
			snapshot, err := s.store.Snapshot(ctx)
			if err != nil {
				return 0, err
			}
			if err := s.cancelPendingEntries(ctx, snapshot, &d.Order, d.Instrument.ID, now, false); err != nil {
				return 0, err
			}
			snapshot, err = s.store.Snapshot(ctx)
			if err != nil {
				return 0, err
			}
			held := pendingLot(snapshot, &d.Order, d.Instrument.ID).SignedSteps
			d.Order.Entry.ReservedSteps = 0
			d.Order.Entry.FilledSteps = absSteps(held)
			d.Order.Entry.VenueLimit = nil
			d.Order.Desired = held
			if c.Action == PendingCancelEntry {
				d.Order.Canceled = true
				d.Order.Entry.State = Canceled
			} else {
				d.Order.Entry.Conditions.Limit = c.Limit
				d.Order.Entry.ID = VirtualIntentID(fmt.Sprintf("pending-entry/%x", hash))
				d.Order.Entry.State = PendingCondition
				if d.Order.Entry.FilledSteps >= d.Order.Entry.QuantitySteps {
					d.Order.Entry.State = Filled
				}
			}
		}
	}
	if err := validatePendingBook(definitions); err != nil {
		return 0, err
	}
	revision++
	if index < 0 {
		index = len(definitions) - 1
	}
	definitions[index].CommandRevision = revision
	planID := fmt.Sprintf("pending-command-%x", hash)
	body, _ = json.Marshal(pendingCommandReceipt{Hash: hash, Revision: revision, PlanID: planID})
	version, _ := json.Marshal(revision)
	checkpoints := []StrategyCheckpoint{{Strategy: c.Strategy, Name: receiptName, Payload: body}, {Strategy: c.Strategy, Name: revisionName, Payload: version}}
	checkpoints = append(checkpoints, ordering...)
	if c.Metadata != nil {
		metadataCopy := *c.Metadata
		if exit := definitions[index].Order.Exit; exit != nil {
			conditions := exit.Conditions
			metadataCopy.ExitConditions = &conditions
			metadataCopy.ExitQuantitySteps = exit.QuantitySteps
		} else {
			metadataCopy.ExitConditions = nil
			metadataCopy.ExitQuantitySteps = 0
		}
		c.Metadata = &metadataCopy
		metadata, err := timeSeriesMetadataCheckpoint(c.Strategy, c.Lot, *c.Metadata)
		if err != nil {
			return 0, err
		}
		checkpoints = append(checkpoints, metadata)
	}
	kind := EntryIntent
	if c.Action == PendingSetExit || c.Action == PendingEditExitLimit || c.Action == PendingCancelExit {
		kind = ExitIntent
	}
	checkpoints[0].Events = []StrategyAcceptedEvent{{Strategy: c.Strategy, Lot: c.Lot, Kind: kind, CommandID: c.ID, AtMS: now, Metadata: c.Metadata}}
	if err := s.advancePendingDefinitions(ctx, definitions, now, checkpoints, planID); err != nil {
		return 0, err
	}
	return revision, nil
}

func (s *SharedAccount) applyPendingExit(ctx context.Context, d *PendingDefinition, c PendingCommand, hash [32]byte, now int64) error {
	o := &d.Order
	if c.Action != PendingSetExit && (o.Exit == nil || o.Exit.State == Filled || o.Exit.State == Canceled || o.Exit.State == Expired) {
		return errors.New("execution: exit command requires an active exit")
	}
	if c.Action == PendingEditExitLimit && !o.Exit.Conditions.Limit.IsPositive() {
		return errors.New("execution: only a limit exit can be edited")
	}
	if err := s.cancelNativeForLot(ctx, o, now); err != nil {
		return err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.cancelPendingEntries(ctx, snapshot, o, d.Instrument.ID, now, false); err != nil {
		return err
	}
	if err := s.cancelPendingReservations(ctx, snapshot, o, d.Instrument.ID, ExitIntent, now, false); err != nil {
		return err
	}
	snapshot, err = s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	held := pendingLot(snapshot, o, d.Instrument.ID).SignedSteps
	o.Canceled, o.Entry.State = true, Canceled
	o.Entry.ReservedSteps, o.ResumeEntrySteps = 0, 0
	o.Entry.VenueLimit, o.NativeExitOrders = nil, nil
	if c.Action == PendingCancelExit {
		remaining := max(int64(0), absSteps(held)-absSteps(o.Desired))
		o.Exit.FilledSteps = o.Exit.QuantitySteps - min(o.Exit.QuantitySteps, remaining)
		o.Exit.State, o.Exit.ReservedSteps = Canceled, 0
		o.Exit.VenueLimit = nil
		o.Desired = held
		return nil
	}
	goal := o.Desired
	if c.Action == PendingSetExit {
		if held == 0 {
			o.Desired, o.Exit = 0, nil
			return nil
		}
		steps := c.Exit.QuantitySteps
		if steps == 0 {
			steps = absSteps(held)
		}
		goal = max(int64(0), absSteps(held)-steps)
		if held < 0 {
			goal = -goal
		}
		side := Sell
		if o.Entry.Side == Sell {
			side = Buy
		}
		o.Exit = &EligibleIntent{Account: s.AccountKey(), Strategy: c.Strategy, Lot: c.Lot, Instrument: d.Instrument.ID, Kind: ExitIntent, Side: side, Conditions: c.Exit.Conditions}
		o.LastProtection = ""
	} else {
		o.Exit.Conditions.Limit = c.Limit
	}
	// Editing keeps the original absolute goal. Final fills can satisfy it, but
	// must never turn an edit into a second reduction or reopen a closed lot.
	steps := max(int64(0), absSteps(held)-absSteps(goal))
	o.Desired = held
	o.Exit.ID = VirtualIntentID(fmt.Sprintf("pending-exit/%x", hash))
	o.Exit.FilledSteps, o.Exit.ReservedSteps = 0, 0
	o.Exit.VenueLimit = nil
	o.Exit.State = Filled
	if steps > 0 {
		o.Exit.QuantitySteps = steps
		o.Desired, o.Exit.State = goal, PendingCondition
	} else {
		o.Exit.FilledSteps = o.Exit.QuantitySteps
	}
	return nil
}
