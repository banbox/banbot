package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	PendingBookStrategy StrategyID = "execution-pending"
	PendingBookName                = "book-v1"
)

// PendingDefinition contains everything the account needs to advance a lot
// without a strategy facade or its presentation metadata.
type PendingDefinition struct {
	Order           PendingOrder
	Instrument      Instrument
	Specs           []PositionProtection
	Policy          OrderPolicy
	CommandRevision int64 `json:",omitempty"`
}

// LoadPendingBook reads the authoritative execution state, not facade metadata.
// An absent checkpoint is an empty book, including on accounts predating it.
func (s *Store) LoadPendingBook(ctx context.Context) ([]PendingDefinition, error) {
	body, err := s.StrategyCheckpoint(ctx, PendingBookStrategy, PendingBookName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var definitions []PendingDefinition
	if err := json.Unmarshal(body, &definitions); err != nil {
		return nil, fmt.Errorf("execution: decode pending book: %w", err)
	}
	if err := validatePendingBook(definitions); err != nil {
		return nil, err
	}
	for _, definition := range definitions {
		if definition.Order.Entry.Account != s.Account() {
			return nil, errors.New("execution: pending book belongs to another account")
		}
	}
	return definitions, nil
}

// PendingBookCheckpoint is committed with the plan that advanced its state.
func PendingBookCheckpoint(definitions []PendingDefinition) (StrategyCheckpoint, error) {
	if err := validatePendingBook(definitions); err != nil {
		return StrategyCheckpoint{}, err
	}
	ordered := append([]PendingDefinition{}, definitions...)
	sort.SliceStable(ordered, func(i, j int) bool { return pendingLess(&ordered[i].Order, &ordered[j].Order) })
	body, err := json.Marshal(ordered)
	if err != nil {
		return StrategyCheckpoint{}, err
	}
	return StrategyCheckpoint{Strategy: PendingBookStrategy, Name: PendingBookName, Payload: body}, nil
}

// SeedPendingBook upgrades an attributed facade checkpoint exactly once. It
// neither replaces a newer execution book nor prepares/sends exchange orders.
func (s *SharedAccount) SeedPendingBook(ctx context.Context, definitions []PendingDefinition) error {
	return s.SeedPendingBookWithFeeds(ctx, definitions, nil, nil)
}

func validatePendingBook(definitions []PendingDefinition) error {
	type lotKey struct {
		strategy StrategyID
		lot      VirtualLotID
	}
	lots := make(map[lotKey]bool)
	intents := make(map[VirtualIntentID]bool)
	for _, definition := range definitions {
		order := definition.Order
		if err := definition.Instrument.Validate(); err != nil {
			return err
		}
		if err := definition.Policy.Validate(); err != nil {
			return err
		}
		if order.EntryFeedExpiry != nil {
			if err := order.EntryFeedExpiry.validate(); err != nil {
				return err
			}
			if order.Entry.Conditions.StopBars != 0 {
				return errors.New("execution: pending entry cannot use two bar clocks")
			}
		}
		key := lotKey{order.Strategy, order.Lot}
		if lots[key] {
			return errors.New("execution: duplicate pending lot")
		}
		lots[key] = true
		sources := []*EligibleIntent{&order.Entry}
		if order.Exit != nil {
			sources = append(sources, order.Exit)
		}
		for _, source := range sources {
			if err := source.Validate(); err != nil {
				return err
			}
			if source.Strategy != order.Strategy || source.Lot != order.Lot || source.Instrument != definition.Instrument.ID || source.Account != order.Entry.Account {
				return errors.New("execution: pending source identity mismatch")
			}
			if intents[source.ID] {
				return errors.New("execution: duplicate pending source intent")
			}
			intents[source.ID] = true
		}
		if order.Entry.Kind != EntryIntent || order.Exit != nil && order.Exit.Kind != ExitIntent {
			return errors.New("execution: pending source kind mismatch")
		}
	}
	return nil
}

// AdvancePendingExecution is an account-owned tick. Quotes, risk admission,
// trigger checkpoints, and sends remain available after facade borrowers leave.
func (b *SharedAccountBorrow) AdvancePendingExecution(callerCtx context.Context, now int64) error {
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		return s.AdvancePendingExecution(ctx, now)
	})
	if err == nil {
		b.service.notifyCommitted()
	}
	return err
}

// AdvancePendingExecution runs inside WithState; it must not reenter a borrow.
func (s *SharedAccount) AdvancePendingExecution(ctx context.Context, now int64) error {
	definitions, err := s.store.LoadPendingBook(ctx)
	if err != nil || len(definitions) == 0 {
		return err
	}
	return s.advancePendingDefinitions(ctx, definitions, now, nil, "")
}

// Command admission and autonomous ticks share the same account-owned planner.
// Extra checkpoints commit command identity/revision with the book and targets.
func (s *SharedAccount) advancePendingDefinitions(ctx context.Context, definitions []PendingDefinition, now int64, checkpoints []StrategyCheckpoint, commandPlanID string) error {
	if !s.ready {
		return errors.New("execution: shared account is not reconciled")
	}
	allDefinitions := definitions
	var quoteFailures []error
	quotes := make(map[string]VisibleQuote)
	ids := make([]string, 0)
	for _, definition := range definitions {
		if _, exists := quotes[definition.Instrument.ID]; !exists {
			quotes[definition.Instrument.ID] = VisibleQuote{}
			ids = append(ids, definition.Instrument.ID)
		}
	}
	sort.Strings(ids)
	// Quote validity fences preparation/send, not the lifetime of an accepted
	// resting order. Source intent expiry and feed progress cancel it separately.
	expires := int64(math.MaxInt64)
	for _, id := range ids {
		quote, err := s.ReadQuote(ctx, id, now)
		if err != nil {
			if commandPlanID != "" || ctx.Err() != nil {
				return err
			}
			quoteFailures = append(quoteFailures, err)
			delete(quotes, id)
			continue
		}
		quotes[id] = quote
		now = max(now, quote.ReceivedMS)
	}
	now = s.SendTime(now)
	for _, id := range ids {
		quote := quotes[id]
		if _, _, err := pendingQuote(quotes, id, now); err != nil {
			if commandPlanID != "" {
				return err
			}
			quoteFailures = append(quoteFailures, err)
			delete(quotes, id)
			continue
		}
		if err := s.AdvancePaperQuote(ctx, id, quote, now); err != nil {
			return err
		}
	}
	// Autonomous ticks advance only instruments with a current quote. Keep the
	// full persisted book, and let account risk still fence any unvalued carried
	// exposure; unavailable market data is not ledger uncertainty.
	activeIndices := make([]int, 0, len(allDefinitions))
	definitions = make([]PendingDefinition, 0, len(allDefinitions))
	for n, definition := range allDefinitions {
		if _, ok := quotes[definition.Instrument.ID]; ok {
			activeIndices = append(activeIndices, n)
			definitions = append(definitions, definition)
		}
	}
	if len(definitions) == 0 {
		return errors.Join(quoteFailures...)
	}
	// Validate carried account marks before protection preflight can cancel a
	// native reservation. A missing mark must not remove a venue protection
	// only to discover later that its replacement cannot pass risk admission.
	valuation := CombinedRebalance{DecisionMS: now, ExpiresMS: expires}
	seen := make(map[string]bool)
	for _, definition := range definitions {
		if !seen[definition.Instrument.ID] {
			seen[definition.Instrument.ID] = true
			valuation.Requests = append(valuation.Requests, InstrumentRebalance{Instrument: definition.Instrument, Quote: quotes[definition.Instrument.ID]})
		}
	}
	if _, _, err := s.accountRisk(ctx, &valuation); err != nil {
		return err
	}
	protections := make([]PendingProtectionRequest, 0, len(definitions))
	for n := range definitions {
		definition := &definitions[n]
		protections = append(protections, PendingProtectionRequest{Order: &definition.Order, Instrument: definition.Instrument, Specs: definition.Specs, Policy: definition.Policy})
	}
	if _, err := s.AdvancePendingProtections(ctx, protections, quotes, now); err != nil {
		return err
	}
	requests, err := s.CoordinatePending(ctx, definitions, quotes, now)
	if err != nil {
		return err
	}
	for n, index := range activeIndices {
		allDefinitions[index] = definitions[n]
	}
	checkpoint, err := PendingBookCheckpoint(allDefinitions)
	if err != nil {
		return err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return err
	}
	identity, err := json.Marshal(struct {
		Now, Checkpoint int64
		Book            json.RawMessage
		Requests        []InstrumentRebalance
		Checkpoints     []StrategyCheckpoint
	}{now, snapshot.Checkpoint, checkpoint.Payload, requests, checkpoints})
	if err != nil {
		return err
	}
	planID := commandPlanID
	if planID == "" {
		planID = fmt.Sprintf("pending-tick-%x", sha256.Sum256(identity))
	}
	byStrategy := make(map[StrategyID]*StrategyRebalance)
	for _, definition := range definitions {
		strategy := definition.Order.Strategy
		byStrategy[strategy] = &StrategyRebalance{Strategy: strategy, PlanID: planID, DecisionMS: now, ExpiresMS: expires, Mode: StrategyTargetsPatch}
	}
	for _, request := range requests {
		for strategy, update := range byStrategy {
			input := InstrumentRebalance{Instrument: request.Instrument, Quote: request.Quote}
			for _, target := range request.Targets {
				if target.Strategy == strategy {
					input.Targets = append(input.Targets, target)
				}
			}
			for _, reservation := range request.ConditionalReservations {
				if reservation.Intent.Strategy == strategy {
					input.ConditionalReservations = append(input.ConditionalReservations, reservation)
				}
			}
			for _, constraint := range request.IntentConstraints {
				if constraint.Strategy == strategy {
					input.IntentConstraints = append(input.IntentConstraints, constraint)
				}
			}
			if len(input.Targets) > 0 {
				update.Requests = append(update.Requests, input)
			}
		}
	}
	updates := make([]StrategyRebalance, 0, len(byStrategy))
	for _, update := range byStrategy {
		updates = append(updates, *update)
	}
	checkpoints = append(append([]StrategyCheckpoint(nil), checkpoints...), checkpoint)
	prepared, err := s.PrepareStrategiesWithCheckpoints(updates, ctx, checkpoints)
	if err != nil {
		return err
	}
	if err := s.SendPrepared(prepared, now, ctx); err != nil {
		return err
	}
	return nil
}
