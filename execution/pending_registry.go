package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sort"
	"strings"
	"sync"
	"weak"

	"github.com/banbox/banexg/log"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
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
		if order.OCOGroup != "" && !validOCOGroup(order.OCOGroup) || order.OCOSeq < 0 || order.OCOGroup == "" && order.OCOSeq != 0 {
			return errors.New("execution: invalid pending OCO group")
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
	var notVisible []string
	cache := s.pendingQuoteState()
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
			if errors.Is(err, ErrAccountQuoteNotVisible) {
				notVisible = append(notVisible, id)
			}
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
		cache.recordMark(id, quote, now)
		if err := s.AdvancePaperQuote(ctx, id, quote, now); err != nil {
			return err
		}
	}
	cache.noteSkipped(ids, quotes, notVisible)
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
	// Account risk values every instrument with carried exposure, including the
	// skipped ones. A skipped instrument whose depth is merely not visible keeps
	// its last valid mark for this tick (see holdStaleMarks), so the healthy
	// instruments' protections are not stalled by it.
	defer s.holdStaleMarks(notVisible, cache, now)()
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
	// OCO siblings executable in the same tick are cancelled before coordination,
	// so they never become targets (no self-cross, no risk refusal). The audit
	// records commit with the plan.
	ocoRecords, err := s.resolvePendingOCO(ctx, definitions, quotes, now)
	if err != nil {
		return err
	}
	requests, err := s.CoordinatePending(ctx, definitions, quotes, now)
	if err != nil {
		return err
	}
	// A reduction hold mark lives for one tick: it only orders a conflict in the
	// tick right after the hold (a retry below re-marks what is held again).
	holds := make(map[lotRef]int)
	for n := range definitions {
		order := &definitions[n].Order
		if order.ReductionHolds != 0 {
			holds[lotRef{order.Strategy, order.Lot}] = order.ReductionHolds
			if commandPlanID == "" {
				order.ReductionHolds = 0
			}
		}
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
	if len(ocoRecords) > 0 {
		checkpoints = append(append([]StrategyCheckpoint(nil), checkpoints...), ocoRecords...)
	}
	commandCheckpoints := checkpoints
	planFor := func(requests []InstrumentRebalance) (string, error) {
		if commandPlanID != "" {
			return commandPlanID, nil
		}
		identity, err := json.Marshal(struct {
			Now, Checkpoint int64
			Book            json.RawMessage
			Requests        []InstrumentRebalance
			Checkpoints     []StrategyCheckpoint
		}{now, snapshot.Checkpoint, checkpoint.Payload, requests, commandCheckpoints})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("pending-tick-%x", sha256.Sum256(identity)), nil
	}
	build := func(requests []InstrumentRebalance, planID string) []StrategyRebalance {
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
		return updates
	}
	checkpoints = append(append([]StrategyCheckpoint(nil), checkpoints...), checkpoint)
	// refreshBook re-derives the pending-book checkpoint after a retry changed a
	// definition (a reduction hold mark or an excluded entry).
	refreshBook := func() error {
		for n, index := range activeIndices {
			allDefinitions[index] = definitions[n]
		}
		updated, err := PendingBookCheckpoint(allDefinitions)
		if err != nil {
			return err
		}
		checkpoint = updated
		checkpoints[len(checkpoints)-1] = updated
		return nil
	}
	prepare := func(requests []InstrumentRebalance, records []StrategyCheckpoint) (PreparedRebalance, error) {
		if len(records) > 0 {
			if err := refreshBook(); err != nil {
				return PreparedRebalance{}, err
			}
		}
		planID, err := planFor(requests)
		if err != nil {
			return PreparedRebalance{}, err
		}
		return s.PrepareStrategiesWithCheckpoints(build(requests, planID), ctx, append(append([]StrategyCheckpoint(nil), checkpoints...), records...))
	}
	autonomous := commandPlanID == ""
	prepared, err := prepare(requests, nil)
	if code, ok := RejectionCode(err); ok && code == RejectSelfCross {
		// Preparation rolled back atomically. Sequence the opposite deltas of one
		// strategy across ticks instead of failing every retry (see
		// sequenceOpposite); anything it cannot resolve keeps the rejection. Only
		// the autonomous tick may also delay one of two opposite reductions: at
		// admission the caller gets the rejection.
		held, outcome := sequenceOpposite(snapshot, requests, autonomous, func(ref lotRef) int { return holds[ref] })
		if len(outcome.Held) > 0 {
			marked := false
			for n := range definitions {
				ref := lotRef{definitions[n].Order.Strategy, definitions[n].Order.Lot}
				for _, h := range outcome.HeldReductions {
					if h == ref {
						definitions[n].Order.ReductionHolds = holds[ref] + 1
						marked = true
					}
				}
				for _, e := range outcome.Executed {
					if e == ref && definitions[n].Order.ReductionHolds != 0 {
						definitions[n].Order.ReductionHolds = 0
						marked = true
					}
				}
			}
			requests = held
			if marked {
				if err := refreshBook(); err != nil {
					return err
				}
			}
			prepared, err = prepare(requests, nil)
		}
	}
	if err != nil && autonomous {
		// Isolate, don't freeze: see isolateRejections. Refusals it cannot
		// attribute to an excludable entry stay errors (fail -> freeze).
		prepared, err = isolateRejections(snapshot, definitions, requests, now, err, prepare)
	}
	if err != nil {
		return err
	}
	if err := s.SendPrepared(prepared, now, ctx); err != nil {
		return err
	}
	return nil
}

// maxStaleMarkAgeMS bounds how old the last valid mark of an instrument with
// no visible depth may be when account risk values its carried exposure. Beyond
// it (or with no mark yet, e.g. right after a restart) the valuation fails with
// the instrument's own not-visible error and the tick is refused as before.
const maxStaleMarkAgeMS = 5 * 60 * 1000

type pendingMark struct {
	price decimal.Decimal
	atMS  int64
}

// pendingQuoteState is the in-process, non-durable quote bookkeeping of one
// account's pending execution: last valid marks and the instruments skipped on
// the last tick. Losing it only makes the next tick refuse more, never less.
type pendingQuoteState struct {
	mu      sync.Mutex
	marks   map[string]pendingMark
	skipped []string
}

var pendingQuoteStates sync.Map // weak.Pointer[SharedAccount] -> *pendingQuoteState

func (s *SharedAccount) pendingQuoteState() *pendingQuoteState {
	key := weak.Make(s)
	if v, ok := pendingQuoteStates.Load(key); ok {
		return v.(*pendingQuoteState)
	}
	v, loaded := pendingQuoteStates.LoadOrStore(key, &pendingQuoteState{marks: map[string]pendingMark{}})
	if !loaded {
		runtime.AddCleanup(s, func(k weak.Pointer[SharedAccount]) { pendingQuoteStates.Delete(k) }, key)
	}
	return v.(*pendingQuoteState)
}

func (c *pendingQuoteState) recordMark(id string, quote VisibleQuote, now int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marks[id] = pendingMark{price: quote.Bid.Add(quote.Ask).Div(decimal.NewFromInt(2)), atMS: min(now, quote.ReceivedMS)}
}

func (c *pendingQuoteState) lastMark(id string, now int64) (pendingMark, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	mark, ok := c.marks[id]
	return mark, ok && mark.atMS > 0 && now-mark.atMS <= maxStaleMarkAgeMS
}

// noteSkipped records which instruments had no usable quote this tick and logs
// only when that set changes, so a long outage is not logged per tick.
func (c *pendingQuoteState) noteSkipped(ids []string, quotes map[string]VisibleQuote, notVisible []string) {
	var skipped []string
	for _, id := range ids {
		if _, ok := quotes[id]; !ok {
			skipped = append(skipped, id)
		}
	}
	c.mu.Lock()
	changed := strings.Join(skipped, ",") != strings.Join(c.skipped, ",")
	c.skipped = skipped
	c.mu.Unlock()
	if !changed {
		return
	}
	if len(skipped) == 0 {
		log.Info("pending execution: every instrument has a visible quote again")
		return
	}
	log.Warn("pending execution: instruments without a usable quote are skipped; their definitions stay pending",
		zap.Strings("skipped", skipped), zap.Strings("depthNotVisible", notVisible))
}

// PendingQuoteSkipped lists the instruments the last autonomous pending tick
// skipped for lack of a usable quote (transient: they proceed once it returns).
// The tick itself succeeds for the rest, so callers need not treat it as a
// failure; it is advisory.
func (s *SharedAccount) PendingQuoteSkipped() []string {
	c := s.pendingQuoteState()
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.skipped...)
}

// holdStaleMarks lets account risk value instruments whose depth is not
// visible at their last valid mark for the duration of one tick and returns the
// restore function. The substitute feeds margin/gross valuation only: skipped
// instruments carry no targets, so nothing is ever priced or sent from it.
func (s *SharedAccount) holdStaleMarks(ids []string, cache *pendingQuoteState, now int64) func() {
	original := make(map[string]accountMarket)
	for _, id := range ids {
		market, ok := s.markets[id]
		mark, fresh := cache.lastMark(id, now)
		if !ok || !fresh {
			continue
		}
		original[id] = market
		market.quote = func(_ context.Context, _ string, at int64) (VisibleQuote, error) {
			received := min(mark.atMS, at)
			return VisibleQuote{Bid: mark.price, Ask: mark.price, AtMS: received, ReceivedMS: received, ValidUntilMS: math.MaxInt64, Bar: received}, nil
		}
		s.markets[id] = market
	}
	return func() {
		for id, market := range original {
			s.markets[id] = market
		}
	}
}
