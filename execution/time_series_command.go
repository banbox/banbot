package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/banbox/banexg/utils"
	"github.com/shopspring/decimal"
)

// TimeSeriesMetadata is presentation and selection state, committed with the
// account plan. Payload is opaque to execution; it cannot change lot ownership.
type TimeSeriesMetadata struct {
	OrderID           int64
	StrategyName      string
	Symbol            string
	TimeFrame         string
	EnterTag          string
	Short             bool
	CreatedMS         int64
	InitPrice         decimal.Decimal
	ExitTag           string
	ExitQuantitySteps int64             `json:",omitempty"`
	ExitConditions    *IntentConditions `json:",omitempty"`
	Payload           json.RawMessage
}

func (m TimeSeriesMetadata) sameIdentity(other TimeSeriesMetadata) bool {
	return m.OrderID == other.OrderID && m.StrategyName == other.StrategyName && m.Symbol == other.Symbol && m.TimeFrame == other.TimeFrame && m.EnterTag == other.EnterTag && m.Short == other.Short && m.CreatedMS == other.CreatedMS && m.InitPrice.Equal(other.InitPrice)
}

func TimeSeriesMetadataName(lot VirtualLotID) string {
	hash := sha256.Sum256([]byte(lot))
	return "time-series/lot:" + hex.EncodeToString(hash[:])
}

func (m TimeSeriesMetadata) validate() error {
	if m.OrderID <= 0 || !canonicalID(m.StrategyName) || !canonicalID(m.Symbol) || !canonicalID(m.TimeFrame) || m.CreatedMS < 0 || m.InitPrice.IsNegative() || len(m.Payload) > 1<<20 || len(m.Payload) > 0 && !json.Valid(m.Payload) {
		return errors.New("execution: invalid time-series metadata")
	}
	if m.TimeFrame != "ws" {
		if seconds, err := utils.TFToSecSafe(m.TimeFrame); err != nil || seconds <= 0 {
			return errors.New("execution: invalid time-series timeframe")
		}
	}
	return nil
}

func timeSeriesMetadataCheckpoint(strategy StrategyID, lot VirtualLotID, m TimeSeriesMetadata) (StrategyCheckpoint, error) {
	if err := m.validate(); err != nil {
		return StrategyCheckpoint{}, err
	}
	body, err := json.Marshal(m)
	return StrategyCheckpoint{Strategy: strategy, Name: TimeSeriesMetadataName(lot), Payload: body}, err
}

// TimeSeriesExitCommand selects lots inside the account owner. Amount is in
// instrument quantity units (not steps) and is consumed once across all lots.
// Rate takes precedence over Amount when strictly between zero and one.
type TimeSeriesExitCommand struct {
	ID               string
	Strategy         StrategyID
	StrategyEpoch    uint64 `json:",omitempty"`
	StrategySequence uint64 `json:",omitempty"`
	Symbols          []string
	TimeFrame        string
	OrderID          int64
	EnterTag         string
	Side             OrderSide // empty selects both entry directions
	FilledOnly       bool
	UnFillOnly       bool
	Force            bool
	Amount           decimal.Decimal
	Rate             decimal.Decimal
	Conditions       IntentConditions
	ExitTag          string
}

type TimeSeriesExitResult struct {
	Strategy           StrategyID
	Lot                VirtualLotID
	OrderID            int64
	Revision           int64
	ReducedSteps       int64
	CanceledEntrySteps int64
}

type timeSeriesExitReceipt struct {
	Hash    [32]byte
	PlanID  string
	Results []TimeSeriesExitResult
}

func (b *SharedAccountBorrow) ApplyTimeSeriesExit(caller context.Context, command TimeSeriesExitCommand, now int64) ([]TimeSeriesExitResult, error) {
	body, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	var owned TimeSeriesExitCommand
	if err = json.Unmarshal(body, &owned); err != nil {
		return nil, err
	}
	var results []TimeSeriesExitResult
	err = b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, caller)
		defer done()
		var err error
		results, err = s.applyTimeSeriesExit(ctx, owned, sha256.Sum256(body), now)
		return err
	})
	b.service.notifyCommitted()
	return results, err
}

func (c TimeSeriesExitCommand) validate() error {
	if !canonicalID(c.ID) || len(c.ID) > 512 || !canonicalID(string(c.Strategy)) || c.OrderID < 0 || c.FilledOnly && c.UnFillOnly || c.Side != "" && c.Side != Buy && c.Side != Sell || c.Amount.IsNegative() || c.Rate.IsNegative() || c.Rate.GreaterThan(decimal.NewFromInt(1)) || c.Conditions.StopBars != 0 {
		return errors.New("execution: invalid time-series exit command")
	}
	if c.TimeFrame != "" && c.TimeFrame != "ws" {
		if seconds, err := utils.TFToSecSafe(c.TimeFrame); err != nil || seconds <= 0 {
			return errors.New("execution: invalid time-series exit timeframe")
		}
	}
	for _, symbol := range c.Symbols {
		if !canonicalID(symbol) {
			return errors.New("execution: invalid time-series exit symbol")
		}
	}
	return (&EligibleIntent{ID: "validate-ts-exit", Account: AccountKey{"validate", "validate", "validate"}, Strategy: c.Strategy, Lot: "validate", Instrument: "validate", Kind: ExitIntent, Side: Sell, QuantitySteps: 1, Conditions: c.Conditions}).Validate()
}

type timeSeriesExitLot struct {
	index    int
	metadata TimeSeriesMetadata
	filled   int64
	pending  int64
	entryAt  int64
}

func (s *SharedAccount) applyTimeSeriesExit(ctx context.Context, c TimeSeriesExitCommand, hash [32]byte, now int64) ([]TimeSeriesExitResult, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	receiptName := "time-series/exit:" + c.ID
	body, err := s.store.StrategyCheckpoint(ctx, c.Strategy, receiptName)
	if err == nil {
		var receipt timeSeriesExitReceipt
		if err = json.Unmarshal(body, &receipt); err != nil {
			return nil, err
		}
		if receipt.Hash != hash {
			return nil, errors.New("execution: time-series exit identity reused")
		}
		if receipt.PlanID == "" {
			return receipt.Results, nil
		}
		latest, err := s.store.LatestPlan(ctx)
		if err != nil {
			return nil, err
		}
		if latest.ID != receipt.PlanID {
			return receipt.Results, nil
		}
		if !s.ready {
			return nil, errors.New("execution: time-series replay requires reconciliation")
		}
		prepared, err := s.restorePreparedRebalance(ctx, receipt.PlanID)
		if err != nil {
			return nil, err
		}
		return receipt.Results, s.SendPrepared(prepared, s.SendTime(now), ctx)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	ordering, err := s.strategyOrderingCheckpoints(ctx, []StrategyRebalance{{Strategy: c.Strategy, StrategyEpoch: c.StrategyEpoch, StrategySequence: c.StrategySequence}})
	if err != nil {
		return nil, err
	}
	policy, err := s.store.AccountPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if !s.ready || !policy.StrategyGrossLimits[c.Strategy].IsPositive() {
		return nil, errors.New("execution: time-series exit requires a reconciled declared strategy")
	}
	definitions, err := s.store.LoadPendingBook(ctx)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return nil, err
	}
	now = s.SendTime(now)
	symbols := map[string]bool{}
	for _, symbol := range c.Symbols {
		symbols[symbol] = true
	}
	matches := []timeSeriesExitLot{}
	remaining := decimal.Zero
	for i, d := range definitions {
		if d.Order.Strategy != c.Strategy {
			continue
		}
		body, err := s.store.StrategyCheckpoint(ctx, c.Strategy, TimeSeriesMetadataName(d.Order.Lot))
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		var m TimeSeriesMetadata
		if err = json.Unmarshal(body, &m); err != nil {
			return nil, err
		}
		if err = m.validate(); err != nil {
			return nil, err
		}
		if len(symbols) > 0 && !symbols[m.Symbol] || c.TimeFrame != "" && c.TimeFrame != m.TimeFrame || c.OrderID != 0 && c.OrderID != m.OrderID || c.EnterTag != "" && c.EnterTag != m.EnterTag || c.Side != "" && c.Side != d.Order.Entry.Side {
			continue
		}
		held := pendingLot(snapshot, &d.Order, d.Instrument.ID).SignedSteps
		filled := absSteps(held)
		pending := max(int64(0), d.Order.Entry.QuantitySteps-filled)
		if d.Order.Canceled {
			pending = 0
		}
		if filled == 0 && pending == 0 || c.FilledOnly && filled == 0 || c.UnFillOnly && pending == 0 || d.Order.Exit != nil && held != d.Order.Desired {
			continue
		}
		entryAt, err := s.store.LotEntryTime(ctx, c.Strategy, d.Order.Lot, d.Order.Entry.Side)
		if err != nil {
			return nil, err
		}
		if entryAt == 0 {
			entryAt = m.CreatedMS
		}
		matches = append(matches, timeSeriesExitLot{i, m, filled, pending, entryAt})
		remaining = remaining.Add(decimal.NewFromInt(filled).Add(decimal.NewFromInt(pending)).Mul(d.Instrument.QuantityStep))
	}
	if c.Rate.IsPositive() && c.Rate.LessThan(decimal.NewFromInt(1)) {
		remaining = remaining.Mul(c.Rate)
	} else if c.Amount.IsPositive() {
		remaining = c.Amount
	}
	// Reject unrepresentable budgets before any exchange cancellation.
	for _, match := range matches {
		if _, err := QuantitySteps(remaining, definitions[match.index].Instrument.QuantityStep); err != nil {
			return nil, err
		}
	}
	preferFilled := c.FilledOnly
	if c.Conditions.Limit.IsPositive() && len(matches) > 0 {
		first := definitions[matches[0].index]
		for _, m := range matches[1:] {
			if definitions[m.index].Instrument.ID != first.Instrument.ID {
				return nil, errors.New("execution: time-series limit exit spans instruments")
			}
		}
		q, err := s.ReadQuote(ctx, first.Instrument.ID, now)
		if err != nil {
			return nil, err
		}
		if c.Side == Buy {
			preferFilled = preferFilled || c.Conditions.Limit.GreaterThan(q.Bid)
		}
		if c.Side == Sell {
			preferFilled = preferFilled || c.Conditions.Limit.LessThan(q.Ask)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		cost := func(m timeSeriesExitLot, steps int64) float64 {
			return math.Round(decimal.NewFromInt(steps).Mul(definitions[m.index].Instrument.QuantityStep).Mul(m.metadata.InitPrice).Mul(decimal.NewFromInt(100)).InexactFloat64())
		}
		af, bf := cost(a, a.filled), cost(b, b.filled)
		if preferFilled && af != bf {
			return af > bf
		}
		ap, bp := cost(a, a.pending), cost(b, b.pending)
		if ap != bp {
			return ap > bp
		}
		if af != bf {
			return af < bf
		}
		// Legacy pre-projection views use CreatedMS as their real entry time.
		if a.metadata.CreatedMS != b.metadata.CreatedMS {
			return a.metadata.CreatedMS < b.metadata.CreatedMS
		}
		if a.metadata.OrderID != b.metadata.OrderID {
			return a.metadata.OrderID < b.metadata.OrderID
		}
		return definitions[a.index].Order.Lot < definitions[b.index].Order.Lot
	})
	var results []TimeSeriesExitResult
	checkpoints := append([]StrategyCheckpoint(nil), ordering...)
	for _, match := range matches {
		if !remaining.IsPositive() {
			break
		}
		d := &definitions[match.index]
		o := &d.Order
		units, err := QuantitySteps(remaining, d.Instrument.QuantityStep)
		if err != nil {
			return nil, err
		}
		if units == 0 {
			continue
		}
		if !c.Force && c.OrderID == 0 && match.filled > 0 && match.metadata.TimeFrame != "ws" && float64(now-match.entryAt) <= float64(utils.TFToSecs(match.metadata.TimeFrame))*900 {
			continue
		}
		if d.CommandRevision == math.MaxInt64 {
			return nil, errors.New("execution: time-series revision overflow")
		}
		// Cancellation is authoritative before sizing; a final entry fill changes
		// the filled/pending split but never increases the command's global budget.
		if err = s.cancelNativeForLot(ctx, o, now); err != nil {
			return nil, err
		}
		snapshot, err = s.store.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		if err = s.cancelPendingEntries(ctx, snapshot, o, d.Instrument.ID, now, false); err != nil {
			return nil, err
		}
		snapshot, err = s.store.Snapshot(ctx)
		if err != nil {
			return nil, err
		}
		held := pendingLot(snapshot, o, d.Instrument.ID).SignedSteps
		filled := absSteps(held)
		pending := max(int64(0), o.Entry.QuantitySteps-filled)
		if o.Canceled {
			pending = 0
		}
		reduce, cancel := filled, pending
		if c.FilledOnly {
			cancel = 0
		}
		if c.UnFillOnly {
			reduce = 0
		}
		reduce = min(reduce, units)
		cancel = min(cancel, units-reduce)
		if reduce == 0 && cancel == 0 {
			continue
		}
		if reduce > 0 {
			perLot := PendingCommand{Strategy: c.Strategy, Lot: o.Lot, Action: PendingSetExit, Exit: &PendingExitSpec{QuantitySteps: reduce, Conditions: c.Conditions}}
			lotHash := sha256.Sum256([]byte(fmt.Sprintf("%x/%s", hash, o.Lot)))
			if err = s.applyPendingExit(ctx, d, perLot, lotHash, now); err != nil {
				return nil, err
			}
			o.ResumeEntrySteps = pending - cancel
		} else {
			remainingSteps := o.Entry.QuantitySteps - cancel
			if remainingSteps > 0 {
				o.Entry.QuantitySteps = remainingSteps
			}
			o.Desired = held
			o.Entry.ReservedSteps = 0
			o.Entry.VenueLimit = nil
			o.Entry.FilledSteps = max(o.Entry.FilledSteps, filled)
			if remainingSteps <= filled {
				o.Canceled = true
				o.Entry.State = Canceled
			} else {
				o.Entry.State = PendingCondition
			}
		}
		d.CommandRevision++
		revision, _ := json.Marshal(d.CommandRevision)
		checkpoints = append(checkpoints, StrategyCheckpoint{Strategy: c.Strategy, Name: "pending-revision:" + string(o.Lot), Payload: revision})
		m := match.metadata
		if reduce > 0 {
			m.ExitTag = c.ExitTag
			m.ExitQuantitySteps = reduce
			conditions := c.Conditions
			m.ExitConditions = &conditions
		}
		metadata, err := timeSeriesMetadataCheckpoint(c.Strategy, o.Lot, m)
		if err != nil {
			return nil, err
		}
		metadata.Events = []StrategyAcceptedEvent{{Strategy: c.Strategy, Lot: o.Lot, Kind: ExitIntent, CommandID: c.ID, AtMS: now, Metadata: &m}}
		checkpoints = append(checkpoints, metadata)
		results = append(results, TimeSeriesExitResult{c.Strategy, o.Lot, m.OrderID, d.CommandRevision, reduce, cancel})
		remaining = remaining.Sub(decimal.NewFromInt(reduce + cancel).Mul(d.Instrument.QuantityStep))
	}
	planID := fmt.Sprintf("time-series-exit-%x", hash)
	if len(definitions) == 0 {
		body, err := json.Marshal(timeSeriesExitReceipt{Hash: hash, Results: results})
		if err != nil {
			return nil, err
		}
		return results, s.store.atomically(ctx, func(ctx context.Context) error {
			for _, checkpoint := range append(checkpoints, StrategyCheckpoint{Strategy: c.Strategy, Name: receiptName, Payload: body}) {
				if err := s.store.SaveProjectionMetadata(ctx, checkpoint); err != nil {
					return err
				}
			}
			return nil
		})
	}
	body, err = json.Marshal(timeSeriesExitReceipt{hash, planID, results})
	if err != nil {
		return nil, err
	}
	checkpoints = append(checkpoints, StrategyCheckpoint{Strategy: c.Strategy, Name: receiptName, Payload: body})
	if err = s.advancePendingDefinitions(ctx, definitions, now, checkpoints, planID); err != nil {
		return nil, err
	}
	return results, nil
}
