package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"reflect"
	"sort"
)

type InstrumentRebalance struct {
	Instrument        Instrument
	Targets           []ExecutableTarget
	Quote             VisibleQuote
	IntentConstraints []EligibleIntent // original contributor conditions/checkpoints, keyed strategy/lot/side/kind
}
type CombinedRebalance struct {
	PlanID           string
	Sequence         int64
	DecisionMS       int64
	ExpiresMS        int64
	Requests         []InstrumentRebalance
	CarryIntents     []EligibleIntent
	CarryTargets     []ExecutableTarget
	CarryInstruments []Instrument
	Risk             PortfolioRisk
}
type PreparedRebalance struct {
	Plan             Plan
	OrderIDs         []string
	InternalMatchIDs []string
	Coordinations    []Coordination
}

func rebalanceID(parts ...any) string {
	body := make([]byte, 0, 256)
	body = append(body, "banbot-execution-id-v2"...)
	var frame [8]byte
	appendFrame := func(value string) {
		binary.BigEndian.PutUint64(frame[:], uint64(len(value)))
		body = append(body, frame[:]...)
		body = append(body, value...)
	}
	for _, part := range parts {
		value := reflect.ValueOf(part)
		if !value.IsValid() {
			appendFrame("<nil>")
			appendFrame("")
			continue
		}
		appendFrame(value.Type().String())
		if value.Kind() == reflect.String {
			appendFrame(value.String())
		} else {
			appendFrame(fmt.Sprint(part))
		}
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:16])
}

// Only persisted-v1 identity lookup and SDK cumulative duplicate IDs retain
// this encoding. New internal IDs frame each typed component independently.
func legacyRebalanceID(parts ...any) string {
	hash := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return hex.EncodeToString(hash[:16])
}

// PrepareRebalance is local owner work. It validates complete account targets,
// creates stable virtual intents, crosses opposing eligible deltas at a frozen
// visible midpoint, and persists only the real residual. It never calls an
// adapter. The returned increase orders carry durable decrease dependencies.
func (e *OwnerExecutor) PrepareRebalance(request CombinedRebalance) (PreparedRebalance, error) {
	return e.prepareRebalance(request, nil)
}

// PrepareRebalanceWithCheckpoint commits the strategy definition and accepted
// plan in one local transaction. Rejection rolls both back; network sends follow
// this commit and must retain the checkpoint even when their result is unknown.
func (e *OwnerExecutor) PrepareRebalanceWithCheckpoint(request CombinedRebalance, checkpoint StrategyCheckpoint) (PreparedRebalance, error) {
	return e.prepareRebalance(request, []StrategyCheckpoint{checkpoint})
}

func (e *OwnerExecutor) PrepareRebalanceWithCheckpoints(request CombinedRebalance, checkpoints []StrategyCheckpoint) (PreparedRebalance, error) {
	return e.prepareRebalance(request, checkpoints)
}

func (e *OwnerExecutor) prepareRebalance(request CombinedRebalance, checkpoints []StrategyCheckpoint) (PreparedRebalance, error) {
	var result PreparedRebalance
	err := e.local(func(ctx context.Context) error {
		return e.Store.atomically(ctx, func(ctx context.Context) error {
			for _, checkpoint := range checkpoints {
				if err := e.Store.saveAcceptedCheckpoint(ctx, checkpoint); err != nil {
					return err
				}
			}
			body, err := payload(request)
			if err != nil {
				return err
			}
			hash := sha256.Sum256([]byte(body))
			requestHash := hex.EncodeToString(hash[:])
			if !canonicalID(request.PlanID) || request.Sequence < 0 || request.DecisionMS < 0 || request.ExpiresMS <= request.DecisionMS || len(request.Requests) == 0 {
				return errors.New("execution: invalid combined rebalance")
			}
			if existing, err := e.Store.Plan(ctx, request.PlanID); err == nil {
				if existing.RequestHash != requestHash {
					return errors.New("execution: rebalance identity reused with different request")
				}
				result.Plan = existing
				return e.Store.commit(ctx, func(tx *storeTxn) error {
					rows, err := tx.Query(opListPlanOrders, e.Store.accountID, request.PlanID)
					if err != nil {
						return err
					}
					defer rows.Close()
					for rows.Next() {
						var id string
						if err := rows.Scan(&id); err != nil {
							return err
						}
						result.OrderIDs = append(result.OrderIDs, id)
					}
					return rows.Err()
				})
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			snapshot, err := e.Store.Snapshot(ctx)
			if err != nil {
				return err
			}
			projectedLots, projectedPositions, err := confirmedExposure(snapshot)
			if err != nil {
				return err
			}
			plan := Plan{ID: request.PlanID, Sequence: request.Sequence, DecisionMS: request.DecisionMS, ExpiresMS: request.ExpiresMS, Intents: append([]EligibleIntent(nil), request.CarryIntents...), CarriedIntents: append([]EligibleIntent(nil), request.CarryIntents...), RequestHash: requestHash}
			plan.RiskValidUntilMS = request.ExpiresMS
			type pending struct {
				request      InstrumentRebalance
				coordination Coordination
				observation  ExecutionObservation
				intents      []EligibleIntent
			}
			var pendingRequests []pending
			seen := make(map[string]bool)
			for _, r := range request.Requests {
				if seen[r.Instrument.ID] {
					return errors.New("execution: duplicate rebalance instrument")
				}
				seen[r.Instrument.ID] = true
			}
			type targetKey struct {
				instrument string
				strategy   StrategyID
				lot        VirtualLotID
			}
			carried := make(map[targetKey]bool)
			for _, target := range request.CarryTargets {
				key := targetKey{target.Instrument, target.Strategy, target.Lot}
				if !canonicalID(target.Instrument) || !canonicalID(string(target.Strategy)) || !canonicalID(string(target.Lot)) || target.SignedSteps == -1<<63 || seen[target.Instrument] || carried[key] {
					return errors.New("execution: invalid/duplicate untouched carried target")
				}
				carried[key] = true
				plan.CarriedTargets = append(plan.CarriedTargets, target)
				plan.Targets = append(plan.Targets, target)
			}
			seen = make(map[string]bool)
			for _, r := range request.Requests {
				if err := r.Instrument.Validate(); err != nil {
					return err
				}
				plan.ContributorConstraints = append(plan.ContributorConstraints, r.IntentConstraints...)
				plan.Instruments = append(plan.Instruments, r.Instrument)
				plan.RebalancedInstruments = append(plan.RebalancedInstruments, r.Instrument.ID)
				plan.RiskValidUntilMS = min(plan.RiskValidUntilMS, r.Quote.ValidUntilMS)
				if seen[r.Instrument.ID] {
					return errors.New("execution: duplicate rebalance instrument")
				}
				seen[r.Instrument.ID] = true
				q := r.Quote
				if !q.Bid.IsPositive() || q.Ask.LessThan(q.Bid) || q.AtMS > q.ReceivedMS || q.ReceivedMS > request.DecisionMS || q.ValidUntilMS <= request.DecisionMS {
					return errors.New("execution: rebalance quote not visible/valid")
				}
				units, _ := q.Bid.Add(q.Ask).Mul(decimal.RequireFromString("0.5")).QuoRem(r.Instrument.PriceTick, 0)
				price := units.Mul(r.Instrument.PriceTick)
				if price.LessThan(q.Bid) || price.GreaterThan(q.Ask) {
					return errors.New("execution: no quote-compatible midpoint tick")
				}
				observation := ExecutionObservation{Price: price, Bid: q.Bid, Ask: q.Ask, AtMS: q.AtMS, ValidUntilMS: q.ValidUntilMS, Bar: q.Bar}
				coordination, err := Coordinate(snapshot, r.Targets, r.Instrument, observation, request.Risk)
				if err != nil {
					return err
				}
				for _, leg := range coordination.Legs {
					if leg.Steps < r.Instrument.MinSteps || r.Instrument.Notional(leg.Steps, price).LessThan(r.Instrument.MinNotional) {
						return errors.New("execution: coordinated real delta below venue minimum")
					}
				}
				p := pending{request: r, coordination: coordination, observation: observation}
				type contributorKey struct {
					strategy StrategyID
					lot      VirtualLotID
					side     OrderSide
					kind     IntentKind
				}
				constraints := make(map[contributorKey]EligibleIntent)
				for _, constraint := range r.IntentConstraints {
					if constraint.Conditions.PostOnly && !e.Adapter.Capabilities().PostOnly {
						return errors.New("execution: adapter does not prove post-only support")
					}
					if err := constraint.Validate(); err != nil {
						return err
					}
					key := contributorKey{constraint.Strategy, constraint.Lot, constraint.Side, constraint.Kind}
					if constraint.Account != e.Store.key || constraint.Instrument != r.Instrument.ID {
						return errors.New("execution: foreign contributor condition")
					}
					if _, ok := constraints[key]; ok {
						return errors.New("execution: duplicate contributor condition")
					}
					constraints[key] = constraint
				}
				for _, target := range r.Targets {
					target.Instrument = r.Instrument.ID
					plan.Targets = append(plan.Targets, target)
				}
				for _, delta := range coordination.Deltas {
					for _, part := range []struct {
						kind  IntentKind
						steps int64
					}{{ExitIntent, delta.ReducingSteps}, {EntryIntent, delta.Steps - delta.ReducingSteps}} {
						if part.steps == 0 {
							continue
						}
						intent := EligibleIntent{ID: VirtualIntentID("virtual-" + rebalanceID(request.PlanID, "/", r.Instrument.ID, "/", delta.Strategy, "/", delta.Lot, "/", part.kind)), Account: e.Store.key, Strategy: delta.Strategy, Lot: delta.Lot, Instrument: r.Instrument.ID, Kind: part.kind, Side: delta.Side, QuantitySteps: part.steps, State: Eligible, Conditions: IntentConditions{CreatedBar: q.Bar}}
						if constraint, ok := constraints[contributorKey{delta.Strategy, delta.Lot, delta.Side, part.kind}]; ok {
							available, err := constraint.Evaluate(price, request.DecisionMS, q.Bar)
							if err != nil || available < part.steps {
								return errors.Join(errors.New("execution: contributor is not eligible for frozen target delta"), err)
							}
							intent.Conditions = constraint.Conditions
							intent.Triggered, intent.TrailingActive, intent.TrailingAnchor = constraint.Triggered, constraint.TrailingActive, constraint.TrailingAnchor
						}
						p.intents = append(p.intents, intent)
						plan.Intents = append(plan.Intents, intent)
					}
				}
				pendingRequests = append(pendingRequests, p)
				result.Coordinations = append(result.Coordinations, coordination)
			}
			descriptors := make(map[string]Instrument)
			if previous, err := e.Store.LatestPlan(ctx); err == nil {
				for _, instrument := range previous.Instruments {
					descriptors[instrument.ID] = instrument
				}
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			for _, instrument := range request.CarryInstruments {
				if err := instrument.Validate(); err != nil {
					return err
				}
				if seen[instrument.ID] {
					return errors.New("execution: carried descriptor overlaps execution instrument")
				}
				descriptors[instrument.ID] = instrument
			}
			added := make(map[string]bool)
			for _, target := range request.CarryTargets {
				if added[target.Instrument] {
					continue
				}
				instrument, ok := descriptors[target.Instrument]
				if !ok {
					return errors.New("execution: carried target requires preserved instrument descriptor")
				}
				plan.Instruments = append(plan.Instruments, instrument)
				added[target.Instrument] = true
			}
			projectedLots, projectedPositions, err = reservedCarriedExposure(snapshot, request.CarryTargets, plan.Instruments)
			if err != nil {
				return err
			}
			// Account net margin is checked for the combined target, rather than
			// granting every instrument its own copy of the account equity budget.
			desiredMargin := decimal.Zero
			gross := decimal.Zero
			for _, p := range pendingRequests {
				desiredMargin = desiredMargin.Add(p.request.Instrument.Notional(absSteps(p.coordination.DesiredSteps), p.observation.Price).Mul(request.Risk.MarginRate))
				for _, t := range p.request.Targets {
					gross = gross.Add(p.request.Instrument.Notional(absSteps(t.SignedSteps), p.observation.Price))
				}
			}
			for _, position := range projectedPositions {
				if !seen[position.Instrument.ID] {
					mark, ok := request.Risk.Marks[position.Instrument.ID]
					if !ok {
						return errors.New("execution: missing untouched actual mark")
					}
					desiredMargin = desiredMargin.Add(position.Instrument.Notional(absSteps(position.SignedSteps), mark).Mul(request.Risk.MarginRate))
				}
			}
			marks := make(map[string]decimal.Decimal)
			for id, mark := range request.Risk.Marks {
				marks[id] = mark
			}
			for _, p := range pendingRequests {
				marks[p.request.Instrument.ID] = p.observation.Price
			}
			equity, err := snapshot.Equity(marks)
			if err != nil {
				return err
			}
			currentMargin := decimal.Zero
			for _, p := range pendingRequests {
				currentMargin = currentMargin.Add(p.request.Instrument.Notional(absSteps(p.coordination.ProjectedSteps), p.observation.Price).Mul(request.Risk.MarginRate))
			}
			for _, position := range projectedPositions {
				if !seen[position.Instrument.ID] {
					currentMargin = currentMargin.Add(position.Instrument.Notional(absSteps(position.SignedSteps), marks[position.Instrument.ID]).Mul(request.Risk.MarginRate))
				}
			}
			previousGross := make(map[StrategyID]decimal.Decimal)
			combinedGross := make(map[StrategyID]decimal.Decimal)
			nav := make(map[StrategyID]decimal.Decimal)
			for strategy, cash := range snapshot.SyntheticStrategyCash {
				nav[strategy] = cash
			}
			for _, lot := range snapshot.Lots {
				mark, ok := marks[lot.Instrument.ID]
				if !ok || !mark.IsPositive() {
					return errors.New("execution: missing combined strategy valuation mark")
				}
				amount := lot.Instrument.Notional(absSteps(lot.SignedSteps), mark)
				if seen[lot.Instrument.ID] {
					previousGross[lot.Strategy] = previousGross[lot.Strategy].Add(amount)
				}
				nav[lot.Strategy] = nav[lot.Strategy].Add(lot.Unrealized(mark))
			}
			for _, lot := range projectedLots {
				if !seen[lot.Instrument.ID] {
					mark, ok := marks[lot.Instrument.ID]
					if !ok || !mark.IsPositive() {
						return errors.New("execution: projected lot mark missing")
					}
					combinedGross[lot.Strategy] = combinedGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
					previousGross[lot.Strategy] = previousGross[lot.Strategy].Add(lot.Instrument.Notional(absSteps(lot.SignedSteps), mark))
				}
			}
			for _, p := range pendingRequests {
				for _, target := range p.request.Targets {
					combinedGross[target.Strategy] = combinedGross[target.Strategy].Add(p.request.Instrument.Notional(absSteps(target.SignedSteps), p.observation.Price))
				}
			}
			oldTotalGross := decimal.Zero
			gross = decimal.Zero
			for strategy, amount := range combinedGross {
				limit, ok := request.Risk.StrategyGrossLimits[strategy]
				if !ok || !limit.IsPositive() {
					return errors.New("execution: missing combined strategy gross limit")
				}
				if amount.GreaterThan(previousGross[strategy]) && (amount.GreaterThan(limit) || amount.Mul(request.Risk.MarginRate).GreaterThan(nav[strategy])) {
					return errors.New("execution: combined strategy gross/NAV budget exceeded")
				}
				gross = gross.Add(amount)
				oldTotalGross = oldTotalGross.Add(previousGross[strategy])
			}
			if desiredMargin.GreaterThan(currentMargin) && (desiredMargin.GreaterThan(request.Risk.MaxAccountMargin) || desiredMargin.GreaterThan(equity)) || gross.GreaterThan(oldTotalGross) && gross.GreaterThan(request.Risk.MaxVirtualGross) {
				return errors.New("execution: combined account budget exceeded")
			}
			if err := e.Store.SavePlan(ctx, plan); err != nil {
				return err
			}
			result.Plan = plan
			frozenRisk := request.Risk
			frozenRisk.Marks = marks
			for _, p := range pendingRequests {
				remaining := make(map[VirtualIntentID]int64)
				var buys, sells []EligibleIntent
				for _, intent := range p.intents {
					remaining[intent.ID] = intent.QuantitySteps
					if intent.Side == Buy {
						buys = append(buys, intent)
					} else {
						sells = append(sells, intent)
					}
				}
				sort.Slice(buys, func(a, b int) bool { return buys[a].ID < buys[b].ID })
				sort.Slice(sells, func(a, b int) bool { return sells[a].ID < sells[b].ID })
				for _, buy := range buys {
					for _, sell := range sells {
						steps := min(remaining[buy.ID], remaining[sell.ID])
						if steps == 0 {
							continue
						}
						if buy.Strategy == sell.Strategy {
							return errors.New("execution: self-cross requires target consolidation")
						}
						if buy.Conditions.PostOnly || sell.Conditions.PostOnly {
							return errors.New("execution: post-only contributor cannot be internally crossed")
						}
						id := "internal-" + rebalanceID(plan.ID, "/", buy.ID, "/", sell.ID)
						match := InternalMatch{ID: id, PlanID: plan.ID, Instrument: p.request.Instrument, BuyIntent: buy.ID, SellIntent: sell.ID, Steps: steps, Quote: p.request.Quote, AtMS: request.DecisionMS}
						if _, err := e.Store.ApplyInternalMatch(ctx, match); err != nil {
							return err
						}
						remaining[buy.ID] -= steps
						remaining[sell.ID] -= steps
						result.InternalMatchIDs = append(result.InternalMatchIDs, id)
					}
				}
				var prerequisite string
				for n, leg := range p.coordination.Legs {
					order := OrderIntent{ID: fmt.Sprintf("%s-%s-%02d", plan.ID, p.request.Instrument.ID, n), PlanID: plan.ID, Instrument: p.request.Instrument, Side: leg.Side, Steps: leg.Steps, Observation: p.observation, ReduceOnly: leg.ReduceOnly, Risk: &frozenRisk}
					if prerequisite != "" {
						order.RequiresFilled = []string{prerequisite}
					}
					left := leg.Steps
					for _, intent := range p.intents {
						if intent.Side != leg.Side || remaining[intent.ID] == 0 {
							continue
						}
						steps := min(left, remaining[intent.ID])
						if steps == 0 {
							continue
						}
						order.Allocations = append(order.Allocations, FillAllocation{ID: string(intent.ID), IntentID: intent.ID, Strategy: intent.Strategy, Lot: intent.Lot, Side: intent.Side, Kind: intent.Kind, Steps: steps})
						limit := intent.Conditions.Limit
						order.PostOnly = order.PostOnly || intent.Conditions.PostOnly
						if limit.IsPositive() {
							_, remainder := limit.QuoRem(p.request.Instrument.PriceTick, 0)
							if !remainder.IsZero() {
								return errors.New("execution: contributor limit violates instrument tick")
							}
							if order.Limit.IsZero() || leg.Side == Buy && limit.LessThan(order.Limit) || leg.Side == Sell && limit.GreaterThan(order.Limit) {
								order.Limit = limit
							}
						}
						remaining[intent.ID] -= steps
						left -= steps
					}
					if left != 0 {
						return errors.New("execution: residual allocation does not conserve coordinated real leg")
					}
					if err := e.Store.PrepareOrder(ctx, order, request.DecisionMS); err != nil {
						return err
					}
					result.OrderIDs = append(result.OrderIDs, order.ID)
					if leg.ReduceOnly {
						prerequisite = order.ID
					}
				}
				for _, units := range remaining {
					if units != 0 {
						return errors.New("execution: unallocated strategy delta after crossing")
					}
				}
			}
			return nil
		})
	})
	return result, err
}
