package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type TargetUpdateMode string

const (
	StrategyTargetsFull  TargetUpdateMode = "full"
	StrategyTargetsPatch TargetUpdateMode = "patch"
)

// StrategyRebalance contains only one strategy's targets. Full clears omitted
// lots of that strategy; Patch changes the supplied lots. The account owner
// carries every other strategy and resolves quotes for omitted Full exits.
type StrategyRebalance struct {
	Strategy              StrategyID
	PlanID                string
	DecisionMS, ExpiresMS int64
	Mode                  TargetUpdateMode
	Requests              []InstrumentRebalance
}

type strategyRevision struct {
	Hash  string
	Empty bool
}

func (b *SharedAccountBorrow) PrepareStrategy(request StrategyRebalance) (PreparedRebalance, error) {
	return b.PrepareStrategyContext(context.Background(), request)
}

func (b *SharedAccountBorrow) PrepareStrategyContext(callerCtx context.Context, request StrategyRebalance) (PreparedRebalance, error) {
	var result PreparedRebalance
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		var err error
		result, err = s.prepareStrategy(request, ctx)
		return err
	})
	return result, err
}

func (b *SharedAccountBorrow) RebalanceStrategy(request StrategyRebalance, nowMS int64) error {
	return b.RebalanceStrategyContext(context.Background(), request, nowMS)
}

func (b *SharedAccountBorrow) RebalanceStrategyContext(callerCtx context.Context, request StrategyRebalance, nowMS int64) error {
	return b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		prepared, err := s.prepareStrategy(request, ctx)
		if err != nil {
			return err
		}
		return s.sendPreparedRebalance(prepared, nowMS, ctx)
	})
}

func (s *SharedAccount) prepareStrategy(request StrategyRebalance, ctx context.Context) (PreparedRebalance, error) {
	return s.PrepareStrategiesWithCheckpoint([]StrategyRebalance{request}, ctx, nil)
}

// PrepareStrategiesWithCheckpoint accepts only caller-owned strategy revisions.
// The owner merges other strategies and commits all definitions and acceptance
// events with the frozen account plan. Call it within WithState.
func (s *SharedAccount) PrepareStrategiesWithCheckpoint(updates []StrategyRebalance, ctx context.Context, checkpoint *StrategyCheckpoint) (PreparedRebalance, error) {
	if !s.ready {
		return PreparedRebalance{}, errors.New("execution: shared account is not reconciled")
	}
	if len(updates) == 0 {
		return PreparedRebalance{}, errors.New("execution: strategy target batch is empty")
	}
	updates = append([]StrategyRebalance(nil), updates...)
	sort.Slice(updates, func(i, j int) bool { return updates[i].Strategy < updates[j].Strategy })
	owned := make(map[StrategyID]TargetUpdateMode)
	for n := range updates {
		r := &updates[n]
		if r.Mode == "" {
			r.Mode = StrategyTargetsFull
		}
		if !canonicalID(string(r.Strategy)) || !canonicalID(r.PlanID) || r.DecisionMS < 0 || r.ExpiresMS <= r.DecisionMS || r.Mode != StrategyTargetsFull && r.Mode != StrategyTargetsPatch || owned[r.Strategy] != "" {
			return PreparedRebalance{}, errors.New("execution: invalid strategy target revision")
		}
		if r.PlanID != updates[0].PlanID || r.DecisionMS != updates[0].DecisionMS || r.ExpiresMS != updates[0].ExpiresMS {
			return PreparedRebalance{}, errors.New("execution: strategy batch requires one plan and decision clock")
		}
		owned[r.Strategy] = r.Mode
	}
	request := updates[0]
	if checkpoint != nil {
		for _, event := range checkpoint.Events {
			if owned[event.Strategy] == "" {
				return PreparedRebalance{}, errors.New("execution: acceptance event does not own strategy")
			}
		}
	}
	body, err := payload(updates)
	if err != nil {
		return PreparedRebalance{}, err
	}
	hash := sha256.Sum256([]byte(body))
	revision := strategyRevision{Hash: hex.EncodeToString(hash[:])}
	name := "target-revision:" + request.PlanID
	if previous, err := s.store.StrategyCheckpoint(ctx, request.Strategy, name); err == nil {
		var accepted strategyRevision
		if err := json.Unmarshal(previous, &accepted); err != nil {
			return PreparedRebalance{}, err
		}
		if accepted.Hash != revision.Hash {
			return PreparedRebalance{}, errors.New("execution: strategy revision identity reused")
		}
		if accepted.Empty {
			return PreparedRebalance{}, nil
		}
		var result PreparedRebalance
		result.Plan, err = s.store.Plan(ctx, request.PlanID)
		if err != nil {
			return result, err
		}
		err = s.store.commit(ctx, func(tx *storeTxn) error {
			rows, err := tx.Query(opListPlanOrders, s.store.accountID, request.PlanID)
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
		return result, err
	} else if !errors.Is(err, sql.ErrNoRows) {
		return PreparedRebalance{}, err
	}
	if len(s.markets) == 0 {
		return PreparedRebalance{}, errors.New("execution: strategy targets require registered account policy and quotes")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return PreparedRebalance{}, err
	}
	projected, _, err := confirmedExposure(snapshot)
	if err != nil {
		return PreparedRebalance{}, err
	}
	type targetKey struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	targets := make(map[targetKey]ExecutableTarget)
	for _, lot := range projected {
		key := targetKey{lot.Instrument.ID, lot.Strategy, lot.ID}
		targets[key] = ExecutableTarget{Instrument: key.instrument, Strategy: key.strategy, Lot: key.lot, SignedSteps: lot.SignedSteps}
	}
	latest, err := s.store.LatestPlan(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return PreparedRebalance{}, err
	}
	latest.Targets, err = s.retainedPlanTargets(ctx, latest, snapshot)
	if err != nil {
		return PreparedRebalance{}, err
	}
	for _, target := range latest.Targets {
		targets[targetKey{target.Instrument, target.Strategy, target.Lot}] = target
	}
	requests := make(map[string]InstrumentRebalance)
	for _, update := range updates {
		seenInstruments := make(map[string]bool)
		for _, input := range update.Requests {
			if err := input.Instrument.Validate(); err != nil {
				return PreparedRebalance{}, err
			}
			if seenInstruments[input.Instrument.ID] {
				return PreparedRebalance{}, errors.New("execution: duplicate strategy instrument")
			}
			seenInstruments[input.Instrument.ID] = true
			seen := make(map[VirtualLotID]bool)
			for _, target := range input.Targets {
				if target.Strategy != update.Strategy || !canonicalID(string(target.Lot)) || target.Instrument != "" && target.Instrument != input.Instrument.ID || seen[target.Lot] {
					return PreparedRebalance{}, errors.New("execution: foreign or duplicate strategy target")
				}
				seen[target.Lot] = true
			}
			for _, constraint := range input.IntentConstraints {
				if constraint.Strategy != update.Strategy {
					return PreparedRebalance{}, errors.New("execution: foreign strategy contributor condition")
				}
			}
			if update.Mode == StrategyTargetsPatch && len(input.Targets) == 0 {
				continue
			}
			if prior, ok := requests[input.Instrument.ID]; ok {
				descriptor, _ := payload(prior.Instrument)
				incoming, _ := payload(input.Instrument)
				quote, _ := payload(prior.Quote)
				incomingQuote, _ := payload(input.Quote)
				if descriptor != incoming || quote != incomingQuote {
					return PreparedRebalance{}, errors.New("execution: strategy batch has inconsistent instrument or quote")
				}
				prior.Targets = append(prior.Targets, input.Targets...)
				prior.IntentConstraints = append(prior.IntentConstraints, input.IntentConstraints...)
				requests[input.Instrument.ID] = prior
			} else {
				input.Targets = append([]ExecutableTarget(nil), input.Targets...)
				input.IntentConstraints = append([]EligibleIntent(nil), input.IntentConstraints...)
				requests[input.Instrument.ID] = input
			}
		}
	}
	for key, target := range targets {
		if owned[key.strategy] != StrategyTargetsFull {
			continue
		}
		target.SignedSteps = 0
		targets[key] = target
		if _, present := requests[key.instrument]; !present {
			market, present := s.markets[key.instrument]
			if !present {
				return PreparedRebalance{}, fmt.Errorf("execution: omitted strategy exit quote missing: %s", key.instrument)
			}
			quote, err := s.ReadQuote(ctx, key.instrument, request.DecisionMS)
			if err != nil {
				return PreparedRebalance{}, err
			}
			requests[key.instrument] = InstrumentRebalance{Instrument: market.instrument, Quote: quote}
		}
	}
	for id, input := range requests {
		for _, target := range input.Targets {
			target.Instrument = id
			targets[targetKey{id, target.Strategy, target.Lot}] = target
		}
		input.Targets = nil
		requests[id] = input
	}
	for key, target := range targets {
		if input, present := requests[key.instrument]; present {
			input.Targets = append(input.Targets, target)
			requests[key.instrument] = input
		}
	}
	combined := CombinedRebalance{PlanID: request.PlanID, DecisionMS: request.DecisionMS, ExpiresMS: request.ExpiresMS}
	ids := make([]string, 0, len(requests))
	for id := range requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		input := requests[id]
		sort.Slice(input.Targets, func(i, j int) bool {
			if input.Targets[i].Strategy != input.Targets[j].Strategy {
				return input.Targets[i].Strategy < input.Targets[j].Strategy
			}
			return input.Targets[i].Lot < input.Targets[j].Lot
		})
		combined.Requests = append(combined.Requests, input)
	}
	revision.Empty = len(combined.Requests) == 0
	data, _ := json.Marshal(revision)
	var checkpoints []StrategyCheckpoint
	if checkpoint != nil {
		checkpoints = append(checkpoints, *checkpoint)
	}
	for _, update := range updates {
		checkpoints = append(checkpoints, StrategyCheckpoint{Strategy: update.Strategy, Name: name, Payload: data})
	}
	if revision.Empty {
		err := s.executorFor(ctx).local(func(ctx context.Context) error {
			return s.store.atomically(ctx, func(ctx context.Context) error {
				for _, item := range checkpoints {
					if err := s.store.saveAcceptedCheckpoint(ctx, item); err != nil {
						return err
					}
				}
				return nil
			})
		})
		return PreparedRebalance{}, err
	}
	return s.prepareRebalanceWithCheckpoints(combined, ctx, checkpoints)
}

// Retire inherited zero declarations only after all corresponding execution
// state settles. Newly supplied targets and immutable old plans stay intact.
func (s *SharedAccount) retainedPlanTargets(ctx context.Context, plan Plan, snapshot AccountSnapshot) ([]ExecutableTarget, error) {
	type key struct {
		instrument string
		strategy   StrategyID
		lot        VirtualLotID
	}
	zero := make(map[key]bool)
	for _, target := range plan.Targets {
		if target.SignedSteps == 0 {
			zero[key{target.Instrument, target.Strategy, target.Lot}] = true
		}
	}
	if len(zero) == 0 {
		return plan.Targets, nil
	}
	active := make(map[key]bool)
	for _, lot := range snapshot.Lots {
		if lot.SignedSteps != 0 {
			active[key{lot.Instrument.ID, lot.Strategy, lot.ID}] = true
		}
	}
	for _, order := range snapshot.Orders {
		for _, allocation := range order.Intent.Allocations {
			if allocation.Steps > order.AllocationFilled[allocation.ID] {
				active[key{order.Intent.Instrument.ID, allocation.Strategy, allocation.Lot}] = true
			}
		}
	}
	for _, intent := range plan.Intents {
		k := key{intent.Instrument, intent.Strategy, intent.Lot}
		if !zero[k] || active[k] {
			continue
		}
		current, err := s.store.Intent(ctx, intent.ID)
		if err != nil {
			return nil, err
		}
		if current.State != Filled && current.State != Canceled && current.State != Expired {
			active[key{current.Instrument, current.Strategy, current.Lot}] = true
		}
	}
	targets := make([]ExecutableTarget, 0, len(plan.Targets))
	for _, target := range plan.Targets {
		if target.SignedSteps != 0 || active[key{target.Instrument, target.Strategy, target.Lot}] {
			targets = append(targets, target)
		}
	}
	return targets, nil
}
