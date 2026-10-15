package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/banbox/banbot/execution"
	"github.com/banbox/banbot/factor"
)

// FullZeroOmissions names synthetic zero declarations that need no new quote
// after their SID leaves execution membership. Explicit targets are retained;
// consumers must still verify flat settled owner evidence before omitting them.
func FullZeroOmissions(target, previous *factor.PortfolioTarget, declaredSIDs []int32) map[int32]bool {
	result := map[int32]bool{}
	if target == nil || previous == nil || target.Spec().Mode != factor.Full {
		return result
	}
	explicit := target.Allocations()
	for sid, a := range previous.Allocations() {
		if _, supplied := explicit[sid]; !supplied && a.Value == "0" && !slices.Contains(declaredSIDs, sid) {
			result[sid] = true
		}
	}
	return result
}

// RetainPolicyScope prepares a fresh generation's execution-only membership.
// It does not inherit accepted policy state or alter ranking/data membership.
// The returned owned config must also be used by the subscription planner.
func (l *Live) RetainPolicyScope(previous *Live, snapshot execution.AccountSnapshot) (Config, error) {
	var old Config
	var oldAccount *AccountSink
	if previous != nil {
		previous.mu.Lock()
		old = previous.c
		if account, ok := previous.sink.(*AccountSink); ok {
			oldAccount = account
		}
		previous.mu.Unlock()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	config, err := CloneConfig(l.c)
	if err != nil {
		return Config{}, err
	}
	if l.policy == nil {
		return config, nil
	}
	if l.liveStarted || l.stopped || l.warmGridCount != 0 {
		return Config{}, errors.New("runner: policy scope retention must precede candidate warmup")
	}
	if previous != nil && (config.StrategyID != old.StrategyID || config.AccountID != old.AccountID) {
		return Config{}, errors.New("runner: policy scope owner changed")
	}
	account, ok := l.sink.(*AccountSink)
	if !ok {
		return Config{}, errors.New("runner: automatic policy scope requires native account evidence")
	}
	evidence, err := account.Account.PolicyEvidenceContext(context.Background(), execution.StrategyID(config.StrategyID))
	if err != nil {
		return Config{}, err
	}
	var checkpoint policySinkCheckpoint
	if evidence.State.Version > 0 {
		if err := json.Unmarshal(evidence.State.Payload, &checkpoint); err != nil {
			return Config{}, err
		}
		if checkpoint.Version != 1 {
			return Config{}, errors.New("runner: unsupported policy scope checkpoint")
		}
	}
	metadata := maps.Clone(checkpoint.Instruments)
	if metadata == nil {
		metadata = map[int32]execution.Instrument{}
	}
	symbols := maps.Clone(checkpoint.SIDMap)
	if symbols == nil {
		symbols = map[int32]string{}
	}
	funding := maps.Clone(checkpoint.FundingInstruments)
	if funding == nil {
		funding = map[int32]execution.Instrument{}
	}
	for sid, instrument := range old.Execution.Instruments {
		metadata[sid] = instrument
	}
	for sid, symbol := range old.Snapshot.SIDMap {
		symbols[sid] = symbol
	}
	if oldAccount != nil {
		for sid, i := range oldAccount.Instruments {
			metadata[sid] = i
		}
		for sid, i := range oldAccount.FundingInstruments {
			funding[sid] = i
		}
	}
	for sid, i := range account.Instruments {
		if _, exists := metadata[sid]; !exists {
			metadata[sid] = i
		}
	}
	for sid, symbol := range config.Snapshot.SIDMap {
		if _, exists := symbols[sid]; !exists {
			symbols[sid] = symbol
		}
	}
	selected := map[int32]bool{}
	activeIDs := map[string]execution.Instrument{}
	collect := func(snap execution.AccountSnapshot) {
		for _, lot := range snap.Lots {
			if lot.Strategy == execution.StrategyID(config.StrategyID) && lot.SignedSteps != 0 {
				activeIDs[lot.Instrument.ID] = lot.Instrument
			}
		}
		for _, order := range snap.Orders {
			for _, a := range order.Intent.Allocations {
				if a.Strategy == execution.StrategyID(config.StrategyID) && a.Steps > order.AllocationFilled[a.ID] {
					activeIDs[order.Intent.Instrument.ID] = order.Intent.Instrument
				}
			}
		}
	}
	collect(snapshot)
	collect(evidence.Snapshot)
	for _, intent := range evidence.PendingIntents {
		if intent.QuantitySteps > intent.FilledSteps {
			for _, i := range metadata {
				if i.ID == intent.Instrument {
					activeIDs[i.ID] = i
					break
				}
			}
		}
	}
	for id, instrument := range activeIDs {
		found := false
		for sid, i := range metadata {
			if i.ID == id {
				if !reflect.DeepEqual(i, instrument) {
					return Config{}, fmt.Errorf("runner: retained instrument %s units changed", id)
				}
				selected[sid] = true
				found = true
			}
		}
		if !found {
			return Config{}, fmt.Errorf("runner: active policy instrument %s has no stable SID metadata", id)
		}
	}
	if checkpoint.Target != nil {
		for sid, a := range checkpoint.Target.Allocations() {
			if a.Value != "0" {
				selected[sid] = true
			}
		}
	}
	if config.Manifest.Portfolio.Policy == "lifecycle-v1" && len(checkpoint.State) > 0 {
		var state factor.LifecycleState
		if err := json.Unmarshal(checkpoint.State, &state); err != nil {
			return Config{}, err
		}
		for sid, a := range state.Assets {
			if a.Last.Value != "" && a.Last.Value != "0" {
				selected[sid] = true
			}
		}
		for _, cohort := range state.Cohorts {
			for _, x := range cohort.Contributions {
				if x.Outstanding || x.Filled != "" && x.Filled != "0" || cohort.Expires > l.clock() && x.Planned != "" && x.Planned != "0" {
					selected[x.SID] = true
				}
			}
		}
	}
	if config.Snapshot.SIDMap == nil {
		config.Snapshot.SIDMap = map[int32]string{}
	}
	if config.Execution.Instruments == nil {
		config.Execution.Instruments = map[int32]execution.Instrument{}
	}
	copyAccount := *account
	copyAccount.acceptedPolicy = nil
	copyAccount.acceptedPolicyOrder = nil
	copyAccount.Instruments = maps.Clone(account.Instruments)
	if copyAccount.Instruments == nil {
		copyAccount.Instruments = map[int32]execution.Instrument{}
	}
	copyAccount.FundingInstruments = maps.Clone(account.FundingInstruments)
	if copyAccount.FundingInstruments == nil {
		copyAccount.FundingInstruments = map[int32]execution.Instrument{}
	}
	for sid := range selected {
		symbol := symbols[sid]
		instrument, present := metadata[sid]
		if symbol == "" || !present {
			return Config{}, fmt.Errorf("runner: retained policy SID %d lacks persisted symbol/units", sid)
		}
		if next := config.Snapshot.SIDMap[sid]; next != "" && next != symbol {
			return Config{}, fmt.Errorf("runner: retained policy SID %d identity changed", sid)
		}
		if next, ok := config.Execution.Instruments[sid]; ok && !reflect.DeepEqual(next, instrument) {
			return Config{}, fmt.Errorf("runner: retained policy SID %d execution units changed", sid)
		}
		config.Snapshot.SIDMap[sid] = symbol
		config.Execution.Instruments[sid] = instrument
		copyAccount.Instruments[sid] = instrument
		config.Snapshot.Universe.Tracked = append(config.Snapshot.Universe.Tracked, sid)
		for fundingSID, i := range funding {
			if i.ID == instrument.ID {
				if next, ok := copyAccount.FundingInstruments[fundingSID]; ok && !reflect.DeepEqual(next, i) {
					return Config{}, fmt.Errorf("runner: retained funding SID %d units changed", fundingSID)
				}
				copyAccount.FundingInstruments[fundingSID] = i
			}
		}
	}
	slices.Sort(config.Snapshot.Universe.Tracked)
	config.Snapshot.Universe.Tracked = slices.Compact(config.Snapshot.Universe.Tracked)
	copyAccount.PolicySIDMap = maps.Clone(config.Snapshot.SIDMap)
	l.c = config
	l.sink = &copyAccount
	return CloneConfig(config)
}
