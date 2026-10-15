package execution

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"sort"
)

const PolicyCheckpointName = "portfolio-policy-v1"
const MaxPolicyStateBytes = 1 << 20
const maxPolicyStateBytes = MaxPolicyStateBytes

type PolicyCheckpoint struct {
	Strategy        StrategyID
	Name            string
	ExpectedVersion uint64
	Payload         json.RawMessage
	PlanSequence    uint64
	ProposalHash    string
}
type PolicyAcceptance struct {
	ID                    string
	ExpectedLedgerCursor  int64
	DecisionMS, ExpiresMS int64
	Updates               []StrategyRebalance
	Checkpoints           []PolicyCheckpoint
}
type PolicyReceipt struct {
	Accepted bool
	PlanID   string
	Versions map[StrategyID]uint64
	// SendError cannot undo acceptance. Resume this plan instead of proposing
	// another identity or rolling lifecycle state backwards.
	SendError error `json:"-"`
}
type PolicyState struct {
	Version      uint64
	Payload      json.RawMessage
	LedgerCursor int64
	PlanSequence uint64
}
type acceptedPolicy struct {
	Hash           string
	Receipt        PolicyReceipt
	Cursor         int64
	Sequences      map[StrategyID]uint64
	ProposalHashes map[StrategyID]string
	TargetStrategy StrategyID
}

var ErrPolicyEvidenceChanged = errors.New("execution: policy evidence changed; propose again")

func readPolicyState(ctx context.Context, store *Store, strategy StrategyID, name string) (PolicyState, error) {
	var state PolicyState
	body, err := store.StrategyCheckpoint(ctx, strategy, name)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	err = json.Unmarshal(body, &state)
	if err == nil && (state.Version == 0 || !json.Valid(state.Payload) || len(state.Payload) > maxPolicyStateBytes) {
		err = errors.New("execution: invalid portfolio policy state envelope")
	}
	return state, err
}

// AcceptPolicyBatchContext performs evidence validation, plan acceptance and
// all strategy state updates inside the serialized account owner. A nil target
// batch is an ordinary state transaction and never invents a zero order.
func (b *SharedAccountBorrow) AcceptPolicyBatchContext(callerCtx context.Context, request PolicyAcceptance, now int64) (PolicyReceipt, error) {
	var receipt PolicyReceipt
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		var err error
		receipt, err = s.acceptPolicy(ctx, request, now)
		return err
	})
	return receipt, err
}

// ResumePolicyContext checks immutable acceptance evidence in this strategy's
// owner scope. It sends only the previously frozen plan and never creates a
// plan or rewrites the current policy checkpoint.
func (b *SharedAccountBorrow) ResumePolicyContext(callerCtx context.Context, strategy StrategyID, id, proposalHash string, now int64) (PolicyReceipt, bool, error) {
	var receipt PolicyReceipt
	var found bool
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		if !s.ready || !canonicalID(string(strategy)) || !canonicalID(id) || !validProposalHash(proposalHash) {
			return errors.New("execution: invalid policy resume identity")
		}
		body, err := s.store.StrategyCheckpoint(ctx, strategy, "policy-acceptance:"+id)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		var accepted acceptedPolicy
		if err := json.Unmarshal(body, &accepted); err != nil {
			return err
		}
		if accepted.ProposalHashes[strategy] != proposalHash {
			return errors.New("execution: policy acceptance identity reused with different proposal")
		}
		if !accepted.Receipt.Accepted || accepted.Receipt.PlanID != id || accepted.Receipt.Versions[strategy] == 0 {
			return errors.New("execution: invalid persisted policy receipt")
		}
		receipt = accepted.Receipt
		if accepted.TargetStrategy == "" {
			return nil
		}
		body, err = s.store.StrategyCheckpoint(ctx, accepted.TargetStrategy, "target-revision:"+id)
		if err != nil {
			return err
		}
		var revision strategyRevision
		if err := json.Unmarshal(body, &revision); err != nil {
			return err
		}
		if revision.Empty {
			return nil
		}
		prepared, err := s.restorePreparedRebalance(ctx, id)
		if err != nil {
			return err
		}
		receipt.SendError = s.sendPreparedRebalance(prepared, now, ctx)
		return nil
	})
	return receipt, found, err
}

func validProposalHash(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func (s *SharedAccount) acceptPolicy(ctx context.Context, request PolicyAcceptance, now int64) (PolicyReceipt, error) {
	if !s.ready || !canonicalID(request.ID) || request.ExpectedLedgerCursor < 0 || len(request.Checkpoints) == 0 {
		return PolicyReceipt{}, errors.New("execution: invalid policy acceptance")
	}
	request.Checkpoints = append([]PolicyCheckpoint(nil), request.Checkpoints...)
	sort.Slice(request.Checkpoints, func(i, j int) bool { return request.Checkpoints[i].Strategy < request.Checkpoints[j].Strategy })
	seen := map[StrategyID]bool{}
	for n := range request.Checkpoints {
		c := &request.Checkpoints[n]
		if c.Name == "" {
			c.Name = PolicyCheckpointName
		}
		if !canonicalID(string(c.Strategy)) || c.Name != PolicyCheckpointName || seen[c.Strategy] || c.ExpectedVersion == ^uint64(0) || len(c.Payload) > maxPolicyStateBytes || !json.Valid(c.Payload) {
			return PolicyReceipt{}, errors.New("execution: invalid policy checkpoint")
		}
		seen[c.Strategy] = true
		if c.ProposalHash != "" && !validProposalHash(c.ProposalHash) {
			return PolicyReceipt{}, errors.New("execution: invalid proposal fingerprint")
		}
	}
	for _, update := range request.Updates {
		if !seen[update.Strategy] || update.PlanID != request.ID {
			return PolicyReceipt{}, errors.New("execution: policy target/checkpoint ownership mismatch")
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		return PolicyReceipt{}, err
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	strategy := request.Checkpoints[0].Strategy
	name := "policy-acceptance:" + request.ID
	if previous, err := s.store.StrategyCheckpoint(ctx, strategy, name); err == nil {
		var accepted acceptedPolicy
		if err := json.Unmarshal(previous, &accepted); err != nil {
			return PolicyReceipt{}, err
		}
		if accepted.Hash != hash {
			return PolicyReceipt{}, errors.New("execution: policy acceptance identity reused with different checkpoint or target")
		}
		receipt := accepted.Receipt
		if len(request.Updates) > 0 {
			prepared, err := s.PrepareStrategiesWithCheckpoints(request.Updates, ctx, policyRecords(request.Checkpoints, accepted, name))
			if err != nil {
				return receipt, err
			}
			receipt.SendError = s.sendPreparedRebalance(prepared, now, ctx)
		}
		return receipt, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return PolicyReceipt{}, err
	}
	if request.ExpiresMS != 0 && (request.DecisionMS < 0 || request.ExpiresMS <= request.DecisionMS || now < request.DecisionMS || now >= request.ExpiresMS) {
		return PolicyReceipt{}, errors.New("execution: policy acceptance outside decision window")
	}
	snapshot, err := s.store.Snapshot(ctx)
	if err != nil {
		return PolicyReceipt{}, err
	}
	if snapshot.Checkpoint != request.ExpectedLedgerCursor {
		return PolicyReceipt{}, ErrPolicyEvidenceChanged
	}
	receipt := PolicyReceipt{Accepted: true, PlanID: request.ID, Versions: map[StrategyID]uint64{}}
	for _, c := range request.Checkpoints {
		previous, err := readPolicyState(ctx, s.store, c.Strategy, c.Name)
		if err != nil {
			return PolicyReceipt{}, err
		}
		if previous.Version != c.ExpectedVersion {
			return PolicyReceipt{}, ErrPolicyEvidenceChanged
		}
		if c.PlanSequence > uint64(^uint64(0)>>1) || c.PlanSequence > 0 && c.PlanSequence <= previous.PlanSequence || previous.PlanSequence > 0 && c.PlanSequence == 0 {
			return PolicyReceipt{}, errors.New("execution: stale policy target sequence")
		}
		receipt.Versions[c.Strategy] = c.ExpectedVersion + 1
	}
	accepted := acceptedPolicy{Hash: hash, Receipt: receipt, Cursor: request.ExpectedLedgerCursor, Sequences: map[StrategyID]uint64{}, ProposalHashes: map[StrategyID]string{}}
	if len(request.Updates) > 0 {
		accepted.TargetStrategy = request.Updates[0].Strategy
	}
	for _, c := range request.Checkpoints {
		accepted.Sequences[c.Strategy] = c.PlanSequence
		accepted.ProposalHashes[c.Strategy] = c.ProposalHash
	}
	checkpoints := policyRecords(request.Checkpoints, accepted, name)
	var prepared PreparedRebalance
	if len(request.Updates) > 0 {
		prepared, err = s.PrepareStrategiesWithCheckpoints(request.Updates, ctx, checkpoints)
	} else {
		err = s.executorFor(ctx).local(func(ctx context.Context) error {
			return s.store.atomically(ctx, func(ctx context.Context) error {
				for _, c := range checkpoints {
					if err := s.store.saveAcceptedCheckpoint(ctx, c); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
	if err != nil {
		return PolicyReceipt{}, err
	}
	if len(request.Updates) > 0 {
		receipt.SendError = s.sendPreparedRebalance(prepared, now, ctx)
	}
	return receipt, nil
}

func policyRecords(checkpoints []PolicyCheckpoint, accepted acceptedPolicy, name string) []StrategyCheckpoint {
	records := make([]StrategyCheckpoint, 0, len(checkpoints))
	for _, c := range checkpoints {
		body, _ := json.Marshal(PolicyState{Version: c.ExpectedVersion + 1, Payload: c.Payload, LedgerCursor: accepted.Cursor, PlanSequence: c.PlanSequence})
		record := StrategyCheckpoint{Strategy: c.Strategy, Name: c.Name, Payload: body}
		receipt, _ := json.Marshal(accepted)
		record.Records = []StrategyCheckpoint{{Strategy: c.Strategy, Name: name, Payload: receipt}}
		records = append(records, record)
	}
	return records
}

type PolicyEvidence struct {
	Snapshot       AccountSnapshot
	State          PolicyState
	FirstFillMS    map[VirtualLotID]int64
	PendingIntents []EligibleIntent
	Fills          []PolicyFill
}

type PolicyFill struct {
	Lot           VirtualLotID
	Instrument    Instrument
	QuantityDelta int64
	Cursor        uint64
	PlanSequence  uint64
	AtMS          int64
}

type policyFillCache struct {
	cursor     int64
	quantities map[VirtualLotID]int64
	firstFill  map[VirtualLotID]int64
}

// PolicyEvidenceContext restores state and derives current-position age from
// immutable fill postings. Increasing a live position does not reset its age;
// a close or reversal starts a new lifecycle. Genesis has unknown age (zero).
func (b *SharedAccountBorrow) PolicyEvidenceContext(callerCtx context.Context, strategy StrategyID) (PolicyEvidence, error) {
	var result PolicyEvidence
	err := b.use(func(s *SharedAccount) error {
		ctx, done := joinedOperationContext(b.ctx, callerCtx)
		defer done()
		var err error
		result.Snapshot, err = s.store.Snapshot(ctx)
		if err != nil {
			return err
		}
		result.State, err = readPolicyState(ctx, s.store, strategy, PolicyCheckpointName)
		if err != nil {
			return err
		}
		cache := s.policyEvidenceCache[strategy]
		quantities := maps.Clone(cache.quantities)
		if quantities == nil {
			quantities = map[VirtualLotID]int64{}
		}
		firstFill := maps.Clone(cache.firstFill)
		if firstFill == nil {
			firstFill = map[VirtualLotID]int64{}
		}
		cursor := cache.cursor
		for {
			page, err := s.store.EventsAfter(ctx, cursor, 512)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, event := range page {
				for _, entry := range event.Ledger {
					if entry.Strategy != strategy || entry.QuantityDelta == 0 {
						continue
					}
					old := quantities[entry.Lot]
					next := old + entry.QuantityDelta
					quantities[entry.Lot] = next
					if entry.Kind != "ExchangeFill" && entry.Kind != "InternalFill" {
						delete(firstFill, entry.Lot)
						continue
					}
					if next == 0 {
						delete(firstFill, entry.Lot)
						delete(quantities, entry.Lot)
					} else if old == 0 || old > 0 && next < 0 || old < 0 && next > 0 {
						firstFill[entry.Lot] = entry.AtMS
					}
				}
			}
			cursor = page[len(page)-1].Checkpoint
		}
		if s.policyEvidenceCache == nil {
			s.policyEvidenceCache = map[StrategyID]policyFillCache{}
		}
		s.policyEvidenceCache[strategy] = policyFillCache{cursor: cursor, quantities: quantities, firstFill: firstFill}
		result.FirstFillMS = maps.Clone(firstFill)
		for cursor := result.State.LedgerCursor; ; {
			page, err := s.store.EventsAfter(ctx, cursor, 512)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, event := range page {
				var planID string
				var instrument Instrument
				switch event.Kind {
				case "ExchangeFill":
					var fill FillReport
					if err := json.Unmarshal(event.Payload, &fill); err != nil {
						return err
					}
					order, err := s.store.Order(ctx, fill.OrderID)
					if err != nil {
						return err
					}
					planID = order.Intent.PlanID
					instrument = order.Intent.Instrument
				case "InternalFill":
					var match InternalMatch
					if err := json.Unmarshal(event.Payload, &match); err != nil {
						return err
					}
					planID = match.PlanID
					instrument = match.Instrument
				default:
					continue
				}
				seq := uint64(0)
				body, err := s.store.StrategyCheckpoint(ctx, strategy, "policy-acceptance:"+planID)
				if err == nil {
					var accepted acceptedPolicy
					if err := json.Unmarshal(body, &accepted); err != nil {
						return err
					}
					seq = accepted.Sequences[strategy]
				} else if !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				for _, entry := range event.Ledger {
					if entry.Strategy == strategy && entry.QuantityDelta != 0 {
						entrySequence := seq
						plan, err := s.store.Plan(ctx, planID)
						if err != nil {
							return err
						}
						for _, target := range plan.Targets {
							if target.Strategy == strategy && target.Lot == entry.Lot && target.Instrument == instrument.ID && target.SourceSequence > 0 {
								entrySequence = target.SourceSequence
								break
							}
						}
						sfill := PolicyFill{Lot: entry.Lot, Instrument: instrument, QuantityDelta: entry.QuantityDelta, Cursor: uint64(event.Checkpoint), PlanSequence: entrySequence, AtMS: entry.AtMS}
						result.Fills = append(result.Fills, sfill)
					}
				}
			}
			cursor = page[len(page)-1].Checkpoint
		}
		plan, err := s.store.LatestPlan(ctx)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, intent := range plan.Intents {
			if intent.Strategy != strategy {
				continue
			}
			current, err := s.store.Intent(ctx, intent.ID)
			if err != nil {
				return err
			}
			if current.State != Filled && current.State != Canceled && current.State != Expired {
				result.PendingIntents = append(result.PendingIntents, current)
			}
		}
		return nil
	})
	return result, err
}
