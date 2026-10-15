package execution

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestPolicyAcceptanceAtomicBatchRetryAndEvidence(t *testing.T) {
	for _, memory := range []bool{false, true} {
		name := "SQLite"
		if memory {
			name = "Memory"
		}
		t.Run(name, func(t *testing.T) {
			borrow, venue := strategyService(t, memory)
			snapshot, _ := borrow.Snapshot(context.Background())
			request := PolicyAcceptance{ID: "policy-batch", ExpectedLedgerCursor: snapshot.Checkpoint, Updates: []StrategyRebalance{
				strategyRequest("a", "policy-batch", 10, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 4}),
				strategyRequest("b", "policy-batch", 10, StrategyTargetsFull, ExecutableTarget{Lot: "b", SignedSteps: -2}),
			}, Checkpoints: []PolicyCheckpoint{{Strategy: "a", Payload: json.RawMessage(`{"step":1}`)}, {Strategy: "b", Payload: json.RawMessage(`{"cohort":1}`)}}}
			receipt, err := borrow.AcceptPolicyBatchContext(context.Background(), request, 11)
			if err != nil || !receipt.Accepted || receipt.SendError != nil || receipt.Versions["a"] != 1 || receipt.Versions["b"] != 1 {
				t.Fatal(receipt, err)
			}
			fills := venue.Metrics().Fills
			if receipt, err = borrow.AcceptPolicyBatchContext(context.Background(), request, 12); err != nil || !receipt.Accepted || venue.Metrics().Fills != fills {
				t.Fatal("retry", receipt, err)
			}
			request.Checkpoints[0].Payload = json.RawMessage(`{"step":2}`)
			if _, err = borrow.AcceptPolicyBatchContext(context.Background(), request, 12); err == nil {
				t.Fatal("changed checkpoint accepted under same identity")
			}
			evidence, err := borrow.PolicyEvidenceContext(context.Background(), "a")
			if err != nil || evidence.State.Version != 1 || evidence.FirstFillMS["a"] != 10 && evidence.FirstFillMS["a"] != 11 {
				t.Fatal("restore", evidence, err)
			}
			stateOnly := PolicyAcceptance{ID: "state-only", ExpectedLedgerCursor: evidence.Snapshot.Checkpoint, DecisionMS: 20, ExpiresMS: 30, Checkpoints: []PolicyCheckpoint{{Strategy: "a", ExpectedVersion: 1, Payload: json.RawMessage(`{"step":2}`)}}}
			receipt, err = borrow.AcceptPolicyBatchContext(context.Background(), stateOnly, 20)
			if err != nil || !receipt.Accepted || receipt.Versions["a"] != 2 || venue.Metrics().Fills != fills {
				t.Fatal("state-only traded", receipt, err)
			}
			if receipt, err = borrow.AcceptPolicyBatchContext(context.Background(), stateOnly, 40); err != nil || !receipt.Accepted || receipt.Versions["a"] != 2 {
				t.Fatal("expired accepted retry lost receipt", receipt, err)
			}
			stateOnly.ID = "stale-state"
			if _, err = borrow.AcceptPolicyBatchContext(context.Background(), stateOnly, 21); !errors.Is(err, ErrPolicyEvidenceChanged) {
				t.Fatal("stale version accepted", err)
			}
			stateOnly.Checkpoints[0].ExpectedVersion = 2
			stateOnly.ExpectedLedgerCursor = 0
			if _, err = borrow.AcceptPolicyBatchContext(context.Background(), stateOnly, 21); !errors.Is(err, ErrPolicyEvidenceChanged) {
				t.Fatal("stale evidence accepted", err)
			}
		})
	}
}

func TestCheckpointContentParticipatesInLegacyPlanRetry(t *testing.T) {
	borrow, _ := strategyService(t, true)
	request := strategyRequest("a", "checkpoint-retry", 10, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 1})
	checkpoint := StrategyCheckpoint{Strategy: "a", Name: "custom", Payload: json.RawMessage(`{"n":1}`)}
	call := func() error {
		return borrow.WithState(func(s *SharedAccount) error {
			_, err := s.PrepareStrategiesWithCheckpoint([]StrategyRebalance{request}, context.Background(), &checkpoint)
			return err
		})
	}
	if err := call(); err != nil {
		t.Fatal(err)
	}
	if err := call(); err != nil {
		t.Fatal("same checkpoint rejected", err)
	}
	checkpoint.Payload = json.RawMessage(`{"n":2}`)
	if err := call(); err == nil {
		t.Fatal("same plan changed checkpoint silently succeeded")
	}
}

func TestPolicyReceiptRemainsAcceptedAfterUnknownSend(t *testing.T) {
	borrow, _ := strategyService(t, true)
	failure := errors.New("connection lost after submit")
	adapter := &fakeExecutionAdapter{submit: func(context.Context, OrderIntent, string) (SubmitReceipt, error) { return SubmitReceipt{}, failure }}
	if err := borrow.WithState(func(s *SharedAccount) error { s.executor.Adapter = adapter; return nil }); err != nil {
		t.Fatal(err)
	}
	baseline, _ := borrow.Snapshot(context.Background())
	request := PolicyAcceptance{ID: "send-unknown", ExpectedLedgerCursor: baseline.Checkpoint, Updates: []StrategyRebalance{strategyRequest("a", "send-unknown", 10, StrategyTargetsFull, ExecutableTarget{Lot: "a", SignedSteps: 2})}, Checkpoints: []PolicyCheckpoint{{Strategy: "a", Payload: json.RawMessage(`{"step":1}`), ProposalHash: strings.Repeat("a", 64)}}}
	receipt, err := borrow.AcceptPolicyBatchContext(context.Background(), request, 11)
	if err != nil || !receipt.Accepted || receipt.SendError == nil || receipt.Versions["a"] != 1 {
		t.Fatal("accepted plan rolled back on send failure", receipt, err)
	}
	evidence, err := borrow.PolicyEvidenceContext(context.Background(), "a")
	if err != nil || evidence.State.Version != 1 || len(evidence.Snapshot.Orders) != 1 || evidence.Snapshot.Orders[0].State != OrderUnknown {
		t.Fatal("checkpoint lost after unknown send", evidence, err)
	}
	if receipt, err = borrow.AcceptPolicyBatchContext(context.Background(), request, 12); err != nil || !receipt.Accepted || receipt.SendError == nil || len(adapter.calls()) != 1 {
		t.Fatal("retry resubmitted unknown order", receipt, err, adapter.calls())
	}
	if resumed, found, err := borrow.ResumePolicyContext(context.Background(), "a", request.ID, request.Checkpoints[0].ProposalHash, 2000); err != nil || !found || !resumed.Accepted || resumed.SendError == nil || len(adapter.calls()) != 1 {
		t.Fatal("historical resume resubmitted unknown order", resumed, found, err)
	}
	if _, found, err := borrow.ResumePolicyContext(context.Background(), "b", request.ID, request.Checkpoints[0].ProposalHash, 2000); err != nil || found {
		t.Fatal("historical receipt leaked across strategy scope", found, err)
	}
	if _, found, err := borrow.ResumePolicyContext(context.Background(), "a", request.ID, strings.Repeat("b", 64), 2000); err == nil || !found {
		t.Fatal("historical resume accepted a changed fingerprint", found, err)
	}
}
