package strat

import (
	"sync"
	"testing"
)

func TestExecutionSnapshotMethodsAreConcurrentSafe(t *testing.T) {
	job := &StratJob{}
	const rounds = 500
	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			job.SetWarmUp(i%2 == 0)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			job.SetOpenLimits(i, -i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			job.SetPairRemovalPending(i%2 == 0)
			job.AddOrderCount(1)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_ = job.ExecutionSnapshot()
			_ = job.PairRemovalPending()
		}
	}()
	wg.Wait()
	snapshot := job.ExecutionSnapshot()
	if snapshot.OrderNum != rounds {
		t.Fatalf("order count = %d, want %d", snapshot.OrderNum, rounds)
	}
}

func TestBeginOrderProcessingRejectsRecursiveDrain(t *testing.T) {
	job := &StratJob{}
	if !job.BeginOrderProcessing() {
		t.Fatal("first order drain did not start")
	}
	if job.BeginOrderProcessing() {
		t.Fatal("recursive order drain started while one was active")
	}
	job.EndOrderProcessing()
	if !job.BeginOrderProcessing() {
		t.Fatal("order drain did not restart after release")
	}
	job.EndOrderProcessing()
}

func TestFinishOrderProcessingKeepsOwnershipForQueuedWork(t *testing.T) {
	job := &StratJob{}
	if !job.BeginOrderProcessing() {
		t.Fatal("order drain did not start")
	}
	if !job.enqueueEntry(&EnterReq{Tag: "queued"}) {
		t.Fatal("entry request was not queued")
	}
	if job.FinishOrderProcessing() {
		t.Fatal("active drain released while work was queued")
	}
	// The current owner must retain the processing lease until it drains the
	// request; a second owner cannot take it over.
	if job.BeginOrderProcessing() {
		t.Fatal("second order drain acquired active lease")
	}
	_, _ = job.DrainOrderRequests()
	if !job.FinishOrderProcessing() {
		t.Fatal("empty order drain did not release ownership")
	}
}
