package strat

import "sync"

// tradeStratOutputState owns synchronization for the public Outputs slice.
// Strategy plugins keep the field for compatibility, while runtime/report
// code uses the typed methods below to drain it without racing WriteOutput.
type tradeStratOutputState struct {
	mu sync.Mutex
}

var outputStateInitMu sync.Mutex

func (s *TradeStrat) outputStateOwner() *tradeStratOutputState {
	if s == nil {
		return nil
	}
	// External strategies may construct TradeStrat with a struct literal. The
	// global lock makes pointer publication race-free without adding a lock
	// value to the exported strategy struct (which is copied by compatibility
	// helpers). Output is a low-frequency side channel.
	outputStateInitMu.Lock()
	if s.outputState == nil {
		s.outputState = &tradeStratOutputState{}
	}
	state := s.outputState
	outputStateInitMu.Unlock()
	return state
}

// SnapshotOutputs returns a copy of pending output lines. If reset is true,
// the lines are cleared as part of the same critical section.
func (s *TradeStrat) SnapshotOutputs(reset bool) []string {
	state := s.outputStateOwner()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	lines := append([]string(nil), s.Outputs...)
	if reset {
		s.Outputs = nil
	}
	state.mu.Unlock()
	return lines
}

// DrainOutputs atomically takes and clears all pending output lines.
func (s *TradeStrat) DrainOutputs() []string {
	return s.SnapshotOutputs(true)
}
