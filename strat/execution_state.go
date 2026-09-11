package strat

import (
	"sync"

	"github.com/banbox/banbot/orm/ormo"
)

// ExecutionSnapshot is a coherent, short-lived view of a job's mutable
// execution state. The order pointers are borrowed; the slices themselves are
// copied so callers can inspect them after the job advances to the next bar.
//
// Strategy plugins keep the legacy exported fields on StratJob for source
// compatibility. Runtime-owned code should use this snapshot and the typed
// mutation methods below when crossing an event boundary.
type ExecutionSnapshot struct {
	IsWarmUp           bool
	MaxOpenLong        int
	MaxOpenShort       int
	LongOrders         []*ormo.InOutOrder
	ShortOrders        []*ormo.InOutOrder
	EnteredNum         int
	OrderNum           int
	Entrys             []*EnterReq
	Exits              []*ExitReq
	PairRemovalPending bool
}

// executionStateMu is intentionally per job. It protects only the mutable
// execution fields; registry locks continue to protect job membership maps.
// Keeping this separate from network and callback code gives callers a short
// critical section and avoids lock re-entry when a strategy callback queues a
// follow-up order.
type executionStateMu struct {
	mu              sync.Mutex
	orderProcessing bool
}

func (s *StratJob) executionState() *executionStateMu {
	if s == nil {
		return nil
	}
	state := s.executionStateMu.Load()
	if state == nil {
		candidate := &executionStateMu{}
		if s.executionStateMu.CompareAndSwap(nil, candidate) {
			state = candidate
		} else {
			state = s.executionStateMu.Load()
		}
	}
	return state
}

// ExecutionSnapshot returns a coherent copy of the job's mutable execution
// state. Returned order/request slices are independent from the job slice
// headers, while their elements remain owned by the order manager/strategy.
func (s *StratJob) ExecutionSnapshot() ExecutionSnapshot {
	if s == nil {
		return ExecutionSnapshot{}
	}
	state := s.executionState()
	state.mu.Lock()
	snapshot := ExecutionSnapshot{
		IsWarmUp:           s.IsWarmUp,
		MaxOpenLong:        s.MaxOpenLong,
		MaxOpenShort:       s.MaxOpenShort,
		LongOrders:         append([]*ormo.InOutOrder(nil), s.LongOrders...),
		ShortOrders:        append([]*ormo.InOutOrder(nil), s.ShortOrders...),
		EnteredNum:         s.EnteredNum,
		OrderNum:           s.OrderNum,
		Entrys:             append([]*EnterReq(nil), s.Entrys...),
		Exits:              append([]*ExitReq(nil), s.Exits...),
		PairRemovalPending: s.pairRemovalPending,
	}
	state.mu.Unlock()
	return snapshot
}

func (s *StratJob) SetWarmUp(value bool) {
	if s == nil {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	s.IsWarmUp = value
	state.mu.Unlock()
}

func (s *StratJob) IsWarmUpState() bool {
	if s == nil {
		return false
	}
	state := s.executionState()
	state.mu.Lock()
	warmup := s.IsWarmUp
	state.mu.Unlock()
	return warmup
}

func (s *StratJob) PendingEntryCount() int {
	if s == nil {
		return 0
	}
	state := s.executionState()
	state.mu.Lock()
	num := len(s.Entrys)
	state.mu.Unlock()
	return num
}

func (s *StratJob) OrderCounts() (orderNum, enteredNum int) {
	if s == nil {
		return 0, 0
	}
	state := s.executionState()
	state.mu.Lock()
	orderNum, enteredNum = s.OrderNum, s.EnteredNum
	state.mu.Unlock()
	return orderNum, enteredNum
}

func (s *StratJob) SetOpenLimits(long, short int) {
	if s == nil {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	s.MaxOpenLong, s.MaxOpenShort = long, short
	state.mu.Unlock()
}

func (s *StratJob) SetPairRemovalPending(value bool) {
	if s == nil {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	s.pairRemovalPending = value
	state.mu.Unlock()
}

func (s *StratJob) PairRemovalPending() bool {
	if s == nil {
		return false
	}
	state := s.executionState()
	state.mu.Lock()
	pending := s.pairRemovalPending
	state.mu.Unlock()
	return pending
}

func (s *StratJob) SetOrderCounts(orderNum, enteredNum int) {
	if s == nil {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	s.OrderNum, s.EnteredNum = orderNum, enteredNum
	state.mu.Unlock()
}

func (s *StratJob) AddOrderCount(delta int) {
	if s == nil || delta == 0 {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	s.OrderNum += delta
	state.mu.Unlock()
}

func (s *StratJob) enqueueEntry(req *EnterReq) bool {
	if s == nil || req == nil {
		return false
	}
	state := s.executionState()
	state.mu.Lock()
	if s.IsWarmUp {
		state.mu.Unlock()
		return false
	}
	s.Entrys = append(s.Entrys, req)
	s.OrderNum++
	state.mu.Unlock()
	return true
}

func (s *StratJob) enqueueExit(req *ExitReq) bool {
	if s == nil || req == nil {
		return false
	}
	state := s.executionState()
	state.mu.Lock()
	if s.IsWarmUp {
		state.mu.Unlock()
		return false
	}
	s.Exits = append(s.Exits, req)
	state.mu.Unlock()
	return true
}

// DrainOrderRequests atomically takes pending requests. Processing and
// callbacks happen after the lock is released, so a callback can safely queue
// another request for the next loop iteration.
func (s *StratJob) DrainOrderRequests() ([]*EnterReq, []*ExitReq) {
	if s == nil {
		return nil, nil
	}
	state := s.executionState()
	state.mu.Lock()
	enters, exits := s.Entrys, s.Exits
	s.Entrys, s.Exits = nil, nil
	state.mu.Unlock()
	return enters, exits
}

func (s *StratJob) DropEntryRequests() []*EnterReq {
	if s == nil {
		return nil
	}
	state := s.executionState()
	state.mu.Lock()
	enters := s.Entrys
	s.Entrys = nil
	state.mu.Unlock()
	return enters
}

// beginOrderProcessing serializes concurrent order-event and bar-triggered
// processing for one job. A recursive ProcessOrders call from OnOrderChange
// returns immediately; the owning loop drains requests queued by that
// callback before it releases the flag.
func (s *StratJob) beginOrderProcessing() bool {
	if s == nil {
		return false
	}
	state := s.executionState()
	state.mu.Lock()
	if state.orderProcessing {
		state.mu.Unlock()
		return false
	}
	state.orderProcessing = true
	state.mu.Unlock()
	return true
}

// BeginOrderProcessing starts the job-owned order drain. It is exported for
// order-manager implementations in the biz package; a false result means a
// concurrent/recursive caller should let the active drain loop pick up any
// newly queued requests.
func (s *StratJob) BeginOrderProcessing() bool {
	return s.beginOrderProcessing()
}

func (s *StratJob) endOrderProcessing() {
	if s == nil {
		return
	}
	state := s.executionState()
	state.mu.Lock()
	state.orderProcessing = false
	state.mu.Unlock()
}

// EndOrderProcessing releases the job-owned order drain started by
// BeginOrderProcessing.
func (s *StratJob) EndOrderProcessing() {
	s.endOrderProcessing()
}

// finishOrderProcessing atomically releases the active drain only when no
// request is waiting. A producer can enqueue while the processor is deciding
// whether its queue is empty; checking and clearing the flag under the same
// mutex closes that hand-off race. It returns false when the caller must run
// another drain iteration.
func (s *StratJob) finishOrderProcessing() bool {
	if s == nil {
		return true
	}
	state := s.executionState()
	state.mu.Lock()
	if len(s.Entrys) > 0 || len(s.Exits) > 0 {
		state.mu.Unlock()
		return false
	}
	state.orderProcessing = false
	state.mu.Unlock()
	return true
}

// FinishOrderProcessing releases the active order drain if the queue is
// empty. It returns false when a producer queued more work and the owner must
// continue draining. This is exported for the biz order-manager package.
func (s *StratJob) FinishOrderProcessing() bool {
	return s.finishOrderProcessing()
}
