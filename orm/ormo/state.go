package ormo

import (
	"reflect"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/com"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/sasha-s/go-deadlock"
)

// OrderState owns the mutable order and task registries for one runtime.
//
// The fields are references so LegacyState can expose the existing package
// globals without changing their names or the APIs that still use them. New
// states allocate every registry independently.
type OrderState struct {
	openOrders    *map[string]map[int64]*InOutOrder
	syncStamps    *map[string]int64
	triggerOrders *map[string]map[string]map[int64]*InOutOrder
	openLocks     *map[string]*deadlock.Mutex
	triggerLocks  *map[string]*deadlock.Mutex
	orderLocks    *map[string]*deadlock.Mutex

	openGuard    *deadlock.Mutex
	triggerGuard *deadlock.Mutex
	orderGuard   *deadlock.Mutex
	stateGuard   *deadlock.Mutex

	historicalOrders *([]*InOutOrder)
	doneOrderIDs     *map[int64]bool
	fakeID           *int64
	editListener     *func(*InOutOrder, string)
	liveMode         bool
	runtimeCore      *core.State
	runtimeClock     *btime.ClockState
	runtimePrices    *com.PriceState
	runtimeExchange  banexg.BanExchange
	runtimeConfig    *config.Config
	tradesPath       string
	executionOptions ExecutionOptions

	tasks          *map[string]*BotTask
	taskIDAccounts *map[int64]string
}

// ExecutionOptions is the small execution policy bound to one runtime's
// order registry. Keeping it here prevents hot paths from reading global
// backtest flags.
type ExecutionOptions struct {
	StrictBacktest     bool
	LegacyOrderMetrics bool
}

// NewOrderState creates isolated order and task registries for one runtime.
func NewOrderState() *OrderState {
	openOrders := make(map[string]map[int64]*InOutOrder)
	syncStamps := make(map[string]int64)
	triggerOrders := make(map[string]map[string]map[int64]*InOutOrder)
	openLocks := make(map[string]*deadlock.Mutex)
	triggerLocks := make(map[string]*deadlock.Mutex)
	orderLocks := make(map[string]*deadlock.Mutex)
	historicalOrders := make([]*InOutOrder, 0)
	doneOrderIDs := make(map[int64]bool)
	fakeID := int64(1)
	var editListener func(*InOutOrder, string)
	tasks := make(map[string]*BotTask)
	taskIDAccounts := make(map[int64]string)

	return &OrderState{
		openOrders:       &openOrders,
		syncStamps:       &syncStamps,
		triggerOrders:    &triggerOrders,
		openLocks:        &openLocks,
		triggerLocks:     &triggerLocks,
		orderLocks:       &orderLocks,
		openGuard:        &deadlock.Mutex{},
		triggerGuard:     &deadlock.Mutex{},
		orderGuard:       &deadlock.Mutex{},
		stateGuard:       &deadlock.Mutex{},
		historicalOrders: &historicalOrders,
		doneOrderIDs:     &doneOrderIDs,
		fakeID:           &fakeID,
		editListener:     &editListener,
		tasks:            &tasks,
		taskIDAccounts:   &taskIDAccounts,
	}
}

var legacyOrderState = &OrderState{
	openOrders:       &accOpenODs,
	syncStamps:       &accSyncStamps,
	triggerOrders:    &accTriggerODs,
	openLocks:        &lockOpenMap,
	triggerLocks:     &lockTriggerMap,
	orderLocks:       &lockOds,
	openGuard:        &mOpenLock,
	triggerGuard:     &mTriggerLock,
	orderGuard:       &mLockOds,
	stateGuard:       &deadlock.Mutex{},
	historicalOrders: &HistODs,
	doneOrderIDs:     &doneODs,
	fakeID:           &FakeOdId,
	editListener:     &OdEditListener,
	tasks:            &accTasks,
	taskIDAccounts:   &taskIdAccMap,
}

func legacyStateView() *OrderState {
	return legacyOrderState
}

// IsLegacyState reports whether state is the package compatibility view.
// Explicit runtimes use it only to reject accidental global-state binding.
func IsLegacyState(state *OrderState) bool {
	return state == legacyOrderState
}

// GetOpenODs returns an account's open-order registry and its account lock.
func (s *OrderState) GetOpenODs(account string) (map[int64]*InOutOrder, *deadlock.Mutex) {
	if s == nil || s.openOrders == nil || s.openLocks == nil || s.openGuard == nil {
		return make(map[int64]*InOutOrder), &deadlock.Mutex{}
	}
	s.openGuard.Lock()
	if *s.openOrders == nil {
		*s.openOrders = make(map[string]map[int64]*InOutOrder)
	}
	orders := (*s.openOrders)[account]
	if orders == nil {
		orders = make(map[int64]*InOutOrder)
		(*s.openOrders)[account] = orders
	}
	if *s.openLocks == nil {
		*s.openLocks = make(map[string]*deadlock.Mutex)
	}
	lock := (*s.openLocks)[account]
	if lock == nil {
		lock = &deadlock.Mutex{}
		(*s.openLocks)[account] = lock
	}
	s.openGuard.Unlock()
	return orders, lock
}

// OpenNum returns the number of orders at or above status for one account.
func (s *OrderState) OpenNum(account string, status int64) int {
	if s == nil {
		return 0
	}
	orders, lock := s.GetOpenODs(account)
	lock.Lock()
	defer lock.Unlock()
	num := 0
	for _, order := range orders {
		if order.Status >= status {
			num++
		}
	}
	return num
}

// GetTriggerODs returns an account's trigger-order registry and its account lock.
func (s *OrderState) GetTriggerODs(account string) (map[string]map[int64]*InOutOrder, *deadlock.Mutex) {
	if s == nil || s.triggerOrders == nil || s.triggerLocks == nil || s.triggerGuard == nil {
		return make(map[string]map[int64]*InOutOrder), &deadlock.Mutex{}
	}
	s.triggerGuard.Lock()
	if *s.triggerOrders == nil {
		*s.triggerOrders = make(map[string]map[string]map[int64]*InOutOrder)
	}
	orders := (*s.triggerOrders)[account]
	if orders == nil {
		orders = make(map[string]map[int64]*InOutOrder)
		(*s.triggerOrders)[account] = orders
	}
	if *s.triggerLocks == nil {
		*s.triggerLocks = make(map[string]*deadlock.Mutex)
	}
	lock := (*s.triggerLocks)[account]
	if lock == nil {
		lock = &deadlock.Mutex{}
		(*s.triggerLocks)[account] = lock
	}
	s.triggerGuard.Unlock()
	return orders, lock
}

// AddTriggerOd registers a local trigger in this runtime's trigger registry.
func (s *OrderState) AddTriggerOd(account string, order *InOutOrder) {
	if s == nil || order == nil {
		return
	}
	order.BindState(s)
	orders, lock := s.GetTriggerODs(account)
	lock.Lock()
	bySymbol := orders[order.Symbol]
	if bySymbol == nil {
		bySymbol = make(map[int64]*InOutOrder)
		orders[order.Symbol] = bySymbol
	}
	bySymbol[order.ID] = order
	lock.Unlock()
}

// GetSyncStamp returns the last order-sync timestamp for account.
func (s *OrderState) GetSyncStamp(account string) int64 {
	if s == nil || s.syncStamps == nil || s.openGuard == nil {
		return 0
	}
	s.openGuard.Lock()
	stamp := (*s.syncStamps)[account]
	s.openGuard.Unlock()
	return stamp
}

// SetSyncStamp records the last order-sync timestamp for account.
func (s *OrderState) SetSyncStamp(account string, stamp int64) {
	if s == nil || s.syncStamps == nil || s.openGuard == nil {
		return
	}
	s.openGuard.Lock()
	if *s.syncStamps == nil {
		*s.syncStamps = make(map[string]int64)
	}
	(*s.syncStamps)[account] = stamp
	s.openGuard.Unlock()
}

// GetOrderLock returns the lock for one order key, allocating it on demand.
func (s *OrderState) GetOrderLock(key string) *deadlock.Mutex {
	if s == nil || s.orderLocks == nil || s.orderGuard == nil {
		return &deadlock.Mutex{}
	}
	s.orderGuard.Lock()
	if *s.orderLocks == nil {
		*s.orderLocks = make(map[string]*deadlock.Mutex)
	}
	lock := (*s.orderLocks)[key]
	if lock == nil {
		lock = &deadlock.Mutex{}
		(*s.orderLocks)[key] = lock
	}
	s.orderGuard.Unlock()
	return lock
}

func (s *OrderState) DeleteOrderLock(key string) {
	if s == nil || s.orderLocks == nil || s.orderGuard == nil {
		return
	}
	s.orderGuard.Lock()
	delete(*s.orderLocks, key)
	s.orderGuard.Unlock()
}

// GetTask returns the task registered for account.
func (s *OrderState) GetTask(account string) *BotTask {
	if s == nil || s.tasks == nil || s.stateGuard == nil {
		return nil
	}
	s.stateGuard.Lock()
	task := (*s.tasks)[account]
	s.stateGuard.Unlock()
	return task
}

// GetTaskID returns an account's task ID, or -1 when it is not registered.
func (s *OrderState) GetTaskID(account string) int64 {
	if task := s.GetTask(account); task != nil {
		return task.ID
	}
	return -1
}

// GetTaskAcc returns the account associated with taskID.
func (s *OrderState) GetTaskAcc(taskID int64) string {
	if s == nil || s.taskIDAccounts == nil || s.stateGuard == nil {
		return ""
	}
	s.stateGuard.Lock()
	account := (*s.taskIDAccounts)[taskID]
	s.stateGuard.Unlock()
	return account
}

// SetTask registers a task and keeps the reverse task-ID index in sync.
func (s *OrderState) SetTask(account string, task *BotTask) {
	if s == nil || s.tasks == nil || s.taskIDAccounts == nil || s.stateGuard == nil {
		return
	}
	s.stateGuard.Lock()
	if *s.tasks == nil {
		*s.tasks = make(map[string]*BotTask)
	}
	if *s.taskIDAccounts == nil {
		*s.taskIDAccounts = make(map[int64]string)
	}
	if previous := (*s.tasks)[account]; previous != nil && (task == nil || previous.ID != task.ID) {
		if (*s.taskIDAccounts)[previous.ID] == account {
			delete(*s.taskIDAccounts, previous.ID)
		}
	}
	if task == nil {
		delete(*s.tasks, account)
	} else {
		(*s.tasks)[account] = task
		(*s.taskIDAccounts)[task.ID] = account
	}
	s.stateGuard.Unlock()
}

// HistoricalOrders returns a shallow copy of the historical-order registry.
func (s *OrderState) HistoricalOrders() []*InOutOrder {
	if s == nil || s.historicalOrders == nil || s.stateGuard == nil {
		return nil
	}
	s.stateGuard.Lock()
	orders := append([]*InOutOrder(nil), (*s.historicalOrders)...)
	s.stateGuard.Unlock()
	return orders
}

// FilterUnfilledHistoricalOrders removes simulated orders that never filled
// from this runtime's history. The registry owner performs the filtering so
// cleaning one runtime cannot rewrite another runtime's order history.
func (s *OrderState) FilterUnfilledHistoricalOrders() {
	if s == nil || s.historicalOrders == nil || s.doneOrderIDs == nil || s.stateGuard == nil {
		return
	}
	s.stateGuard.Lock()
	valid := (*s.historicalOrders)[:0]
	done := make(map[int64]bool, len(valid))
	for _, order := range *s.historicalOrders {
		if order == nil || order.Enter == nil || order.Enter.Filled == 0 {
			continue
		}
		valid = append(valid, order)
		done[order.ID] = true
	}
	clear((*s.historicalOrders)[len(valid):])
	*s.historicalOrders = valid
	*s.doneOrderIDs = done
	s.stateGuard.Unlock()
}

// AddHistoricalOrder appends an order once, keyed by its ID.
func (s *OrderState) AddHistoricalOrder(order *InOutOrder) bool {
	if s == nil || order == nil || s.historicalOrders == nil || s.doneOrderIDs == nil || s.stateGuard == nil {
		return false
	}
	s.stateGuard.Lock()
	if *s.doneOrderIDs == nil {
		*s.doneOrderIDs = make(map[int64]bool)
	}
	if (*s.doneOrderIDs)[order.ID] {
		s.stateGuard.Unlock()
		return false
	}
	(*s.doneOrderIDs)[order.ID] = true
	*s.historicalOrders = append(*s.historicalOrders, order)
	s.stateGuard.Unlock()
	return true
}

// NextFakeID allocates the next in-memory order ID.
func (s *OrderState) NextFakeID() int64 {
	if s == nil || s.fakeID == nil || s.stateGuard == nil {
		return 0
	}
	s.stateGuard.Lock()
	id := *s.fakeID
	*s.fakeID = id + 1
	s.stateGuard.Unlock()
	return id
}

// GetEditListener returns the order-edit callback for this state.
func (s *OrderState) GetEditListener() func(*InOutOrder, string) {
	if s == nil || s.editListener == nil || s.stateGuard == nil {
		return nil
	}
	s.stateGuard.Lock()
	listener := *s.editListener
	s.stateGuard.Unlock()
	return listener
}

// SetEditListener updates the order-edit callback for this state.
func (s *OrderState) SetEditListener(listener func(*InOutOrder, string)) {
	if s == nil || s.editListener == nil || s.stateGuard == nil {
		return
	}
	s.stateGuard.Lock()
	*s.editListener = listener
	s.stateGuard.Unlock()
}

// SaveDirtyODs saves dirty orders owned by this state and removes terminal
// orders from its open-order registry. The path argument matches the legacy
// wrapper and is intentionally unused because an order carries its own state.
func (s *OrderState) SaveDirtyODs(_ string, account string) *errs.Error {
	if s == nil || s.openOrders == nil || s.openGuard == nil {
		return nil
	}
	s.openGuard.Lock()
	accounts := make([]string, 0, len(*s.openOrders))
	if account != "" {
		accounts = append(accounts, account)
	} else {
		for account := range *s.openOrders {
			accounts = append(accounts, account)
		}
	}
	s.openGuard.Unlock()

	dirty := make([]*InOutOrder, 0)
	for _, account := range accounts {
		orders, lock := s.GetOpenODs(account)
		lock.Lock()
		for id, order := range orders {
			if order == nil {
				continue
			}
			order.BindState(s)
			if order.IsDirty() {
				dirty = append(dirty, order)
			}
			if order.Status >= InOutStatusFullExit {
				delete(orders, id)
			}
		}
		lock.Unlock()
	}

	var saveErr *errs.Error
	for _, order := range dirty {
		if err := order.Save(); err != nil && saveErr == nil {
			saveErr = err
		}
	}
	return saveErr
}

// SetLive selects database persistence for orders bound to this state. The
// default is in-memory simulation, which matches backtest construction.
func (s *OrderState) SetLive(live bool) {
	if s == nil {
		return
	}
	s.stateGuard.Lock()
	s.liveMode = live
	s.stateGuard.Unlock()
}

// BindCore connects simulation-only counters to the owning Runtime core. The
// pointer is fixed at construction and reads remain direct field accesses on
// the order-matching hot path.
func (s *OrderState) BindCore(state *core.State) {
	if s == nil {
		return
	}
	s.runtimeCore = state
}

// BindRuntime associates the order registry with the runtime-owned services
// used by order methods. The pointers are fixed at composition time so hot
// paths do not consult process-wide clocks, prices, exchanges, or config.
func (s *OrderState) BindRuntime(clock *btime.ClockState, prices *com.PriceState,
	exchange banexg.BanExchange, cfg *config.Config) {
	if s == nil {
		return
	}
	s.runtimeClock = clock
	s.runtimePrices = prices
	s.runtimeExchange = exchange
	s.runtimeConfig = cfg
}

// CanBindRuntime reports whether this order state is unbound or already uses
// the supplied runtime services. Construction checks it before rebinding so
// two tasks cannot accidentally share and overwrite mutable order services.
func (s *OrderState) CanBindRuntime(coreState *core.State, clock *btime.ClockState, prices *com.PriceState,
	exchange banexg.BanExchange, cfg *config.Config) bool {
	if s == nil {
		return false
	}
	s.stateGuard.Lock()
	defer s.stateGuard.Unlock()
	return (s.runtimeCore == nil || s.runtimeCore == coreState) &&
		(s.runtimeClock == nil || s.runtimeClock == clock) &&
		(s.runtimePrices == nil || s.runtimePrices == prices) &&
		(s.runtimeExchange == nil || sameRuntimeExchange(s.runtimeExchange, exchange)) &&
		(s.runtimeConfig == nil || s.runtimeConfig == cfg)
}

// RuntimeBindingsMatch reports whether construction has already bound this
// order state to the supplied runtime. It is read-only so downstream
// consumers cannot race the composition root by rebinding shared state.
func (s *OrderState) RuntimeBindingsMatch(coreState *core.State, clock *btime.ClockState, prices *com.PriceState,
	exchange banexg.BanExchange, cfg *config.Config) bool {
	if s == nil {
		return false
	}
	s.stateGuard.Lock()
	defer s.stateGuard.Unlock()
	return s.runtimeCore == coreState && s.runtimeClock == clock && s.runtimePrices == prices &&
		sameRuntimeExchange(s.runtimeExchange, exchange) && s.runtimeConfig == cfg
}

// BindRuntimeOnce claims an unbound order state for one composition root.
// Unlike a check-then-bind sequence, the claim is held under stateGuard so a
// second runtime cannot overwrite the services while construction is racing.
func (s *OrderState) BindRuntimeOnce(coreState *core.State, clock *btime.ClockState, prices *com.PriceState,
	exchange banexg.BanExchange, cfg *config.Config) bool {
	if s == nil {
		return false
	}
	s.stateGuard.Lock()
	defer s.stateGuard.Unlock()
	if s.runtimeCore != nil || s.runtimeClock != nil || s.runtimePrices != nil || s.runtimeExchange != nil || s.runtimeConfig != nil {
		return s.runtimeCore == coreState && s.runtimeClock == clock && s.runtimePrices == prices &&
			sameRuntimeExchange(s.runtimeExchange, exchange) && s.runtimeConfig == cfg
	}
	s.runtimeCore = coreState
	s.runtimeClock = clock
	s.runtimePrices = prices
	s.runtimeExchange = exchange
	s.runtimeConfig = cfg
	return true
}

func sameRuntimeExchange(left, right banexg.BanExchange) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftType := reflect.TypeOf(left)
	if leftType != reflect.TypeOf(right) || !leftType.Comparable() {
		return false
	}
	return left == right
}

// TimeMS returns the timestamp owned by this order state. Explicit runtimes
// use their bound clock; an unbound state uses wall time without consulting the
// package compatibility clock.
func (s *OrderState) TimeMS() int64 {
	if s == nil {
		return time.Now().UnixMilli()
	}
	if s.runtimeClock != nil {
		return s.runtimeClock.TimeMS()
	}
	if s == legacyOrderState {
		return btime.UTCStamp()
	}
	return time.Now().UnixMilli()
}

// BindExecutionOptions associates deterministic order behavior with this
// runtime's order registry during composition.
func (s *OrderState) BindExecutionOptions(options ExecutionOptions) {
	if s == nil {
		return
	}
	s.executionOptions = options
}

func (s *OrderState) StrictBacktest() bool {
	return s != nil && s.executionOptions.StrictBacktest
}

func (s *OrderState) LegacyOrderMetrics() bool {
	return s != nil && s.executionOptions.LegacyOrderMetrics
}

func (s *OrderState) addSimOrder() {
	if s != nil {
		if s.runtimeCore != nil {
			if s.runtimeCore.SimOrderMatch {
				s.runtimeCore.NewNumInSim++
			}
			return
		}
		if s != legacyOrderState {
			return
		}
	}
	if core.SimOrderMatch {
		core.NewNumInSim++
	}
}

func (s *OrderState) Live() bool {
	if s == nil {
		return false
	}
	s.stateGuard.Lock()
	live := s.liveMode
	s.stateGuard.Unlock()
	return live
}

// Reset clears all registries owned by this state and resets fake IDs to one.
func (s *OrderState) Reset() {
	if s == nil {
		return
	}
	if s.openGuard != nil {
		s.openGuard.Lock()
	}
	if s.triggerGuard != nil {
		s.triggerGuard.Lock()
	}
	if s.orderGuard != nil {
		s.orderGuard.Lock()
	}
	if s.stateGuard != nil {
		s.stateGuard.Lock()
	}

	if s.openOrders != nil {
		*s.openOrders = make(map[string]map[int64]*InOutOrder)
	}
	if s.syncStamps != nil {
		*s.syncStamps = make(map[string]int64)
	}
	if s.triggerOrders != nil {
		*s.triggerOrders = make(map[string]map[string]map[int64]*InOutOrder)
	}
	if s.openLocks != nil {
		*s.openLocks = make(map[string]*deadlock.Mutex)
	}
	if s.triggerLocks != nil {
		*s.triggerLocks = make(map[string]*deadlock.Mutex)
	}
	if s.orderLocks != nil {
		*s.orderLocks = make(map[string]*deadlock.Mutex)
	}
	if s.historicalOrders != nil {
		*s.historicalOrders = nil
	}
	if s.doneOrderIDs != nil {
		*s.doneOrderIDs = make(map[int64]bool)
	}
	if s.fakeID != nil {
		*s.fakeID = 1
	}
	if s.editListener != nil {
		*s.editListener = nil
	}
	if s.tasks != nil {
		*s.tasks = make(map[string]*BotTask)
	}
	if s.taskIDAccounts != nil {
		*s.taskIDAccounts = make(map[int64]string)
	}

	if s.stateGuard != nil {
		s.stateGuard.Unlock()
	}
	if s.orderGuard != nil {
		s.orderGuard.Unlock()
	}
	if s.triggerGuard != nil {
		s.triggerGuard.Unlock()
	}
	if s.openGuard != nil {
		s.openGuard.Unlock()
	}
}
