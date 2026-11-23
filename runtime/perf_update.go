package runtime

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"go.uber.org/zap"
)

// Strategy performance (strat_perf) on execution-backed runtimes.
//
// The legacy OrderMgr.finishOrder recomputes a job's score with
// strat.CalcJobScoresWithState after every finished order, reading the job's
// last max_od_num closed orders from OrderState.HistoricalOrders(). The
// execution-backed managers never populate that history, so closed orders are
// kept here: a bounded per-job ring fed from the manager order callback. The
// scoring itself is the unchanged legacy function, run against a private
// OrderState holding exactly the ring.
//
// Everything is inert unless strat_perf is enabled for the closed order's job:
// no tracker, goroutine, ring or database read exists before then. The callback
// only clones the order and enqueues it; one worker computes, so the managers'
// locks and order path never wait for scoring.

const (
	perfQueueSize = 256
	// perfDefaultRing bounds a job's history when max_od_num is not positive
	// (legacy: unlimited).
	perfDefaultRing = 1000
	perfPrivateTask = int64(1)
)

type perfEntry struct {
	key string
	od  *ormo.InOutOrder
}

type perfJobHist struct {
	seeded bool
	nextID int64
	ring   []perfEntry
}

type perfItem struct {
	od    *ormo.InOutOrder
	flush chan struct{}
}

type perfTracker struct {
	r     *Runtime
	queue chan perfItem
	stop  chan struct{}
	once  sync.Once
	jobs  map[string]*perfJobHist // worker goroutine only
	calcs atomic.Int64            // completed score updates
}

var (
	perfTrackersMu sync.Mutex
	perfTrackers   = map[*Runtime]*perfTracker{}
)

func lookupPerfTracker(r *Runtime) *perfTracker {
	perfTrackersMu.Lock()
	defer perfTrackersMu.Unlock()
	return perfTrackers[r]
}

func (r *Runtime) perfTracker() *perfTracker {
	perfTrackersMu.Lock()
	if t := perfTrackers[r]; t != nil {
		perfTrackersMu.Unlock()
		return t
	}
	t := &perfTracker{r: r, queue: make(chan perfItem, perfQueueSize), stop: make(chan struct{}), jobs: map[string]*perfJobHist{}}
	perfTrackers[r] = t
	perfTrackersMu.Unlock()
	go t.run()
	// Registered outside the lock: OnClose runs at once on a stopped runtime.
	r.OnClose(func() {
		t.once.Do(func() { close(t.stop) })
		perfTrackersMu.Lock()
		if perfTrackers[r] == t {
			delete(perfTrackers, r)
		}
		perfTrackersMu.Unlock()
	})
	return t
}

// observeClosedOrder is called for every manager fill callback. It is a no-op
// unless od is the final exit fill of a lot whose job enables strat_perf.
func (r *Runtime) observeClosedOrder(od *ormo.InOutOrder, isEnter bool) {
	defer func() { _ = recover() }()
	if isEnter || od == nil || od.IOrder == nil || od.Enter == nil || od.Exit == nil {
		return
	}
	// Partial reductions are not finished orders.
	if od.Status != ormo.InOutStatusFullExit || od.Enter.Filled == 0 || r.Strategies == nil {
		return
	}
	cfg := r.Strategies.GetStratPerf(od.Symbol, od.Strategy)
	if cfg == nil || !cfg.Enable {
		return
	}
	t := r.perfTracker()
	select {
	case t.queue <- perfItem{od: od.Clone()}:
	default:
		r.Core.Log().Warn("strat_perf update dropped: queue full", zap.String("key", od.Key()))
	}
}

func (t *perfTracker) flush(wait time.Duration) bool {
	marker := make(chan struct{})
	select {
	case t.queue <- perfItem{flush: marker}:
	case <-time.After(wait):
		return false
	}
	select {
	case <-marker:
		return true
	case <-time.After(wait):
		return false
	}
}

func (t *perfTracker) run() {
	for {
		select {
		case <-t.stop:
			return
		case it := <-t.queue:
			if it.od != nil {
				t.safeHandle(it.od)
			}
			if it.flush != nil {
				close(it.flush)
			}
		}
	}
}

func (t *perfTracker) safeHandle(od *ormo.InOutOrder) {
	defer func() {
		if rec := recover(); rec != nil {
			t.r.Core.Log().Error("strat_perf update panic", zap.Any("panic", rec))
		}
	}()
	t.handle(od)
}

// perfOrderKey is the stable identity of a closed lot within its job: the lot
// ID shared by both managers' views (and the suffix of the Hub archive key).
func perfOrderKey(od *ormo.InOutOrder) string {
	if lot, _ := od.Info["shared_lot"].(string); lot != "" {
		return lot
	}
	return "id:" + strconv.FormatInt(od.ID, 10) + "@" + strconv.FormatInt(od.EnterAt, 10)
}

// perfWithRate mirrors the archive rule: projections carry realized profit but
// not its rate (legacy finishOrder sets it through UpdateProfits).
func perfWithRate(od *ormo.InOutOrder) {
	if od.ProfitRate == 0 {
		// Entry price as in InOutOrder.UpdateProfits; the Shared projection
		// carries no average entry price, only the intended one.
		price := od.InitPrice
		if od.Enter.Average > 0 {
			price = od.Enter.Average
		} else if od.Enter.Price > 0 {
			price = od.Enter.Price
		}
		if notional := price * od.Enter.Filled; notional > 0 {
			od.ProfitRate = od.Profit / notional
		}
	}
}

func (t *perfTracker) handle(od *ormo.InOutOrder) {
	deps := t.r.BizDeps()
	if deps.Strategies == nil || deps.Core == nil || deps.Orders == nil {
		return
	}
	cfg := deps.Strategies.GetStratPerf(od.Symbol, od.Strategy)
	if cfg == nil || !cfg.Enable {
		return
	}
	limit := cfg.MaxOdNum
	if limit <= 0 {
		limit = perfDefaultRing
	}
	jk := core.KeyStratPairTf(od.Strategy, od.Symbol, od.Timeframe)
	job := t.jobs[jk]
	if job == nil {
		job = &perfJobHist{}
		t.jobs[jk] = job
	}
	if !job.seeded {
		job.seeded = true
		t.seed(&deps, job, od, limit)
	}
	key := perfOrderKey(od)
	for _, e := range job.ring {
		if e.key == key {
			return // redelivery or replay of an already counted close
		}
	}
	perfWithRate(od)
	job.add(key, od, limit)

	// Private history holding exactly the ring: the legacy function filters by
	// task/strategy/timeframe/pair and takes the newest max_od_num by ID.
	state := ormo.NewOrderState()
	state.SetTask(deps.DefaultAccount, &ormo.BotTask{ID: perfPrivateTask})
	for _, e := range job.ring {
		state.AddHistoricalOrder(e.od)
	}
	if err := strat.CalcJobScoresWithState(deps.Strategies, deps.Core, state, deps.DefaultAccount,
		od.Symbol, od.Timeframe, od.Strategy); err != nil {
		deps.Logger().Error("calc job performance fail", zap.Error(err),
			zap.Strings("job", []string{od.Symbol, od.Timeframe, od.Strategy}))
		return
	}
	t.calcs.Add(1)
	// Same persistence as the process-global legacy path: strat_perfs.yml is
	// what bindLiveHooks loads at the next start.
	if deps.Core.LiveMode && deps.Config != nil && deps.Config.DataDir != "" {
		deps.Core.DumpPerfs(deps.Config.DataDir)
	}
}

func (j *perfJobHist) add(key string, od *ormo.InOutOrder, limit int) {
	j.nextID++
	od.ID, od.TaskID = j.nextID, perfPrivateTask
	j.ring = append(j.ring, perfEntry{key: key, od: od})
	if over := len(j.ring) - limit; over > 0 {
		clear(j.ring[:over])
		j.ring = append([]perfEntry(nil), j.ring[over:]...)
	}
}

// seed loads the job's earlier closed orders from the trades database (written
// by the Hub archive) so a restart continues from the previous history. It runs
// once per job; a missing task or database leaves the ring to fill from
// callbacks only (Shared does not archive).
func (t *perfTracker) seed(deps *biz.RuntimeDeps, job *perfJobHist, od *ormo.InOutOrder, limit int) {
	taskID := deps.Orders.GetTaskID(deps.DefaultAccount)
	if taskID <= 0 {
		return
	}
	sess, conn, err := deps.Orders.Conn(false)
	if err != nil {
		deps.Logger().Warn("strat_perf history unavailable", zap.Error(err))
		return
	}
	defer conn.Close()
	rows, err := sess.GetOrders(ormo.GetOrdersArgs{
		TaskID: taskID, Status: 2, Strategy: od.Strategy, Pairs: []string{od.Symbol},
		TimeFrame: od.Timeframe, Limit: limit, OrderBy: "id desc",
	})
	if err != nil {
		deps.Logger().Warn("strat_perf history load failed", zap.Error(err))
		return
	}
	for i := len(rows) - 1; i >= 0; i-- {
		row := rows[i]
		if row == nil || row.IOrder == nil || row.Enter == nil {
			continue
		}
		key := "db:" + strconv.FormatInt(row.ID, 10)
		if ak := row.GetInfoString(ormo.OdInfoArchiveKey); ak != "" {
			key = ak[strings.LastIndex(ak, "|")+1:]
		}
		perfWithRate(row)
		job.add(key, row, limit)
	}
}
