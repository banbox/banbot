package runtime

import (
	"fmt"
	"sync"
	"time"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

// Responsibilities that live.CryptoTrader performs around the shared
// biz.Trader and that the execution-backed live path performs here instead:
// batch firing (live/crypto_trader.go handleWarmupSeries/handleLiveSeries/
// delayExecBatch), strategy shutdown hooks (exitCleanUpWithRuntime), strategy
// performance loading and the dashboard series feed (feedDataSeries).
//
// State lives in a registry keyed by Runtime (like Notifier) so Runtime itself
// stays untouched; the entry is released by the runtime stop hook.

// seriesObserverQueueSize bounds the rows waiting for the observer worker.
const seriesObserverQueueSize = 256

type liveHooks struct {
	mu       sync.Mutex
	closed   bool
	batch    bool // some bound strategy uses BatchInOut / BatchInfo callbacks
	timers   map[*time.Timer]struct{}
	shutdown bool // strategy shutdown hooks registered
	observer func(*data.SeriesMsg)
	queue    chan *data.SeriesMsg
	stop     chan struct{}
}

var (
	liveHooksMu  sync.Mutex
	liveHooksMap = map[*Runtime]*liveHooks{}
)

// lookupLiveHooks returns the runtime's hook state or nil when none exists.
func lookupLiveHooks(r *Runtime) *liveHooks {
	liveHooksMu.Lock()
	defer liveHooksMu.Unlock()
	return liveHooksMap[r]
}

func (r *Runtime) liveHooks() *liveHooks {
	liveHooksMu.Lock()
	if h := liveHooksMap[r]; h != nil {
		liveHooksMu.Unlock()
		return h
	}
	h := &liveHooks{timers: map[*time.Timer]struct{}{}, stop: make(chan struct{})}
	liveHooksMap[r] = h
	liveHooksMu.Unlock()
	// Registered outside the lock: OnClose runs the hook at once when the
	// runtime is already stopped and the hook takes the registry lock itself.
	r.OnClose(func() {
		h.mu.Lock()
		h.closed = true
		timers := h.timers
		h.timers = map[*time.Timer]struct{}{}
		close(h.stop)
		h.mu.Unlock()
		for timer := range timers {
			timer.Stop()
		}
		liveHooksMu.Lock()
		if liveHooksMap[r] == h {
			delete(liveHooksMap, r)
		}
		liveHooksMu.Unlock()
	})
	return h
}

// ---------------------------------------------------------------- binding

// bindLiveHooks runs at the end of BindFactorLegacyJobs: it loads persisted
// strategy performance, records whether batch callbacks need firing and
// registers the strategy shutdown hooks once per runtime.
func (r *Runtime) bindLiveHooks(jobs []*strat.StratJob) {
	// Performance-based stake scaling (strat/base.go reads job perf). The loader
	// is inert unless strat_perf.enable is set and the file exists.
	deps := r.BizDeps()
	if cfg := deps.ConfigView(); cfg != nil && deps.Config != nil {
		config.LoadPerfsWithCoreState(deps.Config.DataDir, deps.Core, cfg.StratPerf)
	}
	h := r.liveHooks()
	h.mu.Lock()
	for _, job := range jobs {
		if job == nil || job.Strat == nil {
			continue
		}
		if job.Strat.BatchInOut && job.Strat.OnBatchJobs != nil || job.Strat.BatchInfo && job.Strat.OnBatchInfos != nil {
			h.batch = true
		}
	}
	register := !h.shutdown
	h.shutdown = true
	h.mu.Unlock()
	if register {
		// Join hook: it runs after every owner-side join registered before it
		// (order manager clean-up) and, through the callback barrier, after the
		// provider stopped delivering rows, but before Strategies.Reset.
		r.OnCloseWait(r.runStrategyShutdown)
	}
}

func (r *Runtime) runStrategyShutdown() {
	defer func() {
		if rec := recover(); rec != nil {
			r.Core.Log().Error("strategy shutdown hook panic", zap.Any("panic", rec))
		}
	}()
	// Feeding has stopped: every intake was refused when Stop moved the phase;
	// wait for the ones already running to leave.
	r.callbackWait.Wait()
	if r.Strategies == nil || strat.IsLegacyState(r.Strategies) {
		return
	}
	strat.ExitStratJobsWithState(r.Strategies)
}

// ------------------------------------------------------------ batch firing

// batchFeed mirrors CryptoTrader.feedDataSeries before Trader.FeedDataSeries:
// warmup rows fire pending batches of finished bars synchronously, live rows
// arm the delay timer that collects every symbol of the same bar.
func (r *Runtime) batchFeed(sub *strat.DataSub, series *orm.DataSeries) {
	h := lookupLiveHooks(r)
	if h == nil || r.factorLegacyTrader == nil {
		return
	}
	h.mu.Lock()
	enabled := h.batch && !h.closed
	h.mu.Unlock()
	if !enabled {
		return
	}
	view, err := series.OHLCV(sub.ExSymbol)
	if err != nil {
		return
	}
	state := r.factorLegacyTrader.BatchState()
	if state == nil {
		return
	}
	if view.IsWarmUp {
		tfMSecs := int64(utils.TFToSecs(view.TimeFrame) * 1000)
		barEndMS := view.Time + tfMSecs
		if barEndMS > state.LastBatchMS() {
			// Enter the next timeframe and trigger the batch entry callback.
			execMS := barEndMS + core.DelayBatchMS + 1
			deps := r.BizDeps()
			if waitNum := biz.TryFireBatchesWithRuntimeDeps(&deps, state, execMS, true); waitNum > 0 {
				r.Core.Log().Warn(fmt.Sprintf("batch job exec fail, wait: %v", waitNum))
			}
			state.SetLastBatchMS(barEndMS)
		}
		return
	}
	r.delayExecBatch(h, time.Millisecond*core.DelayBatchMS)
}

func (r *Runtime) delayExecBatch(h *liveHooks, delay time.Duration) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	var timer *time.Timer
	timer = time.AfterFunc(delay, func() {
		h.mu.Lock()
		delete(h.timers, timer)
		closed := h.closed
		h.mu.Unlock()
		if closed || !r.EnterCallback() {
			return
		}
		defer r.LeaveCallback()
		defer func() {
			if rec := recover(); rec != nil {
				r.Core.Log().Error("delayed batch callback panic", zap.Any("panic", rec))
			}
		}()
		r.runDelayedBatch(h)
	})
	h.timers[timer] = struct{}{}
	h.mu.Unlock()
}

func (r *Runtime) runDelayedBatch(h *liveHooks) {
	if r.factorLegacyTrader == nil {
		return
	}
	state := r.factorLegacyTrader.BatchState()
	if state == nil {
		return
	}
	deps := r.BizDeps()
	if waitNum := biz.TryFireBatchesWithRuntimeDeps(&deps, state, r.Clock.TimeMS(), false); waitNum > 0 {
		// There are TF cycles that have not yet completed, retry in a few seconds.
		r.delayExecBatch(h, time.Millisecond*core.DelayBatchMS)
	}
}

// --------------------------------------------------------- series observer

// SetSeriesObserver installs (or, with nil, removes) the live series observer.
// It receives a copy of every non-warmup row the live feed delivers to the
// strategies, in the same message shape the dashboard publisher consumes, so the
// entry layer connects it with:
//
//	rt.SetSeriesObserver(apiServer.PublishSeries)
//
// Delivery is decoupled from the feed: rows go through a bounded queue to one
// worker goroutine; a full queue drops rows, a slow observer never delays the
// feed and a panicking observer is recovered.
func (r *Runtime) SetSeriesObserver(fn func(*data.SeriesMsg)) {
	if r == nil {
		return
	}
	h := r.liveHooks()
	h.mu.Lock()
	h.observer = fn
	start := fn != nil && h.queue == nil && !h.closed
	if start {
		h.queue = make(chan *data.SeriesMsg, seriesObserverQueueSize)
	}
	queue := h.queue
	h.mu.Unlock()
	if start {
		go r.runSeriesObserver(h, queue)
	}
}

func (r *Runtime) runSeriesObserver(h *liveHooks, queue chan *data.SeriesMsg) {
	for {
		select {
		case <-h.stop:
			return
		case msg := <-queue:
			h.mu.Lock()
			fn := h.observer
			h.mu.Unlock()
			if fn == nil {
				continue
			}
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						r.Core.Log().Warn("series observer panic", zap.Any("panic", rec))
					}
				}()
				fn(msg)
			}()
		}
	}
}

// publishSeries hands one live row to the observer without ever blocking.
func (r *Runtime) publishSeries(sub *strat.DataSub, series *orm.DataSeries) {
	if series.IsWarmUp {
		return
	}
	h := lookupLiveHooks(r)
	if h == nil {
		return
	}
	h.mu.Lock()
	queue, active := h.queue, h.observer != nil && !h.closed
	h.mu.Unlock()
	if !active || queue == nil {
		return
	}
	row := *series
	if series.Values != nil {
		row.Values = make(map[string]any, len(series.Values))
		for k, v := range series.Values {
			row.Values[k] = v
		}
	}
	exs := sub.ExSymbol
	msg := &data.SeriesMsg{
		ExgName: exs.Exchange, Market: exs.Market, Pair: exs.Symbol,
		NotifySeries: data.NotifySeries{TFSecs: int((series.EndMS - series.TimeMS) / 1000), Rows: []*orm.DataSeries{&row}},
	}
	select {
	case queue <- msg:
	default:
	}
}
