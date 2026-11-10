package biz

import (
	"fmt"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/orm/ormo"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

// ExitTagDataGap tags orders force-closed because the symbol's kline series has a hole (bt_gap_bars).
const ExitTagDataGap = "data_gap"

// ExitTagDelist tags orders force-closed at the end of a backtest because the symbol's kline data had already
// ended long before (delisted / data ends): closed at the last bar's close, slippage included.
const ExitTagDelist = "delist"

const defaultGapMinHours = 48.0

// barGapExceeded reports whether the distance between the end of the previous bar (prevStopMS) and the start of
// the new bar (curStartMS) is a data hole: more than gapBars periods AND more than minHours hours.
// gapBars <= 0 disables the check. Returns the missing duration in ms.
func barGapExceeded(prevStopMS, curStartMS, tfMS int64, gapBars int, minHours float64) (int64, bool) {
	if gapBars <= 0 || prevStopMS <= 0 || tfMS <= 0 || curStartMS <= prevStopMS {
		return 0, false
	}
	if minHours <= 0 {
		minHours = defaultGapMinHours
	}
	miss := curStartMS - prevStopMS
	if miss <= int64(gapBars)*tfMS || float64(miss) <= minHours*3600*1000 {
		return miss, false
	}
	return miss, true
}

// guardBarGap implements the backtest data-gap guard (config bt_gap_bars). It must run before the new bar's price
// is registered: open orders of the symbol are closed at the last known bar price (the bar before the hole), so
// a position never "earns" the price jump across missing data (e.g. LUNA -> LUNA2 re-listing). Live trading is
// ignored (live has its own lack warning).
func (t *Trader) guardBarGap(timeMS int64, tf, symbol string, prevStopMS, tfMS int64, nextOpen float64) *errs.Error {
	var cfg *config.Config
	if t.runtime != nil {
		cfg = t.runtime.ConfigView()
	} else {
		cfg = &config.Data
	}
	if cfg == nil || cfg.BTGapBars <= 0 || t.liveMode() {
		return nil
	}
	miss, hit := barGapExceeded(prevStopMS, timeMS, tfMS, cfg.BTGapBars, cfg.BTGapMinHours)
	if !hit {
		return nil
	}
	closed := 0
	accounts := executionAccountConfigs(t.runtime)
	for account := range executionAccountNames(t.runtime) {
		if ac := accounts[account]; ac == nil || ac.NoTrade {
			continue
		}
		openOds, lock := t.openOrders(account)
		lock.Lock()
		var hold []*ormo.InOutOrder
		for _, od := range executionOpenOrders(openOds, t.runtime) {
			if od.Symbol == symbol && od.Status < ormo.InOutStatusFullExit {
				hold = append(hold, od)
			}
		}
		lock.Unlock()
		if len(hold) == 0 {
			continue
		}
		odMgr := t.orderManager(account)
		if odMgr == nil {
			return errs.NewMsg(core.ErrRunTime, "order manager is not available for account %s", account)
		}
		req := &strat.ExitReq{Tag: ExitTagDataGap, Dirt: core.OdDirtBoth, Force: true, GapNextOpen: nextOpen, GapRecoverMS: timeMS}
		if err := odMgr.ExitAndFill(hold, req); err != nil {
			return err
		}
		closed += len(hold)
	}
	log.Warn(fmt.Sprintf("%s/%s data gap: %d bars (%.1f days) missing before bar %d; closed %d open orders at last price",
		symbol, tf, miss/tfMS, float64(miss)/86400000, timeMS, closed),
		zap.String("symbol", symbol), zap.Int64("gap_ms", miss), zap.Int("closed", closed))
	return nil
}

// delistedOrders returns the open orders whose symbol's data ended before atMS by more than the data-gap
// thresholds (bt_gap_bars periods of the order's timeframe AND bt_gap_min_hours). Same rule as barGapExceeded:
// a hole that never ends is a delisting. Backtest only; disabled with bt_gap_bars <= 0.
func (o *LocalOrderMgr) delistedOrders(atMS int64) []*ormo.InOutOrder {
	if !o.isBacktest() {
		return nil
	}
	gapBars, minHours := config.Data.BTGapBars, config.Data.BTGapMinHours
	if o.runtimeDeps {
		gapBars, minHours = o.runtimeCfg.gapBars, o.runtimeCfg.gapMinHours
	}
	if gapBars <= 0 {
		return nil
	}
	openOds, lock := o.openOrders()
	lock.Lock()
	all := executionOpenOrders(openOds, o.executionDeps())
	lock.Unlock()
	var hold []*ormo.InOutOrder
	for _, od := range all {
		if od.Status >= ormo.InOutStatusFullExit {
			continue
		}
		last := o.lastBarTime(od.Symbol)
		if last <= 0 {
			continue
		}
		tfMS := int64(utils.TFToSecs(od.Timeframe)) * 1000
		if _, hit := barGapExceeded(last, atMS, tfMS, gapBars, minHours); hit {
			hold = append(hold, od)
		}
	}
	return hold
}

// exitDelisted closes positions of symbols whose data ended long before the backtest end at their last bar's
// close (+slippage) with exit tag "delist", so a dead coin is neither held at a stale price until the end
// (bot_stop, looks like a hold) nor excluded: delisted coins stay in the sample. It runs first in CleanUp.
func (o *LocalOrderMgr) exitDelisted() *errs.Error {
	return o.exitDelistedAt(o.priceNow())
}

// delistSweepEveryMS throttles the in-run delist sweep (simulated time).
const delistSweepEveryMS = 6 * 3600 * 1000

// sweepDelisted (066) is the in-run counterpart of the CleanUp delist exit: called on every new bar, at most once
// per delistSweepEveryMS of simulated time, it closes positions whose symbol's data ended (gap > bt_gap_bars
// periods and > bt_gap_min_hours) as of atMS. The fill time is the symbol's last valid bar, so margin and
// max_open_orders slots are released when the data ended rather than at the backtest end.
func (o *LocalOrderMgr) sweepDelisted(atMS int64) *errs.Error {
	if atMS-o.lastDelistSweepMS < delistSweepEveryMS {
		return nil
	}
	o.lastDelistSweepMS = atMS
	return o.exitDelistedAt(atMS)
}

func (o *LocalOrderMgr) exitDelistedAt(atMS int64) *errs.Error {
	hold := o.delistedOrders(atMS)
	if len(hold) == 0 {
		return nil
	}
	req := &strat.ExitReq{Tag: ExitTagDelist, Dirt: core.OdDirtBoth, Force: true}
	if err := o.ExitAndFill(hold, req); err != nil {
		return err
	}
	syms := map[string]bool{}
	for _, od := range hold {
		syms[od.Symbol] = true
	}
	log.Warn(fmt.Sprintf("delist exit: %d open orders of %d symbols closed at the last bar price/time (data ended before bar %d)",
		len(hold), len(syms), atMS))
	return nil
}

// sweepDelistedAll runs the in-run delist sweep (066) on every local order manager of the trader.
func (t *Trader) sweepDelistedAll(timeMS int64) *errs.Error {
	var cfg *config.Config
	if t.runtime != nil {
		cfg = t.runtime.ConfigView()
	} else {
		cfg = &config.Data
	}
	if cfg == nil || cfg.BTGapBars <= 0 || t.liveMode() {
		return nil
	}
	accounts := executionAccountConfigs(t.runtime)
	for account := range executionAccountNames(t.runtime) {
		if ac := accounts[account]; ac == nil || ac.NoTrade {
			continue
		}
		if mgr, ok := t.orderManager(account).(*LocalOrderMgr); ok && mgr != nil {
			if err := mgr.sweepDelisted(timeMS); err != nil {
				return err
			}
		}
	}
	return nil
}

// gapConfirmDelayMS is how long after its last bar a symbol must stay silent before the hole is a data gap /
// delisting (the same rule as barGapExceeded): the time at which the backtest can first know about it.
func gapConfirmDelayMS(tfMS int64, gapBars int, minHours float64) int64 {
	if minHours <= 0 {
		minHours = defaultGapMinHours
	}
	d := int64(gapBars) * tfMS
	if m := int64(minHours * 3600 * 1000); m > d {
		d = m
	}
	return d + 1
}

func (o *LocalOrderMgr) delistPenalty() float64 {
	if o.runtimeDeps {
		return o.runtimeCfg.delistPenaltyBps
	}
	return config.Data.BTDelistPenaltyBps
}

// applyExitPolicy (071) turns the last bar's close into the exit price of a forced data_gap / delist exit:
//   - data_gap (data resumed after the hole): the worse for the position of the last close before the hole and
//     the first open after it (long: lower, short: higher), so a position never dodges the move inside the hole;
//   - delist (data never resumes): the last close moved adversely by bt_delist_penalty_bps.
//
// Slippage is applied afterwards by the caller.
func (o *LocalOrderMgr) applyExitPolicy(od *ormo.InOutOrder, req *strat.ExitReq, last float64) float64 {
	switch req.Tag {
	case ExitTagDataGap:
		if req.GapNextOpen > 0 {
			if od.Short {
				return max(last, req.GapNextOpen)
			}
			return min(last, req.GapNextOpen)
		}
	case ExitTagDelist:
		if bps := o.delistPenalty(); bps > 0 {
			if od.Short {
				return last * (1 + bps/10000)
			}
			return last * (1 - bps/10000)
		}
	}
	return last
}

// gapFillTime is the booked time of a recovered data_gap exit: min(last bar + confirmation delay, recovery time),
// never before the last bar.
func (o *LocalOrderMgr) gapFillTime(od *ormo.InOutOrder, last, nowMS int64, req *strat.ExitReq) int64 {
	gapBars, minHours := config.Data.BTGapBars, config.Data.BTGapMinHours
	if o.runtimeDeps {
		gapBars, minHours = o.runtimeCfg.gapBars, o.runtimeCfg.gapMinHours
	}
	tfMS := int64(utils.TFToSecs(od.Timeframe)) * 1000
	at := last + gapConfirmDelayMS(tfMS, gapBars, minHours)
	if req.GapRecoverMS > 0 && req.GapRecoverMS < at {
		at = req.GapRecoverMS
	}
	if at > nowMS {
		at = nowMS
	}
	if at < last {
		at = last
	}
	return at
}
