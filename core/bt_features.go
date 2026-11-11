package core

// BtFeatureLevel is the backtest capability level of this banbot build. bansite reads it (it parses this very
// constant from the module source it is about to compile, see bansite biz/compile_check.go) and refuses to build
// a binary that cannot honour the backtest keys its config enables, instead of silently running a backtest without
// slippage / funding / gap / delist handling. Keep the declaration on one line: `const BtFeatureLevel = N`.
//
//	1  bt_slippage_bps, bt_funding_apr, bt_gap_bars/bt_gap_min_hours, delist exit at backtest end (v0.7.90-trend.8)
//	2  delist / data-gap exits are filled at the symbol's last valid bar (funding, holding span, margin and slot
//	   release follow it), in-run delist sweep, factor PIT universe (missing instruments dropped per decision),
//	   WARN on unknown bt_* config keys (v0.7.90-trend.9)
//	3  071 exit policy: data_gap exits fill at the worse of last close / first post-gap open at min(last+confirm delay,
//	   recovery), delist exits pay bt_delist_penalty_bps; BTResult gapExitPnlShare/delistExitPnlShare; factor/statarb PIT
//	   exits follow the same rules, per-pair statarb pause + pausedPeriods (v0.7.90-trend.10)
//	4  factor/statarb manifest.costs.funding_apr (long-notional funding approximation), PIT universe drop of
//	   not-yet-listed instruments under the lifecycle policy (v0.7.90-trend.11)
//	5  ordered backtest (issue 111): every fill-relevant map / set walk (job-per-symbol order within one bar, account,
//	   open-order, batch, rotation, wallet-settle and wallet order view) uses a stable key order in EVERY backtest, not
//	   only with bt_strict. Builds < 5 give different fills / UPI on identical reruns of multi-symbol strategies
//	   (capital competition between symbols on the same bar); no new config key, so bansite does not need to require
//	   it - it marks "results are bit-reproducible" (v0.7.90-trend.12)
//	6  neutral same-bar priority (issue 112): symbols due on the same bar time are replayed in hash(bar time, symbol,
//	   bt_bar_order_seed) order instead of symbol-ascending order, so limited capital is not systematically taken by
//	   early-alphabet names; reproducible for identical input. Changes every multi-symbol backtest result once more
//	   (single-pair results are unchanged). bansite requires >= 6 for every compile (v0.7.90-trend.13)
//	7  bt_no_kline_download without bt_strict (issue 134): nothing is downloaded and orm.FastBulkOHLCV (static-pair volume
//	   filter, pair/timeframe scoring) reads the library; bt_strict still needs historical_coverage; symbols of unknown
//	   listing time are read as stored with a WARN instead of failing the run (v0.7.90-trend.14)
const BtFeatureLevel = 7
