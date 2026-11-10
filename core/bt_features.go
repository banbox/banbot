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
const BtFeatureLevel = 3
