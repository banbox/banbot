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
//	8  factor PIT repair: the PIT universe reduction works for the production static pool (Reference == Investable ==
//	   Tradable == Evaluation == Tracked; only Reference-only instruments are never dropped), also per pair in statarb;
//	   under the lifecycle policy an instrument that delists / has a hole while held is closed at its last mark and
//	   forgotten by the policy checkpoint, decisions go on (before: every later decision was voided and the position
//	   kept); the pit_min_universe_ratio gate uses a rolling denominator (survivors of the last accepted decision), so
//	   gradual delistings below half of the nominal pool keep being decided; static pairs missing from the current
//	   market table resolve from the catalog only in factor Weights storage replay and only when stored kline ranges
//	   overlap the backtest window; factor strategy keys pit_min_universe_ratio / pit_delist_penalty_bps; Result.Pairs (pair-level statistics),
//	   Result.Attribution (long/short attribution) and the panel output switch
//	9  factor Weights backtests (PIT rule): a static pool member the exchange no longer lists whose catalog identity is known
//	   but that has no stored kline inside the backtest window (delisted before it / listed after it) is left out of the pool
//	   for that window (WARN, goods.RuntimeDeps.SkippedPairs) instead of failing the run with InvalidSymbol; all members
//	   unavailable still fails; statarb legs are unchanged. Pools whose members all have data are byte-identical (v0.7.90-trend.16)
//	10 account liquidation (wallet BOMB, charge_on_bomb off) ends EVERY backtest (plain run, optimisation trial, holdout /
//	   full-range run) normally with exit code 0 instead of failing with "context canceled": positions are closed at the
//	   liquidation bar (exit tag "liquidation"), equity is 0 from there to the end of the run window (total profit -100%,
//	   max drawdown 100%, flat zero equity curve, finite sharpe / ulcer / UPI) and detail.json carries liquidated,
//	   liquidated_at_ms, liquidated_symbol, liquidated_reason (v0.7.90-trend.17). Runs that never liquidate are byte-identical
//	11 statarb legs that have no stored data inside the run window (retired / not yet listed; the pool refresh already
//	   leaves them out) no longer fail the run with "pair ... has a symbol missing from this run's universe": the pairs
//	   that use them are skipped for the whole run (PairResult.Unavailable, run.json skipped_pairs entries with reason
//	   leg_unavailable + pair, per-pair Gross share unchanged so the other pairs are never scaled up), the other pairs
//	   trade as usual; when every pair is skipped the run ends normally with no trades, a flat equity curve and
//	   run.json all_pairs_unavailable. A leg absent for any other reason is still an error. ts and factor runs and
//	   statarb runs whose legs all have data are byte-identical (v0.7.90-trend.18)
const BtFeatureLevel = 11
