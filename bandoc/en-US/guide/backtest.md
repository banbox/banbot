# BanBot Backtesting Guide

Backtesting is the process of simulating a trading strategy on historical data. It is an important step for strategy optimization and evaluation. The BanBot backtesting engine has the following features:

- Event-driven historical replay; strategy logic, data visibility, and universe selection still need checks for lookahead.
- Supports time-series strategies of any complexity, without modification for live trading.
- High-performance indicator library `banta` for faster backtesting speed.
- Flexible combination of different trading pairs, strategies, and timeframes.

## Quick Start

### Backtesting from the WebUI
BanBot provides a WebUI for backtesting research. You can write strategies, configure parameters, and run backtests directly in the WebUI.

Just compile your Go project into an executable file and start it to run the WebUI:
```bash
go build -o bot
./bot web --datadir /path/to/data
```

The WebUI has four options at the top: [Strategy]/[Backtest]/[Data]/[Live Trading].

You can first write a strategy in [Strategy]. If you want your new or modified strategy to take effect, you need to click the [Compile] button (refresh icon) in the upper right corner.

Then, configure the parameters and run the backtest in [Backtest].

After the backtest is finished, you can go to the backtest report page to view the detailed backtest report.

### Backtesting from the Command Line
Open `config.local.yml` (or `config.yml`) in the directory pointed to by the `BanDataDir` environment variable, and configure the important backtesting parameters:

```yaml
exchange: {name: binance}
market_type: linear
contract_type: swap
leverage: 10
time_start: "20251101"
time_end: "20251112"
stake_amount: 30
pairs: [BTC, ETH]
stake_currency: [USDT]
wallet_amounts:
  USDT: 1000
run_policy:
  - engine: time_series
    name: ma:demo
    run_timeframes: [1h]
```

This is a backtest fragment. `ma:demo` must be registered in the binary; prepare database and shared settings separately. Leverage is illustrative and should match your experiment. This is not a Hub live configuration; see the [execution guide](./execution.md) before live use. Then compile and run:
```bash
go build -o bot
./bot backtest --datadir /path/to/data --out /path/to/report
```

Pure time-series reports default to `<datadir>/backtest/<config-hash>/`; `--out` selects a directory. See [configuration](./configuration.md) for layering.

```bash
bot backtest --datadir /path/to/data --config experiment.yml --timerange 20250101-20250401 --pairs BTC/USDT:USDT
bot backtest --datadir /path/to/data --config experiment.yml --separate
```

`--separate` runs pure time-series policy entries separately; by default they run together as a combined report. Other flags include `--timestart` / `--timeend` (the latter requires the former), `--stake-amount`, `--prg`, and `--bt-strict`. Check `bot backtest --help`; flags declared by other commands are not interchangeable.

## Data preparation and download limits

Ordinary time-series backtests can fill required bars and write them to the database, given available data sources and network access. Prepare strategy, `refine_tf`, and warmup coverage first. `bt_no_kline_download: true` independently disables implicit K-line downloads, including static symbol filtering/timeframe scoring reads. Used alone it may simply use incomplete stored data; it does not prove completeness. `--net-off` is not a coverage check either.

Recheck coverage after changing symbols, timeframes, fields, or intervals. Unknown listing times may only warn outside strict mode. Existing rows do not prove historical decision-time visibility. See the strict historical contract below.

## Backtesting Notes

### Order Matching Timeframe (`refine_tf`)

By default, BanBot matches orders on the strategy's K-line timeframe (`run_timeframes`). For example, if the strategy runs on a `1h` timeframe, the order's execution price will be matched based on the `1h` K-line. However, this may not be consistent with live trading in some scenarios, for example:

- **High-frequency volatility**: When the K-line price fluctuates violently, it may meet both take-profit and stop-loss conditions at the same time. A single K-line loses internal details. To avoid deviation, BanBot will trigger the stop-loss first (this applies only within one matching bar; the finer the matching timeframe, the smaller this bias).
- **Drawdown analysis**: It is impossible to observe the more granular capital drawdown within a large-period K-line.

To solve this problem, BanBot introduces the `refine_tf` configuration item, which allows order matching on a smaller timeframe to simulate more realistic market behavior.

Add `refine_tf` for the specified `job` in the `run_policy` of `config.yml`:

```yaml
run_policy:
  - engine: time_series
    name: ma:demo
    run_timeframes: [1h]
    refine_tf: "1m" # or 4, "3-6"
```

`refine_tf` supports multiple formats:

- **Fixed timeframe (string)**: Such as `"1m"`, `"5m"`. Directly specify a precise matching timeframe.
- **Relative multiple (integer)**: Such as `4`. It means that the `run_timeframes` period is divided into N equal parts. For example, if `run_timeframes` is `1h` and `refine_tf` is `4`, the matching timeframe is `15m`.
- **Relative multiple range (string)**: Such as `"3-6"`. The program will automatically select a common timeframe within the range. For example, if `run_timeframes` is `1d` and `refine_tf` is `"3-6"`, it will choose `4h` (24h / 6) as the matching timeframe.

`refine_tf` in `run_policy` takes precedence; when unset, the strategy's own `TradeStrat.RefineTF` (same formats) is used. A `1m` run timeframe is not refined. An unparsable value logs an error and falls back to the main timeframe.

Notes:

- Prepare K-lines for the matching timeframe in advance, including the warmup range; nothing is downloaded when `bt_no_kline_download: true`.
- If the matching-timeframe bar has not arrived yet at a given moment, matching falls back to the main timeframe automatically.
- Pending orders created inside an order callback (`OnOrderChange`) are matched only on the remaining price path after their creation time (including the `bt_net_cost` delay), so they never fill at prices before the callback.
- Finer granularity only reduces cases where the order of events inside one bar is undecidable; it is not tick-by-tick execution.

### Matching semantics

Orders are matched along each bar's O→H→L→C price path; ordering inside one bar (main timeframe or `refine_tf` sub-bar) follows that path.

| Order type | Backtest behavior |
| --- | --- |
| Limit / market | Limit orders fill when touched, without slippage; market orders get adverse slippage from `bt_slippage_bps` |
| Stop-Limit entry (`Stop` + `Limit`) | Armed when price touches `Stop`, then fills at `Limit` or becomes a resting order; market stop entries also get slippage |
| Local stop-loss / take-profit (`ExgStopLoss` / `ExgTakeProfit` set to `false`) | No exchange conditional order is placed; a local price monitor triggers it, in both backtest and live |
| Stop-loss / take-profit | If both are hit within one bar, the stop-loss wins; stop fills are never better than the opening gap price |
| Multi-level take-profit (`TakeProfits`) | Each target is resolved from the average entry price and closed partially from nearest to farthest; once a level with `MoveSLToBreakeven` fires, the remaining stop-loss tightens to no worse than the entry average, and later levels continue on the rest of the same bar |
| Trailing stop (`CallbackPct` / `ActivationPrice`) | Tracking starts once price reaches the activation price; exit triggers on a `CallbackPct`% pullback from the best price, tagged `trailing_stop` |
| OCO group (`OCOGroup`) | Among pending entries with the same account, symbol, strategy and group name, the one triggered earliest on the bar's price path fills and the others are cancelled immediately, tagged `oco_cancel` |

See [custom strategies](./strat_custom.md) for field meaning and validation. Slippage applies only to market-style entries, stop/take-profit exits and forced exits; limit orders get none.

### Data gaps and delisting exits

Enabled when `bt_gap_bars > 0`: if the gap between two consecutive bars of a symbol exceeds both `bt_gap_bars` periods and `bt_gap_min_hours` hours (default 48), its open orders are force-closed with exit tag `data_gap`. Symbols whose data has ended are swept every 6 hours of simulated time and force-closed with tag `delist` at the last valid bar's close, shifted adversely by `bt_delist_penalty_bps`. Backtest only; live trading ignores it. These exits are caused by missing data and should not be read as strategy signals.

## Advanced Backtesting Features

### Hyper-Optimization

Hyper-Optimization can automatically find the optimal combination of strategy parameters. For details, please refer to the [Hyper-Optimization](./hyperopt.md) documentation.

### Rolling Backtesting

Rolling Backtesting is a more rigorous backtesting method that divides the data into multiple time windows, performing "training" (parameter optimization) and "testing" in each window to simulate the strategy's adaptability in different market environments. For details, please refer to the [Rolling Backtesting](./roll_btopt.md) documentation.

## Factor and cross-sectional engine

`bot backtest` loads one `run_policy` and dispatches time-series, factor or mixed engines. Factor replay mode comes from `--mode weights|events`, then `execution.mode`, then `events`. Root `bot research` emits matured labels/diagnostics. Weights is an approximate quantity book; mixed replay requires events, observable tick/event or 1m prices, instrument units and risk limits. Latest-value storage requires explicit data.pit_policy: static-approximation; strict PIT needs version archives/attested providers.

Ordinary factor backtests write `config.yml`, `resolved.json`, `events.jsonl`, `run.json`, and account-directory manifests plus event/posting Gob files. The default base is `<datadir>/backtest/factor`; collisions add numbered suffixes, and `--out` selects the base. Check the actual path printed in logs. `events.jsonl` contains panel/decision/diagnostics/summary records; `run.json` summarizes completion. Labels that have not matured at the data tail count as `Unresolved`; check status and unresolved counts even without an execution error. Time-series `orders.gob` alone cannot establish factor success.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).

## Strict replay and historical coverage

Ordinary backtests, including optimization trials, already use stable ordering. Within a bar, symbols follow a deterministic hash of time, symbol, and `bt_bar_order_seed` (default 0). Change the seed to examine capital-competition sensitivity. `bt_legacy_intrabar`, `bt_legacy_wallet_price` and `bt_legacy_order_metrics` only reproduce older backtest results. Stable ordering does not prove unbiased or lookahead-free results. `bt_strict: true` enables strict historical coverage checks; it is not required for deterministic ordering and does not freeze database contents. Strict historical replay also requires `bt_no_kline_download: true` and valid `historical_coverage`. Prepare missing data before replay.


`historical_coverage` records `baseline_end_ms`, optional `historical_result_end_ms`, and symbol/timeframe maps named `bars`, `physical_bars` and `listing_prefixes`. Ranges are `[start_ms, stop_ms)`. Baseline must follow the run start and be no later than its end; result end must be between baseline and run end. Rows physically present today are not automatically authorized coverage at a historical cutoff.

Strict reads validate the current task’s fields, ranges and coverage contract; this is not a database-wide read-only switch. Preparation, source bootstrap and replay are separate phases. Refreshing symbols/subscriptions prepares requirements and initializes loaders again; it must not bypass coverage authorization through implicit backfill. Factor providers also need archive-version and visibility/PIT evidence; K-line switches do not establish that evidence.

## Costs, equity, and checking results

These fields apply to time-series backtests. See [factor strategies](./factor.md) for factor costs; the two models are not interchangeable.

```yaml
bt_net_cost: 15
bt_slippage_bps: 5
bt_funding_apr: 10
bt_delist_penalty_bps: 20
bt_fresh_equity: true
charge_on_bomb: false
```

These values illustrate sensitivity experiments, not recommendations or market estimates. `bt_net_cost` is order latency in seconds (default 15), not a replacement for slippage. `bt_slippage_bps` is one-sided adverse slippage in basis points on market fills, stop/take-profit exits and forced exits; limit orders get none. `bt_delist_penalty_bps` adds adverse basis points beyond base slippage on delisting exits. Exchange configuration can override trading fees.

`bt_funding_apr` approximates annualized funding on linear contracts, in percent per year (`10` means 10%/year), symmetric for both sides: with a positive value longs pay and shorts receive, and a 1:1 hedge nets to zero; inverse contracts are skipped. When real funding-rate settlement records exist in the backtest data (stored table `funding_rate_8h`, or a replay stream), the real settlement points crossed during the holding period are charged instead and this approximation is not used. `bt_legacy_approx_funding: true` restores the old long-only charging, for reproducing old results only.

For the data-gap guard see "Data gaps and delisting exits" above, and inspect `gapExits` / `delistExits` and their absolute-profit shares. See the [configuration guide](./configuration.md) for the defaults of each `bt_*` key.

`bt_fresh_equity` defaults on: equity curves, daily equity, drawdown, and risk metrics use balances plus current open-order unrealized P&L. Spot base assets are already marked in wallet value, without adding contract P&L twice. Set false only to compare the older cached-P&L curve; bankruptcy detection still uses fresh equity. Record this setting and the data version when comparing builds.

With `charge_on_bomb: false`, margin liquidation or depleted equity ends normally and may exit 0. Final return is −100%, maximum drawdown 100%, later equity zero, and no further entries occur. `detail.json` may contain `liquidated`, `liquidated_at_ms`, `liquidated_symbol`, `liquidated_reason`, and `liquidated_cause` (`margin_liquidation` / `equity_depleted`). `charge_on_bomb: true` resets balances and continues, equivalent to additional capital; that is not a fixed-capital result.

Ordinary time-series reports include `detail.json`, order CSV/Gob, and asset/entry HTML files. `orders.csv` field `funding` is approximate funding and is excluded from `exitFee`. Failure to write final `detail.json` returns a nonzero error. Undefined floating-point metrics may serialize as JSON null, not zero. Factor/mixed outputs have their own status, NAV, and ledger fields; not all time-series fields apply.

### Metric sample units

Check units before comparing reports. `opt.BTResult.WinRatePct` is a percentage; live `winRate` is a 0–1 fraction. `CalcExpectancy` counts nonnegative samples as wins. Expectancy is win rate × average win minus loss rate × average absolute loss, equal to the arithmetic mean of the input samples. Its ratio is `(1 + avgWin/avgLoss) × winRate - 1`, returning 0 when there are no losses.

The live dashboard supplies daily `dayProfits`, so its expectancy is per daily sample, not per trade. `opt.BTResult` currently exports no expectancy field; live dashboard fields are not mandatory outputs of every backtest report. Factor weights, events and research IC/labels also have different units; see the [factor guide](./factor.md).

### Backtest Runtime

Ordinary entry points create a Runtime, clock, strategy jobs and account execution state for each task; Snapshot configuration is read-only. Development WebUI creates tasks through factories rather than switching package-level config/core/btime state between concurrent backtests. Embedded owners must request `Close` before `Join`; see [Runtime API](../api/runtime.md).
