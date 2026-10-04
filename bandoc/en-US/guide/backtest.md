# BanBot Backtesting Guide

Backtesting is the process of simulating a trading strategy on historical data. It is an important step for strategy optimization and evaluation. The BanBot backtesting engine has the following features:

- Event-driven backtesting engine to prevent lookahead bias and ensure the reliability of backtesting results.
- Supports time-series strategies of any complexity, without modification for live trading.
- High-performance indicator library `banta` for faster backtesting speed.
- Flexible combination of different trading pairs, strategies, and timeframes.

## Quick Start

### Backtesting from the WebUI
BanBot provides a WebUI for backtesting research. You can write strategies, configure parameters, and run backtests directly in the WebUI.

Just compile your Go project into an executable file and start it to run the WebUI:
```bash
go build -o bot
./bot web
```

The WebUI has four options at the top: [Strategy]/[Backtest]/[Data]/[Live Trading].

You can first write a strategy in [Strategy]. If you want your new or modified strategy to take effect, you need to click the [Compile] button (refresh icon) in the upper right corner.

Then, configure the parameters and run the backtest in [Backtest].

After the backtest is finished, you can go to the backtest report page to view the detailed backtest report.

### Backtesting from the Command Line
Open `config.local.yml` (or `config.yml`) in the directory pointed to by the `BanDataDir` environment variable, and configure the important backtesting parameters:

```yaml
market_type: linear
leverage: 10
time_start: "20251101"
time_end: "20251112"
stake_amount: 30
pairs: [BTC, ETH]
stake_currency: [USDT]
wallet_amounts:
  USDT: 1000
run_policy:
  - name: ma:demo
    run_timeframes: [1h]
```

Then compile your Go project into an executable file and run the backtest command:
```bash
go build -o bot
./bot backtest
```

After the backtest is complete, when `-out` is not specified, you can find the detailed report in `BanDataDir/backtest/<config-hash>`. You can also use `-out` to specify the output directory.

## Backtesting Notes

### Order Matching Timeframe (`refine_tf`)

By default, BanBot matches orders on the strategy's K-line timeframe (`run_timeframes`). For example, if the strategy runs on a `1h` timeframe, the order's execution price will be matched based on the `1h` K-line. However, this may not be consistent with live trading in some scenarios, for example:

- **High-frequency volatility**: When the K-line price fluctuates violently, it may meet both take-profit and stop-loss conditions at the same time. A single K-line loses internal details. To avoid deviation, BanBot will trigger the stop-loss first.
- **Drawdown analysis**: It is impossible to observe the more granular capital drawdown within a large-period K-line.

To solve this problem, BanBot introduces the `refine_tf` configuration item, which allows order matching on a smaller timeframe to simulate more realistic market behavior.

Add `refine_tf` for the specified `job` in the `run_policy` of `config.yml`:

```yaml
run_policy:
  - name: ma:demo
    run_timeframes: [1h]
    refine_tf: "1m" # or 4, "3-6"
```

`refine_tf` supports multiple formats:

- **Fixed timeframe (string)**: Such as `"1m"`, `"5m"`. Directly specify a precise matching timeframe.
- **Relative multiple (integer)**: Such as `4`. It means that the `run_timeframes` period is divided into N equal parts. For example, if `run_timeframes` is `1h` and `refine_tf` is `4`, the matching timeframe is `15m`.
- **Relative multiple range (string)**: Such as `"3-6"`. The program will automatically select a common timeframe within the range. For example, if `run_timeframes` is `1d` and `refine_tf` is `"3-6"`, it will choose `4h` (24h / 6) as the matching timeframe.

## Advanced Backtesting Features

### Hyper-Optimization

Hyper-Optimization can automatically find the optimal combination of strategy parameters. For details, please refer to the [Hyper-Optimization](./hyperopt.md) documentation.

### Rolling Backtesting

Rolling Backtesting is a more rigorous backtesting method that divides the data into multiple time windows, performing "training" (parameter optimization) and "testing" in each window to simulate the strategy's adaptability in different market environments. For details, please refer to the [Rolling Backtesting](./roll_btopt.md) documentation.

## Factor and cross-sectional engine

Ordinary bot backtest dispatches by engine. bot factor research emits matured labels/diagnostics; factor backtest --mode weights|events is the dedicated replay entry. Weights is an approximate quantity book; mixed replay requires events, observable tick/event or 1m prices, instrument units and risk limits. Latest-value storage requires explicit data.pit_policy: static-approximation; strict PIT needs version archives/attested providers.

Factor JSON lines contain panels/decisions/diagnostics/summaries. Ordinary replay adds resolved.json and account-&lt;account&gt;/manifest.json plus event/posting Gob. Legacy orders.gob alone is not the factor report. Result.Unresolved retains labels beyond available history. Completion follows output closure and resource cleanup.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).

## Strict replay and historical coverage

`bt_strict: true` canonicalizes key execution order. It does not freeze database contents or imply a fixed overhead percentage. A backtest also needs `bt_no_kline_download: true` and valid `historical_coverage` to enable strict historical replay with a coverage contract. Prepare missing data before replay instead of relying on implicit downloads.

`historical_coverage` records `baseline_end_ms`, optional `historical_result_end_ms`, and symbol/timeframe maps named `bars`, `physical_bars` and `listing_prefixes`. Ranges are `[start_ms, stop_ms)`. Baseline must follow the run start and be no later than its end; result end must be between baseline and run end. Rows physically present today are not automatically authorized coverage at a historical cutoff.

Strict reads validate the current task’s fields, ranges and coverage contract; this is not a database-wide read-only switch. Preparation, source bootstrap and replay are separate phases. Refreshing symbols/subscriptions prepares requirements and initializes loaders again; it must not bypass coverage authorization through implicit backfill. Factor providers also need archive-version and visibility/PIT evidence; K-line switches do not establish that evidence.

### Inspecting a data plan

The internal command `bot internal inspect-data-plan --request request.json --output output.json` lets integration tools inspect a compiled strategy’s data requirements. The request must follow `runtimeplan.RequestV1`, including configuration, market snapshot/Universe, initial symbols, time range and compilation/source/configuration hashes. It does not accept ordinary config.yml as a download or backtest request. Use a complete generated request instead of placeholder hashes. The output file must already exist and is truncated before writing. Unsupported requirements can produce diagnostics and an error exit. Inspect uses local state without installing globals. Passing inspection proves plan inspection, not database coverage or venue readiness.

### Metric sample units

Check units before comparing reports. `opt.BTResult.WinRatePct` is a percentage; live `winRate` is a 0–1 fraction. `CalcExpectancy` counts nonnegative samples as wins. Expectancy is win rate × average win minus loss rate × average absolute loss, equal to the arithmetic mean of the input samples. Its ratio is `(1 + avgWin/avgLoss) × winRate - 1`, returning 0 when there are no losses.

The live dashboard supplies daily `dayProfits`, so its expectancy is per daily sample, not per trade. `opt.BTResult` currently exports no expectancy field; live dashboard fields are not mandatory outputs of every backtest report. Factor weights, events and research IC/labels also have different units; see the [factor guide](./factor.md).

### Backtest Runtime

Ordinary entry points create a Runtime, clock, strategy jobs and account execution state for each task; Snapshot configuration is read-only. Development WebUI creates tasks through factories rather than switching package-level config/core/btime state between concurrent backtests. Embedded owners must request `Close` before `Join`; see [Runtime API](../api/runtime.md).
