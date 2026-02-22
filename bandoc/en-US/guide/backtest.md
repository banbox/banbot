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

After the backtest is complete, you can find the detailed report in the `backtests` directory within `BanDataDir`.

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
