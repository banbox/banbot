# Rolling optimization backtests

Rolling optimization searches parameters on a past window, backtests the next out-of-sample window, then advances and repeats. It checks a parameter-update procedure historically and cannot guarantee future returns. `bt-opt` and `tool test-pickers` currently support time-series `TradeStrat` only, not factor/mixed optimization.

## Configure windows and run

Set the complete interval, symbols, wallet, and policies in YAML. Declare search parameters through `pol.Def` / `pol.DefInt`. The interval must include the review period and at least one following test window; prepare warmup data too.

```bash
bot bt-opt --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker score
bot bt-opt --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker goodAvg
```

`--review-period` defaults to `3y`; `--run-period` defaults to `6M` and must be at least one hour. `M` means months and `m` means minutes. Periods use the program's duration conversion, not calendar-month scheduling. The first test begins one review duration after the configured start, and the last test window is capped at the configured end.

For a three-year interval with a one-year review and two-month run period, the first year is training only. Thereafter, every two months the preceding year is optimized again. Parameter selection uses only each window's past data; strategy logic and universe selection must also avoid future information.

`--sampler`, `--opt-rounds`, `--each-pairs`, `--concur`, and `--picker` match [hyperparameter optimization](./hyperopt.md). `bt-opt` also offers `--alpha` (default 1) for smoothing selected parameters between windows and `--pair-picker` for registered symbol selection. Set interval and symbols in YAML; this command does not accept `--timerange`, `--pairs`, or `--out`.

## Output and repeated runs

Output is under `<datadir>/backtest/bt_opt_<hash>/`; startup logs print the exact path:

| Path | Contents |
| --- | --- |
| `opt_<training-start-unix-seconds>.log` | Search log for each training window; concurrent runs may add numbered suffixes |
| `detail/` | Optimization candidate JSON details |
| `<picker>/` | Accumulated out-of-sample rolling backtest report |
| `config.yml` | Saved task configuration |

The rolling backtest carries wallet balances and accumulates completed orders and reports across windows; it does not simply add window return percentages. Existing usable optimization logs are reused. Preserve configuration, data, and logs for resumption. For an independent experiment, check the printed output path and isolate the data directory to avoid reusing old searches.

## Compare selection methods

```bash
bot tool test-pickers --datadir /path/to/data --config experiment.yml --review-period 12M --run-period 2M --opt-rounds 20 --picker score,good3,goodAvg
```

This reuses each training window's search and backtests the following window for each selected picker. `pickerScores.csv` and `pickerRanks.csv` in the output root record window scores and ranks. This is a per-window picker comparison, not a full cumulative report with capital carried separately for each picker. Default `--picker good3` tests one method; explicitly use `--picker ''` for all registered methods.

Evaluate holdout profit, drawdown, trade count, and consistency across windows. Comparing pickers also consumes holdout information; retain a final interval excluded from method selection. Strategies, data, and cost assumptions can change the outcome, so example chart returns are not expected results for your experiment.
