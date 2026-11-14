# Hyperparameter optimization

`bot optimize` searches parameter ranges declared with `pol.Def` / `pol.DefInt` by time-series `TradeStrat` strategies. Factor and mixed hyperparameter search is not implemented here; run separate explicit parameter configurations for [factor research/backtests](./factor.md).

## Run a search

Compile and register the strategy, then configure `run_policy`, symbols, a historical interval, and a backtest wallet. Prepare training and warmup data coverage, and create the output file's parent directory:

```bash
mkdir -p /path/to/results
bot optimize --datadir /path/to/data --config experiment.yml --out /path/to/results/opt.log --opt-rounds 30 --sampler bayes --picker good3
```

`--out` is a required log file, not a report directory. Currently omitting it only logs a warning and returns; that does not prove a search ran. See [configuration](./configuration.md) for layering.

| Flag | Meaning and default |
| --- | --- |
| `--opt-rounds` | Rounds per optimization task, default 30; convergence or search-space limits may end a search earlier |
| `--sampler` | `bayes` (default), `tpe`, `random`, `cmaes`, `ipop-cmaes`, `bipop-cmaes` |
| `--picker` | Result selection method, default `good3` |
| `--each-pairs` | Search separately for each symbol |
| `--concur` | Concurrent tasks, default 1; multiple policy groups can run in child processes of the compiled executable, rather than parallel trials inside one group |
| `--bt-strict` | Override strict backtest settings; see [backtesting](./backtest.md) for coverage requirements |

Optimization does not bind `--timerange` / `--pairs`; set the training interval and symbols in YAML or `--config-data`. Empty `run_policy[].dirt` runs both directions together, while `long` / `short` restrict direction. `any` searches long, short, and combined cases separately and may produce multiple policy entries.

## Read and apply results

Logs contain parameters, `loss`, order count, profit, drawdown, and other metrics. `loss = -score`: lower loss means higher score, and score is not profit percentage. Selected YAML policy fragments print to standard output. Review them, write the complete `run_policy` list to a separate overlay, and run `bot backtest` on data excluded from the search.

Sequential search writes the `--out` file. Multiple concurrent policy groups write `<out>.1`, `<out>.2`, etc. Trial details are JSON in the sibling `detail/` directory. A search log is not a complete ordinary backtest report.

Re-sort a file or collect a directory:

```bash
bot tool collect-opt --datadir /path/to/data --config experiment.yml --in /path/to/results/opt.log
bot tool collect-opt --datadir /path/to/data --config experiment.yml --in /path/to/results --picker good3
```

File input only sorts and rewrites that file. Directory input recursively collects files ending in `.log` and prints selected policy YAML. Concurrent files ending in `.log.1`, etc. are not automatically collected by that scan. This tool is not read-only; retain copies of logs that need archiving.

## Pickers and overfitting

Candidates are ordered by score descending. `good*` uses nonnegative-score candidates, falling back to negative-score candidates when none are nonnegative; it does not guarantee profit.

| Picker | Behavior |
| --- | --- |
| `score` | Select the highest-scoring candidate |
| `good2` / `good3` / `good4` / `good5` / `good7` | Select approximately the 20% / 30% / 40% / 50% / 70% position |
| `goodAvg` | Average parameters across the entire candidate sequence |
| `good0t3` / `good1t4` / `good2t5` | Average parameters over the 0–30% / 10–40% / 20–50% interval |
| `good3t7` / `good0t7` / `good3t10` | Average parameters over the 30–70% / 0–70% / 30–100% interval |

Averaged parameters may never have been evaluated as a group; backtest them again. Default `good3` is not a promise of universally better performance. Compare out-of-sample profit, drawdown, and trade count rather than selecting only the highest training score.

Compare rolling windows with `bot tool test-pickers --picker score,good3,goodAvg`. The default tests only `good3`; explicitly pass `--picker ''` to test all registered pickers. See [rolling backtests](./roll_btopt.md) for windows and output. Repeatedly selecting methods on the same holdout can also overfit.
