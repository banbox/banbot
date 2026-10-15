# entry Package

The entry package provides system entry points and command-line interface.

Configuration-based commands parse a `config.Snapshot`, open explicit storage and exchange dependencies, and create a `runtime.Process` and `runtime.Runtime` for that execution. Backtests, live runs, and optimizations obtain their own configuration, clock, strategies, orders, and lifecycle from the Runtime; callers should close and join it when the task finishes. A small number of maintenance APIs still use compatibility paths.

## Public Methods

### RunCmd
This is the command-line entry method for banbot. You can call this method in your strategy project's entry file to access various banbot subcommands from the terminal.

## Factor-engine integration

ValidateBacktestRunSpec and execution share resource-free checks; mixed replay requires events. RegisterFactorLiveBinding supplies verified current-session evidence.

Root `backtest` and `trade` use the same configuration loader and dispatch time-series, factor or mixed `run_policy` entries. `--mode` overrides factor-engine backtest `execution.mode` (default `events`); `trade --dry-run` is historical replay for factor/mixed runs, while time-series live simulation uses `env: dry_run`. Root `research` uses the same YAML loader, default files, `--datadir` and `--no-default` rules. Root `validate --spec`, `explain --spec` and `data archive` handle standalone expressions and version-data archives.

Explicit task Runtime state is separate from its cancellation context. Multiple tasks retain their own configuration, clock, strategies and orders; same-account engine consumers deliberately share account execution and strategy attribution. Stop intake and join work before releasing resources, including on cancellation or startup failure. A shared account borrow must not shut down other consumers.

[Factor API](factor.md) / [Guide](../guide/factor.md)
