# entry Package

The entry package provides system entry points and command-line interface.

Configuration-based commands parse a `config.Snapshot`, open explicit storage and exchange dependencies, and create a `runtime.Process` and `runtime.Runtime` for that execution. Backtests, live runs, and optimizations obtain their own configuration, clock, strategies, orders, and lifecycle from the Runtime; callers should close and join it when the task finishes. A small number of maintenance APIs still use compatibility paths.

## Public Methods

### RunCmd
This is the command-line entry method for banbot. You can call this method in your strategy project's entry file to access various banbot subcommands from the terminal.
