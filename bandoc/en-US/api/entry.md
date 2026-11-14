# entry package

Strategy applications use `entry` to build their CLI. See the [command guide](../guide/bot_usage.md) for ordinary use and [custom commands](../advanced/custom_cmd.md) for extensions.

## Entry points and registration

| API | Purpose |
| --- | --- |
| `RunCmd()` | Process entry: arguments, signals, and error exit handling |
| `ExecuteContext(ctx context.Context, args []string) error` | Embedded execution; arguments exclude the executable name, and errors return to the caller |
| `NewRootCommand() *cobra.Command` | Build a fresh Cobra tree |
| `AddGroup(name, help string)` | Register a parent group |
| `AddCommand(parent string, command *cobra.Command)` | Register a command; empty parent means root |
| `AddCommandFactory(parent string, factory CommandFactory)` | Create fresh commands for repeated/concurrent tree construction |

`type CommandFactory func() *cobra.Command`. Register groups and commands before creating a root or calling `RunCmd`. Each command owns its flags and local variables. Reusing one `*cobra.Command` for a second tree fails; use a factory instead.

`ExecuteContext` accepts canonical double-dash long options and normalizes legacy single-dash long options. No subcommand starts development Web; passing only ordinary options may also select implicit Web. Integrations should specify a subcommand, such as `[]string{"backtest", "--datadir", dataDir}`, and check the returned error.

## Unified strategy dispatch

`config.LoadRunSpec` loads configuration first. `backtest` / `trade` select time-series, factor, or mixed execution from `run_policy[].engine`. The time-series subset in `RuntimeSnapshot` cannot replace the full policy list.

| Command/option | Contract |
| --- | --- |
| `backtest --mode weights\|events` | Override factor `execution.mode`; otherwise YAML, then default `events` |
| Mixed `backtest` | Requires events; `--separate` supports pure time-series only |
| Pure time-series `trade` | `env: dry_run` for Hub live simulation; factor-only `--dry-run` / `--live-provider` are rejected |
| Factor/mixed `trade --dry-run` | Historical-input events replay |
| Real factor/mixed `trade` | Requires verified live bindings; insufficient capabilities fail rather than fall back to paper |
| `research` | Factor research with mature labels and diagnostics, using the same YAML loading rules |
| `optimize` / `bt-opt` | Time-series optimization, not factor-runner hyperparameter search |

`ValidateBacktestRunSpec(spec *config.RunSpec) error` shares resource-free preflight with execution. `InspectBacktestRunSpec` provides Web preflight information; successful preflight does not prove database coverage or venue readiness. `RegisterFactorLiveBinding` registers custom live-binding factories; an unregistered configured name fails. See [factor API](./factor.md).

## Resources and cancellation

Configuration entries assemble task-owned Process/Runtime, storage, exchange, clock, and order dependencies. Embedded callers propagate cancellation and wait for execution to return so output flushing and cleanup finish. Release directly owned Runtimes with `Close` followed by `Join`. Do not switch concurrent tasks by mutating `config` / `core` globals.

Consumers of the same account may share execution and strategy attribution; releasing one borrower must not stop other consumers. See [Runtime API](./runtime.md) for ownership contracts.

Default live entry, including pure time-series, uses the shared Hub with `prod` / `dry_run` linear perpetual accounts and complete execution declarations. `test` / spot / inverse, legacy dynamic leverage, and `fatal_stop` are unsupported on this path; missing Hub never falls back to a local sender. See [shared accounts and Hub](../guide/execution.md) for deployment and recovery.
