# Using the bot CLI

Run `go build -o bot` in your strategy project root (`go build -o bot.exe` on Windows) to compile registered strategies and BanBot into one executable. Here `bot` denotes that binary; use `./bot` when running from the current directory.

```bash
bot --help
bot backtest --help
bot tool --help
```

Commands use hyphens, such as `bt-opt` and `collect-opt`. Some underscore names remain compatibility aliases; use canonical names and double-dash `--flag` options in new scripts. Each subcommand declares its own flags; flags from one command are not automatically supported by another.

## Configuration and common flags

Configuration commands normally load data-directory `config.yml`, then `config.local.yml`, repeated `--config` files, inline `--config-data`, and explicit CLI overrides. `--datadir` takes precedence over `BanDataDir`. Missing default files are skipped; `--no-default` disables them. Explicit-file loading can work without a data directory, but storage and output may still require one. See [configuration](./configuration.md).

```bash
bot backtest --datadir /path/to/data --config experiment.yml
bot backtest --datadir /path/to/data --no-default --config experiment.yml
```

Common flags include `--logfile`, `--level`, and `--max-pool-size`; diagnostic flags include `--cpu-profile`, `--mem-profile`, and `--net-off`. Supporting commands also expose `--dlock`. `--net-off` neither validates data coverage nor supplies missing history.

## Development WebUI and Hub

```bash
bot web --datadir /path/to/data
bot hub --datadir /path/to/data
```

Running `bot` without a subcommand also opens the development WebUI, at `http://127.0.0.1:8000` by default. Development Web has its own `--host`, `--port`, `--password`, and `--tz` flags; it is separate from the trading Dashboard configured through `api_server`.

`hub` (compatibility alias `spider`) provides public market data and local account execution. `spider_addr` controls public market-data listening, default `127.0.0.1:6789`. The private execution socket defaults to `<datadir>/execution/hub.sock`, overridable with `--execution-socket`. Hub accepts only `prod` / `dry_run`; backtests run in process. A public listening address does not grant private account authorization.

## Trading, backtesting, and research

```bash
bot trade --datadir /path/to/data
bot backtest --datadir /path/to/data --out /path/to/report
bot backtest --datadir /path/to/data --separate
bot research --datadir /path/to/data --config factor.yml
```

`backtest` / `trade` dispatch time-series, factor, or mixed policies using `run_policy[].engine`. Omitting `engine` retains legacy time-series semantics; declare it explicitly in new configurations.

Default live time-series, factor, and mixed entries all use the shared Hub. Start `hub` first, then use `env: dry_run` for Hub paper live simulation or `env: prod` for production. The default entry requires linear perpetuals, an explicit settlement currency, and complete execution declarations; it does not support `test`, spot, or inverse contracts. Adapter support for testnets/spot does not make them supported by ordinary `trade`. `--spider` cannot replace the private execution Hub; a missing Hub fails without falling back to a local sender. See [shared accounts and Hub](./execution.md) for configuration, limitations, and recovery. The trading Dashboard separately needs `api_server` users and account roles; see [live trading](./live_trading.md).

For factor backtests, `--mode weights|events` overrides root `execution.mode`, defaulting to `events` when both are absent. Mixed backtests require `events`. Factor/mixed `trade --dry-run` replays historical events; it is not the time-series live-simulation switch. Real factor/mixed trading can select `--live-provider`; missing verified session capabilities fail preflight without falling back to paper execution.

`--separate` supports pure time-series backtests only; split factor tasks into separate configurations. See [backtesting](./backtest.md), [optimization](./hyperopt.md), and [rolling backtests](./roll_btopt.md). `optimize` / `bt-opt` are time-series tools, not factor/mixed hyperparameter search.

## Data and maintenance tools

Check `bot <group> <command> --help` for input formats. Unless a positional argument is explicitly declared, files use `--in` and output uses `--out`.

| Command | Purpose |
| --- | --- |
| `data export` / `data import` | Export/import database data as protobuf files |
| `data archive` | Versioned archives; see the [factor guide](./factor.md) |
| `kline down` / `load` / `agg` | Download, import, or aggregate bars |
| `kline export` / `purge` | Export CSV / delete matching bars |
| `kline repair-ranges` / `verify` / `correct` | Rebuild range metadata, verify coverage, synchronize timeframes |
| `kline adj-calc` / `adj-export` | Calculate/export adjustment factors |
| `series down` / `list` | Download registered custom series / inspect ranges |
| `tick convert` / `to-kline` | Convert ticks / aggregate bars |
| `tool list-strats` | List strategies compiled into the binary |
| `tool collect-opt` / `test-pickers` | Collect optimization results / compare pickers over rolling windows |
| `tool sim-bt` / `bt-result` | Simulate from a report / rebuild a report from orders |
| `tool test-live-bars DUMP_FILE` | Compare live bar dumps with local data |
| `tool cmp-orders` | Compare venue and backtest orders |
| `tool calc-perfs` / `corr` / `merge-assets` | Performance metrics, correlation matrices, combined asset curves |
| `tool load-cal` / `data-server` | Import calendars / serve gRPC data |
| `tool bt-factor` | Rolling symbol selection over existing `orders.gob`, separate from native factor runner |
| `live down-order` / `close-order` | Download account orders / close by account, symbol, or strategy |

```bash
bot live down-order --datadir /path/to/data --account user1 --timestart 20250401 --timeend 20250405 --pairs BTC/USDT:USDT
bot tool bt-result --datadir /path/to/data --config report-config.yml --in /path/to/orders.gob --out /path/to/rebuilt
```

Empty `--account`, `--pair`, or `--strat` on `live close-order` leaves that dimension unrestricted; `--exg` closes venue positions directly. Select the scope explicitly before execution. Custom factors for `tool bt-factor` register in `opt.RegisterFactor`, not the nonexistent `strat.FactorMap`.

Standalone expressions use root `bot validate --spec formula.yml` / `bot explain --spec formula.yml`. See the [factor guide](./factor.md) for configuration and data requirements.
