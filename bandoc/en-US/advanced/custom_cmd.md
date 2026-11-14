# Custom terminal commands

Besides [built-in commands](../guide/bot_usage.md), strategy applications can register Cobra commands. Keep command flags in local closure variables; no changes to `config.CmdArgs` or a central flag registry are needed.

## Register and run

This is a minimal entry point. When integrating into a strategy project, retain its strategy registrations and add command registration before `entry.RunCmd()`:

```go
package main

import (
    "fmt"

    "github.com/banbox/banbot/entry"
    "github.com/spf13/cobra"
)

func main() {
    entry.AddGroup("custom", "custom tools")
    entry.AddCommandFactory("custom", func() *cobra.Command {
        var greeting string
        cmd := &cobra.Command{
            Use:   "hello",
            Short: "Print a greeting",
            Args:  cobra.NoArgs,
            RunE: func(cmd *cobra.Command, args []string) error {
                _, err := fmt.Fprintln(cmd.OutOrStdout(), greeting)
                return err
            },
        }
        cmd.Flags().StringVar(&greeting, "greeting", "hello", "greeting text")
        return cmd
    })
    entry.RunCmd()
}
```

```bash
go build -o bot
./bot custom hello --greeting "hello BanBot"
./bot custom hello --help
```

An empty parent registers at the root; register new groups with `entry.AddGroup` first. `AddCommand(parent, *cobra.Command)` suits applications building one tree. For repeated or concurrent `entry.NewRootCommand()` calls, use `AddCommandFactory` and return a fresh command with fresh flag variables each time. One Cobra command cannot attach to two trees.

Custom commands do not automatically inherit built-in configuration flags. Declare needed `--config` / `--datadir` flags and call `config.LoadRunSpec`; do not treat unparsed arguments as effective configuration or mutate configuration globals for a local flag. Propagate cancellation with `cmd.Context()` and return errors to the entry point.

## Compare live bars with local data

```bash
bot trade --datadir /path/to/data --out /path/to/live-bars.gob
bot tool test-live-bars --datadir /path/to/data /path/to/live-bars.gob
```

The `trade` command above requires a complete runnable Hub configuration; see [execution](../guide/execution.md). This tool takes one positional `DUMP_FILE` and compares a time-series live bar dump with the configured market's local storage. It needs the matching exchange, market, and database. It is neither a generic Gob validator nor proof that a strategy avoids lookahead.

## Rebuild a report from orders

```bash
bot tool bt-result --datadir /path/to/data --config report-config.yml --in /path/to/orders.gob --out /path/to/rebuilt
```

`bt-result` uses `--in`, not a positional file argument. Supply configuration, market, wallet, and required historical data matching the original orders to generate reports such as `assets.html`, `enters.html`, `detail.json`, and `orders.csv`. It does not guarantee offline operation without database/market data and does not rebuild factor `events.jsonl` / account-ledger reports.

See [entry API](../api/entry.md) for extension and cancellation interfaces and [opt API](../api/opt.md) for backtest dependencies.
