In addition to using the built-in terminal commands of banbot, you can also register and execute your own terminal commands.

### Registering Regular Functions
You can register terminal command functions using the `entry.AddCmdJob` method:
```go
entry.AddCmdJob(&entry.CmdJob{
    Name:    "hello",
    Parent:  "",
    Run: func(args *config.CmdArgs) *errs.Error {
        fmt.Println("hello")
        return nil
    },
    Options: []string{},
    Help:    "show hello",
})
```
You can then trigger the execution of the `showHello` function by typing `bot hello`. This function will accept a `*config.CmdArgs` parameter, which stores the parsed command-line arguments. If you need to access specific arguments, please fill in the required field names in the `Options` section.
All available field names can be found in the `bindSubFlags` function in [entry/main](https://github.com/banbox/banbot/blob/main/entry/main.go).

### Registering Functions with Arbitrary Arguments
Fixed `Options` and `*config.CmdArgs` might not meet your personalized needs.
You can also use `RunRaw` to replace `Run` and parse the command-line arguments yourself:
```go
entry.AddCmdJob(&entry.CmdJob{
    Name:   "hello",
    Parent: "",
    RunRaw: func(args []string) error {
        fmt.Println(strings.Join(args, " "))
        return nil
    },
    Help:    "show hello",
})
```

## Built-in Tool Commands

banbot provides several built-in tool commands for data validation, analysis, and management:

### K-line Data Consistency Check

Check the correctness of K-line data dumped from live trading:

```bash
# Check K-line data consistency
bot tool test_live_bars /path/to/dump/file.gob
```

The input gob file is generated when executing live trading with `bot trade -out @file.gob` by adding the -out parameter.

Executing this command will compare the K-line data dumped from live trading with the K-line data in the local database to check that the live K-line data is correct and complete.

**Output example:**
```
BTCUSDT_1m 2025-01-01 00:00:00 - 2025-01-01 23:59:00, live: [5, 10], local: [3, 8]
ETHUSDT_1m 2025-01-01 00:00:00 - 2025-01-01 23:59:00, live: [], local: [15]
total: 2880, pairs: 2
```

### Backtest Report Generation

Generate backtest reports from order gob files:

```bash
# Generate backtest report from order file
bot tool bt_result /path/to/orders.gob
```

**Features:**
- Support regenerating reports from historical order data
- Can be used for offline analysis and report generation
- Support multiple output formats

### Example of Reading Data to Generate a Candlestick Chart
By using the custom terminal commands described above, you can freely call the relevant interfaces provided by banbot to implement various complex tasks, not just the built-in backtesting, trading, etc.

For example, you can read the K-line data of a specified instrument and generate a static HTML candlestick chart: [demo](https://github.com/banbox/banstrats/tree/main/adv)
