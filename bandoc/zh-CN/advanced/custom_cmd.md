# 自定义终端命令

除[内置命令](../guide/bot_usage.md)外，策略应用可以注册 Cobra 命令。命令参数保存在闭包的局部变量中，无需修改 `config.CmdArgs` 或中央注册表。

## 注册并执行

下面是最小入口；合入现有策略项目时保留原有策略注册，只在 `entry.RunCmd()` 前添加命令注册：

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

父命令组为空字符串时注册到根；新组须先调用 `entry.AddGroup`。`AddCommand(parent, *cobra.Command)` 适合仅构造一棵命令树的应用。重复或并发构造 `entry.NewRootCommand()` 时使用 `AddCommandFactory`，每次返回全新命令和参数变量；同一个 Cobra 命令不能挂到两棵树。

自定义命令不会自动获得内置配置参数。需要 YAML 时自行声明 `--config` / `--datadir` 等并调用 `config.LoadRunSpec`，不要把未解析的参数当成已生效配置，也不要为了单个命令参数改写配置全局变量。使用 `cmd.Context()` 传递取消，返回错误交由入口处理。

## 实盘 K 线一致性检查

```bash
bot trade --datadir /path/to/data --out /path/to/live-bars.gob
bot tool test-live-bars --datadir /path/to/data /path/to/live-bars.gob
```

上面的 `trade` 命令以完整可运行的 Hub 配置为前提，见[执行指南](../guide/execution.md)。此工具接收一个位置参数 `DUMP_FILE`，将时序实盘 dump 的 K 线与配置市场的本地存储比较。需要同一交易所、市场及可用数据库；它不是任意 Gob 文件的格式验证器，也不能证明策略无未来信息。

## 从订单重建报告

```bash
bot tool bt-result --datadir /path/to/data --config report-config.yml --in /path/to/orders.gob --out /path/to/rebuilt
```

`bt-result` 使用 `--in`，不接受位置文件参数。提供与原订单一致的配置、市场、钱包及所需历史数据，可生成 `assets.html`、`enters.html`、`detail.json`、`orders.csv` 等报告。它不是保证无需数据库/行情的离线工具，也不用于重建因子 `events.jsonl` / 账户账本报告。

其他扩展接口和取消上下文见 [entry API](../api/entry.md)，回测依赖见 [opt API](../api/opt.md)。
