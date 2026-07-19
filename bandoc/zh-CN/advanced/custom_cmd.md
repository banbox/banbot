除了使用banbot的[内置终端命令](../guide/bot_usage.md)，您也可以注册并执行自己的终端命令。

### 注册命令

Banbot 使用 Cobra。通过 `entry.AddCommand` 注册 `*cobra.Command`，命令专属参数直接保存在命令自己的局部变量中：

```go
var greeting string

helloCmd := &cobra.Command{
    Use:   "hello",
    Short: "显示问候语",
    Args:  cobra.NoArgs,
    RunE: func(cmd *cobra.Command, args []string) error {
        fmt.Println(greeting)
        return nil
    },
}
helloCmd.Flags().StringVar(&greeting, "greeting", "hello", "问候语")
entry.AddCommand("", helloCmd)
```

`AddCommand` 的第一个参数是父命令组；根命令使用空字符串。需要新命令组时，先调用 `entry.AddGroup`。帮助、参数校验、flag 解析和错误输出由 Cobra 统一处理。新增命令或参数不再需要修改 `config.CmdArgs` 或中央参数注册表。

## 内置工具命令

banbot提供了一些内置的工具命令，用于数据验证、分析和管理：

### K线数据一致性检查

检查实盘dump的K线数据正确性：

```bash
# 检查K线数据一致性
bot tool test_live_bars /path/to/dump/file.gob
```

输入的gob文件是执行实盘`bot trade -out @file.gob`时附加-out参数生成的。

执行此命令会将实盘dump的K线与本地数据库中的K线进行对比，检查实盘K线正确且无遗漏。

**输出示例：**
```
BTCUSDT_1m 2025-01-01 00:00:00 - 2025-01-01 23:59:00, live: [5, 10], local: [3, 8]
ETHUSDT_1m 2025-01-01 00:00:00 - 2025-01-01 23:59:00, live: [], local: [15]
total: 2880, pairs: 2
```

### 回测报告生成

从订单gob文件输出回测报告：

```bash
# 从订单文件生成回测报告
bot tool bt_result /path/to/orders.gob
```

**功能特性：**
- 支持从历史订单数据重新生成报告
- 可用于离线分析和报告生成
- 支持多种输出格式

### 读取数据生成K线图示例
您可以通过上面自定义终端命令，自由调用banbot提供的相关接口，实现各种各样的复杂任务，而不仅仅是内置的回测、交易等。

比如，您可以读取指定品种的K线，然后生成静态html的K线图：[参考代码](https://github.com/banbox/banstrats/tree/main/adv)


