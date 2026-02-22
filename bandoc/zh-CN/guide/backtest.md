# BanBot 回测指南

回测（Backtesting）是在历史数据上模拟运行交易策略，是进行策略优化和评估的重要步骤。BanBot回测引擎有下面特点：

- 事件驱动回测引擎，杜绝未来函数，确保回测结果的可靠性。
- 支持任意复杂度的时间序列策略，无需修改用于实盘。
- 高性能指标库banta，更快的回测速度。
- 灵活组合不同品种、策略和时间周期；

## 快速开始

### 从WebUI回测
banbot提供了用于回测研究的WebUI，您可以在WebUI中直接编写策略、配置参数并运行回测。

只需编译您的golang项目为可执行文件，然后启动即可运行WebUI：
```bash
go build -o bot
./bot web
```

WebUI顶部有[策略]/[回测]/[数据]/[实盘交易]四个选项。

您可先在[策略]中编写策略，如果您想要使新修改或添加的策略生效，您需要在右上角点击[编译]按钮（刷新图标）。

然后在[回测]中配置参数并运行回测。

回测结束后您可进入回测报告页面，查看详细的回测报告。

### 从命令行回测
在`BanDataDir`环境变量指向目录下打开 `config.local.yml`（或`config.yml`），对重要的回测参数进行配置：

```yaml
market_type: linear
leverage: 10
time_start: "20251101"
time_end: "20251112"
stake_amount: 30
pairs: [BTC, ETH]
stake_currency: [USDT]
wallet_amounts:
  USDT: 1000
run_policy:
  - name: ma:demo
    run_timeframes: [1h]
```

然后编译您的golang项目为可执行文件，并运行回测命令：
```bash
go build -o bot
./bot backtest
```

回测完成后，你可以在`BanDataDir`中的 `backtests` 目录下找到详细的报告。

## 回测注意事项

### 订单撮合周期 (`refine_tf`)

默认情况下，BanBot 在策略的 K 线周期（`run_timeframes`）上撮合订单。例如，如果策略在 `1h` 周期上运行，订单的成交价将基于 `1h` K 线进行撮合。然而，这在某些场景下可能与实盘不符，例如：

- **高频波动**：K线价格剧烈波动时，可能同时满足止盈和止损，单个K线丢失了内部细节，banbot为避免偏差，会优先触发止损。
- **回撤分析**：无法观察到在一个大周期 K 线内部的更细粒度的资金回撤情况。

为了解决这个问题，BanBot 引入了 `refine_tf` 配置项，允许在更小的时间周期上进行订单撮合，以模拟更真实的市场行为。

在 `config.yml` 的 `run_policy` 中为指定的 `job` 添加 `refine_tf`：

```yaml
run_policy:
  - name: ma:demo
    run_timeframes: [1h]
    refine_tf: "1m" # 或 4, "3-6"
```

`refine_tf` 支持多种格式：

- **固定周期 (字符串)**: 如 `"1m"`, `"5m"`。直接指定一个精确的撮合周期。
- **相对倍数 (整数)**: 如 `4`。表示将 `run_timeframes` 周期进行 N 等分。例如，`run_timeframes` 为 `1h`，`refine_tf` 为 `4`，则撮合周期为 `15m`。
- **相对倍数范围 (字符串)**: 如 `"3-6"`。程序会自动在范围内选择一个常见的周期。例如，`run_timeframes` 为 `1d`，`refine_tf` 为 `"3-6"`，则会选择 `4h` (24h / 6) 作为撮合周期。

## 高级回测功能

### 超参数优化

超参数优化（Hyper-Optimization）可以自动寻找策略参数的最优组合。详情请参阅 [超参数优化](./hyperopt.md) 文档。

### 滚动回测

滚动回测（Rolling Backtesting）是一种更严谨的回测方法，它将数据分为多个时间窗口，在每个窗口上进行“训练”（参数优化）和“测试”，以模拟策略在不同市场环境下的适应性。详情请参阅 [滚动回测](./roll_btopt.md) 文档。

