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

回测完成后，未指定 `-out` 时可在 `BanDataDir/backtest/<配置哈希>` 中找到详细报告；也可通过 `-out` 指定输出目录。

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

## 多因子与截面引擎

普通 bot backtest 按 engine 装配。bot factor research 输出成熟标签和诊断；factor backtest --mode weights|events 提供因子专用入口。纯因子 weights 是近似数量账本，混合回放必须 events；events 需要 tick/event 或 1m 可观察价格、标准 instrument 单位及风险限制。最新值数据库需显式 data.pit_policy: static-approximation；严格 PIT 需要版本归档/受验证 provider。

因子 JSON lines 输出 panel/decision/diagnostics/summary，普通回测另写 resolved.json 和 account-&lt;account&gt;/manifest.json、event/posting Gob。不能仅使用时序 orders.gob 判断因子结果；Result.Unresolved 保留超出数据尾部的标签。完成必须等待输出关闭与资源清理。

参见[多因子与截面指南](./factor.md)和[因子 API](../api/factor.md)。

## 严格回放与历史覆盖

`bt_strict: true` 固定关键执行顺序；它本身不冻结数据库内容，也没有通用的固定耗时比例。回测任务同时配置 `bt_no_kline_download: true` 和有效的 `historical_coverage`，才启用带覆盖合同的严格历史回放。缺失数据应在运行前准备，不能依赖回放时隐式下载。

`historical_coverage` 记录 `baseline_end_ms`、可选 `historical_result_end_ms`，以及按标的/周期分组的 `bars`、`physical_bars`、`listing_prefixes`。区间使用 `[start_ms, stop_ms)`；baseline 必须晚于回测起点且不晚于终点，result end 位于 baseline 与回测终点之间。不能把当前物理表所有数据直接视为历史时点已授权的覆盖。

严格读取按当前任务的字段、区间和覆盖合同校验，不是整个数据库的只读开关。数据预备、source bootstrap 和回放是不同阶段。刷新品种/订阅会重新准备需求并初始化 loader；不能借刷新绕过授权覆盖去隐式补数据。因子 provider 还必须满足归档版本与可见性/PIT 合同，K 线的开关不能代替因子数据证明。

### 检查数据计划

内部命令 `bot internal inspect-data-plan --request request.json --output output.json` 用于集成工具检查编译策略的数据需求。request 必须符合 `runtimeplan.RequestV1`：包括配置、市场快照/Universe、初始标的、时间范围和编译/源码/配置哈希。它不是接受普通 config.yml 的下载或回测命令；使用工具生成的完整请求，不要手填伪哈希。output 文件必须预先存在，命令截断后写入；发现不支持的需求时可能写出诊断并返回错误。Inspect 使用局部状态，不安装进程 globals。检查通过只证明计划检查，不证明数据库覆盖或真实交易所可用。

### 指标的采样单位

比较报告时先检查指标单位。`opt.BTResult.WinRatePct` 是百分数，实盘统计接口的 `winRate` 是 0–1 比例。`CalcExpectancy` 将非负样本计为胜：期望值为胜率×平均盈利−败率×平均亏损绝对值，等于输入样本的算术平均；期望比率为 `(1 + avgWin/avgLoss) × winRate - 1`，无亏损样本时返回 0。

实盘面板传入的是每日收益 `dayProfits`，因此该期望值按日采样，不能标为每笔交易收益。当前 `opt.BTResult` 没有导出 expectancy 字段；不要把实盘面板字段当作所有回测报告的固定输出。因子 weights、events 和研究 IC/标签的结果单位也不同，见[因子指南](./factor.md)。

### 回测 Runtime

普通入口为每次任务创建独立 Runtime、时钟、策略作业及账户执行状态；配置 Snapshot 只读。开发 WebUI 通过工厂创建任务，不应以包级 config/core/btime 状态切换并行回测。嵌入程序须按资源所有权执行 `Close` 后 `Join`；见 [Runtime API](../api/runtime.md)。
