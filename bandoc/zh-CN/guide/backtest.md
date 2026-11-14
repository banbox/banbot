# BanBot 回测指南

回测（Backtesting）是在历史数据上模拟运行交易策略，是进行策略优化和评估的重要步骤。BanBot回测引擎有下面特点：

- 事件驱动历史回放；策略逻辑、数据可见性与品种选择仍需自行避免未来信息。
- 支持任意复杂度的时间序列策略，无需修改用于实盘。
- 高性能指标库banta，更快的回测速度。
- 灵活组合不同品种、策略和时间周期；

## 快速开始

### 从WebUI回测
banbot提供了用于回测研究的WebUI，您可以在WebUI中直接编写策略、配置参数并运行回测。

只需编译您的golang项目为可执行文件，然后启动即可运行WebUI：
```bash
go build -o bot
./bot web --datadir /path/to/data
```

WebUI顶部有[策略]/[回测]/[数据]/[实盘交易]四个选项。

您可先在[策略]中编写策略，如果您想要使新修改或添加的策略生效，您需要在右上角点击[编译]按钮（刷新图标）。

然后在[回测]中配置参数并运行回测。

回测结束后您可进入回测报告页面，查看详细的回测报告。

### 从命令行回测
在`BanDataDir`环境变量指向目录下打开 `config.local.yml`（或`config.yml`），对重要的回测参数进行配置：

```yaml
exchange: {name: binance}
market_type: linear
contract_type: swap
leverage: 10
time_start: "20251101"
time_end: "20251112"
stake_amount: 30
pairs: [BTC, ETH]
stake_currency: [USDT]
wallet_amounts:
  USDT: 1000
run_policy:
  - engine: time_series
    name: ma:demo
    run_timeframes: [1h]
```

这是回测配置片段；`ma:demo` 必须是程序中已注册的策略，数据库和其他公共设置仍需准备。上面的杠杆值仅是示例，应按实验目标设置。不要将此片段当作 Hub 实时配置，实盘需按[执行指南](./execution.md)单独准备。然后编译并运行：
```bash
go build -o bot
./bot backtest --datadir /path/to/data --out /path/to/report
```

回测完成后，纯时序默认报告在 `<datadir>/backtest/<配置哈希>/`，可用 `--out` 指定目录。配置分层见[配置指南](./configuration.md)。

```bash
bot backtest --datadir /path/to/data --config experiment.yml --timerange 20250101-20250401 --pairs BTC/USDT:USDT
bot backtest --datadir /path/to/data --config experiment.yml --separate
```

`--separate` 仅用于把纯时序的策略条目分别回测；默认一起运行形成组合报告。还可使用 `--timestart` / `--timeend`（后者要求前者）、`--stake-amount`、`--prg` 和 `--bt-strict`；以 `bot backtest --help` 为准，不支持的其他子命令参数不能混用。

## 数据准备与下载限制

普通时序回测可按需补齐 K 线并写入数据库，需可用的数据源和网络。先准备策略周期、`refine_tf` 及预热区间的数据。`bt_no_kline_download: true` 可独立使用，禁止回测隐式下载，静态筛选/周期打分也改用库内数据；它单独开启时可能只使用已有的不完整数据，不是完整性证明。`--net-off` 也不能代替覆盖检查。

更改品种、周期、字段或区间后重新核对数据。未知上市时间的品种在非严格模式可能仅告警；库内已有数据不能证明该数据在历史决策时已可见。严格历史合同见下文。

## 回测注意事项

### 订单撮合周期 (`refine_tf`)

默认情况下，BanBot 在策略的 K 线周期（`run_timeframes`）上撮合订单。例如，如果策略在 `1h` 周期上运行，订单的成交价将基于 `1h` K 线进行撮合。然而，这在某些场景下可能与实盘不符，例如：

- **高频波动**：K线价格剧烈波动时，可能同时满足止盈和止损，单个K线丢失了内部细节，banbot为避免偏差，会优先触发止损。
- **回撤分析**：无法观察到在一个大周期 K 线内部的更细粒度的资金回撤情况。

为了解决这个问题，BanBot 引入了 `refine_tf` 配置项，允许在更小的时间周期上进行订单撮合，以模拟更真实的市场行为。

在 `config.yml` 的 `run_policy` 中为指定的 `job` 添加 `refine_tf`：

```yaml
run_policy:
  - engine: time_series
    name: ma:demo
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

`bot backtest` 加载一份 `run_policy` 并调度时序、因子或混合引擎。因子回放模式依次取 `--mode weights|events`、`execution.mode`、默认 `events`。根命令 `bot research` 输出成熟标签和诊断。纯因子 weights 是近似数量账本，混合回放必须 events；events 需要 tick/event 或 1m 可观察价格、标准 instrument 单位及风险限制。最新值数据库需显式 data.pit_policy: static-approximation；严格 PIT 需要版本归档/受验证 provider。

因子普通回测输出 `config.yml`、`resolved.json`、`events.jsonl`、`run.json`，以及账户目录的 manifest 和 event/posting Gob。默认根目录为 `<datadir>/backtest/factor`，输出冲突时加编号后缀，也可用 `--out` 选择根路径；以日志打印的实际目录为准。`events.jsonl` 包含 panel/decision/diagnostics/summary 等记录；`run.json` 汇总完成状态。数据尾部标签尚未成熟会计入 `Unresolved`，即使命令未报执行错误也应核对状态和未完成数量。不能仅使用时序 `orders.gob` 判断因子结果。

参见[多因子与截面指南](./factor.md)和[因子 API](../api/factor.md)。

## 严格回放与历史覆盖

普通回测（包括优化试验）已采用稳定执行顺序，同一 bar 内多品种使用由时间、品种和 `bt_bar_order_seed`（默认 0）决定的哈希顺序。修改 seed 可检查资金竞争下的顺序敏感性；稳定顺序本身不证明无偏或无未来信息。`bt_strict: true` 用于严格历史覆盖校验，不是开启确定性顺序的必要条件，也不会冻结数据库内容。回测任务同时配置 `bt_no_kline_download: true` 和有效的 `historical_coverage`，才启用带覆盖合同的严格历史回放。缺失数据应在运行前准备，不能依赖回放时隐式下载。

`historical_coverage` 记录 `baseline_end_ms`、可选 `historical_result_end_ms`，以及按标的/周期分组的 `bars`、`physical_bars`、`listing_prefixes`。区间使用 `[start_ms, stop_ms)`；baseline 必须晚于回测起点且不晚于终点，result end 位于 baseline 与回测终点之间。不能把当前物理表所有数据直接视为历史时点已授权的覆盖。

严格读取按当前任务的字段、区间和覆盖合同校验，不是整个数据库的只读开关。数据预备、source bootstrap 和回放是不同阶段。刷新品种/订阅会重新准备需求并初始化 loader；不能借刷新绕过授权覆盖去隐式补数据。因子 provider 还必须满足归档版本与可见性/PIT 合同，K 线的开关不能代替因子数据证明。

## 成本、权益与结果检查

以下为时序回测字段；因子成本见[因子指南](./factor.md)，两套费用模型不能混用。

```yaml
bt_net_cost: 15
bt_slippage_bps: 5
bt_funding_apr: 10
bt_delist_penalty_bps: 20
bt_fresh_equity: true
charge_on_bomb: false
```

这些成本值仅是敏感性实验示例，不是推荐或真实市场估计。`bt_net_cost` 是秒级下单延迟，不能代替滑点；`bt_slippage_bps` 为市价/止损成交的单边不利滑点，单位 bp。`bt_funding_apr` 是多头合约的近似年化资金费百分比，不能代替交易所真实历史结算。`bt_delist_penalty_bps` 是退市强平时在基础滑点之外的额外不利 bp；费用可由交易所配置覆盖。

用 `bt_gap_bars` / `bt_gap_min_hours` 配置数据空洞守卫，并检查 `gapExits` / `delistExits` 及对应绝对利润占比。空洞和退市可能按最后有效 bar 强平，不能把因缺数据造成的退出当作策略信号。默认值及完整字段见配置模板。

`bt_fresh_equity` 默认开启：权益曲线、日权益、回撤和风险指标使用余额加未平仓订单当下浮盈。现货 base 币已在钱包市值中，不重复加合约浮盈。设为 false 仅用于对比旧缓存浮盈曲线；破产判定仍使用新鲜权益。比较不同构建的报告时应记录此设置和数据版本。

`charge_on_bomb: false` 时，账户强平或权益耗尽正常结束回测，退出码可为 0；最终收益为 −100%、最大回撤 100%、后续权益为 0，不再开仓。`detail.json` 可出现 `liquidated`、`liquidated_at_ms`、`liquidated_symbol`、`liquidated_reason` 和 `liquidated_cause`（`margin_liquidation` / `equity_depleted`）。`charge_on_bomb: true` 会重置余额继续，相当于追加本金；不可把这种报告当作固定本金结果。

普通时序报告包含 `detail.json`、订单 CSV/Gob 和资产/入场 HTML 等。`orders.csv` 的 `funding` 是近似资金费，`exitFee` 不包含它。写不出最终 `detail.json` 会返回非零错误；无效浮点指标可能为 JSON null，不能视为 0。因子/混合结果使用它们自己的状态、NAV 和账本字段，不能套用所有时序字段。

### 指标的采样单位

比较报告时先检查指标单位。`opt.BTResult.WinRatePct` 是百分数，实盘统计接口的 `winRate` 是 0–1 比例。`CalcExpectancy` 将非负样本计为胜：期望值为胜率×平均盈利−败率×平均亏损绝对值，等于输入样本的算术平均；期望比率为 `(1 + avgWin/avgLoss) × winRate - 1`，无亏损样本时返回 0。

实盘面板传入的是每日收益 `dayProfits`，因此该期望值按日采样，不能标为每笔交易收益。当前 `opt.BTResult` 没有导出 expectancy 字段；不要把实盘面板字段当作所有回测报告的固定输出。因子 weights、events 和研究 IC/标签的结果单位也不同，见[因子指南](./factor.md)。

### 回测 Runtime

普通入口为每次任务创建独立 Runtime、时钟、策略作业及账户执行状态；配置 Snapshot 只读。开发 WebUI 通过工厂创建任务，不应以包级 config/core/btime 状态切换并行回测。嵌入程序须按资源所有权执行 `Close` 后 `Join`；见 [Runtime API](../api/runtime.md)。
