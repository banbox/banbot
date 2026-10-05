# 多因子与截面策略

Banbot 支持两种策略引擎：时序引擎 `time_series` 按品种驱动 `TradeStrat`，因子引擎 `factor` 在一个决策轮次冻结整个 Universe，计算因子、截面变换、组合分数并输出目标组合。两者使用同一份 YAML 和普通回测/交易入口，省略 `engine` 时仍选择时序引擎。

## 从数据到目标组合

因子图由字段、时序节点和截面节点组成。多列输出经等权、固定权重或历史 IC 组合后，由默认 top/bottom-k 或自定义 portfolio builder 生成 `TargetPortfolio`。Full 表示完整目标；Patch 只更新指定标的，省略的持仓不会隐式清零。

Universe 分别声明 investable、reference、tradable、evaluation 和 tracked 集合；参与截面估计不意味着允许下单。计算共享只发生于命名空间、时钟、采样、图和快照兼容的消费者，组合预算、目标和账户仍各自独立。

输入统一使用 `orm.DataSeries.Values map[string]any`，包含自定义列、具体类型和 NULL；缺失键和显式 NULL 不等价。时序指标的数值视图不会替换原始字段。


`backtest`/`trade`/`research` 默认从 `--datadir` 或 `BanDataDir` 找到市场基础配置。需要跳过默认文件时提供 `--no-default --config /absolute/config.yml`；`@`/`$` 配置路径仍要求数据目录。归档、输出和账户资源要求仍需满足。

## 统一命令

`banbot backtest` 和 `banbot trade` 加载一份 `run_policy`，按配置选择时序、因子或混合引擎路径，无需另选因子启动命令。回测模式依次取 `--mode weights|events`、`execution.mode`、默认 `events`；混合回放必须使用 `events`。纯时序任务保留已有回测行为。

因子诊断使用根命令 `research --config strategy.yml`，版本归档使用 `data archive --input records.jsonl --out chunk.gob`，独立表达式使用根命令 `validate --spec formula.yml` / `explain --spec formula.yml`。策略配置统一使用 YAML。

因子或混合配置的 `trade --dry-run` 启动历史 `events` 回放，不是实时行情模拟。纯时序的实时模拟继续使用 YAML `env: dry_run`，不要用历史回放标志启动该流程。真实因子或混合交易可传 `--live-provider`，且必须具备当前会话已验证的 binding。策略配置使用 YAML `run_policy`。

每个任务拥有显式 `runtime.Runtime`、配置快照、时钟、策略状态和取消上下文。运行态隔离不意味着交易所余额隔离：主动绑定同一账户的策略共享账户执行协调器，保留各自的持仓归因和资本预算。要求交易执行也互不影响的任务应使用不同账户及资源身份。取消时先停止接收，再等待在途回调和计算结束后释放资源；释放一个消费者不会停止仍在借用共享服务的其他消费者。

## 配置一个内置策略

以下是添加到已有数据库、交易所、账户、交易对池和 `time_range` 配置的覆盖文件，不是独立可运行的完整市场配置：

```yaml
data:
  pit_policy: static-approximation
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    id: momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
```

一个因子策略只接受一个决策周期。默认 24 期窗口至少需要 25 根已闭合小时观测才能形成有效动量。最新值数据库不能还原历史修订和发布时间，`static-approximation` 是明确的研究假设。严格 PIT 使用不可变版本归档或具有版本/可见性证明的历史 provider。

无需版本标记，普通加载不改写原文件。显式 `engine: time_series` 或 `engine: factor` 选择新引擎语义；未声明引擎的旧策略保留原自定义参数。`id` 是稳定策略身份，`account` 选择账户，`capital_weight` 分配同账户策略资本，不等同于 `stake_rate`。不要把非投资集合的参考资产分配成可交易持仓。

## 用表达式编写多因子

表达式在启动时编译为原生图，运行时不逐事件解释字符串。以下替换上面的 `run_policy`；两个命名输出共享底层字段和时序节点：

```yaml
run_policy:
  - name: MyFactors
    id: my_factors
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      outputs:
        momentum: 'cs.zscore(ts.return(positive(kline.close), param.window))'
        momentum_rank: 'cs.rank(ts.return(positive(kline.close), param.window))'
      combine: {method: equal}
```

`params.k` 是组合选股数量，`expressions.params.window` 是公式窗口，不会自动互相复制。表达式策略不需要注册同名 Go definition，不要同时指定 `definition`。可使用 fixed 组合；history-ic 只使用决策时已经成熟可见的历史样本，当前实盘驱动拒绝 history-ic。

自定义 Go 策略通过 `runner.RegisterDefinition(name, builder)` 注册，builder 签名为 `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`。自定义组合通过 `runner.RegisterPortfolioBuilder` 注册唯一版本名。字段节点、缺失策略和算子版本都应显式声明。详细接口见 [因子 API](../api/factor.md)。

## 历史回测与研究

```sh
./bot backtest --config base.yml --config factors.yml
./bot research --config base.yml --config factors.yml
./bot backtest --mode weights --config base.yml --config factors.yml
./bot backtest --mode events --config base.yml --config factors.yml
```

| 模式 | 适用范围 |
| --- | --- |
| research | 因子截面、成熟标签及诊断，不创建执行账户 |
| weights | 数量保持、变化名义额成本及近似权重账本 |
| events | 共享账户、整数数量/价格约束、保证金/风险校验和 paper 成交 |

价格源与因子源独立；events 需要 tick/event 或显式 1m 可观察执行价格、每个 SID 的标准化 instrument 单位和账户风险限制。粗周期已完成 K 线不能还原区间内成交。执行价格严格晚于决策完成加 LatencyMS，且早于 exclusive expiry。`explicit-zero` 是明确忽略 funding 的假设；`required-stream` 必须提供所需标的的结算流，已知零费率也使用显式记录。

用归档时，设置 `archive`，并使 Universe、SIDMap、schemas、source versions 和价格流匹配实际文件。归档制作命令：

```sh
./bot data archive --input records.jsonl --out chunk.gob --max-records 100000
```

输入为 `factor.VersionRecord` JSON lines；整数宽度需要 `--schema fields.yml` 显式声明。所有原始修订保留；快照按 event time、available/published time 和本地接收门槛选取当轮可见版本。归档 `DecisionDelayMS` 调整可见性截止，不改变逻辑决策网格；实盘使用实际接收时间。

普通配置默认一个决策周期的 executable-return 标签。`research.labels` 可覆盖；当前归档驱动只支持一个 executable-return 周期。标签成熟后才进入 IC/诊断，未来收益绝不能进入当期推理。`Result.Unresolved` 记录超出数据终点的标签。固定/等权纯交易可设置 `research: {labels: []}`，research/history-ic 不能关闭标签。

## 时序与截面混合

同一 `run_policy` 可放入时序和因子策略。混合历史回放要求 `execution.mode: events`；配置中每个策略设置稳定 id、account，同账户多个策略显式声明 capital_weight，并让每个因子策略都具备所需价格/资金费率/风险证据：

```yaml
execution: {mode: events, funding_policy: explicit-zero}
run_policy:
  - name: YourRegisteredTS
    engine: time_series
    id: ts_alpha
    account: default
    capital_weight: 0.5
    run_timeframes: [1h]
  - name: momentum-vol
    engine: factor
    id: cs_alpha
    account: default
    capital_weight: 0.5
    run_timeframes: [1h]
    params: {window: 24, k: 3}
```

`YourRegisteredTS` 必须替换为项目中实际注册的时序策略。账户服务保留各策略归属和发送分配，净额执行不意味着合并策略状态。不同账户隔离；Stop/Join 只释放自己的借用，不能关闭其他策略仍使用的账户或计算 Session。

## 实盘接入

verified-session 是用户工厂的示例注册名，不是内置 provider。先注册 entry.RegisterFactorLiveBinding("verified-session", factory)，factory 必须实现真实会话验证；省略 provider 或 banexg 走内置适配，缺能力仍拒绝启动。

provider 选择顺序为显式 CLI 参数、`accounts.<账户名>.live_provider`、`execution.live_provider`、内置默认。账户覆盖直接放在根 `accounts`，可为不同账户选择各自已注册的 binding；启动在打开会话前验证所有 provider。

真实实盘配置移除 archive，使用 `execution.live_provider: verified-session`，以 `./bot trade --config live.yml` 启动。嵌入程序必须通过 `entry.RegisterFactorLiveBinding` 接入当前会话已验证的 banexg transport、标准 symbol metadata、publication/revision 映射和 funding-policy 验证。

**当前普通 banexg 会话缺少完整 verified binding 时会明确拒绝启动，不能仅凭 YAML 获得真实截面交易能力，也不会降级为 paper。本文没有宣称真实交易所端到端已验收。** 干运行使用独立的 `trade --dry-run` 历史 paper 回放，不是 verified live 的自动替代。

启动先编译数据需求与决策计划，安装历史 warmup 和当前源订阅，对账账户后提交代际。轮次只接受闭合/当轮可见记录；barrier 达成或超时后 Flush，缺失数据按节点政策处理。候选代失败保留原代，取消时先 Stop 再 Join，等待回调/计算完成才释放共享资源。

预算来自对账后的账户，InitialNAV 不向真实账户充值。已有现金/仓位需要 ledger 归属和启动对账。required-stream 实盘记录包含精确十进制字符串 `mark`、`rate`、`account_amount` 和稳定 `settlement_id`；重复结算幂等，迟到且跨持仓变更的结算需要历史对账。`explicit-zero` 需要会话确证没有 funding 义务。

## 读取结果与排障

因子引擎输出 JSON lines：panel、decision、成熟 diagnostics 和最终标量 summary。普通回测另写 resolved.json（最终默认值和配置来源）、account-&lt;account&gt;/manifest.json 与版本化 event/posting Gob 块。成功必须在输出 sync/close 与资源 cleanup 之后判定，失败保留主错误和清理错误。

大型模拟可用 `execution.history: cold/history.sqlite` 归档已结算记录；必须新路径，不是恢复快照，真实 trade 和 durable store 不接受。小回测默认 MemoryStore；page_bytes 限制解码逻辑载荷，不是进程 RSS 上限。

遇到启动失败先检查引擎选择、单周期、定义注册、Universe/SID/schema、价格周期、funding 政策和账户单位。预检本身不创建交易所、存储、账户或输出目录。`bot tool bt_factor` 仍是 orders.gob 的旧滚动筛选工具，不是新 factor runner。

继续阅读：[配置](./configuration.md)、[回测](./backtest.md)、[实盘](./live_trading.md)、[自定义数据](./custom_data.md)。

## 完整的普通存储回测配置

下面是一份配置结构完整的 weights 回测示例，可保存为 factors.yml。数据库 URL、市场和交易对是待替换的本地示例；执行前需要实际可读数据库、已导入行情和对应 metadata。无资源预检只能证明配置与静态装配合法，不能证明数据库内容或行情供应能力。

```yaml
time_start: '20240101'
time_end: '20240201'
exchange: {name: binance}
market_type: linear
pairs: ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']
stake_currency: [USDT]
wallet_amounts: {USDT: 10000}
database:
  db_type: questdb
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
  auto_create: false
data:
  pit_policy: static-approximation
  page_rows: 1000
  max_records: 100000
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    id: momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 1}
```

```sh
./bot backtest --config factors.yml
```

用表达式时将 run_policy 替换为前面的多列示例，保持其余根配置。events 与实盘需要额外账户风险、单位和 verified capability；不能直接把 mode 改成 events/live 即认为就绪。
