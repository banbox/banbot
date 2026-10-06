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

## 独立调仓与持仓生命周期

在已有因子策略的 `portfolio` 中显式启用 `lifecycle-v1`，可分别控制调仓、选股、持仓、退出与资金分配。省略 policy 时继续使用原有 builder 和纯权重目标。

| 未指定的设置 | `lifecycle-v1` 默认值 |
| --- | --- |
| rebalance | every_bars: 1；日历默认时区 UTC，必须声明 calendar_version |
| selection | 省略整个 selection 时，启用的多/空侧沿用 params.k；missing_scores: skip |
| holding | 无最短/最长限制，无冷却；启用年龄限制后，缺首次成交证据的已有持仓须显式 adopt，否则拒绝 |
| transition | direct；linear-exit/geometric 的 on_reselect: restore，basis 须明确指定 |
| cohort | startup: gradual、sizing: entry-nav、entry_window_bars 等于 every_bars |
| allocation | equal，无现金预留及额外上限；显式 builder 且省略 allocation 时保留其权重 |

selection 的多/空 K 与分位在同侧互斥；分位选择不要求旧 params.k。missing_scores 可选 skip/shrink/cash，分别跳过不足样本的普通选择、缩小选择数或显式现金目标。retain_rank 提供排名保留缓冲；dropout 为每侧每轮允许退出的落选持仓数量，优先退出排名最差者。纯 research 不默认创建持仓账本：以下 policy 示例用于 weights/events 或具备证据的 live；普通 CLI research 请使用未启用 policy 的配置。嵌入 research 开启 policy 必须自行提供 `runner.PolicySink`。adopt 从当前网格开始计龄，不会还原未知的历史成交时间。

以下片段表示基础时钟 1h、每 2 个 bar 调仓；首次实际成交满 16 个 bar 后，排名落选的持仓分 8 个合格轮次减到零：

```yaml
run_policy:
  - name: momentum-vol
    id: rotation
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
    portfolio:
      long_notional: 1.0
      short_notional: 0.0
      policy: lifecycle-v1
      rebalance: {every_bars: 2}
      holding: {min_bars: 16}
      transition: {mode: linear-exit, exit_steps: 8, basis: quantity}
      allocation: {method: equal, reserve_ratio: 0.02}
```

这是已有市场/数据/账户配置的覆盖片段。`basis: quantity` 固定退出开始时的真实数量，后续净值上涨不会买回退出仓；`basis: weight` 按权重衰减并乘当轮净值。新增持仓按当前策略 NAV 配置，退出尾仓先占预算。目标接纳和成交分别记录，未成交减仓不能当成已释放现金。

如果需要“每两小时换一批、每批持有十六小时”，使用下列组合，替换上例的 holding/transition：

```yaml
rebalance: {every_bars: 2}
transition: {mode: cohort, period_bars: 16, startup: gradual, sizing: entry-nav}
```

`gradual` 首次投入一个批次的预算；`seed-all` 按当前排名一次创建所有初始批次。`entry-nav` 保持批次建仓数量，`current-nav` 每次重估活跃批次。cohort 按计划批次窗口到期，期限须为调仓间隔的整数倍。它与“满 16h 后才开始八次退出”具有不同持仓路径。

| 维度 | 内置能力 |
| --- | --- |
| rebalance | every_bars、anchor/phase、duration，日/周/月民用日历与时区/version |
| selection | long_k/short_k、long_quantile/short_quantile、retain_rank、dropout、group_quota、缺分数处理 |
| holding | min/max bars 或 duration、by_asset、cooldown_bars、显式接管 adopt |
| transition | direct、linear-exit、cohort、geometric、target-step，适用模式的重入及每资产规则 |
| allocation | equal、score、inverse-volatility、fixed-notional、vol-target，reserve、asset/group/net/beta cap、turnover_limit |

direct 在允许退出时直接归零；geometric 每轮乘 ratio，低于 final_threshold 后归零，两者参数须在 (0,1)。target-step 按 alpha 向新目标靠近，alpha 须在 (0,1]。linear-exit/geometric 的 on_reselect 可选 restore（恢复理想目标）、resume（保留当前退出水平，之后再落选时继续）、finish/new-cohort（先完成退出）；反向入场先等待归零。风险强制退出不受普通换手预算阻挡。

`holding.max_bars` 在第一个到期监控网格直接产生零目标，绕过普通调仓门和退出曲线；实际成交可能延迟。缺 score 不推进普通退出阶梯。持仓/在途的资产退出选股池后继续保留执行价格与 funding 订阅，归零且结算后才释放。

cohort 的入场窗口结束后不再补足原计划，迟到真实 fill 按原计划来源归属。同资产的批次贡献先内部转移再净额执行，不虚构成交或手续费。当前是聚合执行贡献，独立逐批 execution lot、严格逐批成交期限和逐批 PnL 属于后续扩展。

例如 `holding.by_asset: {'BTC/USDT:USDT': {min_bars: 24}}` 覆盖默认期限；显式零值表示取消该资产的相应限制。资产名使用数据 SIDMap 的统一 symbol。分组、波动率、beta 或自定义条件通过 Go `Config.PolicyContext` 提供当时可见数据；需要这些输入的规则在数据缺失时返回诊断或错误。

自定义 `RegisterPortfolioPolicy` 工厂为每策略创建独立实例，使用带版本的名称与 `policy_params`。原 `RegisterPortfolioBuilder` 继续生成理想目标；显式 builder 优先，未配置 allocation 时保留其权重。复杂交易日历与其他退出曲线可通过调度回调或完整 policy 实现。

`swapPerBars`/`holdBars` 未作为全局别名：使用明确的 every_bars、min/max_bars 或 period_bars。未知字段、非整数 bar、冲突/未使用字段及不支持的目标消费端会拒绝运行。数量目标输出为版本化 `allocation-decision`/`allocation-accepted`，不会隐式转回旧权重格式。

实施与边界见仓库 `doc/factor_opt_implementation.md`，多期限、稳健中性化、历史合成、参数扫描、模型与风险 builder 见 `doc/factor_research_extensions.md`。研究报告与高级插件提供原生 Go API；当前 CLI 不自动生成完整逐批成交归因。历史 IC 系列在 live 未接成熟历史 provider 前仍禁用。

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

`params.k` 是组合选股数量，`expressions.params.window` 是公式窗口，不会自动互相复制。表达式策略不需要注册同名 Go definition，不要同时指定 `definition`。组合可选 equal、fixed、history-ic、history-rank-ic、history-icir、history-rank-icir、history-ewma；历史方法只使用决策时已成熟可见的样本，当前实盘驱动拒绝全部 history 方法。

自定义 Go 策略通过 `runner.RegisterDefinition(name, builder)` 注册，builder 签名为 `func(runner.Config) (*factor.Plan, research.ComboSpec, error)`。自定义组合通过 `runner.RegisterPortfolioBuilder` 注册唯一版本名。字段节点、缺失策略和算子版本都应显式声明。详细接口见 [因子 API](../api/factor.md)。

## 稳健表达式与研究扩展

可在 outputs 中使用以下稳健截面处理和多暴露中性化：

```text
cs.mad_winsorize(kline.close, 3)
cs.robust_zscore(kline.close)
group.ols(factor.signal, style.size, style.beta)
group.wls(factor.signal, style.weight, style.size, style.beta)
group.demean(factor.signal, "style", "industry")
group.zscore(factor.signal, "style", "industry")
```

`factor.signal` 是已声明输出/中间因子，style 必须在 bindings 中声明；较慢暴露源使用 asof 与 max_age_ms。行业可来自原始字符串字段，保留 NULL。拟合只用当轮冻结 Reference 池，Evaluation 池不参与拟合。MAD 尺度为 1.4826 × 中位绝对偏差；尺度为零时稳健 zscore 返回零，winsorize 返回中位数，NULL 仍保留。OLS/WLS 含截距，WLS 权重须为正；共线暴露按声明顺序丢弃，样本不足返回 Warmup。

历史合成可通过 ComboSpec.Label 指定标签，MinSamples/MinPairs 分别限制历史截面数/有效资产对数。Direction 默认 signed；Fallback 默认 equal，也可 fixed/error；EWMA Decay 零值取 0.2。MinConfidence 未调整重叠收益，不能据此宣称独立显著性。

高级研究通过原生 Go API 使用，无需普通用户配置 ML 依赖。ScanPortfolioTrials 在 weights 模式隔离各试验账本并限制扫描数；SelectParameters/ResolveParameters 提供组/全局收缩与 PIT 参数产物。模型接口支持 ridge/OLS、滚动训练、purge/embargo、hash/PIT 发布恢复，风险接口支持对角/收缩协方差与投影优化。注册模型/风险 builder 后可在 portfolio.builder 选择版本名；无可见模型时跳过新目标，不可行风险结果不能当普通目标。

FactorRegistry、JSONL TrialLedger、样本外比较、随机基线和有界 LRU 帮助保留实验证据。生命周期、资本、capacity/funding 与归因报告消费调用者提供的可核验事件，当前 CLI 不自动生成完整逐批成交归因。没有自动线上训练平台或全局最优保证。完整合同见[研究 API](../api/factor.md#原生-go-研究扩展)，仓库详细示例位于 `doc/factor_research_extensions.md`。

## 历史回测与研究

```sh
./bot backtest --config base.yml --config factors.yml
./bot research --config base.yml --config factors.yml
./bot backtest --mode weights --config base.yml --config factors.yml
./bot backtest --mode events --config base.yml --config factors.yml
```

| 模式 | 适用范围 |
| --- | --- |
| research | 因子截面、成熟标签及诊断，无默认执行账户/持仓证据；CLI 配置省略 policy |
| weights | 数量保持、变化名义额成本及近似权重账本 |
| events | 共享账户、整数数量/价格约束、保证金/风险校验和 paper 成交 |

价格源与因子源独立；events 需要 tick/event 或显式 1m 可观察执行价格、每个 SID 的标准化 instrument 单位和账户风险限制。粗周期已完成 K 线不能还原区间内成交。执行价格严格晚于决策完成加 LatencyMS，且早于 exclusive expiry。`explicit-zero` 是明确忽略 funding 的假设；`required-stream` 必须提供所需标的的结算流，已知零费率也使用显式记录。

用归档时，设置 `archive`，并使 Universe、SIDMap、schemas、source versions 和价格流匹配实际文件。归档制作命令：

```sh
./bot data archive --input records.jsonl --out chunk.gob --max-records 100000
```

输入为 `factor.VersionRecord` JSON lines；整数宽度需要 `--schema fields.yml` 显式声明。所有原始修订保留；快照按 event time、available/published time 和本地接收门槛选取当轮可见版本。归档 `DecisionDelayMS` 调整可见性截止，不改变逻辑决策网格；实盘使用实际接收时间。

普通配置默认一个决策周期的 executable-return 标签。`research.labels` 可声明多个 horizon（毫秒），各期限独立捕获执行价、成熟和记录未完成标签，共享冻结 Frame 与有界队列。例如在策略中加入：

```yaml
research:
  labels:
    - {Name: 2h, Kind: executable-return, Horizon: 7200000, PeriodsPerYear: 4383}
    - {Name: 16h, Kind: executable-return, Horizon: 57600000, PeriodsPerYear: 547.875}
```

标签成熟后才进入 IC/诊断，未来收益不能进入当期推理。历史组合省略 Label 时，多标签选最短期限（同期限按名称排序）并纳入 manifest；单标签隐式身份保持兼容。`Result.Unresolved` 记录超出数据终点的标签。固定/等权纯交易可设置 `research: {labels: []}`，research 和全部 history 方法不能关闭标签。

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
