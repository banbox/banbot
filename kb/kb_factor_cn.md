# banbot 截面因子 / 多因子策略知识库

范围：`engine: factor` 的截面因子、多因子合成、组合构建、研究、回放、实盘、结算。前提：已读时序策略文档（DataSeries/Kline、banexg 类型、通用配置、banta），本文不重复；配对统计套利见统计套利文档。基准 banbot v0.7.1。

## 0. 总览

入口：跨品种排名/中性化/多因子/统一调仓 = `engine: factor` + YAML `expressions`，或 Go `runner.RegisterFactor`（最简：一个 Go 因子定义 + 默认 top/bottom-K）/`RegisterDefinition`；自定义理想权重 = `runner.RegisterPortfolioBuilder` / YAML `portfolio.builder`；自定义完整组合策略（调仓/持仓/退出/状态）= `runner.RegisterPortfolioPolicy("name-vN", f)` / YAML `portfolio.policy`。

- 因子策略不返回 `*strat.TradeStrat`、不逐品种下单；runner 每决策周期生成目标组合交账户执行。
- 一个 `run_policy` 条目 = 一个因子策略 = 一个决策周期（`run_timeframes` 多值报 `one decision timeframe is required`）。
- `engine` 缺省 `time_series`，因子条目必须写 `engine: factor`。可选根键 `config_version: 1|2`；为 2 时无 `engine` 的条目视为 time_series，其它 engine 值拒绝。
- 含因子策略的账户有多个策略时，每个都须显式 `capital_weight`（0–1，总和 ≤1，账户预算份额，非时序 `stake_rate`）；单策略未写占满账户；纯时序账户不受限。
- `id` 全局唯一（跨账户重复也报 `duplicate strategy id`）；缺省用 `name`；`name` 必填。`account` 缺省：存在 `default` 账户用它，否则按名字排序第一个未 `no_trade` 账户，都无则 `default`。
- 旧写法 `factor: {...}` 仍可读（键提升到 `run_policy` 同层，同名冲突报 `X is also set in factor`；要求 `engine: factor`）。

## 1. YAML 配置

### 1.1 `run_policy` 条目键（engine: factor）

| 键 | 含义/默认 |
| --- | --- |
| `name` | 无 `definition`/`expressions` 时作 Go 定义名（`MomentumVol`→`momentum-vol`；未注册报 `unregistered definition`），否则仅展示名 |
| `engine` `id` `account` `capital_weight` | 见第 0 节；`capital_weight` 同比缩小 `initial_nav` |
| `run_timeframes` | 决策周期，如 `[1h]`；缺省取根级 `run_timeframes`，再缺省 `1h`；决定 `DecisionInterval`、默认标签 |
| `params` | `{name: float}`；`window`、`k` 须整数；其它键由 Go `Params()` 读；全部进入 manifest.parameters（改参=新身份）。`swapPerBars`/`holdBars` 报错（用 `portfolio.rebalance`/`holding`/`transition`） |
| `archive` | 归档 `.gob` 路径；相对路径相对声明它的配置文件，`$`/`@` 开头相对 DataDir；与 `chunks` 同写时 `archive` 生效 |
| `chunks` | `[{path, from, to}]`，`from/to` 毫秒，0=取档内 EventTime 范围；按 `max(EventTime, AvailableAt)`（价格/资金费等接收型源再取 IngestedAt）落入 `[from,to]` 的批次重放；多 chunk 须严格递增不重叠 |
| `definition` | 已注册 Go 定义名；与 `expressions`/`Plan` 互斥（显式写出则报 `mutually exclusive`） |
| `prices` | `{source, timeframe, field}` 成交/估值价格流，见 1.7 |
| `funding_source` | 资金费流 source 名（`required-stream` 必填；存储回放订阅 `{funding_source, event, rate}`） |
| `option_expiry_source` `terminal_settlement_source` | 期权到期 / 终局结算流 source，见 7.2 |
| `pit_min_universe_ratio` `pit_delist_penalty_bps` | 回测 PIT 参数：`pit_min_universe_ratio`（≤1，0=默认 0.5，<0 关闭 PIT 缩减）为一轮内至少保留的 Investable 比例，分母是上一次被接受决策时的存活数（滚动，首个决策用名义池）；`pit_delist_penalty_bps`（有限数，0=默认 100，<0 关）为被剔持仓平仓时额外的不利惩罚 bp，数据恢复则退回并改按缺口前后更差价计价 |
| `initial_nav` | 策略初始 NAV（正数）；缺省 `wallet_amounts[manifest.currency]`，无则 10000；再乘 `capital_weight` |
| `max_records` | 每批最大记录数，缺省 100000；被 `data.max_records` 覆盖 |
| `expressions` `combo` `decision` `research` `portfolio` `manifest` `snapshot` | 见第 2 节、1.4–1.6、4.2、1.9 |
| `pairs` `time_range` `stake_currency` | 同通用配置；存储回放中策略级 `pairs` 决定静态标的池 |

校验行为：
- 根级出现（忽略大小写/下划线）`artifact_path definition expressions computation_context mode chunks max_records max_pending decision_interval decision_delay_ms latency_ms expiry_ms label_wait_ms snapshot factor combo manifest strategy_id account_id initial_nav account_initial_nav prices funding_source` 报 `not a unified YAML configuration field`；条目内 `config` 报 `unknown advanced field`。
- 条目内**未识别的键被静默忽略**（含 `artifact_path`、拼错的键、条目顶层的 `holding`/`transition`/`rebalance`——只能在 `portfolio` 下）；只有 `data/execution/accounts` 与 `decision/portfolio/research/manifest/snapshot/combo/prices/expressions` 等高级块内未知键才报 `unknown advanced field`，且值不得为 null。
- 整数字段写浮点（`k: 2.5`）报错；高级块的正整数/非负数/枚举加载时校验。

### 1.2 `data` / `execution` / `accounts`

| 位置 | 键 |
| --- | --- |
| `data` | `namespace page_rows prefetch_rows page_bytes archive max_records pit_policy` |
| `execution` | `mode store history sender_lease_dir hub_socket live_provider funding_policy protection_mode order_mode near_limit_bps paper_fill_model instruments margin_rate max_account_margin max_virtual_gross strategy_gross_limit` |
| `accounts.<name>` | 与 `execution` 相同的全部键，逐键覆盖全局；`execution.accounts.<name>` 是别名（与 `accounts.<name>` 同键冲突报错；账户须在根 `accounts` 声明，否则 `unknown account`；同账户多策略的纸面 currency/costs/风险限额/store/lease/history/fill model 须一致） |

`data`：
- `page_rows` 默认 20000；`prefetch_rows` 默认各存储策略 `max_records/2` 的最小值；`page_bytes` 默认 0（不限）；存储回放要求 `max_records ≥ prefetch + 流数`；`archive` 为未写策略级 `archive/chunks` 时的全局归档（`@`/`$` 相对 DataDir）。
- 归档驱动下 `data` 仅允许 `max_records/archive/pit_policy`，其余报 `archive driver cannot apply a storage subscription override`。
- `pit_policy`：归档取 `available-at`（默认）或 `strict`，**仅校验，不改变快照可见性标签**（标签由 `snapshot.visibility_policy` 定，默认 `available-at`）；存储回放必须显式 `static-approximation`，`strict` 直接报错。

`execution`：
- `mode`：回测只接受 `weights`/`events`（缺省 `events`；CLI `--mode` 覆盖；混合时序/因子必须 events）。
- `funding_policy`：`explicit-zero`（显式忽略资金费）或 `required-stream`（需 `funding_source`），写入 `manifest.costs.funding_policy`；归档回放未声明时档内有 `funding` source 则自动 `required-stream`+`funding_source=funding`，否则须声明 `explicit-zero`；存储回放必须显式声明。
- `paper_fill_model`：`midpoint`（默认）、`bid-ask`、`mid-entry-market-exit`；仅 Events；同账户策略须一致。`midpoint` 取 `bid`/`ask`（缺省=price）中点向下取整到 `price_tick`，须在买卖价内，故无价差时价格须是 `price_tick` 整数倍（否则 `no quote-compatible midpoint tick`）；`bid-ask` 类须有可见有限的 `bid`/`ask`，缺失则拒绝。
- `order_mode`：`local`（默认）|`exchange_first`；`protection_mode`：`software`|`native_preferred`（缺省仅 `exchange_first` 偏向 native）；`near_limit_bps`∈[0,10000]，0 视为 10。
- `store` + `sender_lease_dir`：Events 持久化纸面账本（SQLite），必须成对；`history` 见 7.3；`live_provider`、`hub_socket` 见 7.4。
- 风险字段为**绝对结算币金额**（非杠杆倍数）：`margin_rate`∈(0,1]、`max_account_margin`、`max_virtual_gross`（账户敞口上限）、`strategy_gross_limit`（策略敞口上限）（非负 decimal）。Events 归档回放这四项与 `instruments` 必须显式（缺则 `paper execution requires instrument units and risk limits`）；Events 存储回放缺省：`margin_rate=1/leverage`（根 `leverage`，缺省 1）、`max_account_margin=账户NAV`、`max_virtual_gross=账户NAV×leverage`、`strategy_gross_limit=策略NAV×leverage`，`instruments` 由交易所市场元数据推导（结算币须等于 `manifest.currency`）。
- `instruments`：键为数字 SID，值 `id, version, valuation(linear_perpetual|cash_european_option), settlement_currency, quantity_step, contract_size, price_tick, money_scale(0–18), min_steps(≥0), min_notional(≥0)`，期权加 `option: {underlying_id, right(call|put), strike, expiry_ms, underlying_stress_max}`（均须正；`underlying_id` 不得等于自身 `id`）。`id` 须等于该 SID 在 `snapshot.sid_map` 的名字（归档默认 `sid:<N>`，存储为交易所符号）且互不重复，`settlement_currency` 须等于 `manifest.currency`；Events/实盘中 Investable 与 Tracked 的每个 SID 都须有单位。

Events 归档回放最小示例：`execution: {mode: events, funding_policy: explicit-zero, margin_rate: 0.1, max_account_margin: 10000, max_virtual_gross: 20000, strategy_gross_limit: 20000, instruments: {1: {id: "sid:1", version: v1, valuation: linear_perpetual, settlement_currency: USDT, quantity_step: '0.01', contract_size: '1', price_tick: '0.01', money_scale: 8}}}`（每 SID 一行，decimal 写字符串；档内价格须为 0.01 整数倍）。

### 1.3 `decision`

`interval_ms`（决策间隔，正数，缺省由 `run_timeframes` 推出；与表达式 `timeframe` 不一致报错）、`delay_ms`（默认 0，数据可见截止向后推移，≥0）、`latency_ms`（默认 1，决策完成到可成交价的延迟，须 >0）、`expiry_ms`（默认 60000，目标有效期，须 >latency_ms）、`max_pending`（默认 64，待处理提案上限，正整数）、`source_event_decisions`（默认 false，见 1.6）、`source`/`timeframe`（仅 `source_event_decisions: true` 时必填，否则出现即报错）。

### 1.4 `combo`（与 `expressions.combine` 同字段）

`method`（`equal|fixed|history-ic|history-rank-ic|history-icir|history-rank-icir|history-ewma`）、`columns`、`weights`（map，`fixed` 必填且覆盖全部 columns）、`label`、`min_samples`、`min_pairs`、`min_confidence`、`decay`（EWMA alpha，[0,1]，0 视为 0.2）、`direction`（`signed|positive`）、`fallback`（`equal|fixed|error`）。

- `columns` 不得重复且须是输出名；**`expressions.combine` 可省 columns（默认全部输出），外层 `combo` 必须写（缺则 `expression combine requires columns`）**（外层 `combo.method` 非空时整体覆盖内层）。
- `equal`：列等权；`fixed`：权重直接线性相加，允许负数，不归一化。非零权重列对某资产无效 → 该资产分数无效，不重分配；零权重列不影响有效性。
- 历史方法只用已成熟、决策时可见、`DecisionTime<asof` 的 IC 样本（窗口保留最近 256 个截面，样本须 ≥2 对；`min_pairs` 可提高）；`rank` 变体用 RankIC，`icir` 用 mean/std，`ewma` 用 `decay`；`min_samples` 至少 1；`min_confidence` 为 |均值|/标准误 门槛（需 ≥2 样本、均值非 0，无多重检验校正，重叠样本 ICIR 仅描述性）；`direction: positive` 把负权重截为 0；权重按 Σ|w|=1 归一；无合格历史按 `fallback`：`equal`（默认，诊断 `ic-equal-fallback`）、`fixed`（用 `weights`）、`error`。
- 历史方法必须有标签（`research.labels` 非空）；多标签省略 `label` 取最短 horizon（同期限按名称），`label` 须已声明。
- 实盘拒绝历史方法，仅 `fixed`/`equal`。

### 1.5 `research`

- `labels`：`[{name, kind, horizon, overlapping, periods_per_year}]`。`kind` 归档流水线只接受 `executable-return`（`close-to-close` 拒绝）；`horizon`（毫秒）与 `periods_per_year` 为正数；`overlapping` 布尔（默认 false）。
- 未写 `research` 时自动一个标签：`name=run_timeframes 值`，`executable-return`，`horizon=决策间隔`，`periods_per_year=365.25×86400000/决策间隔毫秒`；写了 `research.labels` 则覆盖 `manifest.labels`。
- `labels: []`：回测不生成标签；`research`/历史 IC 必须有标签。`label_wait_ms`≥0，默认 0。`research.panel`：`all`（默认）或 `none`。
- 示例：`labels: [{name: forward_16h, kind: executable-return, horizon: 57600000, periods_per_year: 547.875, overlapping: true}]`。未成熟标签记 unresolved（`Result.Unresolved`），不得填零；未来标签不得进入当期选股。

### 1.6 源事件决策（Information bars 等不规则观测）

`decision: {source_event_decisions: true, source: information, timeframe: event, delay_ms: 0}`（source=归档流真实 source；timeframe=流的精确标识，不是时长）

- `GridTime` 取所选流真实闭合记录的 `EventTime`；决策时间为该批发布/接收截止。仅 Research/Weights/Events 历史回放，`trade`/实盘拒绝；`delay_ms` 必须 0。
- 同一发布批次出现两个不同源事件时间 → 拒绝，需拆批。

### 1.7 `prices`（价格流）

- 归档：有 `tick` 流 → `{tick, event, price}`；否则有表达式 → `{kline, 表达式 timeframe, close}`；否则 `{factor.source, factor.timeframe, factor.field}`（档内无 kline/tick 价格流且有表达式报错）。
- 存储回放：Events → `{kline, 1m, close}`；有表达式 → `{kline, 表达式 timeframe, close}`；否则同上。
- 实盘：**无默认**，须显式 `event` 或 `1m` 流。

Events 与实盘只接受 `event`(tick) 或 `1m` 价格，`1h` 等报错。Weights 默认价格为决策周期 close 时（如归档仅 kline 1h），下一价格晚于 `expiry_ms` → 全部目标过期（`Executions=0`、`Skipped=Decisions`），应配 `{tick,event,price}` 或 1m 流。

### 1.8 回放可见性（归档）

`max(AvailableAt, IngestedAt) ≤ 当前时刻` 且 `EventTime ≤ GridTime` 时可见，同流取 EventTime/Revision 最新；`snapshot.replay_time≠0` 时按接收时钟驱动。

### 1.9 `manifest` 与 `snapshot` 子键去向

`manifest` 允许键 `currency code_revision factor_plan_hash universe_version visibility_policy execution_mode latency_assumption static_universe combo portfolio labels parameters costs{fee_rate,slippage_rate,funding_policy} snapshots`（`fee_rate`/`slippage_rate` 非负，缺省 0）。

- `currency`、`code_revision`、`costs.fee_rate/slippage_rate`：**采用用户值**（缺省：`stake_currency[0]`，无则 `USD`；`code_revision`=banbot 版本）。
- `parameters` 与 `params` 合并，同名 `params` 优先；`portfolio` 逐字段叠加：缺省值 → `params.k` → `manifest.portfolio` → `portfolio` 块（后者优先）。
- `labels`：写了 `research.labels`（含 `[]`）则被覆盖；`costs.funding_policy`：有 `execution.funding_policy` 则被覆盖，否则归档推导。
- `combo`、`factor_plan_hash`、`universe_version`、`static_universe`、`visibility_policy`：**总被覆盖**（来自实际组合/计划/`snapshot`）；`execution_mode`、`latency_assumption` 总被 runner 覆盖；`snapshots` 追加归档 SHA-256 引用。

`snapshot` 键 `grid_time decision_time replay_time universe sid_map schemas source_versions adjustment_version visibility_policy`（`universe` 子键 `version investable reference tradable evaluation tracked static`）：

- `grid_time`、`decision_time`：每个决策轮被覆盖（写了无效）；`replay_time`：仅非 0 有意义=启用接收时钟回放（IngestedAt 驱动），值每轮被覆盖。
- `universe`：`version` 非空则采用（可 `static: false`）；空则归档推导 `archive-static`/存储推导 `storage-static:<hash>`。
- `sid_map`：归档缺失 SID 补 `sid:<N>`；存储与交易所符号不一致报错，缺失补齐。
- `schemas`、`source_versions`：归档三者（含 `sid_map`）都给则跳过推断，否则缺失项由档内推导，`source_versions` 与记录 `SourceVersion` 不符报错；存储被存储元数据覆盖。
- `adjustment_version` 采用，缺省 `raw`；`visibility_policy`：归档缺省 `available-at`（仅标签），存储强制等于 `data.pit_policy`，实盘须非空。

## 2. 表达式（expressions）

### 2.1 结构与规则

```yaml
expressions:
  schema_version: 1          # 必须为 1
  timeframe: 1h              # 须等于 run_timeframes（独立 validate 必填）
  bindings:                  # 别名 → 数据流
    kline: {source: kline, timeframe: 1h}
    info:  {source: info, timeframe: 1d, sampling: asof, max_age_ms: 172800000}
  params: {window: 24}       # 只供 param.name 使用（有限数值），不继承外层 params
  lets: {price: 'positive(kline.close)'}   # 中间因子
  outputs: {momentum: 'cs.zscore(ts.return(factor.price, param.window))'}   # 至少一个
  combine: {method: equal}   # 可选，同 1.4
```

- `banbot validate|explain --spec f.yml` 输出相同，JSON 键 `hash timeframe outputs nodes warmup retention inputs combine`；不验数据/账户。文件须单 YAML 文档、≤1 MiB、未知顶层键报错。
- 标识符 `[A-Za-z_][A-Za-z0-9_]*`；binding 名不得用 `factor param label ts cs group`。名字可前向引用，不允许环；未使用的声明也校验；禁用 `label.*`。
- 限制：单表达式 ≤16 KiB，总 ≤256 KiB，AST ≤8192 节点，深度 ≤64，声明 ≤512，窗口 ≤10000。含 `"` 的表达式用 YAML 单引号：`'group.demean(x, "info", "sector")'`。
- binding：`sampling` 缺省或 `source-events` 时 `timeframe` 须等于决策周期且 `max_age_ms=0`；`asof`/`asof-latest` 须 `max_age_ms>0`，慢频/event 源必须用它采样到决策网格；`asof` 后窗口按**决策观察次数**计（24 个小时观察≠24 个交易日）。跨周期未 asof 报 `cross-timeframe data requires asof sampling and positive max_age_ms`。
- `alias.field` 读字段；`field("alias","name")` 用于特殊字段名（两参数须字符串字面量）。

### 2.2 语法与函数

运算 `+ - * /`（`*/` 优先，左结合）、一元 `+ -`、括号；数字十进制（可科学计数法 `1e-8`），无下划线/十六进制；无比较、条件、`%`、`**`；字符串仅用于 `field`/`group.demean/zscore`。窗口参数为整数常数（可带符号）或 `param.name`（不能写 `param.window+1`），范围 [1,10000]，`ts.lag` 另允许 0。

- 逐点：`positive(x)`（x>0 取 x 否则无效，**不做下界截断**）、`abs log sqrt (x)`、`pow min max (a,b)`。
- 时序（x 为输入；n 窗口）：`ts.lag(x,n)`（n=0 原样返回，不计入窗口规则）、`ts.return(x,n)`=x_t/x_{t-n}-1、`ts.ema/sma/rma/wma(x,n)`（指数/简单/Wilder/加权，wma 最新权重 n）、`ts.std(x,n,ddof)`（整数 `0≤ddof<n`）、`ts.rsi(x,n)`（Wilder，0–100）、`ts.roc(x,n)`（百分比，×100）、`ts.mom(x,n)`（与 n 个有效样本前的差）、`ts.cci(x,n)`、`ts.highest/lowest(x,n)`、`ts.vwma(c,v,n)`、`ts.tr(h,l,c)`（首个有效前收为预热）、`ts.atr(h,l,c,n)`（Wilder）、`ts.stoch(h,l,c,n)`（原始 %K 无 D 平滑）、`ts.willr(h,l,c,n)`（-100–0）、`ts.obv(c,v)`（首值取首个有效 volume，相等收盘不变）、`ts.mfi(h,l,c,v,n)`、`ts.macd/macd_signal/macd_hist(x,f,s,g)`（f<s，f/s/g∈[1,10000]）、`ts.bbands_upper/middle/lower(x,n,up,down)`（总体标准差，倍数非负且显式）。
- 截面：`cs.rank(x)`（升序名次，0 起，并列平均，非百分位）、`cs.zscore(x)`（总体标准差；全同输出 0）、`cs.robust_zscore(x)`=(x−中位数)/(1.4826·MAD)（MAD=0 输出 0）、`cs.winsorize(x,tail)`（tail∈[0,0.5)，上下界为参考池插值分位数）、`cs.mad_winsorize(x,k)`（中位数 ± k·1.4826·MAD，k>0；MAD=0 全截到中位数）、`cs.quantile(x,q)`（q∈[0,1]；**输出截面常数**，参考池 q 分位广播，只适合阈值/分组，不能排名）。
- 回归/分组：`group.residual(y,x)`（单变量截面 OLS 残差含截距，参考池拟合，需 ≥2 个有效对）、`group.ols(y,x1,...)`（多元，至少一个暴露）、`group.wls(y,w,x1,...)`（权重须正且有效）、`group.demean(x,"alias","field")` / `group.zscore(...)`（按分类字段组内去均值/z-score（总体标准差）；字段原始类型/NULL 保留，经 binding 采样；缺字段=Missing，NULL=Null）。

- Go 同名：`ts.x`/`cs.x`→`factor.X`（MACD/BBands 返回三节点；`group.ols/wls`→`MultiResidual/WeightedResidual`；`group.demean/zscore`→`...ByField`），见第 6 节。
- 时序算子（含指标）不能作用于截面/回归输出：`TS windows over cross-section results are not supported`（`ts.lag(x,0)` 例外）；需要时先时序后截面：`cs.rank(ts.rsi(kline.close,14))`。
- `max(x,1e-8)` 等下界只对有效值生效，不补 NULL/缺字段/预热不足。
- 截面/回归统计在冻结的 `Universe.Reference` 上拟合，输出仅对 `Investable` 有效资产有意义。
- 预热例：`ts.macd_hist(..,12,26,9)`=33；缺失输入元组延长预热但不推进状态。

## 3. 流水线、输入与快照

- 归档：`banbot data archive --input records.jsonl --out chunk.gob [--schema fields.yml] [--max-records N]`，输出 `{"archive":路径,"digest":内容哈希}`。每行 JSON 为 `factor.VersionRecord`（禁未知字段，键区分大小写）：
  `{"Series":{"Source":"kline","Sid":1,"TimeMS":0,"EndMS":3600000,"TimeFrame":"1h","Closed":true,"Values":{"close":100.5}},"EventTime":3600000,"Revision":1,"AvailableAt":3600000,"IngestedAt":3600000,"SourceVersion":"v1"}`
  `Series` 即 DataSeries（`Source Sid TimeMS EndMS TimeFrame Closed IsWarmUp Values`）。`--out` 已存在：哈希相同幂等成功，不同报 `immutable archive content conflict`。
- tick 流约定：`Source:"tick"`、`TimeFrame:"event"`、`Values:{price}`（`TimeMS=EventTime`，常取收盘后 +1ms 以便决策后成交）；资金费流 `Source:"funding"`、`TimeFrame:"event"`、`Values:{rate}`。
- `--schema` YAML 为 `{source: {field: type}}`，类型 `int int8 int16 int32 int64 uint uint8 uint16 uint32 uint64 float float32 float64 string bool json`。大整数需显式类型（无类型整数超 float64 精确范围被拒），同字段类型冲突报错；缺失键/NULL/字符串/自定义字段保留在 `Values`。
- 归档身份推导：SID 集合、schema 摘要、source 版本、静态 `Universe`（`archive-static`）；`visibility_policy` 缺省 `available-at`；`adjustment_version` 缺省 `raw`；同 source 的 `SourceVersion` 档内须一致。
- 存储回放：`pit_policy: static-approximation` 必须显式声明；标的池取策略级 `pairs`（缺省配置交易对），五池相同、静态，版本 `storage-static:<hash>`；K 线 schema 变化报错；需交易所/数据库。
- 快照要求：Reference 与 Investable 中每个标的的所有计划输入在当前决策网格都必须有可见闭合记录。Weights 回测中，Investable 标的缺数据（上市前、退市后、数据缺口）会被时点宇宙（PIT）剔除，决策在缩减后的宇宙上继续（各池含 Reference 都去掉该标的；持仓按最后有效价平仓；`lifecycle-v1` 同样处理，policy 状态一并清洗，数据恢复后当新标的重新入选）；剔除记入 `UniverseDrops`/`DelistExits`/`DroppedBySID`，不记 `Incomplete`。仅属 Reference、不属 Investable 的标的缺数据、一轮内存活标的少于 `pit_min_universe_ratio × 上一次被接受决策时的存活数`（滚动分母，默认 0.5；逐个退市始终被接受，单轮大面积缺失被拒绝；存活不足 2 个但通过门槛时平仓并暂停该周期，计 `PausedPeriods`）、`pit_min_universe_ratio < 0`、或 Events/Trade 模式，则整轮跳过并计入 `IncompleteStreams`（SID、Source、TimeFrame、Missing、Invalid、FirstGridTime、LastGridTime）。
- `Universe` 五池：`Investable` 候选；`Reference` 统计池；`Tradable` 可交易池（打分与选股均要求）；`Evaluation` 研究池；`Tracked` 退出后仍监控。
- 快照不得混用不同 revision（冲突需显式 reconciliation）；迟到的资金费拒绝而非计入后续仓位。

## 4. 组合构建、约束与持仓生命周期

### 4.1 默认 top/bottom-K（`top-bottom-k-v1`）与 `portfolio` 基础字段

- 输入分数列默认 `score`（合成输出）。`Investable ∩ Tradable` 中有效分数 `<2K` → 返回 nil 目标 + 诊断 `insufficient-scores`；全部分数相等 → `constant-scores`；runner 均跳过替换、保留旧组合。直接调用 Go 函数者须检查非 nil。
- 最高 K 个做多每个 `long_notional/K`，最低 K 个做空每个 `-short_notional/K`，两侧不重叠，同分按 SID。`short_notional: 0` 使空头权重为 0（仍要求 ≥2K 个有效分数）；纯多头建议用 `selection.long_k` 或自定义 builder。

| `portfolio` 键 | 说明（默认） |
| --- | --- |
| `builder` | PortfolioBuilder 名（`top-bottom-k-v1`；未注册报错） |
| `k` | 正整数，缺省取 `params.k`，再缺省 10（`portfolio.k` 优先）；lifecycle 下可由 `selection` 替代 |
| `long_notional` / `short_notional` | 策略 NAV 比例，非负（0.5 / 0.5） |
| `mode` | `full`（默认；本策略旧目标中省略的资产归零）或 `patch`（省略保留，显式 0 才清仓） |
| `policy` | 缺省空（旧路径）；`lifecycle-v1` 或自定义 `name-vN`；`rebalance/selection/holding/transition/allocation/policy_params` **要求显式 `policy`**，否则报 `lifecycle fields require an explicit policy` |
| `policy_params` | 自定义策略的有效 JSON（有界） |

`TargetPortfolio` 是理想权重；`PortfolioTarget` 可混合 NAV 权重和绝对数量（`AbsoluteQuantity`，标准资产数量非合约张数，有符号十进制字符串）。

### 4.2 `lifecycle-v1`

示例（每小时算、每两小时调仓；首次成交满 16 小时后仅落选才进入八步线性退出）：

`portfolio: {long_notional: 1, short_notional: 0, policy: lifecycle-v1, selection: {long_k: 3}, rebalance: {every_bars: 2}, holding: {min_bars: 16}, transition: {mode: linear-exit, exit_steps: 8, basis: quantity}, allocation: {method: equal, reserve_ratio: 0.02}}`

固定寿命批次（替换 `transition`、去掉 `holding.min_bars`）：`transition: {mode: cohort, period_bars: 16, startup: gradual, sizing: entry-nav}`。gradual 每轮建 1/8 预算批次；seed-all 用当前排名一次建八个不同到期窗口的批次。同币批次先汇总净目标，到期与新建内部转移已确认贡献，不伪造成交与手续费；计划 8 单位只成交 4 单位时，首批到期后保留 3.5，不向旧批补足。

**selection**：`long_k`/`short_k` 为当轮选择数（非持仓上限；缺省 `long_k=k`（long_notional>0）、`short_k=k`（short_notional>0））；或 `long_quantile`/`short_quantile`∈[0,1]（与对应 K 互斥）；`retain_rank`（≥0，持仓排名缓冲）；`dropout`（≥0，普通轮次替换数）；`group_quota`（map，值≥0，分组由调用方提供）；`missing_scores`：`skip`（默认）|`shrink`|`cash`。需 `long_notional+short_notional>0`。

**rebalance**：`every_bars`（正整数，默认 1）、`phase`（<every_bars，单位 bar）、`anchor`（绝对毫秒锚点，默认 0）、`duration`（如 `4h`）、`calendar`（`daily|weekly|monthly`，需 `calendar_version`，禁止 anchor/phase，`timezone` 默认 UTC；按时区日期/ISO 周/月换轮）。三者选一。轮号=`floor((grid−anchor−phase×bar)/interval)`，增大才调仓，首个决策总触发；节假日/分数变化触发需 Go 回调。

**holding**：`min_bars`/`max_bars` 或 `min_duration`/`max_duration`（Go duration，正；bars 与 duration 不混用；`min≤max`）。
- `min_bars`：首次成交满该年龄后才允许普通退出（落选才退出）。`max_bars`/`max_duration`：到期后第一个监控网格直接形成零目标，绕过调仓门与普通退出曲线。
- `by_asset`：按 `snapshot.sid_map` 符号覆盖，区分"省略"与"显式 0"；优先级：显式 by_asset → 可见 resolver 规则 → 默认值。
- `cooldown_bars`（≥0）、`adopt`（`adopt`|`reject`，默认 reject）：已有仓位无年龄证据时默认拒绝猜测；`adopt` 从接管时计龄。

**transition**（`mode`：`direct`（默认）|`linear-exit`|`cohort`|`geometric`|`target-step`）：

| mode | 字段/行为 |
| --- | --- |
| `direct` | 调仓时直接收敛到目标 |
| `linear-exit` | 落选后按原始基准分 N 步归零；`exit_steps>0`、`basis: quantity|weight` 必填、`on_reselect` 缺省 `restore`。quantity 锚定退出开始时已确认数量，向零取整，价格上涨不回补；weight 每轮乘最新冻结 NAV，可能回补；第 N 步目标显式为零 |
| `geometric` | 每步按剩余比例退出：`ratio∈(0,1)`、`final_threshold∈(0,1)`、`basis` 必填同上、`on_reselect` 可选 |
| `cohort` | 固定寿命批次：`period_bars` 须为 `every_bars` 的正整数倍（须用 `every_bars` 调度）；`startup: gradual|seed-all`（缺省 gradual）；`sizing: entry-nav|current-nav`（缺省 entry-nav）；`entry_window_bars`（缺省=every_bars）；`holding.min_bars≤period_bars`，不能用 `min_duration` |
| `target-step` | 每步向新目标移动 `alpha∈(0,1]`，新资产从零开始插值；alpha<1 的渐进退出可能长期保留尾仓 |

- `on_reselect`：`restore`（补回理想份额）|`resume`（保留当前较小份额）|`finish`（继续原退出）|`new-cohort`；仅 linear-exit/geometric 可写。`by_asset`：`exit_steps`（linear-exit）或 `ratio`（geometric，[0,1)）的每资产覆盖，其它模式不允许。
- 字段与 mode 不匹配（如 `alpha` 用于 linear-exit、`basis` 用于 direct/cohort/target-step）预检失败。

**allocation**：`method`：`equal`（缺省）|`score`|`fixed-notional`（`fixed_notional>0`，仅此可写）|`inverse-volatility`|`vol-target`（`vol_target>0`，仅此可写）。通用：`reserve_ratio∈[0,1)`、`asset_cap`、`group_caps`（分组需 `Config.PolicyContext` 注入）、`turnover_limit`（限调仓金额不限持仓数，强制风险退出绕过）、`net_cap`、`beta_cap`（后两者需 beta/分组证据，均 ≥0）。inverse-volatility 与 vol-target 需注入 `Volatility`（不取同名因子列）。

### 4.3 生命周期语义要点

- 持仓数可超过 K（最短持有期/渐进退出保留落选旧仓）；无 `max_long_positions`。
- 缺分数不推进普通退出步骤，但仍检查最大期限与批次到期。反手默认等待原方向实际归零（未知在途不算已释放）；再入场建立新生命周期；加仓不重置首次成交年龄。
- 首次成交证据来自 `FillEvents`；`RebalanceDue/ForceExit/HoldingRules/TransitionRules` 可经 `Config.PolicyContext` 注入。
- 持仓证据：Weights 用 Book，Events 用共享纸面账户，实盘用真实账户；纯 CLI `research` 无持仓证据，启用有状态 policy 失败。
- policy 只读冻结 context 与已接纳 JSON 状态（≤1 MiB、≤4096 活跃资产），返回目标/下一状态/诊断，不直接下单；账户原子接纳后才推进状态，已接纳但发送失败不回滚、不重复生成计划。

### 4.4 Go 组合扩展

```go
type PortfolioBuilder func(factor.Frame, factor.Universe, factor.PortfolioSpec, research.PortfolioDefinition) (*factor.TargetPortfolio, []factor.Diagnostic, error)
func RegisterPortfolioBuilder(name string, builder PortfolioBuilder) error          // 名唯一，不可为 top-bottom-k-v1
func RegisterPortfolioBuilderIdentity(name, hash string) error                    // 须先注册 builder
type PortfolioFactory func(Config) (PortfolioBuilder, error)
type PortfolioValidator func(Config) error
func RegisterPortfolioFactory(name string, factory PortfolioFactory, validators ...PortfolioValidator) error // 名含 "-v"，至多一个 validator
func RegisterPortfolioPolicy(name string, factory factor.PortfolioPolicyFactory) error   // 名含 "-v"；内置仅 lifecycle-v1
type PortfolioPolicy interface { Propose(factor.PortfolioContext, json.RawMessage) (factor.PortfolioProposal, error) }
type PortfolioPolicyFactory func(factor.PortfolioPolicyConfig) (PortfolioPolicy, error)
func RegisterModelPortfolioBuilder(name string, config ModelBuilderConfig) error  // ModelBuilderConfig{ManifestID, Artifacts []research.ModelArtifact}
func RegisterRiskPortfolioBuilder(name string, config RiskBuilderConfig) error  
```

- 类型：`Frame{GridTime, SnapshotID, PlanHash, DecisionTime, Values map[列名]map[SID]Numeric}`（含输出列与合成列 `score`）；`Universe{Version, Investable, Reference, Tradable, Evaluation, Tracked []int32, Static}`；`Numeric{Value, Validity}`（`valid missing null not-numeric non-finite warmup`）；`Diagnostic{Code, Detail}`；`PortfolioSpec` 含 StrategyID/AccountID/DecisionTime/ExecutableAt/ExpireAt/Budget{Version,Currency,NAV}/Mode 等。
- `PortfolioContext` 含 Frame、Universe、Ideal、Spec、ScoreName、Positions(`map[SID]PositionEvidence{Quantity,PendingQuantity,FirstFillTime,FillEvents..}`)、Marks、Groups、Volatility、Beta、AssetNames、ForceExit、Previous、HoldingRules、TransitionRules、RebalanceDue、CapitalLimit 等；`PortfolioProposal{Target, NextState, Reasons, AcceptanceID, ReconcileSIDs, PlanSequence, DecisionTime, ExpireAt}`。
- `TargetPortfolio`：`NewTargetPortfolio(spec, targets)`、`Spec/ID/Targets/EffectiveTargets(prev)/Notional(sid)`。`PortfolioTarget`：`NewPortfolioTarget(spec, map[SID]Allocation)`；`Allocation{Basis: nav-fraction|absolute-quantity, Value string}`；`Allocations()`、`AsWeightPortfolio()`（数量目标报错）、`PortfolioTargetFromWeights`、`EffectiveAllocations(prev)`。
- 生命周期：`factor.SelectPortfolio(frame, universe, spec, cfg)`、`SelectPortfolioScore(..., scoreName, groups)`、`AllocateSelected(selected, cfg, nav, scores, vol)`、`NewLifecyclePolicy`。
- 注册名须带版本（如 `rotation-v1`），参数/实现变化须升版本或改 identity hash。

## 5. Go 注册入口与接口

### 5.1 `runner.RegisterFactor`

```go
type FactorContext struct { TimeFrame string; Params *Params }
func (f FactorContext) Close() *factor.Node          // factor.Field("kline","close", TimeFrame)
func (f FactorContext) Field(name string) *factor.Node
type FactorSpec struct { Outputs map[string]*factor.Node; Combine research.ComboSpec }
func RegisterFactor(name string, build func(FactorContext) (FactorSpec, error)) error
```
- `Outputs` 非空；默认 `equal`+全部输出，可由 `Combine` 指定（YAML `combo` 覆盖）。

### 5.2 `runner.RegisterDefinition`

```go
type DefinitionBuilder func(runner.Config) (*factor.Plan, research.ComboSpec, error)
func RegisterDefinition(name string, builder DefinitionBuilder) error   
func CompileDefinition(c runner.Config) (*factor.Plan, research.ComboSpec, error)
func DefinitionCatalog() (builders, portfolios []string)
```
- 内置 `momentum-vol`（5.5）；`definition` 缺省用 `run_policy.name`；`expressions`/`Plan`/`definition` 互斥（传 `Plan` 须显式 `Combo`）。

### 5.3 `runner.Config`

```go
type Config struct {
    Definition string; Expressions *expr.Spec; Plan *factor.Plan
    Combo research.ComboSpec          // 外层覆盖
    Manifest research.ManifestSpec    // Parameters、Labels、Portfolio、Costs、Currency
    Factor research.MomentumVolConfig // Source, Field, TimeFrame, Window, DDOF, WinsorTail, Standardize
    PortfolioBuilder PortfolioBuilder // 非 nil 时覆盖 YAML
    PolicyContext func(context.Context, *factor.PortfolioContext) error
    Prices PriceStream; FundingSource, OptionExpirySource, TerminalSettlementSource string
    DecisionInterval, LatencyMS, ExpiryMS, LabelWaitMS, DecisionDelayMS int64
    MaxRecords, MaxPending int; InitialNAV, AccountInitialNAV float64
    SourceEventDecisions bool; Execution ExecutionConfig; StrategyID, AccountID string
}
func (c Config) Params() *Params   // 读 Manifest.Parameters（私有副本）
```
- `Params`：`Float/Int/Bool(name, def)`（Int 小数报错，Bool 取 0/1）、`Err()`（首个错误）、`Unused()`（未读参数，查拼写；`window`、`k` 不计）。

### 5.4 研究与标签接口（`factor/research`）

```go
type LabelSpec struct{ /* 同 1.5 labels；Kind: executable-return|close-to-close */ }
func ReturnLabel(spec LabelSpec, sid int32, decision, begin, end, available int64, beginPrice, endPrice factor.Numeric) (Label, error)
type ComboSpec struct{ /* 字段同 1.4，Method 为 ComboMethod */ }
func Combine(frame factor.Frame, universe factor.Universe, spec ComboSpec, history *ICHistory) (map[int32]factor.Numeric, []factor.Diagnostic, error)
func NewICHistory(window int, label string, columns []string) (*ICHistory, error)
func Evaluate(frame factor.Frame, universe factor.Universe, labels []Label, spec EvaluationSpec) (Report, error)
type ManifestSpec struct{ /* 字段同 1.9 manifest 键 */ }
func BuildManifest(spec ManifestSpec) (*Manifest, error)
type PortfolioDefinition struct{ /* 字段同 4.1 portfolio 键；Rebalance/Selection/Holding/Transition/Allocation 为 *factor.XxxConfig */ }
```
- `Report`：每列 Coverage、缺失原因计数、每标签 IC/RankIC/五分位收益/单调性、换手、排名自相关、风险暴露；截面相关矩阵；`Performance`（毛/净收益、单边换手、成本）；`EvaluationSpec`：`AsOf PrimaryLabel CostRate CurrentWeights PreviousWeights PreviousColumns Exposures`。
- 滚动训练：`VisibleTrainingRows(rows, window)`、`RollingWindows(start, end, trainMS, testMS, stepMS, embargoMS, minSamples)`；模型：`RegisterModel`、`FitModel`、`RestoreModel/PublishModel/LoadModel`、`LinearModel`；试验：`OpenTrialLedger(path, max)`、`CompareTrials`、`SelectParameters`、`ResolveParameters`、`ResearchCache`；风险：`EstimateCovariance(returns, shrinkage, diagonal)`、`OptimizePortfolio(OptimizationSpec)`（超限结果不可作普通目标）。
- 其它：`HACMean(values, lag)`（重叠收益用 Newey-West）、`RankAutocorrelation`、`SummarizeLifecycles`、`Attribute`、`EstimateCapacityCost`。

### 5.5 内置 `momentum-vol`

价格 `kline.close`；`Window=24`（`params.window`，整数 ≥2）、`DDOF=1`、`WinsorTail=0.01`（>0 才缩尾）、`Standardize=true`（才 zscore）。动量=`zscore(winsorize(ts.return(close,window)))`；波动=`zscore(winsorize(ts.std(ts.return(close,1),window,ddof)))`；`fixed`：动量 +1、波动 −1。参数 `window`、`k`。

### 5.6 Go 示例

```go
package factors

import (
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/runner"
)

func init() {
    if err := runner.RegisterFactor("my-mom-v1", func(c runner.FactorContext) (runner.FactorSpec, error) {
        ret := factor.Return(c.Close(), c.Params.Int("window", 24))
        return runner.FactorSpec{Outputs: map[string]*factor.Node{"mom": factor.ZScore(ret)}}, c.Params.Err()
    }); err != nil {
        panic(err)
    }
}
```
对应 YAML（无 `expressions`）：`run_policy: [{name: my-mom-v1, engine: factor, run_timeframes: [1h], params: {window: 24, k: 10}, archive: data.gob, research: {labels: []}}]`
入口程序须 `import _` 该包。

## 6. 节点 API（`factor`）

```go
type Numeric struct { Value float64; Validity Validity }  // Valid, Missing, Null, NotNumeric, NonFinite, Warmup
func Number(values map[string]any, field string) Numeric
func Field(source, field, timeframe string) *Node
func AsOfField(source, field, sourceTF, decisionTF string, maxAge int64) *Node
func Constant(v float64, tf string) *Node
func Add/Sub/Mul/Div/Pow/Min/Max(a, b *Node) *Node
func Neg/Abs/Log/Sqrt/Positive(x *Node) *Node
func Lag/Return/EMA/SMA/RMA/WMA/RSI/ROC/MOM/CCI/Highest/Lowest(x *Node, period int) *Node
func StdDev(x *Node, period, ddof int) *Node
func VWMA(p, v *Node, period int) *Node; ATR(h,l,c *Node, period int); Stoch/WillR(h,l,c *Node, period int)
func TR(h,l,c *Node) *Node; OBV(c,v *Node) *Node; MFI(h,l,c,v *Node, period int) *Node
func MACD(x *Node, fast, slow, signal int) (line, signalLine, hist *Node)
func BBands(x *Node, period int, stdUp, stdDown float64) (upper, middle, lower *Node)
func Rank/ZScore/RobustZScore(x *Node) *Node
func Winsorize(x *Node, tail float64) *Node; MADWinsorize(x *Node, k float64) *Node; Quantile(x *Node, q float64) *Node
func GroupDemeanByField(x, field *Node) *Node; GroupZScoreByField(x, field *Node) *Node
func Residual(y, x *Node) *Node; MultiResidual(y *Node, xs ...*Node) *Node; WeightedResidual(y, w *Node, xs ...*Node) *Node
func Linear(inputs []*Node, weights []float64) *Node
func Custom(version string, inputs []*Node, evaluate func([]Numeric) Numeric) *Node   // 纯逐点，须传播无效原因
func New() *Builder; (*Builder).Add(name string, n *Node) *Builder; (*Builder).Compile() (*Plan, error)
func Compile(outputs map[string]*Node) (*Plan, error)
func (p *Plan) Hash/TimeFrame/Outputs/Inputs/WarmupLength/StateRetention/NodeCount
```
- `GroupDemean`/`GroupZScore`（不带 ByField）为旧接口，无年龄限制；新代码用 `...ByField`，分类字段须为 `factor.Field` 或 `AsOfField`。
- `Custom` 不得在闭包里保存滚动状态；需要历史算子须扩展内核。
- 指标窗口 [1,10000]；MACD 要求 fast<slow；BBands 倍数非负。`Session`/`Plan.Batch` 由运行时持有，策略不直接调用。

## 7. 命令、回放、结算、实盘

### 7.1 命令

- `banbot validate|explain --spec f.yml`：独立表达式编译（无数据、无账户）。
- `banbot data archive --input records.jsonl --out chunk.gob [--schema s.yml] [--max-records N]`：冻结 VersionRecord JSON 行为归档。
- `banbot research --config cfg.yml`：因子研究，输出面板行、`decision`、`evaluation`（成熟诊断）、摘要 JSON 行；不创建账户。
- `banbot backtest [--mode weights|events] --config cfg.yml [--out DIR]`：因子回测；`--separate` 不支持因子；`--mode weights` 要求有因子策略。
- `banbot trade --dry-run --config cfg.yml`：因子/混合 Events 历史回放（强制 events；纯时序模拟仍用 `env: dry_run`）。
- `banbot trade --config cfg.yml [--live-provider NAME]`：实盘。
- `--no-default` 跳过 `config.yml`、`config.local.yml`；`@`/`$` 路径需数据目录；归档回放可纯离线。
- 事件 JSON 行写 stdout 与 `events.jsonl`。`--out DIR` 已存在则用 `DIR_1`、`DIR_2`…；缺省 `<DataDir|BanDataDir|归档目录>/backtest/factor`。产物：`config.yml`（生效配置）、`resolved.json`（各字段实际值与来源）、`events.jsonl`、`strategy-N.json`、`run.json`（`{Version, Status(complete|incomplete), Errors, Results}`；有 Unresolved 即 incomplete）；Events 另有 `account-<account>/manifest.json`、`chunk-NNNNNN.gob`。`Result` 含 Decisions、Executions、Skipped、Incomplete、Unresolved、TargetsAccepted、Fills、Book、Account、Summary、IncompleteStreams 等；Weights 回测另有 `Attribution`（多头/空头腿盈亏、换手、平均敞口、费用/滑点/资金费/PIT 成本、`TopSymbols` 逐标的净贡献与占比，`FinalNAV−InitialNAV = LongPnL+ShortPnL−成本+Residual`，Residual 仅浮点舍入）；配对策略另有 `Pairs`（每对 Episodes、PnL、Trades、WinRate、InMarketBars、PausedBars、AvgHoldBars、LastBeta、Turnover）与对账项 `PairsGrossPnL/PairsNAVReturn/PairsResidual`（逐对盈亏按决策时刻价格近似、不含成本，残差≈成本+执行近似）。`research.panel: none` 不写每决策的 panel 行（事件行、Summary、账本不变，百万行级输出时使用）。

### 7.2 回放语义

- 成交于决策完成后、过期（exclusive）前、严格晚于决策且 ≥`decision+latency_ms` 的可观测价格。
- 费用/滑点来自 `manifest.costs`。Events 纸面：全市价成交、方向性滑点、手续费；不模拟排队/部分成交/网络失败；限价单在后续报价穿越限价时成交。Weights：保留数量在显式决策之间保持，按实际变化名义金额计费。
- 终局结算 `terminal_settlement_source`（Weights/Events）与期权到期 `option_expiry_source`（仅 Events）须是与价格、资金费都不同的显式 source。
  - 终局结算记录：`Values["terminal_cash_value"]` 非负十进制**字符串**，`TimeFrame:"event"`，`TimeMS==EventTime`，`SourceVersion` 非空，`AvailableAt≥EventTime`，`IngestedAt≥AvailableAt`，SID 为已声明纸面标的；终局后再给非零目标报错。
  - 期权到期记录：SID=期权，`EventTime`=到期时刻，`AvailableAt`=公布时间，`Values["fixing"]` 十进制字符串（超过 `underlying_stress_max` 拒绝）。
  - 期权仅 Events 纸面可用（现金结算欧式，无提前行权/实物交割）；Weights 报 `cash options require Events paper accounting`、实盘拒绝；风险预留：多头=权利金；空 put=`strike×单位`；空 call=`max(stress_max−strike,0)×单位`；不同合约不抵消；到期前才可下单。

### 7.3 冷历史（`execution.history`）

冷历史 SQLite 归档，仅 Events：路径解析后须为绝对路径且文件**不存在**（否则 `file exists`），与 `store`/`sender_lease_dir` 互斥，实盘拒绝，各账户路径不同；Weights 下静默忽略。

### 7.4 实盘（`trade`）

- 实盘 YAML **不做归档/存储推导**，须显式：`prices`(event|1m)、`snapshot`（`universe`、`sid_map`、`source_versions`、非空 `visibility_policy`；`universe.version` 非空）、`instruments`（`id` 须等于 `sid_map` 名）、四项风险限额、`funding_policy`（`explicit-zero`（需已验证无资金费义务）|`required-stream`（需 `funding_source`））。`archive/chunks`、`history`、`paper_fill_model`、`source_event_decisions`、期权、终局结算流、历史 IC 方法校验时拒绝。
- `live_provider`（优先级 `--live-provider` > `accounts.<name>` > `execution` > 缺省）：缺省/`banexg`/`hub` = **Hub 模式**：独立 `banbot hub` 进程持有凭据，机器人经 Unix socket 连接（`execution.hub_socket`，缺省 `<datadir>/execution/hub.sock`）只做公共行情与计算；要求 `env` 为 `prod` 或 `dry_run` 且 `market_type: linear`，`dry_run` 要求 `funding_policy: explicit-zero`；Hub 与嵌入式提供者不可混用，嵌入式不允许 `dry_run`。其它名称须调用 `entry.RegisterFactorLiveBinding(name, factory)` 注册（否则 `unsupported live capability`），factory 提供 Banexg 传输、符号元数据、发布/修订映射与资金费验证器，且 `store`/`sender_lease_dir` 须绝对路径。
- 内置 banexg 嵌入式绑定（非 Hub 路径）：`env: prod`；仅线性永续（`Linear && Swap`，单位 step/tick/合约规模/精度须与交易所元数据一致，`min_steps`/`min_notional` 不低于交易所）；仅单向净持仓（hedge 持仓拒绝）；`funding_policy` 必须 `required-stream` 且交易所有权威资金费现金能力。缺失能力直接失败，不降级、不回退 paper。
- 已有仓位须先有账本归属与启动对账；`InitialNAV` 不向真实账户入金；账户/策略/币种身份与已提交 checkpoint 被校验。

## 8. 研究侧要点与统计工具

- 不得用全样本最优参数反推历史表现。
- `factor/statistics`：
  - 标签：`TripleBarrierLabel`、`CalendarBarrierLabels`、`EventUniqueness`、`SequentialBootstrap`、`FractionalDifference`、`SelectStationaryFFD`。
  - 交叉验证：`PurgedKFold(spans, folds, embargoBars)`、`CombinatorialPurgedCV`、`RunCPCVResearch`；模型：`TrainRandomForest`、`TrainSequentialBaggedClassifier`、`PurgedParameterSearch`、`RefitPurgedSearch`、`PurgedFeatureImportance`（拟合须在训练折内完成）。
  - 降维/组合/检验：`FitPCA`、`FitFactorAnalysis`、`FactorNeutralWeights`、`HierarchicalRiskParity`、`LongOnlyMinimumVariance`、`PCARiskWeights`、`ProbabilisticSharpe`、`DeflatedSharpe`、`CombinatorialSymmetricCV`。
- 迭代拟合与组合实验须显式设迭代/工作/试验上限。

## 9. 完整示例

动量 + 低波动表达式，Weights 回测，归档输入（`banbot backtest --no-default --mode weights --config s.yml --out res`）：

```yaml
stake_currency: [USDT]
wallet_amounts: {USDT: 10000}
execution: {mode: weights, funding_policy: explicit-zero}
run_policy:
  - name: MultiFactor
    id: multi_v1
    engine: factor
    run_timeframes: [1h]
    params: {k: 3}
    archive: chunk.gob
    prices: {source: tick, timeframe: event, field: price}
    portfolio: {long_notional: 0.5, short_notional: 0.5, mode: full}
    research: {labels: []}
    expressions:
      schema_version: 1
      timeframe: 1h
      bindings:
        kline: {source: kline, timeframe: 1h}
      params: {window: 24}
      lets:
        price: 'positive(kline.close)'
        ret1: 'ts.return(factor.price, 1)'
      outputs:
        momentum: 'cs.zscore(ts.return(factor.price, param.window))'
        low_vol: 'cs.zscore(-ts.std(factor.ret1, param.window, 0))'
      combine:
        method: fixed
        weights: {momentum: 0.7, low_vol: 0.3}
```

- 存储回放：去掉 `archive`，加 `exchange: {name: binance}`、`pairs`、`timerange: 20240101-20240601`、`data: {pit_policy: static-approximation}`。
- 研究：去掉 `research: {labels: []}`；生命周期/批次：替换 `portfolio` 为 4.2 示例。

## 10. 未确认点

仅代码核对、未实测：存储回放与 Hub 实盘（Hub 对资金费策略的限制未知）、嵌入式 banexg 绑定与自定义 provider、终局/期权到期记录格式、`retain_rank`/`dropout`/`group_quota`/`on_reselect`/`adopt`/`cooldown_bars`/`geometric`/`target-step` 数值行为、历史 IC 方法（仅验配置校验）；实盘只验到会话初始化。
