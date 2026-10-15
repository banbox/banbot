# v0.5 → 双引擎 YAML 配置对比与调整

以正式标签 `v0.5.7` 的 `config/types.go`、配置解析和 `doc/config.yml` 为基线；这些模型及模板在 `v0.5.0..v0.5.7` 之间没有修改。本文对应当前浅层实现，替代先前关于自动迁移到 v2 的说明。

## 1. 结论与结构调整

**正式 v0.5 的配置 key 不需要重命名、移动或添加版本标记。** 普通加载不再自动迁移、备份或写回 YAML；只读配置也可以加载。因子策略通过显式 `engine: factor` 启用，因子高级字段直接放在 `run_policy[]` 下。账户凭据、原交易参数和新执行参数都在根 `accounts` 维护。

| 先前 v0.6 草案 | 当前推荐路径 | 调整理由 |
| --- | --- | --- |
| 必需 `config_version: 2` | 不需要版本字段 | 使用显式 engine 区分策略语义，无需改写旧文件 |
| `run_policy[].factor.archive/chunks/definition` | `run_policy[].archive/chunks/definition` | 删除多余的 factor 包装 |
| `run_policy[].factor.expressions` | `run_policy[].expressions` | 表达式内容较多，保留有明确用途的分组 |
| `run_policy[].factor.portfolio/decision/research` | `run_policy[].portfolio/decision/research` | 提升一级，内部 key 不改名 |
| `run_policy[].factor.snapshot/manifest/prices/combo` | `run_policy[].snapshot/manifest/prices/combo` | 同上 |
| `run_policy[].factor.funding_source/initial_nav/max_records` | `run_policy[].funding_source/initial_nav/max_records` | 标量直接放策略下 |
| `execution.accounts.<账户名>.<字段>` | `accounts.<账户名>.<字段>` | 消除两处维护账户的问题；不移动原账户参数和凭据 |
| 根 `execution` | 根 `execution` | 保留公共执行默认值，模式、持久化、provider 和风险限制属于相关设置 |
| 根 `data` | 根 `data` | 保留公共数据身份、分页预算与历史可见性设置，不替代 database |

旧 v0.6 嵌套写法和 `config_version: 1/2` 仍可读取；导出使用浅层写法，不输出版本标记。同一配置层同时写同一因子或账户字段的新旧路径会报冲突，不悄悄覆盖；跨文件仍按后层覆盖规则处理。

`frequency → timeframe` 属于尚无用户的 v0.6 字段统一命名，不维护 frequency 别名。它不涉及 v0.5 的 `timeframes/run_timeframes`，后两者继续保留。DSL 字符串中的 `factor.xxx` 是公式节点引用，不能当作 YAML 路径一并删除。

## 2. v0.5 已有字段保持原位

| 旧配置范围 | 保留内容 |
| --- | --- |
| 根 `accounts` | `no_trade/stake_rate/leverage/max_stake_amt/max_pair/max_open_orders/rpc_channels/api_server` 及各交易所 prod/test 凭据 |
| 根 `exchange/database` | 交易所与数据库连接及自身设置；不搬入 execution/data |
| 根金额与风险 | `stake_amount/stake_pct/max_stake_amt/leverage/wallet_amounts/stake_currency/fatal_stop/fatal_stop_hours` 等原 key 和含义 |
| 标的与数据 | `pairs/pairlists/pairmgr/watch_jobs/kline_source` 等 |
| 时间范围和周期 | `timerange/time_start/time_end`，根及策略 `timeframes/run_timeframes` |
| `run_policy[]` | `name/filters/refine_tf/max_pair/max_open/max_simul_open/order_bar_max/stake_rate/dirt/stop_loss/strat_perf/pairs/params/pair_params` |
| 服务、通知、模型 | `api_server/rpc_channels/mail/webhook/llm_models` 等 |
| 回测及数据库扩展 | `bt_strict/bt_no_kline_download/historical_coverage/bt_legacy_*` 和 `database.db_type/sid_registry_url/qdb_mem_pct/qdb_max_mem_mb` 已在 v0.5.7 模型支持；旧模板未列出不等于本次新增 |

原 `doc/config.yml` 中 `pwd: 123` 的整数密码现在针对性地解码为字符串 `"123"`，因此原模板无需改写。没有为其他字段启用宽松类型转换。现货 symbol 示例调整也不是 symbol 迁移规则，合约 symbol 仍使用其标准后缀，不能统一删除 `:USDT`。

## 3. 新字段及保留分组

新身份及预算字段通过显式 `engine` 启用：`engine` 只接受 `time_series/factor` 两种引擎；`id` 是唯一策略身份；`account` 绑定已有且未禁用的根账户；`capital_weight` 是 `[0,1]` 的资本比例，不是 stake_rate 的别名。

因子专属一级字段为 `archive/chunks/snapshot/combo/portfolio/decision/research/manifest/prices/funding_source/initial_nav/max_records/config/definition/expressions`，全部直接位于 `run_policy[]`。`name` 默认选择 Go definition；表达式策略不要求注册同名 builder，不与显式 definition 同时使用。

| 保留的分组（均直接位于 run_policy 下） | 子 key |
| --- | --- |
| `expressions` | `schema_version/timeframe/bindings/params/lets/outputs/combine` |
| `expressions.bindings.<名称>` | `source/timeframe/sampling/max_age_ms` |
| `combo`、`expressions.combine` | `method/columns/weights` |
| `portfolio` | `builder/k/long_notional/short_notional/mode` |
| `decision` | `interval_ms/delay_ms/latency_ms/expiry_ms/max_pending` |
| `research` | `labels/label_wait_ms` |
| `research.labels[]` | `name/kind/horizon/overlapping/periods_per_year` |
| `prices` | `source/timeframe/field` |
| `snapshot` | `grid_time/decision_time/replay_time/universe/sid_map/schemas/source_versions/adjustment_version/visibility_policy` |
| `snapshot.universe` | `version/investable/reference/tradable/evaluation/tracked/static` |
| `manifest` | `currency/code_revision/factor_plan_hash/universe_version/visibility_policy/execution_mode/latency_assumption/static_universe/combo/portfolio/labels/parameters/costs/snapshots` |
| `manifest.costs` | `fee_rate/slippage_rate/funding_policy` |
| `chunks[]` | `path/from/to` |

这些集合内部参数相互关联且可能较多，继续拆成数十个策略标量不利于维护，因此保留语义分组，只删除 factor 中间层。组内仍校验未知 key、NULL 和类型；策略的其他自定义字段仍属于开放 More。

`accounts.<账户名>` 可直接添加 `mode/store/history/sender_lease_dir/live_provider/funding_policy/instruments/margin_rate/max_account_margin/max_virtual_gross/strategy_gross_limit`。根 `execution` 提供公共默认值，账户字段覆盖相应默认值；具体入口仍决定 mode 等设置是否适用，普通因子回测模式由根 `execution.mode` 选择。新执行参数与原账户凭据在内部区分，不会被解码为交易所 API 凭据。

根 `data` 支持 `namespace/page_rows/prefetch_rows/page_bytes/archive/max_records/pit_policy`；database 仍提供连接设置。page_bytes 限制逻辑载荷而非 RSS，归档驱动不能套用存储订阅的分页覆盖。

## 4. 兼容边界

### More 自定义参数

无 engine 的旧时序策略继续保留开放 More，包括与新字段同名的 `id/account/capital_weight/factor/archive/config`。未知的自定义 engine 值也保留为 More。新时序策略需要身份、账户或预算时，应显式写 `engine: time_series`。已有标记为 config_version 2 的 v0.6 输入继续按其原身份/预算约定读取。

不能对任意自定义扩展承诺绝对零冲突：v0.5 More 允许任意 key，如果旧策略恰好把 `engine: factor` 或 `engine: time_series` 用作业务参数，这两个值现在是显式引擎选择。同名冲突需核对旧自定义语义；正式 v0.5 key 和通常省略 engine 的配置不需要更改。

### 启用新引擎的运行要求

纯旧时序、多策略的 stake sizing 保留，不强制资本权重。同账户加入因子策略或启用预算后，多个参与策略需全部显式声明 capital_weight，合计不超过 1；混合时序条目也应显式声明 engine。这是新混合运行的要求，不是加载旧 YAML 的前置条件。

因子采用一个决策周期，先取策略 run_timeframes，再取根列表，最后默认 1h；表达式显式 timeframe 必须匹配。时序多周期、refine_tf、逐标的调参和止损不自动等价于因子的目标组合执行语义。

普通因子 backtest 默认 events，可显式 weights，混合回测要求 events。实盘、funding、instrument 单位、价格和历史可见性仍需预检。取消版本标记没有取消这些校验。任意时序数据仍通过 `orm.DataSeries.Values map[string]any` 传递，具体类型与 NULL 语义保留。

### 合并和路径

原加载顺序保留；`run_policy/wallet_amounts/fatal_stop/watch_jobs/historical_coverage` 整块替换，不自动追加策略。accounts、execution、data 按映射覆盖。

归档、store/history/sender lease 相对路径按最终字段来源文件目录解析，账户路径为 `accounts.<名称>.*`，多文件不能都按最后一个文件定位。`@/$` 前缀按 DataDir，`:memory:` 保持特殊含义。Web 私有回测副本也使用这些规则，普通时序策略的同名 More 路径不会被当作因子归档改写。

## 5. 浅层示例

下面是覆盖已有市场、数据库、时间范围和已注册策略的片段，执行数据和风险能力仍需满足预检：

```yaml
accounts:
  user1:
    stake_rate: 1
    leverage: 2
    history: cold/user1.sqlite
    margin_rate: '0.1'
    # 原交易所 prod/test API 凭据仍在这里
execution:
  mode: events
  funding_policy: explicit-zero
run_policy:
  - name: Demo
    engine: time_series
    id: ts_demo
    account: user1
    capital_weight: 0.5
    run_timeframes: [5m]
    params: {atr: 15}
  - name: MyFactors
    engine: factor
    id: cs_alpha
    account: user1
    capital_weight: 0.5
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
      combine: {method: equal}
```

最新值存储研究需明确 `data.pit_policy: static-approximation`，严格 PIT 使用版本归档或具备证明的 provider。explicit-zero 是忽略 funding 的模拟假设，不是实际账户没有费用的证明。完整要求见[因子指南](../bandoc/zh-CN/guide/factor.md)。

## 6. 实现与验证依据

- [浅层兼容边界](../config/shallow.go)、[统一模型与导出](../config/unified.go)、[原字段和合并集合](../config/types.go)
- [来源和路径](../config/run_spec.go)、[只读加载与原子编辑](../config/migration.go)
- [统一 YAML 因子装配](../entry/factor_config.go)、[最终值来源](../entry/factor_resolved.go)、[Web 私有副本](../web/dev/api_dev.go)
- [v0.5 原模板及浅层回归](../config/shallow_test.go)、[entry 回放回归](../entry/factor_unified_test.go)、[Web 编辑与回测回归](../web/dev/config_editor_test.go)

验证覆盖原 v0.5 模板不修改加载、只读文件、More 保留、别名冲突、根账户凭据与执行覆盖、来源和导出重载。配置及内存回放测试不等于真实数据库或交易所实盘验收。最终执行的测试、静态检查与文档校验随本次变更报告记录。

本次新鲜验证：

- `go test ./config ./entry ./web/... ./opt ./factor/... -count=1 -timeout=180s` 全部通过；新增兼容入口和账户 provider 优先级回归再次定向通过。
- `go vet ./config ./entry ./web/... ./opt`、`go build ./config ./entry ./web/... ./opt` 通过；最终配置、entry、Web 开发入口的静态检查再次通过。
- VitePress 1.6.4 文档构建通过（12.57s）；主要指南的 30 个 YAML 示例和模板语法检查通过，对比文档 12 个本地链接均有效，diff whitespace 检查通过。
