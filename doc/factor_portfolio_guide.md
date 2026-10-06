# 组合策略、换仓与持仓生命周期

适用于 `v0.6.0-beta.6`。因子公式见 [表达式指南](factor_expression_guide.md)，高级研究 API 见 [研究扩展](factor_research_extensions.md)，源码与回归对应见 [实施记录](factor_opt_implementation.md)。

## 先区分三个周期

`run_timeframes` 推进因子计算；`rebalance` 决定普通调仓日程；`holding/transition` 决定持仓资格、退出曲线或批次寿命。它们独立配置。框架不猜测 `swapPerBars`/`holdBars` 的全局含义，也没有固定 16 小时或八步默认。

下面是已有市场、数据库、静态交易对池和 `time_range` 配置的覆盖片段，不能独立连接市场：

```yaml
execution: {mode: weights, funding_policy: explicit-zero}
data: {pit_policy: static-approximation}
run_policy:
  - name: momentum-vol
    id: rotation
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
    prices: {source: kline, timeframe: 1m, field: close}
    decision: {latency_ms: 1, expiry_ms: 120000}
    portfolio:
      long_notional: 1
      short_notional: 0
      policy: lifecycle-v1
      rebalance: {every_bars: 2}
      holding: {min_bars: 16}
      transition: {mode: linear-exit, exit_steps: 8, basis: quantity}
      allocation: {method: equal, reserve_ratio: 0.02}
    research: {labels: []}
```

每小时计算，每两小时普通调仓；首次真实成交满 16 小时后，只有排名落选才进入八步退出。如果首次减仓在第 16h，之后每 2h 一步，最后目标在第 30h 形成。新仓按当前策略 NAV 分配，尾仓先占预算；side notional 表示名义敞口占策略净值的比例，不是杠杆或账户总资金。

## 模式选择

| 模式 | 用途 | 必需/相关字段 |
| --- | --- | --- |
| direct | 调仓时直接收敛到目标 | holding 可限制普通退出 |
| linear-exit | 落选后按原始基准分 N 步归零 | exit_steps、basis: quantity 或 weight |
| cohort | 每轮新建一批，旧批按计划窗口到期 | period_bars，必须为 every_bars 整数倍 |
| geometric | 每步按剩余份额比例退出 | ratio、final_threshold、basis |
| target-step | 每步向新目标移动 alpha 比例 | alpha，取值 (0,1] |

quantity 锚定退出开始时的已确认标准资产数量并向零取整；价格和 NAV 上涨不会补回退出仓，外部风险已减得更低时继续保持更低数量。weight 锚定权重，每轮乘最新冻结 NAV，可能因净值变化形成数量回补。第 N 个线性目标显式为零；最低交易额、在途和部分成交可能延迟真正清仓。

`holding.max_bars/max_duration` 在到期后第一个监控网格直接形成零目标，绕过调仓门与普通退出曲线；想“满期后才开始渐退”使用 min。缺 score 不推进普通退出步骤，但仍检查最大期限和批次到期。反手默认等待原方向实际归零，未知在途不被当作已释放资金。

重入可用 `on_reselect: restore|resume|finish|new-cohort`；默认 restore 补回理想份额，resume 保留当前较小份额，finish 继续原退出。持仓清零后再次入场建立新生命周期；加仓不重置首次成交年龄。

## 固定寿命批次

将上例 holding/transition 替换为：

```yaml
rebalance: {every_bars: 2}
transition: {mode: cohort, period_bars: 16, startup: gradual, sizing: entry-nav}
```

gradual 每轮建立 1/8 预算的批次；seed-all 用当前排名一次建立八个具有不同到期窗口的初始批次。entry-nav 保持批次建仓数量，current-nav 显式重估活跃批次。两者新批都按当前 NAV 配置；cohort 的时间是计划批次窗口，不是所有成交订单统一持有十六小时。

同币批次先汇总净目标，到期与新建可以内部转移已确认贡献，不伪造两笔成交或手续费。计划八单位只成交四单位时，首批到期后的保留数量为 3.5，不会为旧批补成七单位。迟到成交按原计划归属。默认聚合贡献账不能替代严格独立 batch lot/PnL。

## 选股、覆盖和配置校验

`selection.long_k/short_k` 独立控制两侧，或使用 long_quantile/short_quantile。retain_rank 提供持仓排名缓冲，dropout 控制普通轮次替换数量，group_quota 使用调用者提供的可见分组。分位选择不要求旧 K；完整自定义 policy 不要求未使用的 K 或 side notional。显式旧 builder 继续校验自己的合同。

```yaml
holding:
  min_bars: 16
  max_bars: 48
  cooldown_bars: 2
  by_asset:
    'BTC/USDT:USDT': {min_bars: 24}
    'FAST/USDT:USDT': {min_bars: 4, max_bars: 12}
transition:
  mode: linear-exit
  basis: quantity
  exit_steps: 8
  by_asset:
    'FAST/USDT:USDT': {exit_steps: 2}
```

资产名来自统一数据 SIDMap；执行 ID 可以不同。每币显式零覆盖可取消对应限制。覆盖优先级为显式 by_asset → 当前可见 resolver 规则 → 默认值。bar 字段不接受浮点截断，未知/冲突/未使用的模式字段预检失败。已有仓位缺年龄证据时，有年龄限制的 policy 默认拒绝猜测；明确接管可用 `holding.adopt: adopt`，从接管时开始计龄。

日程支持 every_bars、绝对 anchor/phase、duration，或显式日/周/月民用 calendar、timezone、calendar_version；交易节假日及 score 变化触发由 Go 回调提供。周期不能同时混用。实际日历和未知资产身份变更不得静默迁移。

## Go 扩展与持久恢复

无状态 `RegisterPortfolioBuilder` 产生理想权重；显式 builder 优先，未配置 allocation 时保留其权重。`RegisterPortfolioPolicy(versionedName, factory)` 每 run 创建实例，`policy_params` 由工厂验证。policy 消费只读 `PortfolioContext` 和已接纳 JSON 状态，返回目标/下一状态/诊断；不能直接写账户或提交订单。

`Config.PolicyContext` 可注入可见分组、波动率、beta、ForceExit、HoldingRules/TransitionRules 和 RebalanceDue。inverse-volatility/vol-target、分组/beta 规则需相应证据，普通 YAML 不会自动把同名因子列当风险输入。参数 resolver 应使用 `ResolveParameters` 检查 manifest、hash、训练结束和发布时间。自定义完整 policy 可返回有界 opaque JSON，并遵守冻结目标身份、版本及账户风险合同。

新 `PortfolioTarget` allocation 明确区分 NAVFraction 与 AbsoluteQuantity，标准资产单位使用精确十进制。旧 `TargetPortfolio` 和旧 builder 仍为纯权重；数量目标不能通过旧 output/sink 丢失单位。JSON 输出使用版本化 allocation-decision/allocation-accepted，消费端需支持该合同。

owner 原子接纳执行计划、目标与 checkpoint，并校验 state version、ledger cursor 和 sequence。receipt 已接纳但 SendError 非空时，不回滚状态或生成新计划。近期缓存有界，历史重复通过持久记录校验、恢复原 frozen plan。启动先对账，随后加载匹配的 checkpoint 和成交来源；策略配置/schema 不兼容需要显式迁移。

热更新时仍有真实仓位、在途或活跃批次的资产保留价格/funding 订阅，仅加入 Tracked/执行范围，不重新加入排名/Reference/Evaluation。完全归零结算后释放。候选配置与状态私有，成功切换才生效。

## 回测、研究和示例

普通 `backtest` 的 weights 使用 Book 证据，events 使用共享 paper owner；live 使用真实账户证据。纯 CLI `research` 不创建持仓 Book，启用有状态 policy 会失败。做交易规则扫描使用 `ScanPortfolioTrials` 的隔离 weights，再将核验事件/观测交给研究 API；无状态因子预测期限研究可以直接使用多 horizon 的 research 命令。

banstrats 的 `examples/crosssection/lifecycle/` 提供可直接合并基础配置的模式示例、多期限研究与可执行 Go resolver 演示。模型/风险 builder、试验 ledger、HAC、容量和归因详见 [研究接口](factor_research_extensions.md)。当前没有自动线上训练服务、第三方 solver 或 CLI 逐批成交归因报告。
