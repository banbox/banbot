# 截面与多因子扩展实施记录

日期：2026-10-06。设计依据：[factor_opt.md](factor_opt.md)。实现保留未启用 policy 时的旧 builder、权重合同及配置身份；新增字段显式启用，未新增依赖、持仓账户或交易所特例。

发布版本：`v0.6.0-beta.6`。操作配置与模式选择见 [组合指南](factor_portfolio_guide.md)，发布验收见 [版本说明](release_v0.6.0-beta.6.md)。banstrats 配套提供 `examples/crosssection/lifecycle/` YAML 与 `policyresearch` 离线 Go 演示。

## 阶段与源码对应

| 阶段 | 实现 | 主要源码 | 验证 |
| --- | --- | --- | --- |
| P0a 契约与兼容 | 版本化不可变 allocation、Full/Patch、精确十进制数量、纯权重适配；独立日程；每 run 的策略工厂；规范化配置与策略 hash | `factor/allocation.go`、`policy_config.go`、`runner/policy_registry.go`、`research/portfolio_definition.go`、`entry/factor_config.go` | `factor/policy_test.go`、`entry/factor_policy_config_test.go`、原有 manifest/runner 回归 |
| P0b 期限与退出 | 真实首次成交计龄、min/max、每资产覆盖、排名缓冲、重入/冷却；linear-exit weight/quantity；绝对数量贯通 Book、JSON、paper/account | `factor/policy.go`、`runner/policy_run.go`、`runner/live_policy.go`、`backtest/policy.go`、`runner/account_policy_sink.go` | 八步归零、价格/NAV 波动、不回补风险减仓、缺 score、过期/重复/反手、replay/live parity |
| P0b 接纳与恢复 | owner 内原子提交计划/目标/checkpoint；状态与账本版本围栏；无交易状态事务；接纳后发送失败 receipt；恢复 sequence/成交来源；取消过期增仓 | `execution/policy_acceptance.go`、`policy_reconcile.go`、`strategy_rebalance.go` | memory/SQLite、幂等内容冲突、state-only、发送失败、checkpoint 恢复、共享真实订单取消 |
| P0b/P0c 订阅与代际 | 仅执行范围保留实际/在途/活跃批次价格与 funding；退出选股池不丢尾仓；归零结算后释放；候选配置私有；自定义 opaque 状态不按内置 schema 解码 | `runner/policy_scope.go`、`runner/live.go`、`runtime/shared_sources_update.go` | `runtime/policy_scope_test.go`、`runner/policy_scope_custom_test.go`、旧零 SID 完整目标接纳与配置副本 |
| P0c 批次与账户隔离 | cohort gradual/seed-all、entry/current NAV、失效入场窗口、原计划迟到 fill、同币贡献内部转移与净额；策略独立 checkpoint | `factor/policy_cohort.go`、`execution/policy_acceptance.go` | 计划 8 实际 4 到期目标 3.5、净额 8 的内部转移无虚构手续费、多个 CS 与 TS/CS 订单归属 |
| P1 选择与分配 | 两侧独立 K、分位、分组配额、retain/dropout；equal/score/inverse-volatility/fixed-notional/vol-target；现金保留、asset/group/net/beta cap、换手预算；geometric/target-step | `factor/policy_selection.go`、`policy_constraints.go` | 稳定 ties、缺 score 保留、约束冲突、风险退出绕过普通换手、模式差异 |
| P1 多期限与历史 | 多 horizon 独立执行价捕获/成熟/未完成统计；共享冻结 Frame；有界队列；RankIC/ICIR/EWMA、质量门槛与回退；排名自相关 | `runner/runner.go`、`research/combine.go`、`research/diagnostics.go` | `runner/multi_horizon_test.go`、成熟可见性/未来扰动/晚到长周期报告 |
| P1 稳健处理 | MAD winsorize、robust zscore、多暴露 OLS/WLS、原始字段分组表达式，Reference PIT 拟合与 QR 共线处理 | `factor/regression.go`、`operators.go`、`expr/compile.go` | `factor/robust_test.go`、`expr/robust_test.go` |
| P1/P2 参数与实验 | 隔离且有上限的组合扫描；组/全局参数收缩、PIT 参数产物；因子元信息、JSONL trial ledger、样本外比较、随机基线、有界 LRU | `runner/scan.go`、`research/experiment.go` | 扫描隔离、参数可见时点、hash、trial 冲突/损坏恢复、缓存失效 |
| P2 模型与风险 | 模型 factory/fit/predict、rolling/purge/embargo、发布恢复；ridge 基线；对角/收缩协方差及投影优化；可注册 immutable model/risk builders | `research/model.go`、`risk.go`、`runner/model_builder.go`、`risk_builder.go` | 参数 hash、未来产物隔离、模型恢复、PSD、不可行约束、可行性与收敛报告 |
| P1/P2 研究报告 | 生命周期时长/退出延迟、成本后收益、funding、容量/参与率、资本利用、目标/实际换手、benchmark/阶段归因、HAC | `research/lifecycle.go`、`experiment.go` | `research/extensions_test.go` 的确定性数值与边界场景 |

## 使用方式与扩展合同

普通用户在 `run_policy[].portfolio` 选择 `policy: lifecycle-v1`，分别配置 `rebalance`、`selection`、`holding`、`transition`、`allocation`。示例见 [用户指南](../bandoc/zh-CN/guide/factor.md)。bar 是基础决策周期，持仓年龄从真实 fill 起算；max 在到期后的第一个监控网格强制形成零目标，实际清仓仍由 execution 收敛。缺完整 score 的网格也检查最长持仓与批次到期，不消耗普通排名退出轮次。

`swapPerBars` 和 `holdBars` 不作为全局猜测别名：分别使用独立调仓间隔、最短/最长持仓或批次周期。数量退出显式选 `basis: quantity`；weight 退出允许随净值重新估值。每批 16h/每次 2h 与持仓满 16h 后落选再八次退出是两种配置。

原 `RegisterPortfolioBuilder` 保留。显式 builder 决定理想组合；未指定 allocation 时保留其权重。`RegisterPortfolioPolicy(versionedName, factory)` 为每 run 创建完整自定义策略；参数可用 `policy_params`，factory 自行验证 schema。内置配置的校验不强制套入自定义策略。自定义状态可以是任何合法有界 JSON，接纳仍须遵守只读证据、冻结目标身份、版本围栏及原子 checkpoint 合同。

分位 selector 不要求旧 TopK 的 K；完整自定义 policy 不要求未使用的 K 或 long/short_notional，并可自行处理选股和配置。显式选择旧 TopK builder 时仍检查旧契约。对应回归为 `TestCustomPolicyAndQuantilesDoNotRequireLegacyK`。

`Config.PolicyContext` 可填入可见分组、波动率、beta、风险强制退出、调度判断和参数 resolver 的 `HoldingRules/TransitionRules`。显式 `by_asset` 优先于 resolver，再回退默认。配置资产名使用 SIDMap 的统一数据 symbol，执行 instrument ID 仅用于缺失 symbol 时的回退。resolver 应先用 `ResolveParameters` 校验产物 hash、训练截止和可用时间，不把全样本最优参数带入过去。

实盘 sink 的近期接纳缓存限定 64 条指纹；更早的重复提案通过账户持久接纳记录校验内容并恢复原计划，不重算当前报价、不回滚最新 checkpoint。淘汰缓存、超过有效期和冷 wrapper 恢复均有专项测试。

日历内置为日/周/月民用日历，显式时区和 calendar version；节假日、交易日、score 变化等调度通过 `RebalanceDue` 回调实现。cohort 内置要求 period 为 every_bars 的整数倍。不同资产的异构批次寿命、fill-based 批次寿命可使用自定义 policy；方案把它们列为后续/可选扩展，未作为内置默认。

多 horizon 通过原 `Manifest.Labels` 提供，历史 combiner 可指定 `Combo.Label`。多标签默认选择最短期限并纳入 manifest；单标签隐式身份继续兼容。live 未接实时成熟历史 provider，仍拒绝所有 history 方法。

P2 模型、风险优化、报告和实验提供可执行 Go API 及注册 builder，详细用法见 [研究扩展接口](factor_research_extensions.md)。普通用户无需 ML/求解器依赖。生命周期/归因报告接收调用者提供的可核验事件；当前 CLI 不自动构造完整逐批成交归因。

## 已保留的边界

- cohort 记录聚合执行贡献，内部净额转移守恒，不伪造成交与费用；独立 batch execution lot、严格逐批成交期限/PnL 是方案单独标注的扩展。
- 模型为 ridge/OLS 基线，优化器为有界投影算法；收敛和可行性分别报告。第三方 ML 后端、求解器与线上训练平台可注册扩展，不属于本次基础实现。
- 不提供未经样本外验证的“每币最佳 16h”。扫描、收缩与产物可见性是研究工具，收益和成本模型有效性需真实数据验证。
- 原始数据继续通过 `DataSeries.Values map[string]any`，保留自定义字段、类型及 NULL；没有 QuestDB 写入/替表逻辑变更。
- 未测真实交易所、外部数据库及全量性能基准；不声称更快或实盘可靠性已有外部验证。

## 验证记录

专项测试覆盖上述功能，并补充 replay/live 同输入逐轮 allocation 一致、live config 深复制、custom opaque checkpoint 恢复、settled SID 释放后的后续 Full 目标及多策略隔离。最终整体测试、静态检查和构建结果在本节记录。

最终验证环境为 Go 1.26.8 / Windows amd64，结果：

- 全部可构建仓库包的 `go test` 通过（包括 factor、execution、runtime、entry 及其余业务包；最后 runtime 集成测试 135.032s）。通过 `go list -e` 收集包，唯一排除 `tmp/legacy-dualma-replay`。
- `go vet ./factor/... ./execution/... ./runtime/... ./config/... ./entry/...` 通过。
- `go build .` 主程序构建通过。
- 在 `bandoc` 执行 `npm.cmd run build`，文档链接检查与 VitePress 构建通过。
- 改动 Go 文件的 gofmt、`git diff --check` 和实施文档本地链接检查通过；测试产生的 `go.work.sum` 改动已还原，无依赖变更。

直接执行 `go test ./...` 会包含临时目录 `tmp/legacy-dualma-replay`，其引用的 `github.com/banbox/banstrats/ma` 当前不在模块中，因而 setup failed。没有为此临时实验添加依赖；排除该目录后的最终仓库测试 exit 0。

Windows 环境 `CGO_ENABLED=0` 且无 C 编译器，`go test -race` 无法运行；并发路径经过副本/锁/原子 scope 复核与确定性测试，race 验证仍需具备 cgo 的环境。

### beta.6 发布验收（2026-10-06）

禁用本机 workspace（`GOWORK=off`）并使用已发布的 banexg v0.2.65、banta v0.4.1 后，34 个正式包测试通过，`go mod verify` 通过，Windows 与 Linux amd64 主程序构建通过，版本输出为 `banbot v0.6.0-beta.6`。中英文 bandoc 构建通过；banstrats 全部截面示例测试与 vet 通过。示例回放发现的 ComboSpec YAML 字段命名回归已修复，并增加序列化测试。

正式包 `go vet -composites=false` 与其余 33 包的默认 vet 通过；`strat` 默认 vet 的既有 `_testcom/all.go` 外部 Kline 未命名字段告警保留。临时实验目录及 race 的限制同上，完整发布说明见 [beta.6](release_v0.6.0-beta.6.md)。后端版本升级，UI 沿用 beta.5。
