# v0.6.0-beta.6

本次预发布补充截面组合的持仓生命周期与经典多因子研究能力。普通 `backtest`/`trade` 的 YAML 入口继续保留，省略 policy 时沿用原 builder、权重目标与配置身份。

## 组合与执行

- 显式 `portfolio.policy: lifecycle-v1` 分离调仓、选股、持仓期限、过渡与资金配置，支持 direct、linear-exit、cohort、geometric、target-step。没有写死 16 小时或八步规则。
- 支持真实首次成交计龄、min/max、排名缓冲/dropout、分位/分组配额、每币覆盖、重入和冷却。新仓按当前策略 NAV 配置，尾仓先占预算。
- 新版本化 allocation 目标区分 NAV 权重与精确绝对资产数量。quantity 退出不会在 NAV 上涨或外部风险减仓后买回；旧纯权重合同保留，消费端不得静默丢失数量语义。
- cohort 支持 gradual/seed-all、entry/current NAV、未完成入场对账和迟到 fill 来源；同币批次聚合净目标，内部贡献转移不伪造成交或手续费。
- 共享账户 owner 原子提交计划、目标与策略 checkpoint，校验状态版本、账本游标及单调 sequence。receipt 区分接纳与发送失败；恢复原计划并保留未知发送状态。近期缓存有界，历史重试从持久记录验证。
- 实盘更新自动保留持仓、在途和活跃批次所需的价格/funding 订阅，仅加入执行范围，完全归零结算后释放。候选状态私有，自定义 opaque JSON checkpoint 可以恢复。
- 原 builder 继续扩展理想组合，版本化 policy factory 每 run 创建独立实例；Go PolicyContext 可提供可见规则、分组、风险输入和自定义日程。分位和完整自定义 policy 不再受未使用的旧 K 限制。

## 多因子研究

- 多 executable-return horizon 共用冻结 Frame，独立捕获价格、成熟与 unresolved，有界队列；历史合成可指定 label。
- 新增历史 RankIC/ICIR/RankICIR/EWMA、样本/质量门槛与显式回退；新增排名自相关。所有历史方法仍要求成熟可见标签，live 缺 provider 时拒绝。
- 新增 MAD winsorize、robust zscore、多暴露 OLS/WLS、原始字段分组表达式，Reference 时点拟合保留 NULL 与类型语义。
- 原生 Go 接口提供有界组合扫描、PIT 参数收缩产物、因子元信息、trial ledger、随机基线、LRU，以及生命周期/成本/funding/容量/HAC/归因诊断。
- 模型 factory/fit/predict、滚动/purge/embargo、模型发布恢复、对角/收缩协方差和风险优化 builder 提供可扩展基线。未新增 ML 或 solver 依赖。

## 文档、示例与升级

中英文 bandoc 指南/API、表达式/实盘与双引擎文档已更新；详见 [组合指南](https://github.com/banbox/banbot/blob/v0.6.0-beta.6/doc/factor_portfolio_guide.md)、[研究接口](https://github.com/banbox/banbot/blob/v0.6.0-beta.6/doc/factor_research_extensions.md)、[实施记录](https://github.com/banbox/banbot/blob/v0.6.0-beta.6/doc/factor_opt_implementation.md)。配套 banstrats 的 `examples/crosssection/lifecycle/` 提供模式与多期限覆盖配置，`policyresearch` 提供离线 Go 参数 resolver 演示。

`swapPerBars/holdBars` 不作为全局猜测别名，使用明确的 every_bars、min/max_bars 或 period_bars。未知、冲突和模式未使用字段预检失败。新 quantity 输出使用 allocation-decision/allocation-accepted；旧输出消费者需要支持新合同才能消费数量目标。

后端 `Version` 为 **v0.6.0-beta.6**。本次没有修改 `web/ui`，`UIVersion` 保持 **v0.6.0-beta.5**，界面继续使用该版本 release 的 `dist.zip`。最低 Go 为 1.26.0，推荐工具链 1.26.8；发布验证禁用本机 go.work，banexg v0.2.65、banta v0.4.1 使用远程模块。

## 验证与边界

发布验收使用 `GOWORK=off`：34 个正式 Go 包测试通过，模块校验通过，Windows 与 Linux amd64 构建通过，Windows 可执行程序输出 `banbot v0.6.0-beta.6`。中英文 bandoc 构建通过。配套 banstrats 截面示例测试、vet 与离线参数演示通过；修复了组合配置 YAML 序列化的新字段名称并增加回归测试。

正式包 `go vet -composites=false` 通过，其余 33 个包的默认 vet 通过；`strat` 的默认 vet 仍报告既有 `_testcom/all.go` 中外部 Kline 测试夹具的未命名字段。未将被忽略的 `tmp/legacy-dualma-replay` 临时实验目录作为发布包，该目录缺少自己的 banstrats/ma 依赖。上述限制不通过添加发布依赖掩盖。

本版本仍为 beta。真实交易所、外部数据库和完整性能基准未验收；Windows CGO 关闭且无 C 编译器，race 未运行。模型为 ridge/OLS 基线，风险优化为有界投影算法；可行性与收敛分别报告。高级研究报告消费调用者提供的可核验事件，不自动采集 CLI 独立批次成交 PnL。cohort 聚合贡献账、交易节假日日历、异构/fill-based 批次的扩展边界见实施记录。

源码对比：[v0.6.0-beta.5 → v0.6.0-beta.6](https://github.com/banbox/banbot/compare/v0.6.0-beta.5...v0.6.0-beta.6)。
