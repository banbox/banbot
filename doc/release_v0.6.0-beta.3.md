# v0.6.0-beta.3

本次预发布以 **v0.6.0-beta.2** 为基线，统一时序与因子策略的启动入口，并补充混合实盘装配。

## 统一策略命令

- `backtest` 和 `trade` 读取同一份 YAML `run_policy`，按配置选择时序、因子或混合引擎。
- 因子研究使用根命令 `research`；版本记录归档使用 `data archive`；表达式检查与解释使用根命令 `validate` 和 `explain`。
- 因子回测模式按 `--mode weights|events`、`execution.mode`、默认 `events` 的顺序确定。混合回放必须使用 `events`，纯时序任务保留已有回测行为。
- 因子或混合配置的 `trade --dry-run` 使用历史数据进行 `events` 回放；纯时序实时模拟继续使用 YAML `env: dry_run`。
- 统一因子回测输出目录、JSON lines 和最终结果，并补充 CLI 日志、性能剖析及取消上下文的传递。

## 混合实盘与运行隔离

- 为同一账户的时序与因子策略自动装配共享订单桥、策略资本预算和订单归因，并校验配置中的策略是否完整绑定。
- 混合配置中的独立时序账户单独启动，保留旧时序策略在未指定账户时展开到活跃生产账户的行为。
- 启动与资源清理使用任务取消上下文；补充账户装配、取消、失败清理及统一命令的回归覆盖。
- 修复事件订阅的 `event` timeframe 被误当作固定时间周期解析的问题。

## 配置与升级

策略配置统一使用 YAML，不再接受旧 runner JSON 配置、JSON 导入入口或 `run_policy[].config` 包装。因子字段直接写在 `run_policy[]`，账户执行覆盖放在根 `accounts`，公共执行默认值放在 `execution`。

| 旧入口 | 新入口 |
| --- | --- |
| `factor backtest` | `backtest` |
| `factor trade` | `trade` |
| `factor research` | `research` |
| `factor archive` | `data archive` |
| `factor validate` / `factor explain` | `validate` / `explain` |

**原 `factor backtest` 默认使用 `weights`；新统一入口默认使用 `events`。** 如需保留近似权重账本行为，请显式传入 `--mode weights` 或配置 `execution.mode: weights`。旧 `--factor-config` 与 `entry.NewFactorCommandWithSink` 已移除，使用旧入口的脚本或嵌入程序需要更新。

更新中英文指南、API 文档、配置示例及架构说明，并新增 WebUI 与 DashboardUI 双引擎接入改造计划；该计划描述后续工作，不代表本版已实现相关界面和 API。

后端版本为 `v0.6.0-beta.3`；前端源码没有变化，`UIVersion` 保持 `v0.6.0-beta.1`，继续使用该版本已发布的 `dist.zip`。

## 验证

- 使用 `GOWORK=off` 和已发布的远程依赖完成 34 个正式 Go 包测试；同步移除旧 JSON wrapper 的 Web 路径测试 fixture 后，失败用例及 `web/dev` 完整测试复验通过。测试排除被 Git 忽略的临时回放目录。
- 正式 Go 包的 `go vet`、程序构建、模块校验、Go 格式检查及 `git diff --check` 通过。
- 主程序报告 `banbot v0.6.0-beta.3`，统一策略命令、研究、归档及表达式命令的帮助入口检查通过。
- 中英文 VitePress 文档构建通过。Windows Go 1.25.1 构建使用 `-ldflags=-checklinkname=0`。

## 已知限制

本版本为预发布，真实交易所混合实盘端到端验收尚未覆盖。因子与混合实盘仍需当前会话已验证的 live binding、账户、历史数据和资金费能力证据；能力不足时明确拒绝启动，不自动降级为 paper。

完整源码对比：[v0.6.0-beta.2 → v0.6.0-beta.3](https://github.com/banbox/banbot/compare/v0.6.0-beta.2...v0.6.0-beta.3)。从 v0.5.7 升级的完整背景见 [v0.6.0-beta.2 发布说明](release_v0.6.0-beta.2.md)。
