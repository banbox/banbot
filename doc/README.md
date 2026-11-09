# banbot 文档索引

面向用户的完整文档在 [banbot.site](https://www.banbot.site/)（源码见 `bandoc/`）。本目录是仓库内的开发者与 AI 参考资料。

## 项目总览与模块文档

从 [project/README.md](project/README.md) 开始：项目整体概况，以及 `project/01~08` 按模块拆分的实现文档（文件地图、核心类型、关键流程、修改指南、已知问题）。

## AI 知识库（附给 AI 对话即可生成策略）

| 文件 | 内容 |
|------|------|
| [kb_ts_cn.md](../kb/kb_ts_cn.md) / [en](../kb/kb_ts_en.md) | 时序策略引擎（`engine: time_series`）接口与规则 |
| [kb_factor_cn.md](../kb/kb_factor_cn.md) / [en](../kb/kb_factor_en.md) | 截面因子 / 多因子策略（`engine: factor`） |
| [kb_statarb_cn.md](../kb/kb_statarb_cn.md) / [en](../kb/kb_statarb_en.md) | 统计套利、配对策略与研究运行合同 |
| [banbot.md](banbot.md) | 旧版英文单文件（基线 v0.6.0-beta.8，已被 `kb_*_en.md` 取代） |

知识库文件位于 `kb/` 目录，并通过 `go:embed` 内置于 Go 包 `github.com/banbox/banbot/kb`，可在代码中读取：

```go
// 第一个参数 "cn" | "en"；其后至少一个主题名："ts" | "factor" | "statarb"
text, err := kb.GetKB("cn", "ts", "factor")
```

## 架构与实现

| 文件 | 内容 |
|------|------|
| [app_arch.md](app_arch.md) | 后端入口、业务流程、存储与模块交互 |
| [runtime_context.md](runtime_context.md) | `Process -> Runtime` 运行时所有权与生命周期 |
| [db.md](db.md) | QuestDB / TimescaleDB 双后端、WAL 规则、表替换 |
| [banio.md](banio.md) | BanIO TCP 通信层 |
| [help_ui.md](help_ui.md) | WebUI（SvelteKit）项目结构与风格 |
| [contribute.md](contribute.md) | 贡献规范与常见开发问题 |

## 时序数据

| 文件 | 内容 |
|------|------|
| [series_usage.md](series_usage.md) | 定义、读写、订阅任意时序数据 |
| [custom_data.md](custom_data.md) | 自定义数据源接入的完整实现说明 |

## 因子与统计套利

| 文件 | 内容 |
|------|------|
| [factor_expression_guide.md](factor_expression_guide.md) | 表达式与 Go builder 使用指南 |
| [factor_expression_architecture.md](factor_expression_architecture.md) | 表达式编译与执行的设计原理 |
| [factor_indicators.md](factor_indicators.md) | 因子图内置技术指标 |
| [factor_portfolio_guide.md](factor_portfolio_guide.md) | 组合、换仓与持仓生命周期 |
| [factor_research_extensions.md](factor_research_extensions.md) | 研究扩展与原生 Go 接口 |
| [factor_live_trading.md](factor_live_trading.md) | 因子策略实盘 |
| [statarb_guide.md](statarb_guide.md) | 统计套利研究与运行指南 |

## 配置、运维与参考

| 文件 | 内容 |
|------|------|
| [config.yml](config.yml) | 完整配置参考模板 |
| [config_compatibility.md](config_compatibility.md) | v0.5 → 双引擎 YAML 配置对比 |
| [usage.md](usage.md) | 常见问题（滑点、实盘回测对比、邮件通知、优化器选择） |
| [check_bot.sh](check_bot.sh) | 机器人存活检查脚本（见实盘文档） |
| [aifea.proto](aifea.proto) | AI 特征 gRPC 协议 |
| [ulcer_metrics_sources.md](ulcer_metrics_sources.md) | Ulcer Index / UPI 公式依据 |

## 维护约定

- 文档描述当前实现；一次性方案、实施记录、评审记录不入库（历史查 git log）。
- 不在文档中写版本验证流水账，也不用「某日更新」类章节。
