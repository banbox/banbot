# banbot 文档索引

面向用户的完整文档在 [banbot.site](https://www.banbot.site/)（源码见 `bandoc/`）。本目录是仓库内的开发者与 AI 参考资料。

## AI 知识库（附给 AI 对话即可生成策略）

| 文件 | 内容 |
|------|------|
| [banbot_ts_cn.md](banbot_ts_cn.md) | 时序策略引擎（`engine: time_series`）接口与规则 |
| [banbot_factor_cn.md](banbot_factor_cn.md) | 截面因子 / 多因子策略（`engine: factor`） |
| [banbot_statarb_cn.md](banbot_statarb_cn.md) | 统计套利、配对策略与研究运行合同 |
| [banbot.md](banbot.md) | 英文版（基线 v0.6.0-beta.8，尚未随中文三篇更新） |

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
