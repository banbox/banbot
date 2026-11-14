# Banbot 使用指南

banbot是一个高性能、易用、多品种、多策略、多周期、多账户的事件驱动交易机器人。支持回测、超参数调优和实盘交易。
目前支持币安、欧易、bybit交易所，可通过WebUI查看机器人。
::: danger 免责声明
本软件仅用于研究和教育目的，请勿投入超过您承受能力的资金。使用本软件风险自负。作者和所有关联方对您的交易结果不承担任何责任。  

请先进行充分回测、滚动测试、模拟实时交易后，再考虑小金额运行交易机器人，在了解其工作原理以及应预期的利润/损失之前，不要投入资金。  

我们强烈建议您具备基本的golang编程知识，以及通过阅读源代码了解机器人的运行机制。
:::

## 按目标阅读

| 您要做什么 | 阅读入口 |
| --- | --- |
| 安装并完成第一次回测 | [快速开始](./quick_local.md)、[配置](./configuration.md)、[回测](./backtest.md) |
| 编写 Go 时序策略或接入自定义数据 | [初始化项目](./init_project.md)、[自定义策略](./strat_custom.md)、[自定义时序数据](./custom_data.md) |
| 研究截面因子、配对与组合 | [多因子与截面](./factor.md) |
| 运行模拟盘、实盘与共享账户 | [实时交易](./live_trading.md) |
| 管理数据库与历史行情 | [数据库](./database.md)、[数据工具](../advanced/kline_tools.md) |
| 查询命令与开发接口 | [命令行](./bot_usage.md)、[API](../api/index.md) |

## 主要特征
* 高性能：Go 与 banta 缓存指标计算状态；实际耗时取决于策略、数据、周期及品种规模
* 易用：一个版本的策略，同时支持回测、模拟实时交易和实盘
* 灵活：自由组合不同的品种、策略和时间周期
* 事件驱动：按数据可见时间执行；自定义数据仍需正确提供发布时间和修订信息，避免未来数据泄漏
* 规模化：可将一批策略同时应用到多个交易所账户
* 超参数调优：支持bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
* 动态品种：策略运行过程中可即时增删交易品种，无需重启
* websocket：交易所逐笔交易+订单簿数据
* 指标库：内置[banta](https://github.com/banbox/banta)高性能指标库，您可基于其快速开发自定义指标
* 任意时序数据：K 线扩展列、第三方序列及类型/NULL 语义统一传递，见[自定义数据](./custom_data.md)
* 双策略引擎：时序回调、多因子截面组合、配对统计套利及混合 events 回放，见[因子指南](./factor.md)
* LLM 扩展：提供可由策略项目主动调用的模型管理库，见[AI](../advanced/ai.md)

## WebUI
用于策略编辑、数据管理和回测。`bot web` 默认本机 `127.0.0.1:8000`，可不设密码；绑定 `0.0.0.0` 或 `::` 必须传 `--password`（Basic Auth 用户名为 `banbot`）。

![image](https://docs.banbot.site/uidev.gif)

## Dashboard UI
用于管理实盘机器人，推荐部署在离交易所更近的服务器，需密码访问

![image](https://docs.banbot.site/dashboard.gif)

## 支持的交易所
基于[banexg](https://github.com/banbox/banexg)提供的统一接口，目前支持币安、欧易、bybit交易所。

> 我们支持了[agent工作流脚本](https://github.com/banbox/banexg/blob/main/docs/dev_exg.py)，可基于codex/claude code的实现交易所自动化开发对接。如果您有其他交易所需求，并且有一定开发经验，可使用此脚本快速完成开发。

| id | 交易所 |
| --- | --- |
| `binance` | 币安 |
| `okx` | 欧易 |
| `bybit` | Bybit |

数据查询和历史回测的市场能力由 banexg 适配器决定。当前默认 Hub 实时执行只接受 `prod`/`dry_run` 的线性永续账户，不能把适配器的现货、反向、testnet 或期权能力直接当作默认实盘入口支持；见[共享账户与 Hub](./execution.md)。符号命名示例不代表所有交易所都支持该市场；中国市场目录是设计资料，并非已交付的交易所接入。

## 技能要求
* Linux/Windows基本操作（环境变量、命令行、编辑配置文件）
* golang语言基础（[快速入门](https://go.dev/tour/welcome/2)）
* 量化交易基础知识

::: tip tip
建议您通过DeepSeek快速学习相关基础知识
:::

## 安装要求
### 硬件要求
* CPU：2核
* 内存：2GB
* 磁盘：10GB

**本地策略研究**推荐不低于上面配置。

**实盘部署**推荐直接使用上面配置的linux云服务器。

::: tip tip
以上可作为小规模试用的起点。多品种、多账户、因子面板和长区间回放的内存、存储需求取决于数据规模，应按实际负载评估。
:::

## Q & A
**策略项目和banbot有什么区别？我需要拉取banbot源代码吗？**

您不需要拉取banbot的源码，只需在您的策略项目中通过`go get`引用banbot最新版本即可。  
策略项目就是您维护所有策略代码的go项目，也是直接用来`go build`编译为单个可执行文件的地方。

**文档看不明白怎么办？**

您可下载此文档的[github仓库](https://github.com/banbox/bandoc/)，然后使用cursor打开，在chat中@codebase，提问你想知道的任意内容；cursor将会结合文档的所有内容，对您做出更容易理解的解释。

**[更多常见问题](./faq.md)**

## 社区支持
欢迎加入群聊，交流问题、经验、灵感和策略。  
扫描微信绿色二维码请备注banbot。
<p style="display:flex;gap:20px;justify-content:center">
<img style="width:180px" src="/img/wechat_banbot.jpg"/>
<img style="width:180px" src="/img/telegram_banbot.png"/>
<img style="width:180px" src="/img/discord_banbot.png"/>
</p>

### 社群链接
[Discord服务器](https://discord.com/invite/XXjA8ctqga)

[telegram群组](https://t.me/banbot_quant)

## 多因子与截面引擎

Banbot 支持 time_series 和 factor 双引擎：前者按品种事件驱动，后者每轮冻结 Universe，计算多列因子、截面变换和组合目标。两者可在同一 run_policy 中组合，省略 engine 保持时序行为。

参见[多因子与截面指南](./factor.md)和[因子 API](../api/factor.md)。
