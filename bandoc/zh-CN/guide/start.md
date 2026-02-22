banbot是一个高性能、易用、多品种、多策略、多周期、多账户的事件驱动交易机器人。支持回测、超参数调优和实盘交易。
目前支持币安、欧易、bybit交易所，可通过WebUI查看机器人。
::: danger 免责声明
本软件仅用于研究和教育目的，请勿投入超过您承受能力的资金。使用本软件风险自负。作者和所有关联方对您的交易结果不承担任何责任。  

请先进行充分回测、滚动测试、模拟实时交易后，再考虑小金额运行交易机器人，在了解其工作原理以及应预期的利润/损失之前，不要投入资金。  

我们强烈建议您具备基本的golang编程知识，以及通过阅读源代码了解机器人的运行机制。
:::

## 主要特征
* 高性能：1秒回测1年的数据（基于5m测试，具体耗时取决于策略特点）
* 易用：一个版本的策略，同时支持回测、模拟实时交易和实盘
* 灵活：自由组合不同的品种、策略和时间周期
* 事件驱动：杜绝未来信息，更自由地实现您的策略逻辑
* 规模化：可将一批策略同时应用到多个交易所账户
* 超参数调优：支持bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
* 动态品种：策略运行过程中可即时增删交易品种，无需重启
* websocket：交易所逐笔交易+订单簿数据
* 指标库：内置[banta](https://github.com/banbox/banta)高性能指标库，您可基于其快速开发自定义指标
* LLM集成：内置大模型调用与管理能力（多模型故障转移、并发控制、统计追踪等）

## WebUI
用于策略研究回测，仅推荐在本地启动，无密码

![image](https://docs.banbot.site/uidev.gif)

## Dashboard UI
用于管理实盘机器人，推荐部署在离交易所更近的服务器，需密码访问

![image](https://docs.banbot.site/dashboard.gif)

## 支持的交易所
基于[banexg](https://github.com/banbox/banexg)提供的统一接口，目前支持币安、欧易、bybit交易所。

> 我们支持了[agent工作流脚本](https://github.com/banbox/banexg/blob/main/docs/dev_exg.py)，可基于codex/claude code的实现交易所自动化开发对接。如果您有其他交易所需求，并且有一定开发经验，可使用此脚本快速完成开发。

| logo                                                                                                            | id      | name              | ver | websocket | 
|-----------------------------------------------------------------------------------------------------------------|---------|-------------------|-----|-----------|
| ![binance](https://user-images.githubusercontent.com/1294454/29604020-d5483cdc-87ee-11e7-94c7-d1a8d9169293.jpg) | binance | spot/usd-m/coin-m | *   | Y         |
| ![okx](https://user-images.githubusercontent.com/1294454/152485636-38b19e4a-bece-4dec-979a-5982859ffc04.jpg) | okx | spot/usd-m/coin-m | *   | Y         |
| ![bybit](https://private-user-images.githubusercontent.com/81727607/382500134-97a5d0b3-de10-423d-90e1-6620960025ed.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NjkyNTM1NjgsIm5iZiI6MTc2OTI1MzI2OCwicGF0aCI6Ii84MTcyNzYwNy8zODI1MDAxMzQtOTdhNWQwYjMtZGUxMC00MjNkLTkwZTEtNjYyMDk2MDAyNWVkLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNjAxMjQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjYwMTI0VDExMTQyOFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQwODM4YThmMTU2ZmIyMGI1YjRmYWU0MGVkYzJhN2YyMmYzYzhmNTJjZDM1YzFmYzdjNGRlMGY4OTlmM2RmODMmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.3I9CvGTqWpZcZaDBV0_tFQFbOoPyCOxaZ1c6o7q6tMQ) | bybit | spot/usd-m/coin-m | *   | Y         |

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
此配置可同时部署数据库和机器人，每个机器人单个账户占用资源极小，您可在单个机器人进程中配置数百个账户。
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
