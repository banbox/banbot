Banbot is a high-performance, easy-to-use, multi-variety, multi-strategy, multi-period, multi-account event-driven trading robot. It supports backtesting, hyperparameter tuning, and real-time trading.
Currently supports Binance, OKX, and Bybit Exchange, and the robot can be viewed through the WebUI.
::: danger Disclaimer
This software is for research and educational purposes only. Do not invest more money than you can afford. Use this software at your own risk. The author and all related parties are not responsible for your trading results.

Please conduct sufficient backtesting, rolling testing, and simulated real-time trading before considering running the trading robot with a small amount. Do not invest money before understanding how it works and the expected profit/loss.

We strongly recommend that you have basic golang programming knowledge and understand the robot's operating mechanism by reading the source code.
:::

## Main features
* High performance: 1 second backtesting 1 year of data (based on 5m test, the specific time depends on the characteristics of the strategy)
* Easy to use: one version of the strategy, supporting backtesting, simulated real-time trading and real trading at the same time
* Flexible: Freely combine different symbols, strategies and time periods
* Event-driven: No lookahead and implement your strategy logic more freely
* Scalability: A batch of strategies can be applied to multiple exchange accounts at the same time
* Hyperparameter tuning: Support bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
* Dynamic symbols: Add or remove trading symbols on the fly during strategy execution, no restart required
* Websocket: Exchange transaction by transaction + order book data
* Indicator library: Built-in [banta](https://github.com/banbox/banta) high-performance indicator library, you can quickly develop custom indicators based on it
* LLM Integration: Built-in large language model invocation and management capabilities (multi-model failover, concurrency control, statistics tracking, etc.)

## WebUI
For strategy research backtesting, it is recommended to start locally only, without a password.

![image](https://docs.banbot.site/uidev.gif)

## Dashboard UI
For managing live trading robots, it is recommended to deploy on servers closer to the exchange, with password access required.

![image](https://docs.banbot.site/dashboard.gif)

## Supported exchanges
Based on the unified interface provided by [banexg](https://github.com/banbox/banexg), currently supports Binance, OKX, and Bybit Exchange.

> We support [agent workflow script](https://github.com/banbox/banexg/blob/main/docs/dev_exg.py), which enables automated exchange integration based on codex/claude code. If you need support for other exchanges and have some development experience, you can use this script to quickly complete the development.

| logo                                                                                                            | id      | name              | ver | websocket |
|-----------------------------------------------------------------------------------------------------------------|---------|-------------------|-----|-----------|
| ![binance](https://user-images.githubusercontent.com/1294454/29604020-d5483cdc-87ee-11e7-94c7-d1a8d9169293.jpg) | binance | spot/usd-m/coin-m | *   | Y         |
| ![okx](https://user-images.githubusercontent.com/1294454/152485636-38b19e4a-bece-4dec-979a-5982859ffc04.jpg) | okx | spot/usd-m/coin-m | *   | Y         |
| ![bybit](https://private-user-images.githubusercontent.com/81727607/382500134-97a5d0b3-de10-423d-90e1-6620960025ed.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NjkyNTM1NjgsIm5iZiI6MTc2OTI1MzI2OCwicGF0aCI6Ii84MTcyNzYwNy8zODI1MDAxMzQtOTdhNWQwYjMtZGUxMC00MjNkLTkwZTEtNjYyMDk2MDAyNWVkLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNjAxMjQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjYwMTI0VDExMTQyOFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQwODM4YThmMTU2ZmIyMGI1YjRmYWU0MGVkYzJhN2YyMmYzYzhmNTJjZDM1YzFmYzdjNGRlMGY4OTlmM2RmODMmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.3I9CvGTqWpZcZaDBV0_tFQFbOoPyCOxaZ1c6o7q6tMQ) | bybit | spot/usd-m/coin-m | *   | Y         |

## Skill Requirements
* Basic Linux/Windows operations (environment variables, command line, editing configuration files)
* Fundamentals of the Go language ([Quick Start](https://go.dev/tour/welcome/2))
* Basic knowledge of quantitative trading

::: tip tip
It is recommended that you quickly learn the relevant basics through DeepSeek
:::

## Installation Requirements
### Hardware Requirements
* CPU: 2 cores
* Memory: 2GB
* Disk: 10GB

**Local Strategy Research** is recommended to have a configuration no lower than the one mentioned above.

**Live Deployment** is recommended to directly use the Linux cloud server with the configuration mentioned above.

::: tip tip
This configuration allows for the simultaneous deployment of both the database and the bot. Each bot account occupies minimal resources, enabling you to configure hundreds of accounts within a single bot process.
:::

## Q & A
**What is the difference between the strategy project and banbot? Do I need to pull the banbot source code?**

You do not need to pull the banbot source code; you only need to reference the latest version of banbot via `go get` in your strategy project.  
The strategy project is the go project where you maintain all your strategy code, and it is also the place where you directly use `go build` to compile into a single executable file.

**What should I do if I don't understand the document?**

You can download the [GitHub repository](https://github.com/banbox/bandoc/) of this document, then open it with Cursor, and @codebase in the chat to ask any questions you have; Cursor will combine all the content of the document to provide you with a more understandable explanation.

**[More Common Questions](./faq.md)**

## Community Support
Welcome to the group chat, where we share questions, experiences, ideas, and strategies.
When scanning the green WeChat QR code, please add the note “banbot.”
<p style="display:flex;gap:20px;justify-content:center">
<img style="width:180px" src="/img/wechat_banbot.jpg"/>
<img style="width:180px" src="/img/telegram_banbot.png"/>
<img style="width:180px" src="/img/discord_banbot.png"/>
</p>

### Communities Links
[Discord Server](https://discord.com/invite/XXjA8ctqga)

[Telegram Group](https://t.me/banbot_quant)
