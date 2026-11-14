# Banbot user guide

Banbot is a high-performance, easy-to-use, multi-variety, multi-strategy, multi-period, multi-account event-driven trading robot. It supports backtesting, hyperparameter tuning, and real-time trading.
Currently supports Binance, OKX, and Bybit Exchange, and the robot can be viewed through the WebUI.
::: danger Disclaimer
This software is for research and educational purposes only. Do not invest more money than you can afford. Use this software at your own risk. The author and all related parties are not responsible for your trading results.

Please conduct sufficient backtesting, rolling testing, and simulated real-time trading before considering running the trading robot with a small amount. Do not invest money before understanding how it works and the expected profit/loss.

We strongly recommend that you have basic golang programming knowledge and understand the robot's operating mechanism by reading the source code.
:::

## Choose a workflow

| Goal | Start here |
| --- | --- |
| Install and run a first backtest | [Quick start](./quick_local.md), [Configuration](./configuration.md), [Backtesting](./backtest.md) |
| Write Go strategies or add data | [Initialize a project](./init_project.md), [Custom strategy](./strat_custom.md), [Custom time-series data](./custom_data.md) |
| Research factors, pairs and portfolios | [Multi-factor strategies](./factor.md) |
| Run simulated/live trading and shared accounts | [Live trading](./live_trading.md) |
| Manage storage and historical data | [Database](./database.md), [Data tools](../advanced/kline_tools.md) |
| Look up commands and developer APIs | [Commands](./bot_usage.md), [API](../api/index.md) |

## Main features
* High performance: Go and banta cache indicator state; runtime depends on strategies, data, timeframes and universe size
* Easy to use: one version of the strategy, supporting backtesting, simulated real-time trading and real trading at the same time
* Flexible: Freely combine different symbols, strategies and time periods
* Event-driven: processing follows data visibility; custom data must supply correct publication/revision information to avoid lookahead
* Scalability: A batch of strategies can be applied to multiple exchange accounts at the same time
* Hyperparameter tuning: Support bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes
* Dynamic symbols: Add or remove trading symbols on the fly during strategy execution, no restart required
* Websocket: Exchange transaction by transaction + order book data
* Indicator library: Built-in [banta](https://github.com/banbox/banta) high-performance indicator library, you can quickly develop custom indicators based on it
* Arbitrary time-series data: preserve K-line extension columns, external series, types and NULL values; see [Custom data](./custom_data.md)
* Two engines: time-series callbacks, cross-sectional portfolios, statistical pairs and mixed events replay; see [Factor guide](./factor.md)
* LLM extension: an application-invoked model management library; see [AI](../advanced/ai.md)

## WebUI
For strategy editing, data management and backtesting. `bot web` defaults to `127.0.0.1:8000`, where a password is optional. Binding to `0.0.0.0` or `::` requires `--password` (Basic Auth username: `banbot`).

![image](https://docs.banbot.site/uidev.gif)

## Dashboard UI
For managing live trading robots, it is recommended to deploy on servers closer to the exchange, with password access required.

![image](https://docs.banbot.site/dashboard.gif)

## Supported exchanges
Based on the unified interface provided by [banexg](https://github.com/banbox/banexg), currently supports Binance, OKX, and Bybit Exchange.

> We support [agent workflow script](https://github.com/banbox/banexg/blob/main/docs/dev_exg.py), which enables automated exchange integration based on codex/claude code. If you need support for other exchanges and have some development experience, you can use this script to quickly complete the development.

| id | Exchange |
| --- | --- |
| `binance` | Binance |
| `okx` | OKX |
| `bybit` | Bybit |

Market-data and historical replay capabilities depend on the banexg adapter. Default Hub live execution currently accepts only linear perpetual accounts in `prod`/`dry_run`; adapter spot, inverse, testnet or options capabilities do not establish default live support. See [Shared accounts and Hub](./execution.md). Symbol naming examples do not imply support on every exchange. The China-market documents describe designs, not delivered exchange integrations.

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
Use this as a starting point for small trials. Multiple symbols/accounts, factor panels and long replays have memory and storage requirements that depend on the actual workload.
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

## Factor and cross-sectional engine

Banbot supports time_series and factor engines: per-symbol event callbacks or Universe-wide factor rounds with multiple outputs, cross-sectional transforms and target portfolios. Both may appear in one run_policy; omitted engine retains time-series behavior.

See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md).
