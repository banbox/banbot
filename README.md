
banbot
=======

banbot is a high-performance, easy-to-use, multi-symbol, multi-strategy, multi-period, multi-account event-driven trading robot.

[![AGPLv3 licensed][agpl-badge]][agpl-url]
[![Discord chat][discord-badge]][discord-url]
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/banbox/banbot)
[![zread](https://img.shields.io/badge/Ask_Zread-_.svg?style=flat&color=00b0aa&labelColor=000000&logo=data%3Aimage%2Fsvg%2Bxml%3Bbase64%2CPHN2ZyB3aWR0aD0iMTYiIGhlaWdodD0iMTYiIHZpZXdCb3g9IjAgMCAxNiAxNiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTQuOTYxNTYgMS42MDAxSDIuMjQxNTZDMS44ODgxIDEuNjAwMSAxLjYwMTU2IDEuODg2NjQgMS42MDE1NiAyLjI0MDFWNC45NjAxQzEuNjAxNTYgNS4zMTM1NiAxLjg4ODEgNS42MDAxIDIuMjQxNTYgNS42MDAxSDQuOTYxNTZDNS4zMTUwMiA1LjYwMDEgNS42MDE1NiA1LjMxMzU2IDUuNjAxNTYgNC45NjAxVjIuMjQwMUM1LjYwMTU2IDEuODg2NjQgNS4zMTUwMiAxLjYwMDEgNC45NjE1NiAxLjYwMDFaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00Ljk2MTU2IDEwLjM5OTlIMi4yNDE1NkMxLjg4ODEgMTAuMzk5OSAxLjYwMTU2IDEwLjY4NjQgMS42MDE1NiAxMS4wMzk5VjEzLjc1OTlDMS42MDE1NiAxNC4xMTM0IDEuODg4MSAxNC4zOTk5IDIuMjQxNTYgMTQuMzk5OUg0Ljk2MTU2QzUuMzE1MDIgMTQuMzk5OSA1LjYwMTU2IDE0LjExMzQgNS42MDE1NiAxMy43NTk5VjExLjAzOTlDNS42MDE1NiAxMC42ODY0IDUuMzE1MDIgMTAuMzk5OSA0Ljk2MTU2IDEwLjM5OTlaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik0xMy43NTg0IDEuNjAwMUgxMS4wMzg0QzEwLjY4NSAxLjYwMDEgMTAuMzk4NCAxLjg4NjY0IDEwLjM5ODQgMi4yNDAxVjQuOTYwMUMxMC4zOTg0IDUuMzEzNTYgMTAuNjg1IDUuNjAwMSAxMS4wMzg0IDUuNjAwMUgxMy43NTg0QzE0LjExMTkgNS42MDAxIDE0LjM5ODQgNS4zMTM1NiAxNC4zOTg0IDQuOTYwMVYyLjI0MDFDMTQuMzk4NCAxLjg4NjY0IDE0LjExMTkgMS42MDAxIDEzLjc1ODQgMS42MDAxWiIgZmlsbD0iI2ZmZiIvPgo8cGF0aCBkPSJNNCAxMkwxMiA0TDQgMTJaIiBmaWxsPSIjZmZmIi8%2BCjxwYXRoIGQ9Ik00IDEyTDEyIDQiIHN0cm9rZT0iI2ZmZiIgc3Ryb2tlLXdpZHRoPSIxLjUiIHN0cm9rZS1saW5lY2FwPSJyb3VuZCIvPgo8L3N2Zz4K&logoColor=ffffff)](https://zread.ai/banbox/banbot)

[agpl-badge]: https://img.shields.io/badge/license-AGPL--v3-green.svg
[agpl-url]: https://github.com/banbox/banbot/blob/develop/LICENSE
[discord-badge]: https://img.shields.io/discord/1289838115325743155.svg?logo=discord&style=flat-square
[discord-url]: https://discord.com/invite/XXjA8ctqga

### Main Features
* web ui: write strategy, backtest, and deploy without IDE.
* high-performance: backtest for 1 year klines in seconds.
* easy-to-use: write once, support both backtesting and real trading.
* flexible: free combination of symbols, strategies and time frames.
* event-driven: no lookahead, more freedom to implement your trade ideas.
* scalable: trade multiple exchange accounts simultaneously.
* hyper opt: support bayes/tpe/random/cmaes/ipop-cmaes/bipop-cmaes

![image](https://docs.banbot.site/uidev.gif)

### Supported Exchanges
banbot support exchanges powered by [banexg](https://github.com/banbox/banexg):

| logo                                                                                                            | id      | name              | ver | websocket | 
|-----------------------------------------------------------------------------------------------------------------|---------|-------------------|-----|-----------|
| ![binance](https://user-images.githubusercontent.com/1294454/29604020-d5483cdc-87ee-11e7-94c7-d1a8d9169293.jpg) | binance | spot/usd-m/coin-m | *   | Y         |
| ![okx](https://user-images.githubusercontent.com/1294454/152485636-38b19e4a-bece-4dec-979a-5982859ffc04.jpg) | okx | spot/usd-m/coin-m | *   | Y         |
| ![bybit](https://private-user-images.githubusercontent.com/81727607/382500134-97a5d0b3-de10-423d-90e1-6620960025ed.png?jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3NjkyNTM1NjgsIm5iZiI6MTc2OTI1MzI2OCwicGF0aCI6Ii84MTcyNzYwNy8zODI1MDAxMzQtOTdhNWQwYjMtZGUxMC00MjNkLTkwZTEtNjYyMDk2MDAyNWVkLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNjAxMjQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjYwMTI0VDExMTQyOFomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTQwODM4YThmMTU2ZmIyMGI1YjRmYWU0MGVkYzJhN2YyMmYzYzhmNTJjZDM1YzFmYzdjNGRlMGY4OTlmM2RmODMmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0In0.3I9CvGTqWpZcZaDBV0_tFQFbOoPyCOxaZ1c6o7q6tMQ) | bybit | spot/usd-m/coin-m | *   | Y         |

### Quick start

go [runbanbot](https://github.com/banbox/runbanbot) to get started with Docker in minutes!

### Work with AI
Download the [banbot.md](doc/banbot.md) file, then attach this file to any AI conversation to tell it the strategy you want to test.

[中文版 banbot.md](doc/banbot_cn.md)

Or immediately test your idea using the [Banbot Workflow](https://www.banbot.site/zh-CN/ai).

### Document
Please go to [BanBot Website](https://www.banbot.site/) for documents.

### Contributing
Follow the [How to Contribute](/doc/contribute.md). Please do get hesitate to get touch via the [Discord](https://discord.com/invite/XXjA8ctqga) Chat to discuss development, new features, and the future roadmap.  
Unless you explicitly state otherwise, any contributions intentionally submitted for inclusion in a banbot workspace you create shall be licensed under AGPLv3, without any additional terms or conditions.

### Donate
If banbot made your life easier and you want to help us improve it further, or if you want to speed up development of new features, please support us with a tip. We appreciate all contributions!  

| METHOD | ADDRESS                                    |
|--------|--------------------------------------------|
| BTC    | bc1qah04suzc2amupds7uqgpukellktscuuyurgflt |
| ETH    | 0xedBF0e5ABD81e5F01c088f6B6991a623dB14D43b |

### LICENSE
This project is dual-licensed under GNU AGPLv3 License and a commercial license. For free use and modifications of the code, you can use the AGPLv3 license. If you require commercial license with different terms, please contact me.
