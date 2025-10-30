
banbot
=======

banbot is a high-performance, easy-to-use, multi-symbol, multi-strategy, multi-period, multi-account event-driven trading robot.

[![AGPLv3 licensed][agpl-badge]][agpl-url]
[![Discord chat][discord-badge]][discord-url]
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/banbox/banbot)

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

### Supported Exchanges
banbot support exchanges powered by [banexg](https://github.com/banbox/banexg):

| logo                                                                                                            | id      | name              | ver | websocket | 
|-----------------------------------------------------------------------------------------------------------------|---------|-------------------|-----|-----------|
| ![binance](https://user-images.githubusercontent.com/1294454/29604020-d5483cdc-87ee-11e7-94c7-d1a8d9169293.jpg) | binance | spot/usd-m/coin-m | *   | Y         |

### Quick start
![image](https://docs.banbot.site/uidev.gif)
#### 1. start timescaledb
```bash
docker network create mynet
docker run -d --name timescaledb --network mynet -p 127.0.0.1:5432:5432 \
  -v /opt/pgdata:/var/lib/postgresql/data \
  -e POSTGRES_PASSWORD=123 timescale/timescaledb:latest-pg17
```

#### 2. start banbot 
create your `/root/config.yml`:
```yaml
accounts:
  user1:  # you can change this
    binance:
      prod:
        api_key: your_api_key_here
        api_secret: your_secret_here
database:
  url: postgresql://postgres:123@timescaledb:5432/ban
```
```bash
docker run -d --name banbot -p 8000:8000 --network mynet -v /root:/root banbot/banbot:latest -config /root/config.yml
```

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
