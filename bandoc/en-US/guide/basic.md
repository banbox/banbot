This page introduces some basic principles and concepts of banbot.

## Related Terminology
* Strategy: Your trading strategy, including how to calculate indicators and open/close positions when receiving candlestick data.
* Order: A complete entry and exit of a strategy execution corresponds to an order.
* Trading Pair/Symbol: The symbol code for a tradeable market on an exchange.
* Timeframe: The time interval of the trading candlestick (e.g., `5m`, `1h`).
* Indicator: Technical indicators (such as SMA, EMA, RSI, etc.).
* Leverage: Used in futures contracts, Margin = Nominal Value / Leverage.
* TakeProfit: Submitting a limit exit order to the exchange for the current order (e.g., entering a long position at $10, when the price reaches $11, a limit take profit can be set above $11 and wait for execution).
* StopLoss: Setting a stop loss for the current order at the exchange. There are market stop losses and limit stop losses (e.g., entering a long position at $10, when the price reaches $11, a stop loss can be set below $11 as the trigger price).

## Fees
All profit calculations of banbot include fees. For backtesting, hyperparameter optimization, and real-time simulation modes, the default exchange fees will be used. For live trading, the actual fees charged by the exchange (after deducting rebates, etc.) will be applied.

## Symbol Naming
For cryptocurrencies, it is usually in the format Base/Quote (e.g., Spot BTC/USDT, USDT-margined contract BTC/USDT:USDT, Coin-margined contract BTC/USDT:BTC). **It is recommended to write only the Base part directly in `pairs`**, and banbot will automatically complete the trading pair based on the first item in `stake_currency` and `market_type`.

```yaml
stake_currency: [USDT]
market_type: spot
pairs: [BTC, ETH, DASH]
```
The above configuration is exactly the same as the following configuration:
```yaml
stake_currency: [USDT]
market_type: spot
pairs: [BTC/USDT, ETH/USDT, DASH/USDT]
```

In cryptocurrency trading, two concepts are frequently involved: **base currency** and **quote currency**. The former is the object of the current trade, while the latter is the currency used for pricing and exchange. There may be differences in the naming of trading pairs across different exchanges. We maintain a unified naming standard and automatically handle differences between exchanges.

#### Spot Trading Pair Naming
```text
Base currency ↓
       BTC/USDT
       ETH/BTC
      DASH/ETH
            ↑ Quote currency
```
In spot trading, trading pairs are divided into two parts separated by "/", with the base currency in front and the quote currency behind.

#### Contract Trading Pair Naming
```text
// Base currency
// ↓
// ↓  Quote currency
// ↓  ↓
// ↓  ↓    Settlement currency
// ↓  ↓    ↓
// ↓  ↓    ↓
'BTC/USDT:BTC'  // BTC/USDT perpetual contract settled in BTC
'BTC/USDT:USDT' // BTC/USDT perpetual contract settled in USDT
'ETH/USDT:ETH'  // ETH/USDT perpetual contract settled in ETH
'ETH/USDT:USDT' // ETH/USDT perpetual contract settled in USDT
```
In contract trading, trading pairs are divided into three parts separated by ":", representing base currency/quote currency:settlement currency. The settlement currency is the currency used for margin and profit/loss calculation.

Contracts are divided into two types based on the settlement currency: linear contracts (U-based contracts) and inverse contracts (coin-based contracts). The former uses stablecoins such as USDT/USDC as the settlement currency, while the latter uses digital currencies such as BTC/ETH as the settlement currency. In the yml file, `market_type` is configured as `linear` and `inverse` to enable linear or inverse contracts.

Contracts are also divided into perpetual contracts and delivery contracts based on whether there is a delivery date:
```text
// Base currency
// ↓
// ↓  Quote currency
// ↓  ↓
// ↓  ↓    Settlement currency
// ↓  ↓    ↓
// ↓  ↓    ↓     Identifier (delivery date)
// ↓  ↓    ↓     ↓
// ↓  ↓    ↓     ↓
'BTC/USDT:BTC-211225'  // BTC/USDT inverse contract settled in BTC with delivery date 2021-12-25
'BTC/USDT:USDT-210625' // BTC/USDT linear contract settled in USDT with delivery date 2021-06-25
```
In the yml file, `contract_type` is configured as `swap` and `future` to enable perpetual contracts or delivery contracts respectively.

Options:
```text
// Base currency
// ↓
// ↓  Quote currency
// ↓  ↓
// ↓  ↓    Settlement currency
// ↓  ↓    ↓
// ↓  ↓    ↓       Identifier (delivery date)
// ↓  ↓    ↓       ↓
// ↓  ↓    ↓       ↓   Strike price
// ↓  ↓    ↓       ↓   ↓
// ↓  ↓    ↓       ↓   ↓   Type, put (P) or call (C)
// ↓  ↓    ↓       ↓   ↓   ↓
'BTC/USDT:BTC-211225-60000-P'  // BTC/USDT inverse contract settled in BTC, with the right to sell at 60000 on 2021-12-25
'ETH/USDT:USDT-211225-40000-C' // BTC/USDT linear contract settled in USDT, with the right to buy at 40000 on 2021-12-25
'ETH/USDT:ETH-210625-5000-P'   // ETH/USDT inverse contract settled in ETH, with the right to sell at 5000 on 2021-06-25
'ETH/USDT:USDT-210625-5000-C'  // ETH/USDT linear contract settled in USDT, with the right to buy at 5000 on 2021-06-25
```

## Main Components
The execution of backtesting and live trading involves various aspects. To ensure the project's structure remains flexible and clear, the main components involved in banbot include:

* Spider Process: banbot uses a separate spider process to monitor public data from exchanges, write it into the database, and push it in real time via TCP to multiple listening bots. This allows banbot to support running a set of strategies across multiple accounts simultaneously.
* DataProvider: Integrates all the symbols involved in backtesting/live trading (each symbol corresponds to a Feeder, and each Feeder supports multiple timeframes), and executes received candlesticks via callback functions.
* Order Manager (OrderMgr): In backtesting, it matches the order request based on candlesticks; in live trading, it submits the order to the exchange and monitors the execution progress for real-time updates.
* Wallets: During backtesting, it simulates the available balance, frozen amount, unrealized P&L, etc., based on order execution; in live trading, it monitors the exchange's account wallet status for real-time updates.
* Exchange: Based on banexg, it supports Binance, OKX, and Bybit's REST/websocket, used for downloading candlesticks, querying market information, order processing, and listening to websocket streams.
* PairFilters: Supports filtering and sorting all symbols based on given filters. Hardcoded symbol lists can also be used.
* Notifications (Notify): Sends notifications when bots open positions or signals appear. Currently, only WeChat is supported.
* Rest API: The rest API can be enabled during live trading, allowing users to view and manage bots via a web UI.

## Key Conventions
**Strategy Job (StratJob)**

A specific symbol + a specific strategy + a specific direction (both/long/short) + any timeframe is only allowed to exist as one strategy job.

For example, with a simple moving average strategy `ma:demo`, you can set parameters for long and short periods passed from the `yml` configuration:

```yaml
run_policy:
  - name: ma:demo
    run_timeframes: [5m]
    dirt: long
    params: {smlLen: 5, bigLen: 20}
    pairs: [BTC/USDT, ETH/USDT]
  - name: ma:demo
    run_timeframes: [5m, 15m]
    dirt: short
    params: {smlLen: 7, bigLen: 30}
    pairs: [BTC/USDT, ETH/USDT]
  - name: ma:demo
    run_timeframes: [15m]
    dirt: long
    params: {smlLen: 4, bigLen: 15}
    pairs: [BCH/USDT, ETC/USDT, BTC/USDT]
  - name: ma:demo
    run_timeframes: [5m]
    dirt: long
    params: {smlLen: 5, bigLen: 20}
    pairs: [BCH/USDT, ETC/USDT]
```
The configuration above sets four groups of strategy jobs, all using the `ma:demo` strategy. Each group can only generate 2 valid strategy jobs (StratJob), for a total of 8 strategy jobs.

The second group provides two timeframes, `[5m 15m]`, and will calculate the candlestick quality score for each timeframe, selecting the smallest timeframe that meets the score criteria.

The `BTC/USDT` in the third group overlaps with the first group, so it will be ignored.

Note: When the dirt parameter is omitted, it means both long/short orders are allowed.

## Backtesting Execution Logic
* Load the configuration and initialize: database session, exchange, data provider, order manager, wallets, pair manager, etc.
* Filter the symbol list to be traded based on the pair manager.
* Load the strategy list, and generate the strategy job list by combining symbols and timeframes.
* Subscribe to relevant symbols and their timeframes from the data provider.
* Call the LoopMain of the data provider to loop through candlesticks over time, and repeat the following logic for each candlestick:
     * Check if the batch entry callback needs to be triggered (triggered when the candlestick timestamp increases).
     * Update the indicator calculation environment.
     * The order manager updates the order based on the candlestick (triggering entry, take profit, stop loss, calculating profit, etc.).
     * Trigger the OnBar, OnCheckExit, GetDrawDownExitRate, OnInfoBar methods for all strategy jobs, and collect entry/exit requests.
     * Submit the entry/exit requests to the order manager for execution.
* Check if the symbol list needs to be refreshed (can be set to refresh periodically during backtesting to simulate live trading).

## Live Trading Execution Logic
* Load the configuration and initialize: database session, exchange, data provider, order manager, wallets, pair manager, notifications, Rest API, etc.
* Filter the symbol list to be traded based on the pair manager.
* Load the strategy list, and generate the strategy job list by combining symbols and timeframes.
* The data provider connects to the spider process and subscribes to relevant symbols and their timeframes.
* Start background tasks: monitor balance, monitor personal orders, periodically refresh trading pairs, periodically update market information, periodically check global stop-loss, etc.
* Call the LoopMain of the data provider to wait for candlestick updates pushed by the spider, and repeat the following logic for each candlestick:
    * Update the indicator calculation environment.
    * The order manager updates order profit based on the candlestick.
    * Trigger the OnBar, OnCheckExit, GetDrawDownExitRate, OnInfoBar methods for all strategy jobs, and collect entry/exit requests.
    * Submit the entry/exit requests to the order manager for execution.
    * Delay execution of batch entry/exit by 3 seconds (if new candlestick data is received within 3 seconds, cancel execution and delay another 3 seconds).