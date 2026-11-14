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

## Main components

| Component | User-facing behavior |
| --- | --- |
| Hub (`bot hub`, alias `spider`) | Collects exchange data, stores it and distributes it over TCP; may also host account execution over a local Unix socket |
| Data provider | Replays history in time order or delivers subscribed live events; registered sources provide external series |
| Strategy | `time_series` dispatches events to Jobs; `factor` computes factors and portfolio targets from multi-symbol snapshots |
| Order manager | Time-series requests enqueue first, then match locally in backtests or submit/track real orders |
| Shared-account execution | Tracks strategy ownership and budgets, combining logical positions into physical-account plans |
| Wallet | Simulates balances, reservations and P&L in backtests; live balances follow account execution/reconciliation |
| Symbol manager | Uses explicit `pairs` or a filter pipeline to build and refresh the universe |
| Notifications and UI | WeCom, Telegram and email; development WebUI and live Dashboard have separate entry points |

## Two strategy engines

Omitting `run_policy.engine` selects `time_series`. A YAML file may contain both time-series and factor policies; mixed runs use events execution.

Register time-series factories through `strat.AddStratGroup`, then reference them by `run_policy.name`. A strategy, symbol and selected timeframe form a `StratJob` in each account. `run_timeframes` contains candidate timeframes: quality scoring or `PickTimeFrame` chooses one. It does not automatically create a Job for every listed timeframe; declare auxiliary subscriptions separately.

```yaml
run_policy:
  - name: ma:demo
    engine: time_series
    run_timeframes: [15m]
    dirt: long
    pairs: [BTC/USDT, ETH/USDT]
```

Register `ma:demo` in the strategy project first. `dirt` accepts `long`, `short` or `any`; omission allows both directions, but spot rejects shorts. Wallet, account and strategy limits also constrain orders.

Factor rounds freeze the Universe and currently visible data, distinguish reference/investable/tradable sets, then compute factors and target positions. They do not invoke a time-series `OnBar` for every symbol. Statistical pairs use factor/portfolio capabilities. See [Multi-factor strategies](./factor.md).

## Time-series data and callbacks

`orm.DataSeries.Values` carries standard K-line fields and arbitrary extension fields together. Types, missing fields and explicit NULL remain distinct; numerical indicator views do not replace original values.

New strategies can use `OnData(s, event)` and route main, auxiliary and custom events using `event.IsMain()` or the role. Legacy `OnBar`/`OnInfoBar` callbacks remain supported but cannot coexist with `OnData`. Warmup computes indicators without submitting trades. See [Custom strategy](./strat_custom.md) and [Custom data](./custom_data.md).

## From data to orders

1. Merge configuration, choose engines/symbols/timeframes and create a Runtime for the task.
2. Subscribe and warm up. Backtests advance historical time; live tasks wait for events.
3. Main events update indicators/data views, invoke the strategy, check custom exits and process order requests. Auxiliary/custom events follow subscriptions.
4. `OpenOrder`/`CloseOrders` create intents; the order manager matches or submits them. Track updates with `OnOrderChange`. Shared accounts also combine strategy targets and persist the execution ledger.
5. Update wallets, reports, notifications and monitoring; release subscriptions/resources when the task stops.

Batch strategies use `BatchInOut`/`BatchInfo` and batch callbacks. Historical and live batch readiness differ: one arriving event does not guarantee all symbols are ready.

## Choose a run mode

| Goal | Entry point and meaning |
| --- | --- |
| Historical validation | `bot backtest`; factor supports weights/events, mixed requires events |
| Time-series parameter optimization | `bot optimize` or `bot bt-opt` |
| Time-series real-time simulation | `env: dry_run` with `bot trade`, live data and local matching |
| Factor/mixed historical events simulation | `bot trade --dry-run`, not real-time paper trading |
| Real trading | `bot trade`; configure account/data/execution dependencies and pass engine-specific startup checks |

Default live entry points execute through Hub and currently accept only linear perpetual accounts in `prod`/`dry_run`. See [Shared accounts and Hub](./execution.md) for restrictions and recovery, plus [Commands](./bot_usage.md), [Backtesting](./backtest.md) and [Live trading](./live_trading.md).
