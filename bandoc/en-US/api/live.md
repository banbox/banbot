# live Package

The live package provides real-time trading functionality.

## Scheduled Task Related Methods

### CronRefreshPairs
Scheduled task for refreshing trading pair information.

Parameters:
- `dp data.IProvider` - Data provider interface

Features:
- Periodically refresh trading pair information based on configured Cron expression
- Execute actual refresh operation through `biz.AutoRefreshPairs`

### CronLoadMarkets
Scheduled task for loading market data.

Features:
- Update market data every 2 hours
- Get latest market data from exchange through `orm.LoadMarkets`

### CronFatalLossCheck
Scheduled task for checking fatal losses.

Features:
- Periodically check trading accounts for fatal losses
- Check against configured `FatalStop` threshold

### CronKlineDelays
Scheduled task for checking candlestick delays.

Features:
- Execute check every 30 seconds
- Monitor candlestick data delays to ensure data timeliness

### CronKlineSummary
Scheduled task for generating candlestick data summary.

Features:
- Execute every 5 minutes
- Generate statistical summary of candlestick data

### CronCheckTriggerOds
Scheduled task for checking trigger orders.

Features:
- Check limit orders for trigger submission every minute
- Monitor market conditions for order trigger conditions

## Trading Related Classes and Methods

### NewCryptoTrader
Create a new cryptocurrency trader instance.

Returns:
- `*CryptoTrader` - Newly created trader instance

## Web API Related Methods

### StartApi
Start API server.

Returns:
- `*errs.Error` - Returns error information if startup fails

### AuthMiddleware
Create middleware for API authentication.

Parameters:
- `secret string` - Secret key for verification

Returns:
- `fiber.Handler` - Fiber framework handler function 

## Struct Definitions

### CryptoTrader
Cryptocurrency trader responsible for managing and executing cryptocurrency trading operations.

**Fields:**
- `Trader` (biz.Trader): Base trader containing basic trading functionality
- `dp` (*data.LiveProvider): Live data provider for real-time market data

### OrderArgs
Order query parameters struct for filtering and retrieving order information.

**Fields:**
- `StartMs` (int64): Start timestamp (milliseconds)
- `StopMs` (int64): End timestamp (milliseconds)
- `Limit` (int): Limit on number of results returned
- `AfterID` (int): Query orders after specified ID
- `Symbols` (string): Trading pair symbols, comma-separated
- `Status` (string): Order status (open/his)
- `Strategy` (string): Strategy name
- `TimeFrame` (string): Time frame
- `Source` (string): Data source, required (bot/exchange/position)

### OdWrap
Order wrapper struct extending basic order information.

**Fields:**
- `InOutOrder` (*ormo.InOutOrder): Base order information
- `CurPrice` (float64): Current price

### ForceExitArgs
Force exit parameters struct.

**Fields:**
- `OrderID` (string): Order ID, required. Use "all" for all orders

### CloseArgs
Position closing parameters struct.

**Fields:**
- `Symbol` (string): Trading pair symbol, required. Use "all" for all positions
- `Side` (string): Direction (long/short)
- `Amount` (float64): Quantity
- `OrderType` (string): Order type
- `Price` (float64): Price

### DelayArgs
Entry delay parameters struct.

**Fields:**
- `Secs` (int64): Delay in seconds, required

### JobItem
Strategy job item struct.

**Fields:**
- `Pair` (string): Trading pair
- `Strategy` (string): Strategy name
- `TF` (string): Time frame
- `Price` (float64): Current price
- `OdNum` (int): Current number of orders

### PairArgs
Trading pair query parameters struct.

**Fields:**
- `Start` (int64): Start timestamp
- `Stop` (int64): End timestamp

### GroupItem
Group statistics item struct.

**Fields:**
- `Key` (string): Group key value
- `HoldHours` (float64): Position holding duration (hours)
- `TotalCost` (float64): Total cost
- `ProfitSum` (float64): Total profit/loss
- `ProfitPct` (float64): Profit/loss percentage
- `CloseNum` (int): Number of closed positions
- `WinNum` (int): Number of profitable trades

### PerfArgs
Performance query parameters struct.

**Fields:**
- `GroupBy` (string): Grouping method (symbol/month/week/day)
- `Pairs` ([]string): List of trading pairs
- `StartSecs` (int64): Start timestamp (seconds)
- `StopSecs` (int64): End timestamp (seconds)
- `Limit` (int): Limit on number of results returned

### LogArgs
Log query parameters struct.

**Fields:**
- `Num` (int): Number of log lines to return

### AuthClaims
JWT authentication claims struct.

**Fields:**
- `User` (string): Username
- `RegisteredClaims` (jwt.RegisteredClaims): JWT standard claims

### LoginRequest
Login request parameters struct.

**Fields:**
- `Username` (string): Username, required
- `Password` (string): Password, required 