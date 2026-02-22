# exg Package

The exg package provides exchange interface and trading-related functionality.

## Function List

### Setup
Initialize exchange settings.

Returns:
- `*errs.Error` - Returns error information if initialization fails, nil otherwise

### GetWith
Get exchange instance based on specified exchange name, market, and contract type.

Parameters:
- `name string` - Exchange name
- `market string` - Market type
- `contractType string` - Contract type

Returns:
- `banexg.BanExchange` - Exchange instance
- `*errs.Error` - Returns error information if retrieval fails, nil otherwise

### PrecCost
Process transaction cost amount according to exchange precision requirements.

Parameters:
- `exchange banexg.BanExchange` - Exchange instance
- `symbol string` - Trading pair symbol
- `cost float64` - Original cost amount

Returns:
- `float64` - Cost amount processed according to exchange precision
- `*errs.Error` - Returns error information if processing fails, nil otherwise

### PrecPrice
Process transaction price according to exchange precision requirements.

Parameters:
- `exchange banexg.BanExchange` - Exchange instance
- `symbol string` - Trading pair symbol
- `price float64` - Original price

Returns:
- `float64` - Price processed according to exchange precision
- `*errs.Error` - Returns error information if processing fails, nil otherwise

### PrecAmount
Process transaction amount according to exchange precision requirements.

Parameters:
- `exchange banexg.BanExchange` - Exchange instance
- `symbol string` - Trading pair symbol
- `amount float64` - Original amount

Returns:
- `float64` - Amount processed according to exchange precision
- `*errs.Error` - Returns error information if processing fails, nil otherwise

### GetLeverage
Get leverage ratio for specified trading pair and notional value.

Parameters:
- `symbol string` - Trading pair symbol
- `notional float64` - Notional value
- `account string` - Account identifier

Returns:
- `float64, float64` - Returns two float values representing related leverage ratio values

### GetOdBook
Get order book data for specified trading pair.

Parameters:
- `pair string` - Trading pair symbol

Returns:
- `*banexg.OrderBook` - Order book data
- `*errs.Error` - Returns error information if retrieval fails, nil otherwise

### GetTickers
Get market data for all trading pairs.

Returns:
- `map[string]*banexg.Ticker` - Market data mapping with trading pairs as keys
- `*errs.Error` - Returns error information if retrieval fails, nil otherwise

### GetAlignOff
Get alignment offset for specified exchange and time frame.

Parameters:
- `exgName string` - Exchange name
- `tfSecs int` - Time frame (in seconds)

Returns:
- `int` - Alignment offset 