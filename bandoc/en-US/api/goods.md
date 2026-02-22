# goods Package

The goods package provides functionality related to commodities and trading pairs.

## Important Structures

### IFilter
Filter interface that all filters must implement.
- `GetName() string` - Get filter name
- `IsDisable() bool` - Check if filter is disabled
- `IsNeedTickers() bool` - Check if tickers data is needed
- `Filter(pairs []string, tickers map[string]*banexg.Ticker) ([]string, *errs.Error)` - Filter method

### IProducer
Producer interface that inherits from IFilter, used to generate trading pair lists.
- Inherits all methods from IFilter
- `GenSymbols(tickers map[string]*banexg.Ticker) ([]string, *errs.Error)` - Generate trading pair list

### BaseFilter
Base filter structure, the base class for all concrete filters.
- `Name string` - Filter name
- `Disable bool` - Whether disabled
- `NeedTickers bool` - Whether tickers data is needed
- `AllowEmpty bool` - Whether empty results are allowed

### VolumePairFilter
Volume filter that sorts all trading pairs in reverse order by volume value.
- `Limit int` - Limit on number of returned results, takes first 100
- `LimitRate float64` - Limit rate
- `MinValue float64` - Minimum volume value
- `RefreshSecs int` - Cache time in seconds
- `BackTimeframe string` - Time period for calculating volume, defaults to days
- `BackPeriod int` - Multiplier for time range obtained by multiplying with BackTimeframe

### PriceFilter
Price filter configuration structure.
- `MaxUnitValue float64` - Maximum allowable unit price change value (for pricing currency, generally USDT)
- `Precision float64` - Price precision, default requires minimum price change unit is 0.1%
- `Min float64` - Minimum price
- `Max float64` - Maximum price

### RateOfChangeFilter
Price change ratio filter, calculates (high-low)/low ratio over a period.
- `BackDays int` - Number of candlestick days to look back
- `Min float64` - Minimum price change ratio
- `Max float64` - Maximum price change ratio
- `RefreshPeriod int` - Cache time in seconds

### SpreadFilter
Liquidity filter.
- `MaxRatio float32` - Maximum bid-ask spread ratio relative to price, formula: 1-bid/ask

### CorrelationFilter
Correlation filter.
- `Min float64` - Minimum correlation
- `Max float64` - Maximum correlation
- `Timeframe string` - Time period
- `BackNum int` - Lookback number
- `TopN int` - Take top N
- `Sort string` - Sort method

### VolatilityFilter
Volatility filter using StdDev(ln(close / prev_close)) * sqrt(num).
- `BackDays int` - Number of candlestick days to look back
- `Max float64` - Maximum volatility score
- `Min float64` - Minimum volatility score

### AgeFilter
Listing time filter.
- `Min int` - Minimum listing days
- `Max int` - Maximum listing days

### BlockFilter
A variety blacklist filter used to filter specified varieties.
- `Pairs string[]` - Varieties to be filtered

### OffsetFilter
Offset filter.
- `Reverse bool` - Whether to reverse
- `Offset int` - Offset value
- `Limit int` - Limit count
- `Rate float64` - Rate value

### ShuffleFilter
Random shuffle filter.
- `Seed int` - Random seed

### Setup
Initialize the goods package configuration. Mainly used for setting up trading pair filters.


Returns:
- `*errs.Error` - Error information during initialization, returns nil if successful

### GetPairFilters
Create a list of trading pair filters based on configuration.

Parameters:
- `items []*config.CommonPairFilter` - Filter configuration list
- `withInvalid bool` - Whether to include invalid filters

Returns:
- `[]IFilter` - List of filter interfaces
- `*errs.Error` - Error information during creation

### RefreshPairList
Refresh trading pair list to get the latest valid trading pairs.


Returns:
- `[]string` - List of valid trading pairs
- `*errs.Error` - Error information during refresh 