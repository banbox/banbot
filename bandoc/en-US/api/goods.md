# goods Package

The goods package provides functionality related to commodities and trading pairs.

## Runtime-aware filters and pools

RuntimeDeps contains Core, Clock, Config, DataDir, Symbols, Storage, Exchange and ShowLog. RuntimeFilter.FilterWithRuntimeDeps and RuntimeProducer.GenSymbolsWithRuntimeDeps are instance extensions; SymbolStateFilter/Producer and legacy interfaces remain supported. RefreshPairListWithRuntimeDeps/FilterPairsWithRuntimeDeps use task configuration, clocks and symbol identity. Frozen pools, forced filtering and order have distinct contracts.

## Important Structures

### IFilter

The interface exposes GetName() string, IsDisable() bool and Filter(pairs []string, timeMS int64) ([]string, *errs.Error). IsNeedTickers is not part of the current interface; data/configuration comes through explicit RuntimeDeps or implementation state.

### IProducer

Extends IFilter with GenSymbols(timeMS int64) ([]string, *errs.Error). The old tickers parameter is not the current signature.

### BaseFilter

Public fields are Name string, Disable bool and AllowEmpty bool; there is no NeedTickers field.

### VolumePairFilter

Fields: BaseFilter, Limit int, LimitRate float64, MinValue float64, CacheSecs int and BackPeriod string. BackPeriod is a duration/timeframe string, not the old BackTimeframe/integer multiplier.

### PriceFilter
Price filter configuration structure.
- `MaxUnitValue float64` - Maximum allowable unit price change value (for pricing currency, generally USDT)
- `Precision float64` - Price precision, default requires minimum price change unit is 0.1%
- `Min float64` - Minimum price
- `Max float64` - Maximum price

### RateOfChangeFilter

Fields: BaseFilter, BackDays int, Min/Max float64 and CacheSecs int. The cache field is CacheSecs, not RefreshPeriod.

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
