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

Public fields are Name string, Disable bool and AllowEmpty bool; there is no NeedTickers field. `Disable` is the yaml key `disable` and turns the entry off; `AllowEmpty` does not come from yaml and is true when `pairmgr.cron` is not set (symbols with empty data are kept).

### VolumePairFilter

Fields: BaseFilter, Limit int, LimitRate float64, MinValue float64, CacheSecs int and BackPeriod string. BackPeriod is a duration/timeframe string, not the old BackTimeframe/integer multiplier.

### PriceFilter
Price filter configuration structure.
- `MaxUnitValue float64` - Maximum minimum-amount-step times price, in quote currency
- `Precision float64` - Maximum tick/price ratio; 0 disables this check
- `Min float64` - Minimum price
- `Max float64` - Maximum price

### RateOfChangeFilter

Fields: BaseFilter, BackDays int, Min/Max float64 and CacheSecs int. The cache field is CacheSecs, not RefreshPeriod.

### SpreadFilter
Placeholder: currently returns input unchanged; MaxRatio has no filtering effect.
- `MaxRatio float32` - Maximum bid-ask spread ratio relative to price, formula: 1-bid/ask

### CorrelationFilter
Correlation filter.
- `Min float64` - Minimum correlation
- `Max float64` - Maximum correlation
- `Timeframe string` - Time period
- `BackNum int` - Lookback number
- `TopN int` - Take top N
- `TopRate float64` - Maximum result count as a fraction of all symbols (yaml `top_rate`)
- `Sort string` - Sort method

Filtering only happens when `Timeframe` is set, `BackNum` is non-zero, and at least one of `Max`, `TopN` and `TopRate` is non-zero; `BackNum` below 10 is an error.

### VolatilityFilter
Volatility filter using StdDev(ln(close / prev_close)) * sqrt(num), where `num` is the number of daily returns (BackDays-1, fewer when history is short).
- `BackDays int` - Number of candlestick days to look back
- `Max float64` - Maximum volatility score
- `Min float64` - Minimum volatility score

### AgeFilter
Listing time filter.
- `Min int` - Minimum listing days
- `Max int` - Maximum listing days, 0 is unlimited; symbols listed for more than Max days are removed

### BlockFilter
A variety blacklist filter used to filter specified varieties.
- `Pairs []string` - Varieties to be filtered

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
- `withInvalid bool` - Whether to include disabled filters

Returns:
- `[]IFilter` - List of filter interfaces
- `*errs.Error` - Error information during creation

### RefreshPairList

`RefreshPairList(timeMS int64) ([]string, *errs.Error)`
Refresh trading pair list to get the latest valid trading pairs.


Returns:
- `[]string` - List of valid trading pairs
- `*errs.Error` - Error information during refresh

## Registration and explicit calls

This complete example registers a symbol-list filter. Import its package in your strategy project, then select name: KeepBTC in pairlists. The first filter must still be a producer.

```go
package filters

import (
    "strings"
    "github.com/banbox/banbot/goods"
    "github.com/banbox/banexg/errs"
)

type KeepBTC struct { goods.BaseFilter }

func (f *KeepBTC) Filter(pairs []string, timeMS int64) ([]string, *errs.Error) {
    result := make([]string, 0, len(pairs))
    for _, pair := range pairs {
        if strings.HasPrefix(pair, "BTC/") { result = append(result, pair) }
    }
    return result, nil
}

func init() {
    goods.RegisterFilter("KeepBTC", func(base goods.BaseFilter) goods.IFilter {
        return &KeepBTC{BaseFilter: base}
    })
}
```

`RegisterFilter(name, factory)` replaces an existing registration of the same name; `GetFilterFactory(name string) (FilterFactory, bool)` looks up a registered factory by name. `CreateFilter(cfg, allowEmpty)` returns `(IFilter, *errs.Error)` and decodes configuration into the instance. `GetPairFiltersWithConfig(items []*config.CommonPairFilter, withInvalid bool, cfg *config.Config) ([]IFilter, *errs.Error)` uses the given configuration; `GetPairFilters(items, withInvalid)` is the same with a nil cfg (package-level configuration). withInvalid includes disabled filters; it does not tolerate unknown names.

Precedence of the optional extension interfaces: `FilterWithRuntimeDeps` (`RuntimeFilter`) > `FilterWithSymbolState` (`SymbolStateFilter`) > `Filter`; for producers likewise `GenSymbolsWithRuntimeDeps` (`RuntimeProducer`) > `GenSymbolsWithSymbolState` (`SymbolStateProducer`) > `GenSymbols`.

`RuntimeDeps` also has `AllowRetiredPairs bool` and `SkippedPairs []orm.SkippedPair`, used only by factor Weights backtests: the first keeps static pairs the exchange no longer lists in the pool when the catalog has their data inside the backtest window; the second is filled by `RefreshPairListWithRuntimeDeps` with the static pairs left out because they have no data inside the window.

Embedded applications use `RefreshPairListWithRuntimeDeps(deps *RuntimeDeps, timeMS int64)`, returning `([]string, *errs.Error)`. Supply Core/Clock/Config/Symbols/Storage/Exchange and other dependencies from one task; SymbolState alone is insufficient. Custom filters can implement `FilterWithRuntimeDeps(deps *RuntimeDeps, pairs []string, timeMS int64) ([]string, *errs.Error)`, which takes priority over SymbolStateFilter and legacy Filter.

VolumePairFilter.MinValue measures mean(close*volume) per bar; CacheSecs currently does not control caching. Sort recognizes asc/desc only, and CorrelationFilter is skipped with fewer than four usable series. PriceFilter always uses the latest single 1h K-line close; RateOfChangeFilter and VolatilityFilter use 1d K-lines (BackDays bars). See the [filter guide](../guide/pair_filters.md).
