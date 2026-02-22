You can specify the list of trading instruments directly in the yaml configuration file through `pairs`, or you can automatically filter the trading list through the instrument manager.

When you use `pairs` to specify the instrument list, the instrument filter is not enabled by default. But you can set `pairmgr.force_filters` to true to enable it and filter the given instrument list.

## Runtime
When the robot starts, the instrument filter will be run once by default to get the list of tradable instruments.

You can also specify a cron expression to execute and refresh the tradable list regularly.

When a certain instrument needs to be removed, if there is an open order, it will not be removed temporarily.

The corn expression will also be executed during backtesting to refresh the instrument list regularly to achieve an effect similar to real-time trading.

## All supported filters
* VolumePairList
* PriceFilter
* RateOfChangeFilter
* SpreadFilter
* CorrelationFilter
* VolatilityFilter
* AgeFilter
* BlockFilter
* OffsetFilter
* ShuffleFilter
  The first of the yaml instrument filter list must be `VolumePairList`, which can return a list of instruments from all instruments in the market.

## VolumePairList
Sort/filter currency pairs by volume. When it is the first, it will calculate the volume of all instruments in the period specified by `back_period` and return it in descending order.
```yaml
- name: VolumePairList # Sort all trading pairs by volume value in descending order
  limit: 100 # Take the first 100
  min_value: 100000 # Minimum volume value
  cache_secs: 7200 # Cache time
  back_period: 3d # TimePeriod for calculating trading volume
```
## PriceFilter
Allows filtering instrument lists by price.
```yaml
- name: PriceFilter # Price filter
  max_unit_value: 100 # The value corresponding to the maximum allowed unit price change (for the pricing currency, usually USDT).
  precision: 0.0015 # Filter trading pairs by price precision, the default minimum price change unit is 0.1%
  min: 0.001 # Minimum price
  max: 100000 # Maximum price
```
## RateOfChangeFilter
Volatility filter.
```yaml
- name: RateOfChangeFilter # Volatility filter
  back_days: 5 # Number of days to review the candle
  min: 0.03 # Minimum price change ratio
  max: 10 # Maximum price change ratio
  cache_secs: 1440 # Cache time, seconds
```
## SpreadFilter
Liquidity filter. Formula: 1-bid/ask, the maximum ratio of bid-ask spread to price
```yaml
- name: SpreadFilter # Liquidity filter
  max_ratio: 0.005 # Formula: 1-bid/ask, the maximum ratio of bid-ask spread to price
```
## CorrelationFilter
Correlation coefficient filter.
```yaml
- name: CorrelationFilter # Correlation filter
  min: -1 # Used to filter the average correlation between the current currency and the entire market; the default is 0, which means it is not enabled
  max: 1 # Used to filter the average correlation between the current currency and the entire market; the default is 0, which means it is not enabled
  timeframe: 5m # Data period used to calculate correlation
  back_num: 70 # Length of data for calculating correlation review
  top_n: 50 # Only return the top n currencies with the lowest correlation, the default is 0 and there is no limit
```
## VolatilityFilter
Volatility filter. Formula: std(log(c/c1)) * sqrt(back_days)
```yaml
- name: VolatilityFilter # Volatility filter, formula: std(log(c/c1)) * sqrt(back_days)
  back_days: 10 # Number of days to review the candle
  max: 1 # Maximum value of the volatility score. The larger the value, the more it allows for some targets that change very drastically at the 1d level
  min: 0.05 # Minimum value of the volatility score. The smaller the value, the more it allows for some targets that change very little at the 1d level
```
## AgeFilter
Filter by the listing time of the symbol.
```yaml
- name: AgeFilter # Filter by the listing days of the target
  min: 5
```
## BlockFilter
A variety blacklist filter used to filter specified varieties.
```yaml
 - name: BlockFilter
   pairs: [BTC]
```
## OffsetFilter
According to the given offset, take the specified number of symbols. Usually used at the end
```yaml
- name: OffsetFilter # Offset limit selection. Usually used at the end
  offset: 10 # Start from the 10th
  limit: 30 # Take up to 30
```
## ShuffleFilter
Random shuffle
```yaml
- name: ShuffleFilter # Random shuffle
  seed: 42 # Random number seed, optional
```