# core Package

The core package provides system core data structures, constants, and error definitions.

## Runtime State

Task-scoped mutable core state is represented by `core.State` and is owned by `runtime.Runtime` when a task is created. It contains the run mode and environment, symbol and market state, order books, job performance, and that task's own cancellation signal. New components should receive this state through Runtime-injected concrete state or dependency projections rather than reading or writing package-level variables.

Package-level APIs such as `Setup`, `SetRunMode`, `SetRunEnv`, and `Sleep` remain for older embedded callers. They are compatibility paths and should not be used by new execution flows that must run alongside other tasks.

## Core Structures

### Param
Parameter configuration structure, used to define and manage parameter values in the system, especially suitable for hyperparameter search scenarios.

Field descriptions:
- `Name string` - Parameter name
- `VType int` - Parameter value type, can be:
  - `VTypeUniform(0)` - Uniform linear distribution
  - `VTypeNorm(1)` - Normal distribution
- `Min float64` - Minimum parameter value
- `Max float64` - Maximum parameter value
- `Mean float64` - Mean value (effective for normal distribution type)
- `IsInt bool` - Whether it's an integer type
- `Rate float64` - Normal distribution rate parameter, default is 1. Higher values make random values tend towards Mean
- `edgeY float64` - Cache for normal distribution edge y-value calculation

### PerfSta
Strategy performance statistics structure, used to record statistical information for a strategy across all trading targets.

Field descriptions:
- `OdNum int` - Number of orders
- `LastGpAt int` - Order count at last clustering execution
- `Splits *[4]float64` - Split points array for performance grouping
- `Delta float64` - Multiplier for total profit before logarithmic processing

### JobPerf
Job performance structure, used to record strategy performance on specific trading pairs and time periods.

Field descriptions:
- `Num int` - Number of orders
- `TotProfit float64` - Total profit
- `Score float64` - Order multiplier, less than 1 indicates need to reduce order size. When Score equals PrefMinRate(0.001), will directly use MinStakeAmount

## Core Function Methods

### Setup
Initialize system core components.

Returns:
- `*errs.Error` - Error information during initialization, returns nil if successful

### SetRunMode
Set system running mode.

Parameters:
- `mode string` - Running mode identifier

### SetRunEnv
Set system running environment.

Parameters:
- `env string` - Running environment identifier

### Sleep
Sleep function with interrupt check.

Parameters:
- `d time.Duration` - Sleep duration

Returns:
- `bool` - Whether interrupted (true means normal sleep completion, false means interrupted)

## Cache Related Methods

### GetCacheVal
Get value from cache for specified key, supports generics.

Parameters:
- `key string` - Cache key name
- `defVal T` - Default value

Returns:
- `T` - Cache value or default value

## Performance Statistics Methods

### GetPerfSta
Get strategy performance statistics.

Parameters:
- `stagy string` - Strategy name

Returns:
- `*PerfSta` - Performance statistics object

### DumpPerfs
Export performance statistics data to file.

Parameters:
- `outDir string` - Output directory path

## Price Related Methods

## Utility Methods

### IsFiat
Determine if it's a fiat currency code.

Parameters:
- `code string` - Currency code

Returns:
- `bool` - Whether it's a fiat currency

### KeyStratPairTf
Generate combined key name for strategy-pair-timeframe.

Parameters:
- `stagy string` - Strategy name
- `pair string` - Trading pair name
- `tf string` - Time frame

Returns:
- `string` - Combined key name

### MarshalYaml
Serialize object to YAML format.

Parameters:
- `v any` - Object to serialize

Returns:
- `[]byte` - Serialized byte array
- `error` - Error information

### GroupByPairQuotes
Group by trading pairs and quotes.

Parameters:
- `items map[string][]string` - Items to group

Returns:
- `string` - String representation of grouped results

### SplitSymbol
Split trading pair symbol into base currency and quote currency.

Parameters:
- `pair string` - Trading pair name

Returns:
- `string` - Base currency
- `string` - Quote currency
- `string` - Base currency code
- `string` - Quote currency code

## Parameter Generation Methods

### PNorm
Create normal distribution parameter.

Parameters:
- `min float64` - Minimum value
- `max float64` - Maximum value

Returns:
- `*Param` - Parameter object

### PNormF
Create normal distribution parameter with mean and rate.

Parameters:
- `min float64` - Minimum value
- `max float64` - Maximum value
- `mean float64` - Mean value
- `rate float64` - Rate

Returns:
- `*Param` - Parameter object

### PUniform
Create uniform distribution parameter.

Parameters:
- `min float64` - Minimum value
- `max float64` - Maximum value

Returns:
- `*Param` - Parameter object

## Order Related Methods

### IsLimitOrder
Determine if it's a limit order.

Parameters:
- `t int` - Order type

Returns:
- `bool` - Whether it's a limit order

## System Control Methods

### RunExitCalls
Execute callback functions on exit.



## Current responsibilities and migrated APIs

Price helpers belong to com, not core. Explicit tasks use Runtime.Market.Prices PriceState methods (GetPriceSafeExpAt/SetPriceAt/SetPricesAt with explicit nowMS). Package-level com GetPrice/SetPrice helpers are compatibility facades. SetPairMs is also in com. PrintStratGroups belongs to strat; explicit tasks use PrintStratGroupsWithState. SnapMem is no longer a public function. core.CountDigit(text string) int counts Unicode digit characters.

State owns admission/performance/cancellation. Runtime injects concrete state; package Setup/SetRunMode does not provide multi-task isolation. See [com](com.md), [runtime](runtime.md).
