# btime Package

The btime package provides time processing functionality.

## Runtime Clock

Each `runtime.Runtime` owns an independent `btime.ClockState`: a backtest advances its own simulated time, while live trading reads real UTC time. Business code inside a task should receive that ClockState through the Runtime and use it to read or advance time, avoiding interference between tasks.

The package-level time functions below remain for legacy call chains; they are not a task clock for several Runtimes running in parallel.

## ClockState methods and replay example

`NewClockState(backtest bool, loc *time.Location) *ClockState` constructs a clock. `TimeMS()` returns milliseconds, `Time()` floating-point seconds, and `Now()` *time.Time. SetTimeMS sets simulation time; AdvanceMS returns the advanced millisecond timestamp. An unset replay clock initializes from real time on its first read, so set the start first. This is a host/test example; normal strategies must not advance the framework clock themselves.

```go
clock := btime.NewClockState(true, time.UTC)
startMS, err := btime.ParseTimeMS("2025-01-01 00:00:00")
if err != nil { panic(err) }
clock.SetTimeMS(startMS)
nextMS := clock.AdvanceMS(60_000)
fmt.Println(nextMS - startMS) // 60000
```

## Public Methods

### TimeMS
Get the compatibility clock timestamp in milliseconds: simulation time in backtests, real time in live mode.

Returns:
- `int64` - Current timestamp

### UTCTime
Get current UTC time as 10-digit floating-point seconds timestamp.

Returns:
- `float64` - 10-digit floating-point seconds timestamp

### UTCStamp
Get current UTC time as 13-digit milliseconds timestamp.

Returns:
- `int64` - 13-digit milliseconds timestamp

### Time
Get current 10-digit seconds timestamp. Returns real-time in live mode, backtest time in backtest mode.

Returns:
- `float64` - 10-digit seconds timestamp

### MSToTime
Convert 13-digit milliseconds timestamp to time.Time object.

Parameters:
- `timeMSecs int64` - 13-digit milliseconds timestamp

Returns:
- `*time.Time` - Time object pointer

### Now
Get current UTC time. Returns real-time in live mode, backtest time in backtest mode.

Returns:
- `*time.Time` - Time object pointer

### ParseTimeMS
Convert time string to 13-digit milliseconds timestamp.

Supports following time formats:
- Year (2006)
- Year-Month-Day (20060102)
- 10-digit seconds timestamp
- 13-digit milliseconds timestamp
- Year-Month-Day Hour:Minute (2006-01-02 15:04)
- Year-Month-Day Hour:Minute:Second (2006-01-02 15:04:05)

Parameters:
- `timeStr string` - Time string

Returns:
- `int64` - 13-digit milliseconds timestamp
- `error` - Parse failure; check before using the timestamp

### ParseTimeMSBy
Parse time string to 13-digit milliseconds timestamp according to specified time format.

Parameters:
- `layout string` - Time format template
- `timeStr string` - Time string

Returns:
- `int64` - 13-digit milliseconds timestamp
- `error` - Parse failure; check before using the timestamp

### ToDateStr
Convert timestamp to UTC timezone time string.

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)
- `format string` - Time format template (default: 2006-01-02 15:04:05)

Returns:
- `string` - Formatted time string

### ToDateStrLoc
Format in package-level LocShow (UTC when unset). This function does not accept a location argument; for an explicit timezone use ToTime(ts).In(loc).Format(layout).

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)
- `format string` - Time format template (default: 2006-01-02 15:04:05)

Returns:
- `string` - Formatted time string

### ToTime
Convert timestamp to time.Time object.

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)

Returns:
- `time.Time` - Time object



## Digit helpers and clocks

CountDigit belongs to core, not btime. ClockState.TimeMS is task simulation time; UTCStamp is real UTC time. Backtest callbacks should use their task clock rather than package-level compatibility time.
