# btime Package

The btime package provides time processing functionality.

## Public Methods

### TimeMS
Get current timestamp (13-digit milliseconds).

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

### ParseTimeMSBy
Parse time string to 13-digit milliseconds timestamp according to specified time format.

Parameters:
- `layout string` - Time format template
- `timeStr string` - Time string

Returns:
- `int64` - 13-digit milliseconds timestamp

### ToDateStr
Convert timestamp to UTC timezone time string.

Parameters:
- `timestamp int64` - Timestamp (supports 10-digit seconds or 13-digit milliseconds)
- `format string` - Time format template (default: 2006-01-02 15:04:05)

Returns:
- `string` - Formatted time string

### ToDateStrLoc
Convert timestamp to time string in specified timezone.

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

### CountDigit
Count number of digit characters in string.

Parameters:
- `text string` - Input string

Returns:
- `int` - Number of digit characters 