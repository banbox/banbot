# Utils Package

This package provides a series of general utility functions, including file operations, mathematical calculations, string processing, and other functionalities.

## File Operations

### CopyDir
Copy an entire directory and its contents to a target location.

Parameters:
- `src string` - Source directory path
- `dst string` - Target directory path

Returns:
- `error` - Error information during operation

### Copy
Copy a single file to a target location.

Parameters:
- `srcFile string` - Source file path
- `dstFile string` - Target file path

Returns:
- `error` - Error information during operation

### Exists
Check if a file or directory exists.

Parameters:
- `filePath string` - Path of the file or directory to check

Returns:
- `bool` - true indicates exists, false indicates does not exist

### EnsureDir
Ensure a directory exists, create it if it doesn't.

Parameters:
- `dir string` - Directory path
- `perm os.FileMode` - Directory permissions

Returns:
- `error` - Error information during operation

### FindSubPath
Recursively search for a target file or directory in a specified directory.

Parameters:
- `parDir string` - Parent directory path
- `tgtName string` - Target name to find
- `maxDepth int` - Maximum recursion depth

Returns:
- `string` - Found path
- `error` - Error information during search

## Mathematical Calculations

### DecPow
Calculate the power of a decimal number.

Parameters:
- `x decimal.Decimal` - Base
- `y decimal.Decimal` - Exponent

Returns:
- `decimal.Decimal` - Calculation result

### DecArithMean
Calculate the arithmetic mean of a group of decimal numbers.

Parameters:
- `values []decimal.Decimal` - Array of values

Returns:
- `decimal.Decimal` - Mean value
- `error` - Error information during calculation

### DecStdDev
Calculate the population standard deviation of a group of decimal numbers.

Parameters:
- `values []decimal.Decimal` - Array of values

Returns:
- `decimal.Decimal` - Standard deviation
- `error` - Error information during calculation

### SharpeRatio
Calculate the Sharpe ratio, using default 252 trading days and annualization.

Parameters:
- `moReturns []float64` - Array of returns
- `riskFree float64` - Risk-free rate

Returns:
- `float64` - Sharpe ratio
- `error` - Error information during calculation

### SortinoRatio
Calculate the Sortino ratio, using default 252 trading days and annualization.

Parameters:
- `moReturns []float64` - Array of returns
- `riskFree float64` - Risk-free rate

Returns:
- `float64` - Sortino ratio
- `error` - Error information during calculation

## String Processing

### SnakeToCamel
Convert snake case to camel case.

Parameters:
- `input string` - Input string in snake case

Returns:
- `string` - Converted string in camel case

### PadCenter
Center-pad a string to a specified width.

Parameters:
- `s string` - Original string
- `width int` - Target width
- `padText string` - Padding character

Returns:
- `string` - Padded string

### RandomStr
Generate a random string of specified length.

Parameters:
- `length int` - String length

Returns:
- `string` - Generated random string

### FormatWithMap
Format a string using values from a map.

Parameters:
- `text string` - String containing placeholders
- `args map[string]interface{}` - Map of replacement values

Returns:
- `string` - Formatted string

### PrintErr
Format error information.

Parameters:
- `e error` - Error object

Returns:
- `string` - Formatted error message

### GroupByPairQuotes
Group trading pairs by quote currency.

Format: `[key]:pairs...` converts to:
```
[key]
Quote: Base1 Base2 ...
```

Parameters:
- `items map[string][]string` - Trading pairs mapping

Returns:
- `string` - Formatted grouped string

### CountDigit
Count the number of digit characters in a string.

Parameters:
- `text string` - Input string

Returns:
- `int` - Number of digit characters

### SplitSolid
Split string and ignore empty strings.

Parameters:
- `text string` - String to split
- `sep string` - Separator

Returns:
- `[]string` - Array of non-empty strings after splitting

## Slice and Map Operations

### SplitSolid
String splitting, ignoring empty strings in the result.

Parameters:
- `text string` - String to split
- `sep string` - Separator

Returns:
- `[]string` - Array of strings after splitting

### KeysOfMap
Get all keys of a map.

Parameters:
- `m M` - Input map, supports generics

Returns:
- `[]K` - Array of keys

### ValsOfMap
Get all values of a map.

Parameters:
- `m M` - Input map, supports generics

Returns:
- `[]V` - Array of values

### CutMap
Extract a subset of specified keys from a map.

Parameters:
- `m M` - Input map, supports generics
- `keys ...K` - List of keys to extract

Returns:
- `M` - New map containing specified keys

### UnionArr
Merge multiple arrays and remove duplicates.

Parameters:
- `arrs ...[]T` - List of arrays to merge, supports generics

Returns:
- `[]T` - Merged array without duplicates

### ReverseArr
Reverse array elements in place.

Parameters:
- `s []T` - Array to reverse, supports generics

### ConvertArr
Convert an array of one type to another type.

Parameters:
- `arr []T1` - Source array
- `doMap func(T1) T2` - Conversion function

Returns:
- `[]T2` - Converted array

### ArrToMap
Convert an array to a map, multiple elements may map to the same key.

Parameters:
- `arr []T2` - Source array
- `doMap func(T2) T1` - Key mapping function

Returns:
- `map[T1][]T2` - Converted map with array values

### RemoveFromArr
Remove specified elements from an array.

Parameters:
- `arr []T` - Source array
- `it T` - Element to remove
- `num int` - Number of elements to remove, negative means remove all

Returns:
- `[]T` - New array after removing elements

### UniqueItems
Get unique elements and duplicate elements from an array.

Parameters:
- `arr []T` - Input array, supports generics

Returns:
- `[]T` - Array of unique elements
- `[]T` - Array of duplicate elements

### DeepCopyMap
Deep copy a map.

Parameters:
- `dst map[string]interface{}` - Destination map
- `src map[string]interface{}` - Source map

### MapToStr
Convert a map to string representation.

Parameters:
- `m map[string]float64` - Map to convert

Returns:
- `string` - Converted string
- `int` - Total length of numeric parts

## Network Operations

### DoHttp
Execute HTTP request and return result.

Parameters:
- `client *http.Client` - HTTP client
- `req *http.Request` - HTTP request

Returns:
- `*banexg.HttpRes` - HTTP response result

## Network Communication

### NewBanServer
Create a new BanServer instance for TCP network communication.

Parameters:
- `addr string` - Server listening address (e.g., "127.0.0.1:6789")
- `name string` - Server name

Returns:
- `*ServerIO` - Server instance

### NewClientIO
Create a new BanClient instance to connect to BanServer.

Parameters:
- `addr string` - Server address (e.g., "127.0.0.1:6789")

Returns:
- `*ClientIO` - Client instance
- `*errs.Error` - Error information

### GetServerData
Get data from BanServer or BanClient.

Parameters:
- `key string` - Data key name

Returns:
- `string` - Retrieved data value
- `*errs.Error` - Error information

### SetServerData
Set data to BanServer or BanClient.

Parameters:
- `args *KeyValExpire` - Data structure containing key, value, and expiration time
  - `Key string` - Key name
  - `Val string` - Value
  - `ExpireSecs int` - Expiration time (seconds)

Returns:
- `*errs.Error` - Error information

### GetNetLock
Get distributed lock.

Parameters:
- `key string` - Lock key name
- `timeout int` - Lock acquisition timeout (seconds)

Returns:
- `int32` - Lock value (used for unlocking)
- `*errs.Error` - Error information

### DelNetLock
Delete distributed lock.

Parameters:
- `key string` - Lock key name
- `lockVal int32` - Lock value (obtained from GetNetLock)

Returns:
- `*errs.Error` - Error information

## Other Tools

### MD5
Calculate MD5 hash of data.

Parameters:
- `data []byte` - Input data

Returns:
- `string` - Hexadecimal string of MD5 hash

### GetSystemLanguage
Get system language setting.

Returns:
- `string` - System language code

## Correlation Calculations

### CalcCorrMat
Calculate correlation matrix between multiple data series.

Parameters:
- `arrLen int` - Data length
- `dataArr [][]float64` - Two-dimensional data array
- `useChgRate bool` - Whether to use change rate for calculation

Returns:
- `*mat.SymDense` - Correlation matrix
- `[]float64` - Average correlation for each series
- `error` - Error information

### GenCorrImg
Generate heatmap of correlation matrix.

Parameters:
- `m *mat.SymDense` - Correlation matrix
- `title string` - Chart title
- `names []string` - List of series names
- `fontName string` - Font name
- `fontSize float64` - Font size

Returns:
- `[]byte` - PNG image data
- `error` - Error information

### CalcEnvsCorr
Calculate correlation between multiple candlestick environments.

Parameters:
- `envs []*ta.BarEnv` - List of indicator environments
- `hisNum int` - Number of historical data points

Returns:
- `*mat.SymDense` - Correlation matrix
- `[]float64` - Average correlation for each environment
- `error` - Error information

## Performance Metrics Calculation

### CalcExpectancy
Calculate profit expectancy and risk-reward ratio.

Parameters:
- `profits []float64` - Array of profits

Returns:
- `float64` - Expected profit
- `float64` - Risk-reward ratio

### CalcMaxDrawDown
Calculate maximum drawdown.

Parameters:
- `profits []float64` - Array of profits
- `initBalance float64` - Initial balance

Returns:
- `float64` - Maximum drawdown amount
- `float64` - Maximum drawdown percentage
- `int` - Drawdown start position
- `int` - Drawdown end position
- `float64` - Balance at drawdown start
- `float64` - Balance at drawdown end

### AutoCorrPenalty
Calculate autocorrelation penalty factor.

Parameters:
- `returns []float64` - Array of returns

Returns:
- `float64` - Penalty factor

### KMeansVals
Perform K-means clustering on numerical series.

Parameters:
- `vals []float64` - Array of values
- `num int` - Number of clusters

Returns:
- `*ClusterRes` - Clustering result

### StdDevVolatility
Calculate standard deviation volatility.

Parameters:
- `data []float64` - Data array
- `rate float64` - Decay rate

Returns:
- `float64` - Volatility

### NearScore
Calculate score for proximity to center point.

Parameters:
- `x float64` - Current value
- `mid float64` - Center value
- `rate float64` - Decay rate

Returns:
- `float64` - Score (between 0-1)

## File Reading and Writing

### ReadCSV
Read CSV file contents.

Parameters:
- `path string` - CSV file path

Returns:
- `[][]string` - Two-dimensional array of CSV contents
- `*errs.Error` - Error information

### ReadXlsx
Read Excel file contents.

Parameters:
- `path string` - Excel file path
- `sheet string` - Worksheet name (uses first worksheet if empty)

Returns:
- `[][]string` - Two-dimensional array of Excel contents
- `*errs.Error` - Error information

### ReadTextFile
Read text file contents.

Parameters:
- `path string` - File path

Returns:
- `string` - File contents
- `*errs.Error` - Error information

### ReadLastNLines
Read last N lines of a file.

Parameters:
- `filePath string` - File path
- `lineCount int` - Number of lines to read

Returns:
- `[]string` - Last N lines content
- `error` - Error information

### WriteCsvFile
Write to CSV file.

Parameters:
- `path string` - File path
- `rows [][]string` - Data to write
- `compress bool` - Whether to compress

Returns:
- `*errs.Error` - Error information

### WriteFile
Write to regular file.

Parameters:
- `path string` - File path
- `data []byte` - Data to write

Returns:
- `*errs.Error` - Error information

### KlineToStr
Convert candlestick data to string array.

Parameters:
- `klines []*banexg.Kline` - candlestick data array
- `loc *time.Location` - Timezone information

Returns:
- `[][]string` - Converted string array

### GetFontData
Get font file data.

Parameters:
- `name string` - Font name (uses arial.ttf if empty)

Returns:
- `[]byte` - Font file data
- `error` - Error information

### GetOpenFont
Get OpenType font.

Parameters:
- `name string` - Font name

Returns:
- `*opentype.Font` - OpenType font object
- `error` - Error information

### IsTextContent
Check if data is text content.

Parameters:
- `data []byte` - Data to check

Returns:
- `bool` - true indicates text content, false indicates possible binary content

## Utility Functions

### ParallelRun
Execute tasks in parallel.

Parameters:
- `items []T` - List of items to process, supports generics
- `concurNum int` - Maximum concurrency
- `handle func(int, T) *errs.Error` - Handler function, receives index and item

Returns:
- `*errs.Error` - Error during execution

### IsDocker
Check if currently running in Docker container.

Returns:
- `bool` - Whether in Docker container

### OpenBrowser
Open system default browser to visit specified URL.

Parameters:
- `url string` - URL to visit

Returns:
- `error` - Error during execution

### OpenBrowserDelay
Open browser to visit URL after a delay.

Parameters:
- `url string` - URL to visit
- `delayMS int` - Delay in milliseconds

### IntToBytes
Convert uint32 to byte array.

Parameters:
- `n uint32` - Number to convert

Returns:
- `[]byte` - Converted byte array
- `error` - Error during conversion

### MD5
Calculate MD5 hash of data.

Parameters:
- `data []byte` - Data to hash

Returns:
- `string` - Hexadecimal string of MD5 hash

### ReadInput
Read user input from standard input.

Parameters:
- `tips []string` - List of prompt messages

Returns:
- `string` - User input string
- `error` - Error during reading

### ReadConfirm
Read user confirmation from standard input.

Parameters:
- `tips []string` - List of prompt messages
- `ok string` - Confirmation string
- `fail string` - Cancel string
- `exitAny bool` - Whether to allow any input to exit

Returns:
- `bool` - Whether user confirmed

### NewPrgBar
Create new progress bar.

Parameters:
- `totalNum int` - Total progress count
- `title string` - Progress bar title

Returns:
- `*PrgBar` - Progress bar object

### NewStagedPrg
Create multi-stage progress bar.

Parameters:
- `tasks []string` - List of task names, in execution order, no duplicates
- `weights []float64` - Task weights, >0, internally normalized

Returns:
- `*StagedPrg` - Multi-stage progress bar object

## System-Related Functions

### GetSystemLanguage
Get current system language code.

Returns:
- `string` - ISO 639-1 language code (optionally with ISO 3166-1 country code), such as:
  - en-US: English (United States)
  - zh-CN: Chinese (Simplified)
  - zh-TW: Chinese (Traditional)
  - ja-JP: Japanese
  - ko-KR: Korean
  - fr-FR: French
  - de-DE: German
  - es-ES: Spanish
  etc.

