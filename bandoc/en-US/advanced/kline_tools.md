banbot supports rich candlestick processing tools including downloading, importing, exporting, deleting, and correcting.

## candlestick Storage Core
banbot uses QuestDB time-series database for storing candlestick and other data to provide better performance in reading and downloading. banbot will automatically download and install QuestDB on startup, no manual configuration is required.

To achieve a good balance between storage space and reading efficiency, only `1m,5m,15m,1h,1d` time periods are stored. However, you can use any time period like `3m` in your strategy. Unstored time periods will be automatically aggregated dynamically from smaller time period data.

The database uses the `KInfo` table, which stores the candlestick start and end ranges for each symbol + time period: sid, timeframe, start, stop; This helps determine the range when reading or downloading candlesticks.

The `KHole` table stores candlestick discontinuous gaps for each symbol + time period: sid, timeframe, start, stop, no_data;
Where no_data being true indicates that the exchange has no candlestick data for this symbol during this time range (possibly due to trading suspension, etc.);
no_data being false indicates that data download hasn't been attempted for this interval.

The `KLineUn` table stores incomplete candlesticks for each symbol + time period: sid, timeframe, start_ms, stop_ms, open, high, low, close, volume;
sid+timeframe should correspond to a unique record in this table. If the current time is 10:37, there should be a record on `1h` with start_ms corresponding to 10:00 and stop_ms corresponding to 10:37.

## Downloading Candlesticks
You don't need to implement candlestick data downloading, banbot will automatically download the required data during backtesting and live trading.

You can execute the following command to actively download candlestick data:

`bot kline down -timeframes 1h,1d -timerange 20240101-20250101 -pairs BTC,ETH`

Where `timeframes` is a required parameter, and the rest will be parsed from the yaml configuration file if not specified.

## Exporting Candlesticks (protobuf)
When you need to synchronize candlestick data to another banbot's database, it's recommended to export in `protobuf` format, which is optimized for both storage space and execution speed.

You can execute the following command to export candlesticks:

`bot data export -config @export.yml -out @data -concur 4`

Where `-config` and `-out` are required parameters. You can use `-config` to specify both robot configuration and export configuration. banbot will use the last yml configuration file as the export configuration file. Export configuration example:
```yaml
klines:
  - exchange: 'binance'
    market: 'linear'
    timeframes: ['15m', '1h', '1d']
    time_range: '20210101-20250101'
    symbols: []
  # - exchange: 'binance'
  #   market: 'spot'
  #   timeframes: ['1h', '1d']
  #   time_range: '20240101-20250101'
  #   symbols: []
```
As shown above, you can specify multiple export tasks. The following items can be left empty: `exchange`, `market`, `timeframes`, `symbols`. When these items are empty, they will be treated as selecting all. `time_range` cannot be left empty.

Two types of data files will be exported: `exInfo1.dat` and `kline[num].dat`. The former stores symbol information, while the latter contains chunked candlestick data; each `kline[num].dat` file has a maximum size of 1G.

> Note: Do not modify the exported file names, as this will prevent recognition during import

## Exporting Candlesticks (csv.zip)
When you need to export candlesticks for further reading by other programs, it's recommended to export as zip-compressed csv format. This can be easily accessed programmatically.

You can execute the following command to export:

`bot kline export -out @data -timeframes 1h,1d -pairs BTC,ETH -tz UTC`

Where `-out` and `-timeframes` are required parameters. `-out` should be a directory. When `pairs` is not specified, banbot will use all symbols under the current yaml configured exchange and market.

The default timezone for `-tz` is UTC, and time in the exported csv will be displayed in `YYYY-MM-DD HH:mm:SS` format.

During export, data will be saved in the export directory with the naming format `{symbol}_{timeframe}.zip`.

## Importing Candlesticks (protobuf)
You can specify the following command to import from exported data into the current database:

`bot data import -in @data -concur 4`

Where `-in` is a required parameter, `-concur` is the number of concurrent import threads, default 1. This setting doesn't need to match the export setting.

When banbot saves symbol information, its `sid` is random in each database, so during import, existing symbol information will be automatically loaded and maintained with an id mapping with the export data to avoid symbol id conflicts.

## Importing Candlesticks (csv.zip)
You can use the following command to import zip data into the database:

`bot kline load -in @data`

Where `-in` is a required parameter. The path can be a zip file or a folder containing zip files.

banbot will automatically decompress the zip folder and extract csv files. The csv files should be named as `Symbol.csv`, and will automatically determine the unique `ExSymbol` object combined with the exchange and market configured in yaml.

The csv data requires fixed columns: time, open, high, low, close, volume. (Time should be a 13-digit millisecond timestamp)

## Deleting Candlesticks
You can use the following command to clean candlestick data from the database:

`bot kline purge -timeframes 1h,1d -pairs BTC,ETH`

Where `-timeframes` is a required parameter. When `-pairs` is not specified, it will use the `pairs` list from the yaml configuration, and if that's also empty, it will default to deleting all symbol data under the yaml configured exchange and market.

Before starting deletion, summary information will be output, requiring you to input `y` to confirm deletion.

## Correcting Errors in Candlesticks
While using banbot, sometimes misoperations may cause errors in `kInfo` or `KHole` that don't match the actual candlesticks. This will lead to runtime errors. You can run the following command to correct these errors:

`bot kline correct -pairs BTC`

Where `-pairs` is an optional parameter. If left empty, correction will be executed for all symbols, which may take an hour or two depending on the data size.


