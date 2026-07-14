banbot supports rich candlestick processing tools including downloading, importing, exporting, deleting, and correcting.

## Candlestick Storage Core
banbot supports QuestDB and TimescaleDB for storing candlestick and other time-series data. When using QuestDB, banbot can download and install it locally; TimescaleDB must be deployed by the user. Both backends support the download, import, export, deletion, and correction tools on this page. See [Database](../guide/database.md) for configuration.

To achieve a good balance between storage space and reading efficiency, only `1m,5m,15m,1h,1d` time periods are stored. However, you can use any time period like `3m` in your strategy. Unstored time periods will be automatically aggregated dynamically from smaller time period data.

banbot uses an internal coverage mechanism to record downloaded ranges and confirmed no-data ranges for each symbol and timeframe. The mechanism is consistent across both database backends; manage data through the commands on this page instead of relying on or modifying internal metadata tables directly.

## Downloading Candlesticks
You don't need to implement candlestick data downloading, banbot will automatically download the required data during backtesting and live trading.

You can execute the following command to actively download candlestick data:

`bot kline down -timeframes 1h,1d -timerange 20240101-20250101 -pairs BTC,ETH`

Where `timeframes` is a required parameter, and the rest will be parsed from the yaml configuration file if not specified.

## Aggregating Larger Timeframes

When smaller-timeframe candlesticks already exist and you need to generate larger-timeframe data proactively, run:

`bot kline agg -timeframes 1h,1d -pairs BTC,ETH`

`-timeframes` and `-pairs` limit the aggregation scope. Normal backtests and live trading read, download, and aggregate data on demand; run this command manually only when data needs to be prepared in advance.

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
While using banbot, an unexpected interruption or a change to historical data may cause candlesticks to become inconsistent with the internal coverage ranges, affecting later downloads or reads. You can run the following command to correct the inconsistency:

`bot kline correct -pairs BTC`

Where `-pairs` is an optional parameter. If left empty, correction will be executed for all symbols, which may take an hour or two depending on the data size.

## Verifying Candlesticks and Coverage

`bot kline verify` checks whether candlestick data is consistent with the internal coverage metadata:

`bot kline verify -pairs BTC,ETH -tables kline_1m,kline_1h -batch-size 1000`

All parameters are optional. `-pairs` limits symbols, `-tables` limits data tables, and `-batch-size` controls the number of rows checked per batch. When an inconsistency is found, back up the data first, then use `bot kline correct` or download the data again.

