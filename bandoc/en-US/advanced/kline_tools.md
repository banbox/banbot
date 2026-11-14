# K-Line and Series Tools

banbot supports rich candlestick processing tools including downloading, importing, exporting, deleting, and correcting.

## Candlestick Storage Core
banbot supports QuestDB and TimescaleDB for storing candlestick and other time-series data. When using QuestDB, banbot can download and install it locally; TimescaleDB must be deployed by the user. Both backends support the download, import, export, deletion, and correction tools on this page. See [Database](../guide/database.md) for configuration.

To achieve a good balance between storage space and reading efficiency, only `1m,5m,15m,1h,1d` time periods are stored. Strategies can use periods such as `3m` or `4h` that can be aggregated from available base periods. Unstored periods are aggregated dynamically from smaller periods; minute bars cannot produce real second-level data.

banbot uses an internal coverage mechanism to record downloaded ranges and confirmed no-data ranges for each symbol and timeframe. The mechanism is consistent across both database backends; manage data through the commands on this page instead of relying on or modifying internal metadata tables directly.

## Downloading Candlesticks
By default, backtests and live startup download missing K-lines according to coverage. When `bt_no_kline_download` or strict replay is enabled, prepare complete history beforehand instead of relying on automatic downloads.

You can execute the following command to actively download candlestick data:

`bot kline down --timeframes 1h,1d --timerange 20240101-20250101 --pairs BTC,ETH`

`--timeframes` is required; the time range and pairs can come from YAML. Use `--timerange` or a `--timestart`/`--timeend` pair. Examples assume a configured exchange, market, database, and data directory. Select configuration with `--config`; `@` paths are relative to the data directory.

## Aggregating Larger Timeframes

When smaller-timeframe candlesticks already exist and you need to generate larger-timeframe data proactively, run:

`bot kline agg --timeframes 1h,1d --pairs BTC,ETH`

`--timeframes` and `--pairs` limit the aggregation scope. Normal backtests and live trading read, download, and aggregate data on demand; run this command manually only when data needs to be prepared in advance.

## Exporting Candlesticks (protobuf)
When you need to synchronize candlestick data to another banbot's database, it's recommended to export in `protobuf` format, which is optimized for both storage space and execution speed.

You can execute the following command to export candlesticks:

`bot data export --config @config.yml --config @config.local.yml --config @export.yml --out @data --concur 4`

Where `--config` and `--out` are required parameters. You can use `--config` to specify both robot configuration and export configuration. banbot uses the final YAML file only as the export task definition, with earlier files used as bot configuration. Export configuration example:
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

This format exports fixed K-line fields and accompanying metadata. It does not include arbitrary custom columns or independent series tables. Read and save those separately through `SeriesStore` or field queries; this command is not a complete time-series database backup.

## Exporting Candlesticks (csv.zip)
When you need to export candlesticks for further reading by other programs, it's recommended to export as zip-compressed csv format. This can be easily accessed programmatically.

You can execute the following command to export:

`bot kline export --out @data --timeframes 1h,1d --pairs BTC,ETH --adj none --tz UTC`

Where `--out` and `--timeframes` are required parameters. `--out` should be a directory. When `pairs` is not specified, banbot will use all symbols under the current yaml configured exchange and market.

`--tz` defaults to UTC; timestamps use `YYYY-MM-DD HH:mm:ss`. Set `--adj none` for unadjusted prices, or `pre`/`post` for forward/backward adjustment; set this explicitly.

Output uses `{symbol}_{timeframe}.zip`, with `/` and `:` in symbols replaced by `_`. Each ZIP contains the matching CSV with time, open, high, low, close, volume, and buy volume, without arbitrary extension fields. The range comes from YAML; this command does not accept `--timerange`. Use a fresh output directory: an existing ZIP for a symbol currently skips all timeframes for that symbol.

This format is intended for external readers and cannot round-trip directly through `kline load`, which needs millisecond timestamps and a mappable symbol filename. Prefer protobuf for transferring fixed K-lines between databases.

## Importing Candlesticks (protobuf)
You can specify the following command to import from exported data into the current database:

`bot data import --in @data --concur 4`

Where `--in` is a required parameter, `--concur` is the number of concurrent import threads, default 1. This setting doesn't need to match the export setting.

When banbot saves symbol information, its `sid` is allocated within each database and is not guaranteed to match another database, so during import, existing symbol information will be automatically loaded and maintained with an id mapping with the export data to avoid symbol id conflicts.

## Importing Candlesticks (csv.zip)
You can use the following command to import zip data into the database:

`bot kline load --in @data`

Where `--in` is a required parameter. The path can be a zip file or a folder containing zip files.

The current entry point reads ZIP files or `.zip` files in a directory. Inside each ZIP, name CSV files `Symbol.csv`, where Symbol can be mapped by the configured exchange; the exported `symbol_timeframe` name is not directly suitable. For year-named archives such as `2024.zip`, the year also participates in historical symbol mapping.

Each CSV row needs at least six columns: a 13-digit millisecond timestamp, open, high, low, close, and volume. Optional column seven is buy volume. Prepare at least two valid records in ascending time order; the importer infers the timeframe from positive adjacent time differences and selects a built-in storage timeframe. Validate numbers and timestamps beforehand: some conversion errors are currently ignored. This entry point does not import arbitrary columns; use `KLineSeriesStore` for extensions or `SeriesStore` for independent series.

## Deleting Candlesticks
You can use the following command to clean candlestick data from the database:

`bot kline purge --timeframes 1h,1d --pairs BTC,ETH`

Omitting `--timeframes` selects all built-in storage timeframes. Pairs can come from configuration; if the resulting list is empty, all registered symbols in the current exchange/market are selected. This command deletes all history for the selected symbols and timeframes and accepts no time-range flag. Check the confirmation summary carefully.

Before starting deletion, summary information will be output, requiring you to input `y` to confirm deletion.

## Correcting Errors in Candlesticks
`correct` synchronizes K-lines between physical timeframes and maintains range information. It cannot guarantee correction of bad upstream prices and is not a validator for arbitrary custom series:

`bot kline correct --pairs BTC`

`--pairs` is optional; omitting it processes all stored symbols and can take a long time. Back up first and use `verify` to identify the issue.

## Rebuilding Coverage from Stored K-Lines

When rows exist but coverage metadata is missing, specify symbols, timeframes, and a range:

`bot kline repair-ranges --pairs BTC --timeframes 1m,1h --timerange 20240101-20250101`

This requires an explicit static symbol list and rebuilds coverage from stored rows. It does not download missing bars or prove that unfetched intervals are covered.

## Verifying Candlesticks and Coverage

`bot kline verify` checks whether candlestick data is consistent with the internal coverage metadata:

`bot kline verify --pairs BTC,ETH --tables kline_1m,kline_1h --batch-size 1000`

All parameters are optional. `--pairs` limits symbols, `--tables` limits data tables, and `--batch-size` controls the number of rows checked per batch. When an inconsistency is found, back up the data first, then use `bot kline correct` or download the data again.


## Custom Series Commands

```shell
bot series list
bot series down --tables funding_rate --pairs BTC --timestart 20240101 --timeend 20250101
```

`list` emits JSON definitions of sources compiled into the executable, rather than listing database tables. For `down`, `--tables` selects registered source names, not physical tables; omitting it selects all custom sources. Downloads fill coverage gaps and stop at closed regular periods no later than the current time. The command currently does not support `event` timeframes. See [Custom Time-Series Data](../guide/custom_data.md) for integration.
