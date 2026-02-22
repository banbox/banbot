After writing the strategy, you can package the strategy and banbot into a single executable file (Please ensure that you execute the command in the root directory of your strategy project):
```shell
# for linux/macos
go build -o bot

# for windows
go build -o bot.exe
```
Then you can use this executable file directly to start the UI, backtest, hyperparameter optimization, real trading, etc.!

Directly executing `bot` will start the WebUI interface at `http://localhost:8000`, through which you can manage strategies, backtest, analyze transactions, etc.

Running `bot -help` will display a list of subcommand help:
```text
banbot 0.1.5
please run with a subcommand:
    trade:      live trade
    backtest:   backtest with strategies and data
    spider:     start the spider
    optimize:   run hyper parameters optimization
    bt_opt:     backtest over optimize
    kline:      run kline commands
    tick:       run tick commands
    tool:       run tools
    web:        run web dashboard
```
## Common command parameters
**Data directory (-datadir)**

When you start through the command line, unless the environment variable `BanDataDir` is set, you must pass in the parameter `-datadir` to specify the data directory each time you start.

**Default configuration file**

Before reading the additional configuration files you passed in, banbot will try to read two default configuration files from the data directory: `config.yml` and `config.local.yml`.

If these two have already covered the parameters you need this time, you do not need to pass in the configuration file path through `-config`.

**Command help information**

You can display help information by adding `-help` or `-h` parameters after the command.

## Start WebUI Research & Backtesting
To facilitate your strategy research and backtesting, we provide WebUI visualization. You can edit strategies online, start backtesting, view profit and loss curves, analyze orders, etc. through WebUI.

This is a WebUI only for the strategy research stage. By default, only your local computer is allowed to access it, so you can access it without authentication. If you need to access it from the external network, please set `-host 0.0.0.0`.

If you need to access the Dashboard UI during real-time trading in banbot, please refer to [Real-time Trading](#start-real-time-trading-simulated-tradingreal-trading)。

```text
Usage of web:
  -host string
        bind host ip (default "127.0.0.1")
  -port int
        port to listen (default 8000)
  -config value
        config path to use, Multiple -config options may be used
  -datadir string
        Path to data dir.
  -db string
        db file path (default "dev.db")
  -level string
        log level (default "info")
  -logfile string
        log file path, default: system temp dir
  -tz string
        timezone (default "utc")
```

## Start the crawler process
```shell
bot spider [-datadir PATH] [-c PATH] [-c PATH]
```
When you want to start real-time trading, you need to start the crawler process first. The crawler process listens to the `6789` port by default and only accepts local requests. You can change the yml configuration `spider_addr` to `0.0.0.0:6789` to accept external listening requests.

After the crawler process is started, it does not listen to any exchange, market, or symbol by default. It will automatically connect to the specified exchange, market, and listen to the required information based on the client request received.
```text
Usage of spider:
  -config value
        config path to use, Multiple -config options may be used
  -datadir string
        Path to data dir.
  -level string
        set logging level to debug (default "info")
  -logfile string
        Log to the file specified
  -max-pool-size int
        max pool size for db
  -no-compress
        disable compress for hyper table
  -no-default
        ignore default: config.yml, config.local.yml
  -nodb
        dont save orders to database
```
## Start real-time trading (simulated trading/real trading)
```shell
bot trade [-spider] [-pairs PAIRS] ...
```
When you specify `env` as `dry_run` in the yml configuration file, the order will not be submitted to the exchange, but simulated matching locally.

When `env` is set to `prod` or `test`, it will be submitted to the production environment or test network of the exchange respectively (some exchanges do not support test networks).

You can configure `spider_addr` in yml, and the robot will automatically try to connect to the crawler when it starts, and subscribe to the current exchange, market and related symbol data.

You can also add the `-spider` parameter to automatically start the crawler in this process when starting the robot.

When you enable `api_server` in the `yml` configuration file and set the access account and password, you can view and manage your real-time trading robot with the Dashboard UI.

```yaml
api_server:
  enable: true  # enable here
  bind_ip: 0.0.0.0
  port: 8001
  jwt_secret_key: '123456789'  # This should be complicated enough
  users:
    - user: ban
      pwd: '123'
      acc_roles: {user1: admin}  # allow bot accounts and role
```

```text 
Usage of trade:
  -config value
        config path to use, Multiple -config options may be used
  -datadir string
        Path to data dir.
  -level string
        set logging level to debug (default "info")
  -logfile string
        Log to the file specified
  -max-pool-size int
        max pool size for db
  -no-compress
        disable compress for hyper table
  -no-default
        ignore default: config.yml, config.local.yml
  -nodb
        dont save orders to database
  -pairs string
        comma-separated pairs
  -spider
        start spider if not running
  -stake-amount stake_amount
        Override stake_amount in config
  -stg-dir value
        dir path for strategies
  -task-hash string
        hash code to use
  -task-id int
        task
```
## Backtesting
```shell
bot backtest [-nodb] [-separate] ...
```
This will start the backtest according to the `run_policy` in the yml configuration. Since the backtest may generate a large number of orders, it is recommended that you enable the `-nodb` option to not record the orders in the database, but only save them in a csv file.

The default backtest is to run multiple strategy groups configured by `run_policy` at the same time to get a combined backtest report. If you want to test each strategy group in `run_policy` separately, please enable the `-separate` option.

During the backtest, if the candle of the relevant symbol does not exist, it will be automatically downloaded and saved to the database.

After the backtest is completed, the backtest results will be saved in `[data directory]/backtest/task_[TASKID]` by default. If you specify `-nodb`, `[TASKID]` is -1, that is, saved in the `task_-1` directory
```text
Usage of backtest:
  -config value
        config path to use, Multiple -config options may be used
  -cpu-profile
        enable cpu profile
  -datadir string
        Path to data dir.
  -level string
        set logging level to debug (default "info")
  -logfile string
        Log to the file specified
  -max-pool-size int
        max pool size for db
  -mem-profile
        enable memory profile
  -no-compress
        disable compress for hyper table
  -no-default
        ignore default: config.yml, config.local.yml
  -nodb
        dont save orders to database
  -pairs string
        comma-separated pairs
  -separate
        run policy separately for backtest
  -stake-amount stake_amount
        Override stake_amount in config
  -stg-dir value
        dir path for strategies
  -timerange string
        Specify what timerange of data to use
  -timestart string
        set start time, allow multiple formats
  -timeend string
        set end time, timestart is required
```

## CandleStick related tools
[Detail Document](/en-US/advanced/kline_tools.md)

**bot kline down**  
Download candle from the exchange and save to the database

**bot kline load**  
Import candle from csv/zip file to database

**bot kline export**  
Export candle from database to csv file

**bot kline purge**  
Purge candle from database.

**bot kline correct**  
Check if there is any error in database candle and correct it automatically.

**bot kline adj_calc**  
Recalculate the adjust factor (for the Chinese futures market)

**bot kline adj_export**  
Export the adjust factor to a CSV file
```text
banbot kline:
    down:   download kline data from exchange
    load:   load kline data from zip/csv files
    export: export kline to csv files from db
    purge:  purge/delete kline data with args
    correct: sync klines between timeframes
    adj_calc: recalculate adjust factors
```
## Tick related tools
**bot tick convert**  
Convert the csv format ticks of the Chinese futures market and then output a csv format file.

**bot tick to_kline**  
Aggregate the tick files of the Chinese futures market into 1m period candle csv format files.

```text
banbot tick:
    convert:        convert tick data format
    to_kline:       build kline from ticks
```

## Live Trading Tools
**bot live down_order**  
`-account user1 -exchange binance -market linear -timestart 20250401 -timeend 20250405 -pairs BTC,ETH`
Download orders for the specified account to the `@exgOrders/` directory. The saved file format is `[exchange]_[market]_[account]_[apiKeyPrefix].gob`, such as `binance_linear_user1_C1uMI.gob`. This can be used with `bot tool cmp_orders` to compare backtest orders.

**bot live close_order**  
`-account user1 -pair BTC,ETH -strat ma:demo1,ma:demo2 -exg true`
Close positions for the specified user's eligible orders.

## Other tools
**bot tool collect_opt**  
Collect hyperparameter tuning results and display them to the console in order.

**bot tool bt_opt**  
Tuning hyperparameters in time rolling, and then backtesting. For example, for the data of the last three years. Each year's data is used for hyperparameter tuning, and then the tuned parameters are automatically backtested for the next three months; then the tuning is postponed for three months and repeated; this is repeated to simulate the backtest results in real scenarios.

**bot tool load_cal**  
Load trading calendar (for Chinese futures market)

**bot tool cmp_orders**  
Compare the orders exported from Binance with the local backtest order records to check whether the backtest and the real market are consistent.

**bot tool list_strats**  
print registered strategies.

**bot tool data_server**  
Start a gRPC server to provide access to data and metric results for clients written in other languages. It can be used for AI machine learning and other purposes.

**bot tool calc_perfs**  
Input in a CSV/XLSX file, where each row represents one day and each column represents the cumulative income of a variety. Calculate Shape/Sortino for each column

**bot tool corr**  
Calculate the correlation coefficient for a group of varieties selected by YAML, a correlation matrix image can be output at regular intervals, or an average correlation coefficient CSV file can be output.

**Bot Tool sim_bt**
Execute rolling simulation backtesting, extract trading varieties in each interval from the log file, and conduct backtesting; export the order records and enters2.html.

**Bot Tool merge_assets**
Merge the assets.html in multiple backtesting reports into one for convenient comparison.

**Bot Tool bt_factor**
This command is used to test the cross-sectional rotation factor. You can conduct full-volume and full-period backtesting for all varieties using a certain strategy. Then, use this command to filter some varieties based on the performance of some varieties in the previous two years for trading in the following months. Repeat this process until the latest data, combine and output the backtesting report of the filtered varieties.

You can make the following settings in the yml file for full-volume backtesting:
```yaml
pairmgr:
  cron: ''
  use_latest: true  # This will use the latest date to determine the list of varieties
pairlists:
  - name: VolumePairList
    back_period: 30d
```

Then implement your factor and register it in `strat.FactorMap`.

Then execute:
```shell
bot tool bt_factor -factor name -interval 4M -in @backtest\abc\orders.gob -out @backtest\abc_name
```

**Bot Tool bt_result**
Reconstruct the backtesting report from the orders.gob file of the backtesting result. It can output assets.html, enters.html, detail.json, orders.csv, etc.

```text
banbot tool:
    collect_opt:    collect result of optimize, and print in order
    load_cal:       load calenders
    cmp_orders:     compare backTest orders with exchange orders
    data_server:    serve a grpc server as data feeder
    calc_perfs:     calculate sharpe/sortino ratio for input data
    corr:           calculate correlation matrix for symbols
    sim_bt:         run backtest simulation from report
    list_strats:    list registered strategies
    merge_assets:   merge multiple assets.html files into one
    bt_factor:      backtest factors with orders
    bt_result:      build backtest result from orders.gob and config
```
