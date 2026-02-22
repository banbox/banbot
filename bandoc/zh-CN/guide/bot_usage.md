编写好策略后，您可以将策略和banbot打包为单个可执行文件（注意需在策略项目根目录下执行）：
```shell
# for linux/macos
go build -o bot

# for windows
go build -o bot.exe
```
然后您可将此可执行文件直接用于启动UI、回测、超参数优化、实盘交易等！

您直接执行`bot`将启动WebUI界面，您可访问`http://localhost:8000`进行策略管理、回测、分析交易等。

运行`bot -help`将显示一些子命令帮助列表：
```text
banbot 0.1.5
please run with a subcommand:
        trade:      live trade
        backtest:   backtest with strategies and data
        spider:     start the spider
        optimize:   run hyper parameters optimization
        bt_opt:     rolling backtest with hyperparameter optimization
        kline:      run kline commands
        tick:       run tick commands
        tool:       run tools
        web:        run web dashboard
```
## 通用命令参数
**数据目录(-datadir)**

您通过命令行启动时，除非设置了环境变量`BanDataDir`，否则每次启动时，都务必传入参数`-datadir`指定数据目录。

**默认配置文件**

在读取您传入的额外的配置文件前，banbot会从数据目录下，尝试读取两个默认配置文件：`config.yml`和`config.local.yml`。
如果这两个里面已经覆盖了您本次所需的参数，则不需要再通过`-config`额外传入配置文件路径。

**命令帮助信息**

您可以通过在命令后面添加`-help`或`-h`参数，显示帮助信息。

## 启动WebUI研究&回测
为方便您的策略研究和回测，我们提供了WebUI可视化。您可通过WebUI在线编辑策略、开始回测、查看盈亏曲线、分析订单等。

这是仅针对策略研究阶段的WebUI，默认只允许您本地计算机访问，故无需鉴权即可访问，如需外网访问，请设置`-host 0.0.0.0`。

如果您需要在banbot实时交易时访问Dashboard UI，请参考[实时交易](#启动实时交易模拟交易实盘)。

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

## 启动爬虫进程
```shell
bot spider [-datadir PATH] [-c PATH] [-c PATH]
```
当您要启动实时交易时，需要先启动爬虫进程。爬虫进程默认监听`6789`端口，只接受本地的请求。您可以将yml配置`spider_addr`改为`0.0.0.0:6789`，以便接受来自外部的监听请求。

爬虫进程启动后，默认不监听任何交易所、市场、品种。它会根据收到的客户端请求，自动连接到指定交易所，市场，并监听需要的信息。
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
## 启动实时交易(模拟交易/实盘)
```shell
bot trade [-spider] [-pairs PAIRS] ...
```
当您在yml配置文件指定`env`为`dry_run`时，订单不会提交到交易所，而是在本地模拟撮合。

当`env`设置为`prod`或`test`时，会分别提交到交易所的生产环境或者测试网络（部分交易所不支持测试网络）。

您可以在yml配置`spider_addr`，机器人启动时会自动尝试连接爬虫，并订阅当前交易所、市场和相关品种数据。
您也可以添加`-spider`参数，启动机器人时自动在本进程中启动爬虫。

当您在`yml`配置文件中启用`api_server`，且设置访问账号密码时，即可通过Dashboard UI查看并管理您的实时交易机器人。

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
## 回测
```shell
bot backtest [-nodb] [-separate] ...
```
这将按yml配置中的`run_policy`启动回测，由于回测可能生产大量订单，建议您启用`-nodb`选项，不将订单记录到数据库，而是只保存到csv文件。

默认回测是将`run_policy`配置的多个策略组同时运行，得到一个组合的回测报表。如果您希望单独测试`run_policy`中每一个策略组，请启用`-separate`选项。

回测时，如果相关品种的K线不存在，则会自动下载保存到数据库。

回测结束后，回测结果将默认保存在`[数据目录]/backtest/task_[TASKID]`下，如果您指定`-nodb`，则`[TASKID]`是-1，即保存到`task_-1`目录下
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

## K线相关工具
[详细介绍](/zh-CN/advanced/kline_tools.md)

**bot kline down**  
从交易所下载K线保存到数据库

**bot kline load**  
从csv/zip文件中导入K线到数据库

**bot kline export**  
从数据库导出K线到csv文件

**bot kline purge**  
从数据库中清空K线。

**bot kline correct**  
检查数据库K线是否有错误并自动纠正。

**bot kline adj_calc**  
重新计算复权因子（用于中国期货市场）

**bot kline adj_export**  
导出复权因子到csv文件.
```text
banbot kline:
    down:       download kline data from exchange
    load:       load kline data from zip/csv files
    export:     export kline to csv files from db
    purge:      purge/delete kline data with args
    correct:    sync klines between timeframes
    adj_calc:   recalculate adjust factors
    adj_export: export adjust factors to csv
```
## Tick相关工具
**bot tick convert**  
对中国期货市场的csv格式tick进行转换，然后输出csv格式文件。

**bot tick to_kline**  
对中国期货市场的tick文件聚合为1m周期的K线csv格式文件。

```text
banbot tick:
    convert:        convert tick data format
    to_kline:       build kline from ticks
```

## 实盘交易工具
**bot live down_order**  
`-account user1 -exchange binance -market linear -timestart 20250401 -timeend 20250405 -pairs BTC,ETH`
下载指定账户的订单到`@exgOrders/`目录下，保存的文件格式为`[exchange]_[market]_[account]_[apiKeyPrefix].gob`，如`binance_linear_user1_C1uMI.gob`。可用于`bot tool cmp_orders`对比回测订单。

**bot live close_order**  
`-account user1 -pair BTC,ETH -strat ma:demo1,ma:demo2 -exg true`
对指定用户的符合订单进行平仓。

## 其他工具
**bot tool collect_opt**  
收集超参数调优结果，并按顺序显示到控制台。

**bot tool bt_opt**  
按时间滚动进行超参数调优，然后回测。比如对于最近3年的数据。每1年的数据用于超参数调优，然后用调优后的参数自动回测接下来的3个月；然后推迟3个月重复进行调优；如此反复，模拟真实场景下回测结果。

**bot tool load_cal**  
加载交易日历（针对中国期货市场）

**bot tool cmp_orders**  
将从币安导出的订单与本地回测订单记录进行对比，检查回测和实盘是否一致。

**bot tool list_strats**  
输出已注册的策略列表。

**bot tool data_server**  
启动一个grpc服务器，供其他语言端访问数据和指标结果等。可用于AI机器学习等。

**bot tool calc_perfs**  
传入一个csv/xlsx文件，每行表示一天，每列表示一个品种的累计收益。为每列计算Shape/Sortino

**bot tool corr**  
为yaml筛选后的一组品种计算相关系数，可以每隔一段时间输出一个相关矩阵图片，也可以输出平均相关系数csv文件。

**bot tool sim_bt**  
执行滚动模拟回测，从日志文件中提取每个区间的交易品种，并进行回测；导出订单记录和enters2.html

**bot tool merge_assets**  
将多个回测报告中的assets.html合并为一个，方便对比

**bot tool bt_factor**  
此命令用于测试截面轮动因子，您可针对所有品种使用某策略进行全量全周期回测，然后使用此命令，根据前面2年一些品种表现筛选一些品种用于后续几个月交易，如此滚动直至最新，组合并输出筛选品种的回测报告。

您可在yml中进行下面设置来进行全量回测：
```yaml
pairmgr:
  cron: ''
  use_latest: true  # 这会使用最新日期确定品种列表
pairlists:
  - name: VolumePairList
    back_period: 30d
```

然后实现您的因子并注册到`strat.FactorMap`中。

然后执行：
```shell
bot tool bt_factor -factor name -interval 4M -in @backtest\abc\orders.gob -out @backtest\abc_name
```

**bot tool bt_result**  
从回测结果的orders.gob文件重建回测报告。可输出：assets.html, enters.html, detail.json, orders.csv等。

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
