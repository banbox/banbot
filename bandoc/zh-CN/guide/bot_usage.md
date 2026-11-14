# 使用 bot 命令

在策略项目根目录执行 `go build -o bot`（Windows 使用 `go build -o bot.exe`），将已注册策略与 BanBot 编译为同一可执行文件。下文 `bot` 表示该文件；当前目录运行时使用 `./bot`。

```bash
bot --help
bot backtest --help
bot tool --help
```

命令采用连字符，如 `bt-opt`、`collect-opt`。部分旧下划线命令仍作为兼容别名；新脚本使用本页规范名称和 `--flag` 双横线长参数。参数由各子命令声明，不能把一个子命令的参数直接用于另一个。

## 配置与公共参数

配置型命令通常先读数据目录中的 `config.yml`、`config.local.yml`，再读重复的 `--config` 和内联 `--config-data`，最后应用显式 CLI 覆盖。`--datadir` 优先于环境变量 `BanDataDir`。默认文件不存在时跳过；`--no-default` 禁用默认文件。仅加载显式文件可不设数据目录，但实际存储和输出仍可能需要它。详见[配置指南](./configuration.md)。

```bash
bot backtest --datadir /path/to/data --config experiment.yml
bot backtest --datadir /path/to/data --no-default --config experiment.yml
```

常见参数为 `--logfile`、`--level`、`--max-pool-size`；诊断参数包括 `--cpu-profile`、`--mem-profile`、`--net-off`。支持的命令还提供 `--dlock`。`--net-off` 不等于数据覆盖校验，也不补齐缺失历史数据。

## 开发 WebUI 与 Hub

```bash
bot web --datadir /path/to/data
bot hub --datadir /path/to/data
```

直接执行 `bot` 也会启动开发 WebUI，默认地址为 `http://127.0.0.1:8000`。开发 Web 使用自己的 `--host`、`--port`、`--password`、`--tz` 参数；它与交易时 `api_server` 配置的 Dashboard 不同。

`hub`（兼容别名 `spider`）提供公共行情和本地账户执行服务。公共行情地址由 `spider_addr` 控制，默认 `127.0.0.1:6789`；私有执行 socket 默认位于 `<datadir>/execution/hub.sock`，可用 `--execution-socket` 覆盖。Hub 只接受 `prod` / `dry_run` 环境；回测在进程内运行。配置公共行情监听地址不能代替私有账户授权。

## 交易、回测与研究

```bash
bot trade --datadir /path/to/data
bot backtest --datadir /path/to/data --out /path/to/report
bot backtest --datadir /path/to/data --separate
bot research --datadir /path/to/data --config factor.yml
```

`backtest` / `trade` 按 `run_policy[].engine` 分派时序、因子或混合策略。省略 `engine` 为旧时序语义；新配置建议明确声明。

时序、因子和混合策略的默认实时入口均使用共享 Hub。先启动 `hub`，再使用 `env: dry_run` 进行 Hub paper 实时模拟，或用 `env: prod` 生产交易。默认入口只接受线性永续、明确结算币及完整执行声明，不支持 `test`、现货或反向合约；适配器具有测试网络/现货能力不代表普通 `trade` 支持。`--spider` 不能替代私有执行 Hub，Hub 缺失会失败而不回退本地发送。完整配置、限制和恢复步骤见[共享账户与 Hub](./execution.md)。交易 Dashboard 另需 `api_server` 用户和账户角色，见[实盘指南](./live_trading.md)。

因子回测的 `--mode weights|events` 覆盖根 `execution.mode`，都未设置时默认 `events`。混合回测只支持 `events`。因子/混合 `trade --dry-run` 是历史 events 回放；它不是时序实时模拟开关。真实因子/混合交易可使用 `--live-provider`，缺少经过验证的会话能力会预检失败，不回退为模拟交易。

`--separate` 只用于纯时序回测；因子任务请拆分配置。回测数据与报告见[回测指南](./backtest.md)，参数搜索见[超参优化](./hyperopt.md)和[滚动回测](./roll_btopt.md)。`optimize` / `bt-opt` 是时序策略工具，不提供因子/混合超参搜索。

## 数据与维护工具

先运行 `bot <组> <命令> --help` 核对输入格式。除明确声明的位置参数外，输入文件使用 `--in`，输出使用 `--out`。

| 命令 | 用途 |
| --- | --- |
| `data export` / `data import` | 数据库与 protobuf 文件之间导出/导入 |
| `data archive` | 版本数据归档，见[因子指南](./factor.md) |
| `kline down` / `load` / `agg` | 下载、导入、聚合 K 线 |
| `kline export` / `purge` | 导出 CSV / 删除指定 K 线 |
| `kline repair-ranges` / `verify` / `correct` | 重建范围元数据、校验覆盖、同步周期间 K 线 |
| `kline adj-calc` / `adj-export` | 复权因子计算/导出 |
| `series down` / `list` | 注册的自定义时序数据下载/范围查询 |
| `tick convert` / `to-kline` | Tick 格式转换 / 聚合为 K 线 |
| `tool list-strats` | 查看编译进程序的策略 |
| `tool collect-opt` / `test-pickers` | 收集搜索结果 / 滚动比较挑选器 |
| `tool sim-bt` / `bt-result` | 从报告模拟 / 从订单重建报告 |
| `tool test-live-bars DUMP_FILE` | 将实盘 K 线 dump 与本地数据比较 |
| `tool cmp-orders` | 对比交易所订单和回测订单 |
| `tool calc-perfs` / `corr` / `merge-assets` | 收益指标、相关矩阵、资产曲线合并 |
| `tool load-cal` / `data-server` | 导入交易日历 / 启动 gRPC 数据服务 |
| `tool bt-factor` | 对既有 `orders.gob` 做滚动标的筛选，非原生 factor runner |
| `live down-order` / `close-order` | 下载账户订单 / 按账户、标的或策略平仓 |

```bash
bot live down-order --datadir /path/to/data --account user1 --timestart 20250401 --timeend 20250405 --pairs BTC/USDT:USDT
bot tool bt-result --datadir /path/to/data --config report-config.yml --in /path/to/orders.gob --out /path/to/rebuilt
```

`live close-order` 的 `--account`、`--pair`、`--strat` 为空表示不限制相应维度；`--exg` 表示直接平交易所持仓。执行前明确筛选范围。`tool bt-factor` 的自定义因子注册接口是 `opt.RegisterFactor`，不能使用不存在的 `strat.FactorMap`。

独立表达式检查使用根命令 `bot validate --spec formula.yml` / `bot explain --spec formula.yml`。更多因子配置和数据要求见[因子指南](./factor.md)。
