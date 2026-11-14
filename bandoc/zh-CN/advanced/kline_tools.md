# K 线与序列工具

banbot支持下载、导入、导出、删除、纠正等丰富的K线处理工具。

## K线存储核心
banbot 支持使用 QuestDB 或 TimescaleDB 存储 K 线等时序数据。使用 QuestDB 时，banbot 可在本地自动下载安装；TimescaleDB 由用户自行部署。两种后端都支持本页的下载、导入、导出、删除和纠正工具，配置方法请参阅[数据库](../guide/database.md)。

为在存储空间和读取效率间取得良好的平衡，只存储`1m,5m,15m,1h,1d`这些时间周期，策略可使用 `3m`、`4h` 等能由已有基础周期聚合的周期；未存储的周期由更小周期动态聚合，不能由分钟数据生成真实秒级数据。

banbot 通过内部覆盖范围机制记录每个品种、时间周期的已下载区间和已确认无数据区间，并据此判断需要下载的数据。该机制在两个数据库后端中保持一致；请通过本页命令管理数据，不要依赖或直接修改内部元数据表。

## 下载K线
默认情况下，回测与实盘启动会按覆盖缺口下载所需 K 线。启用 `bt_no_kline_download` 或严格回放时，不要依赖自动下载；应先准备完整历史数据。

您可执行下面命令主动下载K线数据：

`bot kline down --timeframes 1h,1d --timerange 20240101-20250101 --pairs BTC,ETH`

`--timeframes` 必填；时间范围和标的可从 YAML 配置读取。支持 `--timerange`，或配对使用 `--timestart`、`--timeend`。以下示例假定已配置交易所、市场、数据库及数据目录；可用 `--config` 指定配置，`@` 路径表示相对数据目录。

## 聚合大周期 K 线

当已有较小周期的 K 线，需要主动生成较大周期数据时，可执行：

`bot kline agg --timeframes 1h,1d --pairs BTC,ETH`

`--timeframes` 和 `--pairs` 用于限定聚合范围。常规回测和实盘会按需读取、下载与聚合数据，只有需要预先处理数据时才需要手动执行此命令。

## 导出K线(protobuf)
当您需要同步K线数据到另一个banbot的数据库中时，建议您导出为`protobuf`格式，此导出过程进行了存储空间和执行速度的双重优化。

您可执行下面命令导出K线：

`bot data export --config @config.yml --config @config.local.yml --config @export.yml --out @data --concur 4`

其中`--config`和`--out`是必填参数，您可使用`--config`同时指定机器人配置和导出配置，banbot 会将最后一个 YAML 配置文件仅作为导出任务文件，前面的文件作为机器人配置，导出配置示例：
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
如上，您可指定多个导出任务，可以留空的项：`exchange`,`market`,`timeframes`,`symbols`，这些项留空时将视为选择全部。`time_range`不可留空。

将导出`exInfo1.dat`和`kline[num].dat`两种数据文件，前者存储了品种等信息，后者是分块的K线数据；每个`kline[num].dat`文件最大1G。

> 注意请勿修改导出的文件名，否则将导致导入时无法识别

此格式导出固定 K 线字段及配套元数据，不包含任意自定义列或独立时序表。需要保留这些数据时，另行通过 `SeriesStore`/字段查询接口读取并保存；不要将本命令作为完整时序库备份。

## 导出K线(csv.zip)
当您需要导出K线供其他程序进一步读取使用时，建议您导出为zip压缩的csv格式。可以很容易地被编程访问。

您可执行下面命令导出：

`bot kline export --out @data --timeframes 1h,1d --pairs BTC,ETH --adj none --tz UTC`

其中`--out`和`--timeframes`是必填参数，`--out`应当是一个目录，当您未指定`pairs`时，banbot将使用当前yaml配置的交易所和市场下所有的品种。

`--tz` 默认 UTC，时间输出为 `YYYY-MM-DD HH:mm:ss`。`--adj none` 导出未复权价格，`pre`/`post` 分别选择前/后复权；建议显式设置。

输出为 `{symbol}_{timeframe}.zip`，标的中的 `/`、`:` 替换为 `_`；ZIP 内是同名 CSV，列为时间、开、高、低、收、成交量、主动买入量，不包含任意扩展字段。导出范围取 YAML 的时间范围，本命令不接受 `--timerange`。使用新的输出目录，已有该标的 ZIP 时当前实现会跳过该标的的所有周期。

这是供外部程序读取的格式，不能直接通过 `kline load` 往返：导入需要毫秒时间戳和可映射的标的文件名。跨库迁移固定 K 线优先使用 protobuf。

## 导入K线(protobuf)
您可指定下面命令从导出数据导入到当前数据库：

`bot data import --in @data --concur 4`

其中`--in`是必填参数，`--concur`是并发导入线程数量，默认1，此设置无需与导出时保持一致。

banbot保存品种信息时，其 `sid` 是库内分配的标识，不保证跨库一致，故在导入过程中会自动加载已有品种信息，与导出数据中维护一个id映射，避免品种id互相覆盖等问题。

## 导入K线(csv.zip)
您可使用下面命令导入zip数据到数据库：

`bot kline load --in @data`

其中`--in`是必填参数，其路径可以是zip文件，也可以是包含zip文件的文件夹。

当前入口只读取 ZIP 文件或目录中的 `.zip`，ZIP 内 CSV 使用 `Symbol.csv`，其中 Symbol 必须能由配置的交易所映射成标的；不能直接用导出文件中的 `symbol_timeframe` 名称。归档按年份命名（如 `2024.zip`）时，年份也参与历史标的映射。

CSV 每行至少六列：13 位毫秒时间戳、开、高、低、收、成交量；可选第七列是主动买入量。按时间升序准备至少两条有效记录，导入器根据相邻正时间差推断周期，并选择内置存储周期。请先校验数字和时间戳：当前解析器会忽略部分转换错误，不能用它做输入校验。该入口不导入任意列；扩展列请用 `KLineSeriesStore`，独立序列请用 `SeriesStore`。

## 删除K线
您可使用下面命令从数据库中清理K线数据：

`bot kline purge --timeframes 1h,1d --pairs BTC,ETH`

`--timeframes` 省略时选择所有内置存储周期；`--pairs` 可来自配置，最终为空时选择当前交易所/市场下的所有已登记标的。命令删除所选标的与周期的全部历史，不接受时间范围参数。执行前务必核对确认概要中的标的和周期。

开始执行删除前，会输出概要信息，要求您输入`y`确认删除。

## 纠正K线中的错误
`correct` 用于同步不同物理周期的 K 线并维护范围信息，不保证修复上游错误价格，也不是任意自定义序列的校验器：

`bot kline correct --pairs BTC`

`--pairs` 可选，省略时处理所有已存标的，可能耗时很长。先备份，并先用 `verify` 确认问题。

## 从已有 K 线重建覆盖范围

数据行存在但范围元数据缺失时，明确指定标的、周期和时间范围：

`bot kline repair-ranges --pairs BTC --timeframes 1m,1h --timerange 20240101-20250101`

该命令要求显式静态标的列表，从已有行重建范围，不下载缺失行情；不要用它证明尚未抓取的区间已覆盖。

## 校验 K 线与覆盖范围

`bot kline verify` 用于检查 K 线数据与内部覆盖范围元数据是否一致：

`bot kline verify --pairs BTC,ETH --tables kline_1m,kline_1h --batch-size 1000`

所有参数均可选。`--pairs` 限制品种，`--tables` 限制数据表，`--batch-size` 控制单批检查数量。发现异常时请先备份数据，再结合 `bot kline correct` 或重新下载处理。

## 自定义序列工具

```shell
bot series list
bot series down --tables funding_rate --pairs BTC --timestart 20240101 --timeend 20250101
```

`list` 输出编译进当前可执行文件的 source 定义 JSON，不是数据库表清单。`down` 的 `--tables` 指已注册的 source 名，不是物理表名；省略时处理全部自定义源。该命令按缺口补齐，只下载不晚于当前时间的已闭合规则周期，当前不支持 `event` 周期。接入步骤见[自定义时序数据](../guide/custom_data.md)。
