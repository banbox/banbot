banbot支持下载、导入、导出、删除、纠正等丰富的K线处理工具。

## K线存储核心
banbot 支持使用 QuestDB 或 TimescaleDB 存储 K 线等时序数据。使用 QuestDB 时，banbot 可在本地自动下载安装；TimescaleDB 由用户自行部署。两种后端都支持本页的下载、导入、导出、删除和纠正工具，配置方法请参阅[数据库](../guide/database.md)。

为在存储空间和读取效率间取得良好的平衡，只存储`1m,5m,15m,1h,1d`这些时间周期，但您在策略中可以使用`3m`等任意时间周期，未存储的时间周期将自动从更小的时间周期数据中动态聚合得到。

banbot 通过内部覆盖范围机制记录每个品种、时间周期的已下载区间和已确认无数据区间，并据此判断需要下载的数据。该机制在两个数据库后端中保持一致；请通过本页命令管理数据，不要依赖或直接修改内部元数据表。

## 下载K线
您无需实现下载K线数据，banbot将在回测、实盘过程中自动下载所需的数据。

您可执行下面命令主动下载K线数据：

`bot kline down -timeframes 1h,1d -timerange 20240101-20250101 -pairs BTC,ETH`

其中`timeframes`是必填参数项，其余的如果未指定则默认从yaml配置文件中解析。

## 聚合大周期 K 线

当已有较小周期的 K 线，需要主动生成较大周期数据时，可执行：

`bot kline agg -timeframes 1h,1d -pairs BTC,ETH`

`-timeframes` 和 `-pairs` 用于限定聚合范围。常规回测和实盘会按需读取、下载与聚合数据，只有需要预先处理数据时才需要手动执行此命令。

## 导出K线(protobuf)
当您需要同步K线数据到另一个banbot的数据库中时，建议您导出为`protobuf`格式，此导出过程进行了存储空间和执行速度的双重优化。

您可执行下面命令导出K线：

`bot data export -config @export.yml -out @data -concur 4`

其中`-config`和`-out`是必填参数，您可使用`-config`同时指定机器人配置和导出配置，banbot会将最后一个yml配置文件作为导出配置文件，导出配置示例：
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

## 导出K线(csv.zip)
当您需要导出K线供其他程序进一步读取使用时，建议您导出为zip压缩的csv格式。可以很容易地被编程访问。

您可执行下面命令导出：

`bot kline export -out @data -timeframes 1h,1d -pairs BTC,ETH -tz UTC`

其中`-out`和`-timeframes`是必填参数，`-out`应当是一个目录，当您未指定`pairs`时，banbot将使用当前yaml配置的交易所和市场下所有的品种。

`-tz`的默认时区是UTC，导出的csv中时间将以`YYYY-MM-DD HH:mm:SS`格式显示。

导出过程中，会以`{symbol}_{timeframe}.zip`的命名格式在导出目录下保存数据。

## 导入K线(protobuf)
您可指定下面命令从导出数据导入到当前数据库：

`bot data import -in @data -concur 4`

其中`-in`是必填参数，`-concur`是并发导入线程数量，默认1，此设置无需与导出时保持一致。

banbot保存品种信息时，其`sid`在每个数据库都是随机的，故在导入过程中会自动加载已有品种信息，与导出数据中维护一个id映射，避免品种id互相覆盖等问题。

## 导入K线(csv.zip)
您可使用下面命令导入zip数据到数据库：

`bot kline load -in @data`

其中`-in`是必填参数，其路径可以是zip文件，也可以是包含zip文件的文件夹。

banbot将自动解压缩zip文件夹，提取其中的csv文件，要求csv文件命名应当为`Symbol.csv`，会自动结合yaml配置的交易所和市场确定唯一`ExSymbol`品种对象。

csv内的数据要求固定列为：时间、开、高、低、收、成交量。（其中时间应当是13位毫秒时间戳）

## 删除K线
您可使用下面命令从数据库中清理K线数据：

`bot kline purge -timeframes 1h,1d -pairs BTC,ETH`

其中`-timeframes`是必填参数。当`-pairs`未指定时，将使用yaml配置中的`pairs`列表，如果也为空，则默认删除yaml配置的交易所市场下所有的品种数据。

开始执行删除前，会输出概要信息，要求您输入`y`确认删除。

## 纠正K线中的错误
您在使用 banbot 过程中，异常中断或历史数据变更可能使 K 线与内部覆盖范围记录不一致，进而影响后续下载或读取。您可运行下面命令进行纠正：

`bot kline correct -pairs BTC`

其中`-pairs`是可选参数，如果留空将对所有品种全部执行纠正，这视数据大小，可能耗费一两个小时。

## 校验 K 线与覆盖范围

`bot kline verify` 用于检查 K 线数据与内部覆盖范围元数据是否一致：

`bot kline verify -pairs BTC,ETH -tables kline_1m,kline_1h -batch-size 1000`

所有参数均可选。`-pairs` 限制品种，`-tables` 限制数据表，`-batch-size` 控制单批检查数量。发现异常时请先备份数据，再结合 `bot kline correct` 或重新下载处理。
