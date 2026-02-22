## 如何升级banbot到最新版本？
**Docker**  
```bash
docker compose pull banbot
docker compose up -d banbot
```

**本地安装**  
在您的策略代码项目（环境变量BanStratDir指向的目录）下编辑`go.mod`，将`github.com/banbox/banbot`后面的版本号，更新为最新；
然后在当前目录下打开终端，执行`go mod tidy`，应用修改加载依赖即可。

::: tip tip
您可在[github](https://github.com/banbox/banbot/releases)或[gitee](https://gitee.com/banbox/banbot/releases)查看最新可用版本。
如果您喜欢测试最新特性，可使用带`-beta.?`后缀的不稳定版本。大多数人可能希望使用最新稳定版本如`va.b.c`
:::

## 为什么不提供预编译的安装包？
量化策略大多需要自行编写代码，banbot为了在兼顾开发效率的同时尽可能提升性能，选择golang作为系统和策略的统一语言，这也对用户提供了最大自由度。  
golang的哲学是将一切（banbot和您的策略代码）编译为单个可执行文件，您可以非常方便地将其分发给任何人并直接运行。  
所以您只需[拉取示例策略项目](./init_project.md)，使用内置策略或实现您自己的策略，编译后即可体验回测、实盘交易等所有功能。

## 支持哪些类型的量化策略，不支持哪些类型策略？
**受支持的策略**：1分钟及以上的时序策略（可多品种多周期）

**有限支持**：多因子截面策略、AI驱动的策略

**暂未支持的策略**：高频交易、套利交易（三角套利、跨所套利、期限套利等）、配对交易、统计套利

## 支持的市场
banbot支持币安、欧易、bybit的现货、U本位合约、币本位合约。欢迎您提交pull request支持更多交易所和市场。

暂未支持：股票、期货、外汇、债券、去中心化加密货币交易所等

## 稳定吗？可用于生产环境真实交易吗？
我们从24年12月1日起使用banbot实盘至今，期间解决了不少bug，现在常用的实盘特性都测试通过，但目前实盘用户尚不够多，很有可能还有未覆盖到的地方导致资金损失；
您如果小资金测试策略可以考虑立即开始使用banbot，如果您资金量较大，建议您先小资金试运行几个月看看。

## 可以开空头仓位吗？
banbot支持开空头仓位，只需要在`OpenOrders(&strat.EnterReq{Tag: "short", Short: true})`中设置`Short`为true即可。

## 可以同时打开几个订单？
banbot不限制您打开订单的数量，您可以在做多或做空打开任意多个数量的订单。

## 可以只退出仓位的一部分吗？
banbot支持仓位或订单的部分退出。只需要`CloseOrders(&strat.ExitReq{Tag: "close", ExitRate: 0.5, FilledOnly: true})`，即可将已入场的仓位，平仓一半。
您也可以传入`OrderID`参数，只退出指定订单的一半仓位。

## 修改配置后需要重新启动机器人吗？
目前您修改配置后，需要重新启动机器人才能生效。不过机器人重新启动后，会自动检测相关仓位和订单，不会丢失。

## 实盘和回测的订单不匹配？
* 排查实盘日志是否有错误
* 检查回测和实盘订单时间的时区是否一致
* 检查配置是否一致：市场、杠杆、策略和时间周期、开单金额等

## 回测时显示"bulk down xx xxx pairs 2024-10-31 ..."，然后一直卡住无响应
可能是数据库连接池大小太小导致批量下载时无法获取数据库会话导致的，请将`database.max_pool_size`改为50或更大后重试

## ormo或ormu报错：constraint failed: NOT NULL constraint failed，对应字段是float64
如果golang某个字段类型float64，而值是nan或inf，写入sqlite时会处理为null，而此列not null时，就会出现上面错误。解决：写入前utils.NanInfTo(v, 0)替换为0

## Get "https://xxx": unexpected EOF
这是无法从go仓库下载的错误，可能有两个原因：`GOPROXY`指向的go仓库不可用，或者VPN代理节点不可用，尝试切换即可。

## 更多其他问题？
推荐您询问[DeepWiki](https://deepwiki.com/banbox/banbot)，它将阅读banbot源代码并准确回答您的相关问题。
