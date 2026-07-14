# web 包

web 包提供了Web服务器和API接口相关的功能。

## API 路由

实时 API 启动后提供以下路由组：

- `/api/kline`：symbols、历史 K 线、指标计算、CSV，以及已注册 source 的 `data_sources` 和 `series` 只读查询。
- `/api/ws/ohlcv`：OHLCV websocket。
- `/api/bot`：余额、订单、策略任务、机器人配置和交易控制，要求认证。
- `/api/login`、`/api/ping`、`/api/strat_call`：公共认证和策略调用接口。

`GET /api/kline/series` 使用 `source`、`sid`、`timeframe`、`start`、`end`、`limit` 和可选 `fields` 查询内置 K 线或已注册的自定义时序数据；该接口只读，单次最多返回 1000 行。

### RunDev
运行 Web UI 机器人控制面板。该方法用于启动开发环境下的 Web 界面，用于机器人的管理和监控。

参数：
- `args []string` - 命令行参数列表

返回：
- `error` - 如果启动过程中出现错误，返回相应的错误信息；启动成功则返回 nil

### StartApi
启动实时交易的 Web 监控面板。该方法用于启动一个 Web 服务器，提供实时交易数据的监控和管理功能。

返回：
- `*errs.Error` - 如果启动过程中出现错误，返回自定义错误类型；启动成功则返回 nil
