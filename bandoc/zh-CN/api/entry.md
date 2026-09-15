# entry 包

entry 包提供了系统的入口点和命令行接口。

配置型命令会解析 `config.Snapshot`，打开显式的存储与交易所依赖，并为该次执行创建 `runtime.Process` 和 `runtime.Runtime`。回测、实盘和优化通过 Runtime 取得各自的配置、时钟、策略、订单与生命周期；调用方应在任务结束时关闭并等待其 Runtime。少数尚未迁移的维护接口仍走兼容路径。

## 公开方法

### RunCmd
这是banbot的命令行入口方法，您可在自己的策略项目入口文件中调用此方法，以便从终端中访问banbot的各个子命令。
