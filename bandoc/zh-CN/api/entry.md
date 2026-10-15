# entry 包

entry 包提供了系统的入口点和命令行接口。

配置型命令会解析 `config.Snapshot`，打开显式的存储与交易所依赖，并为该次执行创建 `runtime.Process` 和 `runtime.Runtime`。回测、实盘和优化通过 Runtime 取得各自的配置、时钟、策略、订单与生命周期；调用方应在任务结束时关闭并等待其 Runtime。少数尚未迁移的维护接口仍走兼容路径。

## 公开方法

### RunCmd
这是banbot的命令行入口方法，您可在自己的策略项目入口文件中调用此方法，以便从终端中访问banbot的各个子命令。

## 因子引擎集成

ValidateBacktestRunSpec 与执行共用无资源校验；混合 replay 要求 events。RegisterFactorLiveBinding 注册真实会话能力，未注册名明确失败。

根命令 `backtest` 与 `trade` 使用同一配置加载器，按 `run_policy` 调度时序、因子或混合引擎。`--mode` 覆盖因子回测 `execution.mode`（默认 `events`）；`trade --dry-run` 是因子/混合历史回放，纯时序实时模拟使用 `env: dry_run`。根命令 `research` 使用同一 YAML 加载器、默认配置、`--datadir` 和 `--no-default` 规则。根命令 `validate --spec`、`explain --spec` 与 `data archive` 分别处理独立表达式和版本数据归档。

任务的显式 Runtime 状态与取消上下文分工独立；多个任务保留各自配置、时钟、策略和订单，同账户的引擎消费者则主动共享账户执行与策略归因。取消或启动失败时先停止接收、Join 在途工作，再释放资源；释放共享账户借用不能停止其他消费者。

[因子 API](factor.md) / [指南](../guide/factor.md)
