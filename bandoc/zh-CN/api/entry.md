# entry 包

策略应用通过 `entry` 构建命令行入口。常规使用请看[命令指南](../guide/bot_usage.md)；扩展示例见[自定义命令](../advanced/custom_cmd.md)。

## 入口与命令注册

| API | 用途 |
| --- | --- |
| `RunCmd()` | 程序入口：解析进程参数、处理信号及错误退出 |
| `ExecuteContext(ctx context.Context, args []string) error` | 嵌入式执行；传入不含可执行文件名的参数并接收错误 |
| `NewRootCommand() *cobra.Command` | 创建新的 Cobra 命令树 |
| `AddGroup(name, help string)` | 注册父命令组 |
| `AddCommand(parent string, command *cobra.Command)` | 注册命令；空 parent 表示根 |
| `AddCommandFactory(parent string, factory CommandFactory)` | 为每棵命令树创建新命令，适合重复/并发构树 |

`type CommandFactory func() *cobra.Command`。先注册组和命令，再创建根命令或调用 `RunCmd`；自定义参数由命令自己的 `Flags()` 和局部变量持有。复用同一 `*cobra.Command` 构造第二棵树会报错，改用工厂。

`ExecuteContext` 接受规范的双横线长参数，并兼容旧单横线长参数。无子命令时启动开发 Web；只传普通选项也可能进入隐式 Web 分支。工具集成应显式提供子命令，如 `[]string{"backtest", "--datadir", dataDir}`，并检查返回错误。

## 统一策略分派

配置先由 `config.LoadRunSpec` 读取。`backtest` / `trade` 按 `run_policy[].engine` 选择时序、因子或混合执行，不能用 `RuntimeSnapshot` 中的时序子集代替整个策略列表。

| 命令/选项 | 合同 |
| --- | --- |
| `backtest --mode weights\|events` | 覆盖因子 `execution.mode`；未指定时取 YAML，再默认 `events` |
| 混合 `backtest` | 必须 events；`--separate` 只支持纯时序 |
| 纯时序 `trade` | `env: dry_run` 经 Hub 实时模拟；不接受因子专用的 `--dry-run` / `--live-provider` |
| 因子/混合 `trade --dry-run` | 历史输入的 events 回放 |
| 因子/混合真实 `trade` | 需要已验证的实时 binding；能力不足报错，不回退为 paper |
| `research` | 因子研究，输出成熟标签和诊断，使用相同 YAML 加载规则 |
| `optimize` / `bt-opt` | 时序优化，不是 factor runner 超参搜索 |

`ValidateBacktestRunSpec(spec *config.RunSpec) error` 与执行共用无资源预检。`InspectBacktestRunSpec` 提供 Web 回测预检信息；通过预检不证明数据库覆盖或交易所已可用。`RegisterFactorLiveBinding` 注册自定义实时绑定工厂，配置引用未注册名称会失败。详见[因子 API](./factor.md)。

## 资源与取消

配置型入口为任务装配 Process/Runtime、存储、交易所、时钟和订单依赖。嵌入调用传递取消上下文，并等待执行返回，使输出刷新和资源回收完成；自行创建的 Runtime 按 `Close` 后 `Join` 释放。不要通过修改 `config` / `core` 全局变量切换并行任务。

同账户的引擎消费者可以共享账户执行与策略归因；释放一个借用者不能停止其他消费者。运行时所有权约定见 [Runtime API](./runtime.md)。

默认实时入口包括纯时序也使用共享 Hub，要求 `prod` / `dry_run` 线性永续账户及完整执行声明；不支持 `test` / spot / inverse，Hub 缺失不会回退本地发送。旧动态 leverage / `fatal_stop` 也不能沿此路径运行。部署与恢复见[共享账户与 Hub](../guide/execution.md)。
