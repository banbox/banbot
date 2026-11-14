# config 包

配置文件的用户操作见[配置指南](../guide/configuration.md)。嵌入程序推荐通过 `LoadRunSpec` 加载统一配置，再按引擎装配；旧包级变量不是并行任务的配置容器。

## 读取统一配置

```go
args := &config.CmdArgs{
    DataDir: "/path/to/data",
    Configs: config.ArrString{"experiment.yml"},
}
spec, err := config.LoadRunSpec(args, false)
if err != nil {
    return err
}
effective, yamlErr := spec.EffectiveYAML(true)
if yamlErr != nil {
    return yamlErr
}
_ = effective
```

`LoadRunSpec(args *CmdArgs, showLog bool) (*RunSpec, *errs.Error)` 读取默认文件、显式文件、内联 YAML 和显式 CLI 覆盖。`LoadUnifiedConfigs(paths []string, showLog bool)` 适合加载已知 YAML 文件列表；它不代替 CLI 分层和任务装配。普通读取不会迁移、备份或写回用户文件。

| 类型 | 使用约定 |
| --- | --- |
| `CmdArgs` | CLI 输入，含 `Configs`、`ConfigData`、`DataDir`、`NoDefault`、时间/标的及优化参数；字段存在不表示每个子命令都绑定相应 flag |
| `UnifiedConfig` | `Root *Config` 保存公共根字段；唯一策略列表在 `RunPolicy []*PolicyV2`，而不是 `Root.RunPolicy` |
| `PolicyV2` | 内嵌 `RunPolicyConfig`，另含 `Engine`、`ID`、`Account`、`CapitalWeight *float64`、`Factor`、`ExplicitEngine` |
| `RunPolicyConfig` | 时序名称、周期、`refine_tf`、方向、pairs、`Params map[string]float64`、`PairParams` 及开单/筛选参数；`Def` / `DefInt` 声明搜索参数 |
| `RunSpec` | 保留合并结果、字段来源和路径基准；导出配置/来源方法返回副本 |
| `Snapshot` | 任务只读配置及显式目录，`View()` 只读，`Clone()` 复制可变容器 |

`CmdArgs.ExplicitFlags` 标记调用方真正传入的 flag；直接以 Go 构造参数时，如需用零值/空值覆盖 YAML，也要提供相应标记。`RawPairs`、`RawTimeFrames` 为逗号分隔的 CLI 原文，对应解析后的 `Pairs`、`TimeFrames`。

## RunSpec API

| API | 结果/用途 |
| --- | --- |
| `Config() *UnifiedConfig` | 返回统一配置副本 |
| `Engines() []string` | 当前策略所需引擎 |
| `Origin(field) (FieldOrigin, bool)` / `Origins()` | 字段来源，包含 `Source`、`Kind` |
| `EffectiveYAML(redact bool) ([]byte, error)` | 生效 YAML；true 时脱敏，高级路径解析为可独立加载的输出 |
| `Hash() (string, error)` | 生效配置身份哈希 |
| `ResolvePath(field string) (string, error)` | 按最终字段来源文件解析相对路径；`@` / `$` 依赖数据目录，`:memory:` 保留原义 |
| `WithExecutionMode(mode, source string) *RunSpec` | 返回记录执行模式覆盖及来源的新副本 |
| `RuntimeSnapshot() (*Snapshot, *errs.Error)` | 归一化公共配置并仅投影时序策略；不执行因子任务 |

例如 `spec.ResolvePath("data.archive")` 按实际提供该字段的文件定位。内联/CLI 层按数据目录定位；没有目录时使用当前工作目录。缺少字段、路径为空或缺少必要基准时返回错误。

`LoadRuntimeSnapshot(args *CmdArgs)` 保留时序加载入口。`UnifiedConfig.TimeSeriesConfig()` 是受限的旧配置投影：拒绝因子、高级覆盖及新账户/身份/预算字段；不要把它当成统一引擎入口。

## 配置结构与验证

省略 `engine` 保留旧时序策略开放参数 `More`；显式 `time_series` / `factor` 启用新策略身份、账户和资本份额语义。引用的账户必须存在，同账户 ID 不可重复，多策略预算需明确且合计不得超过 1。`CapitalWeight` 与开单倍率 `stake_rate` 不同。

规范 YAML 把因子字段直接放在 `run_policy[]`，公共执行默认值放 `execution`，账户覆盖放 `accounts.<name>`。`config_version` 非必需；旧 1/2、`factor` 包装和 `execution.accounts` 仍可读，但同层同字段双路径冲突会被拒绝。高级字段按允许键和类型校验。

配置必须是无重复键的单文档 YAML mapping。`run_policy`、`wallet_amounts`、`fatal_stop`、`watch_jobs`、`historical_coverage` 后层整块替换，其他映射通常深合并。时间表示切换及环境变量展开见[配置指南](../guide/configuration.md)。

常用根结构包括 `DatabaseConfig`（`DbType`、`Url`、`SIDRegistryURL` 等）、`APIServerConfig`、`UserConfig`、`PairMgrConfig`、`StratPerfConfig` 和 `AccountConfig`。完整 YAML 字段以配置模板为准；凭据在根 `accounts`，不能用执行账户配置替代真实交易授权。`RPCChannels` 是类型配置映射，支持 `wework`、`mail`/`email`、`telegram`；根 `Webhook` 是消息模板。通知参数见[通知指南](../guide/notifications.md)。

## 兼容接口与写回

| API | 边界 |
| --- | --- |
| `GetConfig(args, showLog)` / `ParseConfig(path)` / `ParseConfigs(paths, showLog)` | 返回旧时序 `Config`；不支持统一 factor/mixed 分派 |
| `LoadConfig(args)` / `ApplyConfig(args, cfg)` | 安装旧进程全局配置；不能用来切换并行任务；已 Loaded 后 `LoadConfig` 不重新安装 |
| `GetDataDir()` / `GetDataDirSafe()` | 旧全局数据目录访问；前者无目录时报配置错误，后者允许空 |
| `ResolveDataDir(explicit)` / `GetStratDir()` | 前者显式值优先于 `BanDataDir`，后者读取 `BanStratDir` |
| `ParsePath(path)` | 旧数据目录前缀路径解析；新字段使用 `RunSpec.ResolvePath` |
| `GetTakeOverTF(pair, defTF)` / `GetAccLeverage(account)` / `GetStakeAmount(account)` | 依赖旧配置状态的兼容查询 |
| `DumpYaml(desensitize bool)` / `(*Config).DumpYaml()` | 导出旧配置，注意包级函数需传脱敏参数 |
| `ParseTimeRange(raw)` | 返回起止毫秒时间戳和 error |
| `GetExportConfig(path)` / `LoadPerfs(inDir)` | 数据导出配置 / 旧性能加载 |

编辑器写回使用 `WriteConfigAtomic(path string, expected, candidate []byte) *errs.Error`：`expected` 是读取时原始字节，并发修改会拒绝写入；内部校验后原子替换，保留权限。加载不自动触发写回。`GetExgConfig` 已不是导出 API；使用只读 `Snapshot.View().Exchange`。更多生命周期约定见 [Runtime API](./runtime.md)。
