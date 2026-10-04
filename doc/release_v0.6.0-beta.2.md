# v0.6.0-beta.2

本次预发布统一时序与因子引擎的浅层 YAML 配置，并补齐配置参考和中英文文档。

## 配置兼容性

- v0.5 的标准配置 key 保持原名称和位置，无需添加版本标记；加载不自动改写或备份文件。
- 因子字段直接放在 `run_policy[]`；账户凭据和执行覆盖放在根 `accounts`，公共执行默认值仍在 `execution`。
- 旧 v0.6 的 `factor`、`execution.accounts` 包装和 `config_version: 1/2` 仍可读取。规范导出使用浅层结构；同层重复的新旧路径报冲突。
- 显式 `engine: time_series/factor` 启用策略身份、账户和资本预算语义；无 engine 的旧策略保留开放参数。
- 修复嵌套 `chunks[]`、`research.labels[]` 的字段校验，并加入回归测试。

完整说明见 [配置兼容性对比](config_compatibility.md)，完整示例见 [config.yml](config.yml)。

## 引擎和文档

- 更新因子表达式、订阅与实时数据生命周期、共享执行及持久化相关实现和回归测试。
- 保留 `DataSeries.Values` 自定义字段、类型与 NULL 语义。
- 补齐数据、执行、账户覆盖、表达式、研究、组合构建、快照、manifest、混合预算及历史覆盖配置示例。
- 同步中英文指南、API、架构文档和配置编辑器说明。

## 版本与验证

后端 `Version` 为 `v0.6.0-beta.2`。本次没有修改前端源码，`UIVersion` 保持 `v0.6.0-beta.1`，继续使用该版本的 `dist.zip`。

配套依赖固定为正式版本 `banexg v0.2.65`，移除本地目录 `replace`，提供共享执行所需的可选能力接口。

使用 `GOWORK=off` 验证已发布的远程依赖：正式 Go 包测试通过（排除被 Git 忽略的临时回放目录）；相关包 `go vet`、程序构建、模块校验、VitePress 构建和本地文档链接检查通过。Windows Go 1.25.1 构建使用 `-ldflags=-checklinkname=0`。本地验证未覆盖真实交易所实盘验收。

配套 banexg 的新增执行与 WebSocket 回归通过；其原有 `TestOdBookSide`、依赖 `local.json` 的实盘测试及缺少 `okx.md` 的文档生成测试未通过，默认 vet 仍有旧未命名结构体字段诊断，具体限制列于该依赖的发布说明。
