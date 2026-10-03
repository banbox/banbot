# 因子与截面策略实盘

`banbot factor trade --config <v2.yaml>` 默认使用 `banexg` 实盘 binding，也可显式指定 `--live-provider banexg`。旧的 `RegisterFactorLiveBinding` 自定义集成仍然可用。

当前内置 SDK 执行能力支持 Binance 线性永续、单向净持仓、生产环境。交易所相关账户模式、条件单库存、手续费、资金费分页和网络请求控制全部位于 **banexg**。其他交易所必须实现同样的统一能力接口，通过启动检查后才能交易。

## 配置

- `env: prod`，生产账户 API；默认 binding 拒绝测试网或模拟环境。
- `execution.live_provider: banexg` 可省略。`store` 与 `sender_lease_dir` 必须使用持久化的绝对路径；同一共享账户的策略使用相同路径。
- 每个策略设置 `factor.initial_nav`，这是首次分配的真实结算资金，不是保证金或杠杆倍数。分配总和不得超过真实余额。已有账本重启时保持原有资金分配，不重复入账。
- 显式设置 `margin_rate`、`max_account_margin`、`max_virtual_gross`、`strategy_gross_limit` 以及合约 `instruments`；数量步长、合约乘数、价格步长和结算精度必须与交易所元数据一致，最小数量和金额不得低于交易所要求。
- 永续合约必须使用 `execution.funding_policy: required-stream`，声明 `factor.funding_source: funding` 和对应 snapshot schema/source version。内置账户级资金费来源包含 `settlement_id`、`account_amount`、`rate`、`mark`，全部保留精确字符串。共享账户策略使用相同的资金费来源。
- 数据输入继续使用 `DataSeries.Values`。任意额外字段及其类型、NULL 都保留；默认 mapper 使用实际接收时间作为保守的可见时间。已有非 K 线数据源应继续通过运行时 factory 注册。

## 订单与恢复

策略目标经过共享账户协调后，仅发送真实净差额；每个成交按持久化的策略归属记账。提交前先提交订单及 attempt，再请求交易所。超时、断线或丢失响应保留 `Unknown` 和原 client ID，恢复时先查询，不重新提交不确定订单。

启动先恢复持久化订单和资金费，再核对结算现金、净仓位以及普通/条件挂单，最后才开放策略执行。空账本只允许从没有挂单、没有仓位的账户初始化；已有实盘仓位需要明确迁移归属，不会自动接管。

私有流只是恢复提示，账务依据完整累计订单查询，包括手续费。每 5 秒通过 REST 刷新活动订单、恢复资金费并核对账户，覆盖重连期间遗漏的推送。过期的活动订单撤销后再查询最终累计成交。请求取消覆盖 HTTP、并发限额和重试等待；停止进程时先关闭提交入口、等待在途工作、关闭账户流，最后释放账本和发送租约。

无法证明订单状态、资金费历史归属或账户一致性时冻结执行并报告失败；保留账本供下次恢复。停机不会自动平仓生产策略。资金费迟到且结算时的虚拟持仓已经变化时拒绝用当前持仓错误分摊。

周期恢复失败后停止该账户的报告工作器，需要重新启动账户会话并完成恢复、对账才能继续交易。REST 请求支持 context 取消；SDK 首次市场加载的等待和 WebSocket 建连仍受 SDK 自身超时控制，不能承诺即时中断。账户服务停止时先加入报告翻译工作器，入口会话随后关闭共享 SDK 连接。

## 小额实盘验收

`entry/factor_live_smoke_test.go` 是显式启用的生产环境验收，不使用测试网。先执行只读检查：

```powershell
$env:BANBOT_LIVE_SMOKE_CONFIG = 'D:/ban/data/config.local.yml'
$env:BANBOT_LIVE_SMOKE_ACCOUNT = 'your-account'
$env:BANBOT_LIVE_SMOKE_READ_ONLY = '1'
go test ./entry -run '^TestFactorLiveProductionSmoke$' -count=1 -v
```

确认账户平仓且无挂单后，移除 `BANBOT_LIVE_SMOKE_READ_ONLY` 再运行相同测试。默认交易对为 `DOGE/USDT:USDT`，可通过 `BANBOT_LIVE_SMOKE_SYMBOL` 指定。测试计划约 6 USDT 的名义金额，单笔入场不得超过 10 USDT，最小交易量无法满足上限时拒绝下单。两个策略验证开仓、反向净额、进程重启以及退出。

测试在任何退出路径执行独立超时的撤单、平仓和最终账户检查，仅处理测试账本能够证明归属的订单/仓位。持久化账本默认位于配置目录的 `factor-live-smoke/ledger.db`，不要在清理失败时删除它。可用 `BANBOT_LIVE_SMOKE_LEDGER` 指定绝对路径。清理失败会让测试失败并输出恢复路径，不能当作测试成功。

## 配套 SDK

本次修改同时涉及相邻 `banexg` 仓库，`go.mod` 暂时通过 `replace github.com/banbox/banexg => ../banexg` 使用这些统一能力。部署需要一并携带 SDK 修改；发布支持这些接口的 banexg 版本后再改为对应版本并移除本地 replace。
