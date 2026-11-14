# 共享账户与 Hub

时序、因子及混合策略的默认实时入口通过本机 Hub 使用同一个账户执行服务。机器人负责行情输入、策略计算和订单展示；Hub 负责账户账本、私有报告、净额下单和挂单/保护推进。停止机器人不等于平仓，也不等于停止 Hub 中的账户。

## 选择运行方式

| 目标 | 配置和命令 | 账户位置 |
| --- | --- | --- |
| 历史回测 | `backtest`，因子选 weights/events，混合选 events | 进程内模拟账户 |
| 因子/混合历史 paper 回放 | `trade --dry-run` | 历史 events 回放，不连接实时 Hub 账户 |
| 实时模拟 | `env: dry_run`，先 `hub` 后 `trade` | Hub 持久化 paper 账户 |
| 生产交易 | `env: prod`，先 `hub` 后 `trade` | Hub 持久化账户，须验证真实会话能力 |

`execution.live_provider` 省略、`hub`、`banexg` 均选择 Hub。Hub 缺失即启动失败，不回退本地发送。自定义注册 provider 只适用于显式生产嵌入，同一机器人不能与 Hub 混用。旧 `LiveOrderMgr` 发送路径不能作为绕过账户执行服务的替代入口。

## 配置与启动

1. 准备市场/数据库基础配置和已编译策略。当前默认 Hub 只接受 `prod`/`dry_run` 的线性永续账户及明确的结算币；不支持 testnet、现货、反向合约或实盘期权。
2. 在机器人配置中声明稳定策略 `id`、`account` 和同账户策略的 `capital_weight`。配置正的 `wallet_amounts`、`execution.margin_rate`（大于 0 且不超过 1）、`max_account_margin`、`max_virtual_gross` 和 `strategy_gross_limit`。资本分配不是向交易所充值，也不能超过可分配账户资金。
3. 配置 `execution.instruments`，以 SID 为键，逐项提供 `id`、`version`、`valuation: linear_perpetual`、`settlement_currency`、`quantity_step`、`price_tick`、`contract_size`、`money_scale` 及市场要求的最小数量/名义额。纯时序的 instrument ID 为标准 symbol；因子的 SIDMap 必须与 instrument 对应。单位、版本和精度须与 banexg 标准市场元数据一致，不要复制其他品种的数值。
4. 在主机和所有可能发送同一物理账户的进程中使用相同绝对 `execution.sender_lease_dir`。生产 Hub 必须显式配置；不同目录的锁不能防止重复发送。切换前先停止并迁移旧发送方，不可并行启动新 owner。
5. 生产机器人提供账户凭据及 `funding_policy: required-stream`，并通过能力与启动对账检查。实时模拟提供正模拟现金，使用 `funding_policy: explicit-zero`；当前 Hub 未接模拟资金费流，不接受以 `required-stream` 替代。默认路径不保证所有旧配置行为已迁移。

以下仅是已有完整配置的路径覆盖片段，不含策略、instrument 或风险声明：

```yaml
env: dry_run
execution:
  live_provider: hub
  hub_socket: /ban/data/execution/hub.sock
  sender_lease_dir: /ban/sender-leases
  store: /ban/data/execution-declaration.sqlite
  funding_policy: explicit-zero
```

纯时序实时入口的预检仍要求 `execution.store` 与发送租约声明；Hub 实际账户数据库由主机在 `<datadir>/execution` 下按账户身份选择，机器人不能用 `store` 重定向远端账本。账户覆盖直接写在 `accounts.<账户名>`，例如 `accounts.default.hub_socket`，不要新建嵌套 execution 块。

Hub 配置提供数据库、公共市场及发送租约。账户凭据来自机器人的私有注册请求，Hub 不从自己的 YAML 初始化私有账户，也不把凭据传给公共行情采集器。任意 exchange SDK options 不会继承，依赖其中自定义端点/代理的旧配置须单独核查。

先在前台检查启动日志，确认 Hub 监听成功后启动机器人：

```sh
/ban/bot hub --datadir /ban/data --config /ban/hub.yml
# 在另一个终端启动机器人
/ban/bot trade --datadir /ban/data --config /ban/live.yml
```

`spider` 是 `hub` 的 CLI 别名，不需要另起同路径的第二个服务。公共行情地址由 `spider_addr` 指定，默认 `127.0.0.1:6789`；私有 socket 默认 `<datadir>/execution/hub.sock`。改私有路径时，Hub 使用 `--execution-socket /绝对路径`，机器人使用相同 `execution.hub_socket`。

私有 socket 权限为 0600，目录不能对组/其他用户可写；同一操作系统用户下的客户端被信任可操作全部已注册账户。当前没有私有 TCP 传输或策略级多用户权限，不应把它当远程账户服务暴露。

## 多策略与挂单

同一物理账户只应有一个发送 owner。TS/factor 各自保留预算和虚拟仓位，账户内先净额抵消，只为剩余差额向交易所下单；这不提供交易所余额隔离。需要独立资金与风险边界时使用不同物理账户。

首次注册确定账户资本、风险与 instrument 基线。后续机器人只能从未分配现金追加策略，不能更换既有声明，也不能借此动态扩展主机未安装的 instrument。重启保持账户、策略 ID、声明和本地 journal 稳定；不要通过改名逃避恢复。复制同一 journal 启动两个写者会触发锁/epoch 冲突。

挂单和保护由账户继续推进，即使发起机器人断开。软件保护依赖 Hub 运行和可见报价；执行以独立 REST 盘口快照为准，不能保证捕获两次快照间的短暂穿越。原生保护还要求适配器明确证明能力。`cancel_entry` 只撤剩余入场，不平已成交仓位；平仓是独立的退出请求。接受命令、撤单返回或机器人显示更新均不能代替最终成交/撤单证据，回调也可能重复交付。

## 重启与恢复

- **仅机器人重启**：保留 Hub 与其账户，让机器人使用原配置和 journal 重连。Hub 中的账户、报告循环、公共时钟和挂单保护继续存在；客户端断开不会自动销户或平仓。
- **Hub 重启**：保留整个执行目录、SQLite（含 WAL sidecar）、paper 日志、主机声明和机器人 journal。`accounts.json` 只保存无凭据身份，恢复后账户为 `waiting_authentication`，此时本地保护未激活。必须让机器人再次注册认证，恢复并对账成功后才能恢复账户运行；所有机器人离线时没有自动凭据恢复保证。
- **paper 恢复**：再次注册后恢复模拟订单、现金和成本。日志缺失/损坏，或初始模拟现金、费用、滑点/模型变化会拒绝恢复。不要删库当作“重置”来规避冲突。
- **残留 socket**：先确认旧 Hub 已停止并释放账户发送权，再处理残留路径。启动器不会自动删除已有 socket 或接管旧 owner。

发送超时或撤单结果不明会保留 `Unknown`/`CancelPending` 等状态并阻止普通增减仓。用原命令 ID 和相同内容恢复/重试，不要生成新单规避不确定结果。等待稳定 client ID 查询、完整成交历史和账户对账；一次“未找到”不能证明未成交。报告/恢复失败可能持久冻结账户，不要直接修改 SQLite 状态或清除去重记录。

若 `Rejected` 后出现迟到成交，当前没有支持的重开操作命令，反复重启也不能修复：停止该账户的新策略提交和所有发送进程，等待退出后备份数据库及 WAL、脱敏日志与订单/成交证据，交由维护者审计修复。保持冻结，不能重复下单或把同一成交重复记为外部仓位与策略成交。已接受但中断的 policy 入场撤销也没有通用人工完成工具，应报告并保留证据。

停止 Hub 会停止并等待账户工作；停止前核实真实持仓、挂单和保护安排。模拟盘与单元测试只证明软件合同，不能证明真实交易所、收益、容量或滑点已经验收。
