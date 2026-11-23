# 共享账户与 Hub

时序、因子及混合策略的默认实时入口通过本机 Hub 使用同一个账户执行服务。机器人负责行情输入、策略计算和订单展示；Hub 负责账户账本、私有报告、净额下单和挂单/保护推进。停止机器人不等于平仓，也不等于停止 Hub 中的账户。

## 选择运行方式

| 目标 | 配置和命令 | 账户位置 |
| --- | --- | --- |
| 历史回测 | `backtest`，因子选 weights/events，混合选 events | 进程内模拟账户 |
| 因子/混合历史 paper 回放 | `trade --dry-run` | 历史 events 回放，不连接实时 Hub 账户 |
| 实时模拟 | `env: dry_run`，先 `hub` 后 `trade` | Hub 持久化 paper 账户 |
| 生产交易 | `env: prod`，先 `hub` 后 `trade` | Hub 持久化账户，须验证真实会话能力 |

`execution.live_provider` 省略、`hub`、`banexg` 均选择 Hub。Hub 缺失即启动失败，不回退本地发送。自定义注册 provider 只适用于显式生产嵌入，同一机器人不能与 Hub 混用。旧 `LiveOrderMgr` 发送路径已删除，不存在绕过账户执行服务的替代入口。

### 内嵌生产时序账户恒用 Hub 订单管理器

内嵌生产时序机器人（`env: prod` 且 `execution.live_provider` 为注册的自定义内嵌名，非省略、非 `banexg`/`hub`；含混合机器人中的时序账户）恒使用与默认 Hub 机器人相同的 Hub 订单管理器，Hub 是机器人进程内的：直接包住机器人自己的共享账户，不启动 socket 或行情流，也不关闭该账户。没有门面选择项，也不能回退到旧的共享门面（`SharedOrderMgr` 仅用于回测；实盘与 dry_run 一律使用 Hub 订单管理器，同账户混合因子+时序机器人的时序部分也是，其因子部分仍直接使用同一账户，报价复用不会给因子执行早于其可执行时间的报价；默认 socket Hub 下的混合账户，时序部分同样经 Hub 机器人桥）。

- 原 `execution.ts_facade` 键已移除，在 `execution` 或 `accounts.<账户名>` 下出现（任何取值）都会在加载配置时报错，提示共享门面已移除、必须删除该键。
- 升级：原先以旧共享门面运行的账户，在首次启动时（机器人 journal 尚不存在且账户存在活动的旧订单：持仓、止损、阶梯止盈、OCO、挂单入场）自动一次性导入旧检查点；旧数据保持不动。若某个订单无法忠实映射，整个导入被拒绝并带原因启动失败，不写入任何内容。回滚到旧门面已不再可用。
- `take_over_strat`（外部持仓接管）受支持。
- 机器人 journal 为 `<数据目录>/execution/robots/ts-local-<hash>.json`，与 socket Hub 机器人的 journal 相互独立。
- 进程内 Hub 每秒推进一次挂单（入场与保护），失败只记警告日志；账户冻结仍由账户自身的报告循环按原规则处理。

要让账户以 stock banexg 能力走内嵌路径，用自定义名注册 `entry.NewBanexgLiveBinding`（`entry.RegisterFactorLiveBinding("<名称>", entry.NewBanexgLiveBinding)`），并设置 `execution.live_provider: <名称>`。内嵌模式拒绝已声明的 `leverage`（请在交易所侧设置）；相对路径的 `execution.store` 相对于声明它的配置文件所在目录解析。内嵌入口对同一品种的报价复用 1 秒（并发读取共用一次请求，失败不缓存）；资金费由账户私有报告 worker 恢复（现金不符时，以及至多每 5 分钟一次），入口自身不轮询。因子 funding 数据源按账户整体每分钟轮询一次，遇到瞬时交易所错误会重试。

## 配置与启动

1. 准备市场/数据库基础配置和已编译策略。当前默认 Hub 只接受 `prod`/`dry_run` 的线性永续账户及明确的结算币；不支持 testnet、现货、反向合约或实盘期权。
2. 在机器人配置中声明稳定策略 `id`、`account` 和同账户策略的 `capital_weight`。配置正的 `wallet_amounts`、`execution.margin_rate`（大于 0 且不超过 1）、`max_account_margin`、`max_virtual_gross` 和 `strategy_gross_limit`。资本分配不是向交易所充值，也不能超过可分配账户资金。
3. 配置 `execution.instruments`，以 SID 为键，逐项提供 `id`、`version`、`valuation: linear_perpetual`、`settlement_currency`、`quantity_step`、`price_tick`、`contract_size`、`money_scale`，以及最小下单量 `min_steps`（以 `quantity_step` 为单位的步数）和最小名义额 `min_notional`。纯时序的 instrument ID 为标准 symbol；因子的 SIDMap 必须与 instrument 对应。单位、版本和精度须与 banexg 标准市场元数据一致，不要复制其他品种的数值。注册时会与公开市场元数据比对：单位和精度须一致，`min_steps`、`min_notional` 不得低于交易所要求，否则拒绝启动。
4. 在主机和所有可能发送同一物理账户的进程中使用相同绝对 `execution.sender_lease_dir`。生产 Hub 必须显式配置；不同目录的锁不能防止重复发送。切换前先停止并迁移旧发送方，不可并行启动新 owner。
5. 生产机器人提供账户凭据及 `funding_policy: required-stream`，并通过能力与启动对账检查。实时模拟提供正模拟现金，使用 `funding_policy: explicit-zero`；当前 Hub 未接模拟资金费流，不接受以 `required-stream` 替代。默认路径不保证所有旧配置行为已迁移。

6. 生产账户必须处于单向（净额）持仓模式，且 API Key 须允许交易。Hub 在注册生产账户时、任何下单路径建立之前检查：不允许交易、余额或权限拉取失败、账户为对冲（双向）持仓模式时拒绝注册；Key 允许提币、Key 没有 IP 限制只记告警。交易所不报告持仓模式时记告警，对冲持仓之后由适配器拒绝。dry_run 账户没有凭据，不做检查。内嵌凭据的机器人（不经 Hub）启动时执行同一检查，失败则拒绝启动。

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

纯时序实时入口的预检仍要求 `execution.store` 与发送租约声明，`env: dry_run` 同样要求，缺失会以 `requires a durable live ledger` 拒绝启动；`hub` 和 `trade` 的配置都须含 `time_start`，否则无法启动；Hub 实际账户数据库由主机在 `<datadir>/execution` 下按账户身份选择，机器人不能用 `store` 重定向远端账本。账户覆盖直接写在 `accounts.<账户名>`，例如 `accounts.default.hub_socket`，不要新建嵌套 execution 块。

Hub 配置提供数据库、公共市场及发送租约。账户凭据来自机器人的私有注册请求，Hub 不从自己的 YAML 初始化私有账户，也不把凭据传给公共行情采集器。任意 exchange SDK options 不会继承，依赖其中自定义端点/代理的旧配置须单独核查。

先在前台检查启动日志，确认 Hub 监听成功后启动机器人：

```sh
/ban/bot hub --datadir /ban/data --config /ban/hub.yml
# 在另一个终端启动机器人
/ban/bot trade --datadir /ban/data --config /ban/live.yml
```

`spider` 是 `hub` 的 CLI 别名，不需要另起同路径的第二个服务。公共行情地址由 `spider_addr` 指定，默认 `127.0.0.1:6789`；私有 socket 默认 `<datadir>/execution/hub.sock`。改私有路径时，Hub 使用 `--execution-socket /绝对路径`，机器人使用相同 `execution.hub_socket`。Hub 只认命令行参数，不读 YAML 里的 `execution.hub_socket`（那是机器人侧设置）；省略参数时回退到 `<datadir>/execution/hub.sock`，数据目录较长时该路径可能超出 Unix socket 路径上限，生产部署请显式传 `--execution-socket`。

私有 socket 是 Unix domain socket，路径长度受系统限制（macOS 约 104 字节，Linux 约 108 字节）；数据目录路径较长时，请把 `execution.hub_socket` / `--execution-socket` 设为较短的绝对路径（如 `/ban/hub.sock`）。socket 权限为 0600，目录不能对组/其他用户可写；同一操作系统用户下的客户端被信任可操作全部已注册账户。当前没有私有 TCP 传输或策略级多用户权限，不应把它当远程账户服务暴露。

## 多策略与挂单

同一物理账户只应有一个发送 owner。TS/factor 各自保留预算和虚拟仓位，账户内先净额抵消，只为剩余差额向交易所下单；这不提供交易所余额隔离。同一策略在同一计划内平多头 lot 与空头 lot（如“全部平仓”，或多头止盈与空头止损同一 tick 触发）时，两个 lot 在内部同价平仓，不产生交易所订单；同一策略的其他相反意图（双向开仓、翻转）不会内部互抵。需要独立资金与风险边界时使用不同物理账户。

首次注册确定账户资本、风险与 instrument 基线。后续机器人只能从未分配现金追加策略，不能更换既有声明，也不能借此动态扩展主机未安装的 instrument。重启保持账户、策略 ID、声明和本地 journal 稳定；不要通过改名逃避恢复。复制同一 journal 启动两个写者会触发锁/epoch 冲突。

挂单和保护由账户继续推进，即使发起机器人断开。软件保护依赖 Hub 运行和可见报价；执行以独立 REST 盘口快照为准，不能保证捕获两次快照间的短暂穿越。原生保护还要求适配器明确证明能力。`cancel_entry` 只撤剩余入场，不平已成交仓位；平仓是独立的退出请求。接受命令、撤单返回或机器人显示更新均不能代替最终成交/撤单证据，回调也可能重复交付。

## 运行与排障

**dry_run 的模拟成本。** 纯时序账户的模拟手续费取 `exchange.<名称>.fees.<market>.taker`，所有成交按该费率计；未配置时按 0 计并在启动时告警。成交价取实时盘口的买一/卖一（买入按卖一、卖出按买一），不再额外叠加滑点。含因子策略的账户使用策略自身的 `fee_rate` / `slippage_rate`。模拟成本是 dry_run 账户注册内容的一部分：修改费率后，已有的 dry_run 账本会因注册内容不一致而拒绝启动，需要清空该账户的 dry_run 账本（数据目录下 `execution/` 中对应账户的目录）后重新开始。

- **时钟**：所有账户报价读取（行情流、内嵌报价、默认限价盘口）都容忍主机时钟比交易所慢 1 秒以内，此类盘口按接收时间观测；慢得更多时会拒绝盘口，账户注册失败，Hub 日志出现 `invalid or stale account depth timestamp`。请在 Hub 与机器人所在主机启用 NTP 等时间同步。
- **机器人只看到笼统错误**：机器人报 `hub operation failed; inspect account recovery state before retrying` 时，具体原因（可能含凭据信息，因此不回传）只写在 Hub 进程日志里，形如 `hub operation failed operation=... error=...`，请到 Hub 日志查看，而不是机器人输出。
- **入场数量与最小下单量**：机器人把策略入场请求换算为下单步数。若请求给出下单数量（`Amount`），按数量计；否则按金额 `LegalCost`；两者都没有时，金额 = 策略资金 × 单笔比例 × `CostRate`（未设置按 1）。其中单笔比例为 `stake_pct / 100`；`stake_pct` 为 0 时取 `stake_amount ÷ 策略资金`，策略资金为账户初始净值乘该策略的 `capital_weight`。金额再按“数量步长 × 合约面值 × 参考价”折成步数（向下取整）。步数小于 `min_steps`、名义额小于 `min_notional`，或超过该策略的 `strategy_gross_limit` 时，该入场被丢弃，机器人记录告警 `entry rejected: size below the tradable minimum or above the strategy allowance` 并继续运行。出现此告警时请调大 `stake_pct`/`stake_amount`、策略资本或请求金额。Hub 净额后实际要下的差额低于交易所最小值时同样被拒绝并丢弃，不会重发。
- **入场被拒绝时**：默认 Hub 路径下，账户 owner 拒绝的新入场（限价不在价格 tick 上、超出策略或账户的敞口/保证金预算、现货品种做空、同一策略同一计划内方向相反的开仓或翻转、账户未就绪 `account_not_ready`、入场信号已过期 `stale_entry`）会被丢弃并记告警 `hub rejected an entry; it is dropped, not retried`，机器人继续运行，账户不会因被拒的入场而冻结，该入场也不占用当根 bar 的开单名额。网格或多 lot 策略中，同一策略的反向入场可能在止损（减仓）之后晚一个 tick 才下，减仓总是先执行。已被接受、仍在挂单中的入场若之后在账户自主推进时被拒，会被 owner 终止（审计中记录拒绝原因）而不是冻结账户。退出、改单和保护变更被拒不会被丢弃，仍使机器人报错停止，以免持仓无人管理。
- **账户未就绪与瞬时故障**：私有报告 worker 遇到网络失败、超时、限流、临时封禁、交易所过载、结果不确定或账户盘口尚不可见/过期等瞬时读失败时，不冻结账户也不停止：本轮跳过，账户在恢复前未就绪，新入场以 `account_not_ready` 丢弃（机器人继续运行）。Hub 日志在开始时记 WARN，持续超过 1 分钟记 ERROR，恢复时记 INFO；限流按交易所 Retry-After（缺省 30 秒）暂停定时对账和账户变动触发的对账。其他错误仍会冻结账户，并记 ERROR `account frozen by the private report worker`（含原因与错误）。真实的资金差异（如外部划转）同样冻结。
- **入场信号过期**：时序入场的信号时间早于 max(2 × 周期, 2 分钟) 时，账户 owner 以 `stale_entry` 拒绝该入场（机器人丢弃并继续）；已被接受的命令重放仍幂等。机器人重启时对每条重发的冻结命令记 INFO `hub resends a frozen command`。
- **对账与请求权重**：账户全量快照（余额、持仓、全部普通与条件挂单）每 30 秒一次，账户变动推送、订单回报和瞬时故障时会提前；其间每 5 秒只推进挂单。资金费不再每 5 秒拉取：对账发现已结算的现金差异时按需拉取并重试一次，否则至多每 5 分钟一次。账户行情的 REST 快照与 websocket 订阅均请求 5 档深度，同一品种的 REST 快照间隔不小于 min(TTL/4, 1 秒)；交易所报告该档位无效时退回 SDK 默认深度。在 Binance 上实测，单账户两个品种的请求权重由约 1600/分钟降到约 450/分钟（IP 上限 2400/分钟，同一主机上所有进程共用），同机还有其他进程时请预留余量。
- **注册报 `hedged position outside linear net contract`**：账户确实处于对冲（双向）持仓模式，请改为单向模式。依赖 banexg v0.2.67 及更早版本的构建还会把 Binance 单向持仓误报为对冲，导致带持仓重启时注册被拒；当前版本依赖 banexg v0.2.68，已修复。
- **`execution.instruments` 的键必须是品种在数据库中的真实 SID**：键写错时机器人以 `kline ingress emitted foreign source/SID` 停止；请用数据库 `exsymbol` 中该品种的 ID。
- **Dashboard**：时序、因子、混合机器人只在配置了 `api_server.enable: true` 且机器人恰有一个账户时启动 Dashboard；多个账户时不启动并在日志中告警。访问需带 `X-Authorization: Bearer <token>` 和 `X-Account`。余额取自执行账本，只显示结算币种一行；Hub 机器人没有账户凭据，交易所原始订单/持仓/收入及下载成交记录接口会被拒绝，`/close_exg_pos` 也被拒绝，请用策略订单的平仓。
- **不生效的配置**：`pairlists`（在旧路径下本会选品时）、`pairmgr.cron`、`min_open_rate`、`charge_on_bomb`、`margin_add_rate`、`put_limit_secs`、`odbook_ttl`、`account_pull_secs`、`bt_in_live` 被忽略，启动时各记一条告警；`--out` 被拒绝；品种在启动时固定，策略动态 `UpdatePairs` 及 `pairlists`/`pairmgr` 轮动不可用。详见[配置](./configuration.md)与[实盘能力支持矩阵](./live_trading.md#实盘能力支持矩阵)。
- **通知**：见[消息通知](./notifications.md)；消息异步排队，依赖旧钱包的维护任务（选币轮动、余额轮询、触发单校验、钱包快照）不运行。
- **停止**：`hub` 与 `trade` 都响应 SIGINT/SIGTERM（Ctrl-C 或 `kill`）；Hub 退出时会删除自己的 socket。机器人退出原因记录在日志 `time-series robot stopping: ...` 中。

## 重启与恢复

- **仅机器人重启**：保留 Hub 与其账户，让机器人使用原配置和 journal 重连。Hub 中的账户、报告循环、公共时钟和挂单保护继续存在；客户端断开不会自动销户或平仓。
- **Hub 重启**：保留整个执行目录（每个账户一个子目录，内含主机声明 `host-declaration-v1.json`）、SQLite（含 WAL sidecar）、paper 日志和机器人 journal。`accounts.json` 只保存无凭据身份，恢复后账户为 `waiting_authentication`，此时本地保护未激活。必须让机器人再次注册认证，恢复并对账成功后才能恢复账户运行；所有机器人离线时没有自动凭据恢复保证。
- **paper 恢复**：再次注册后恢复模拟订单、现金和成本。日志缺失/损坏，或初始模拟现金、费用、滑点/模型变化会拒绝恢复。不要删库当作“重置”来规避冲突。
- **残留 socket**：先确认旧 Hub 已停止并释放账户发送权，再处理残留路径。启动器不会自动删除已有 socket 或接管旧 owner。

发送超时或撤单结果不明会保留 `Unknown`/`CancelPending` 等状态并阻止普通增减仓。用原命令 ID 和相同内容恢复/重试，不要生成新单规避不确定结果。等待稳定 client ID 查询、完整成交历史和账户对账；一次“未找到”不能证明未成交。报告/恢复失败可能持久冻结账户，不要直接修改 SQLite 状态或清除去重记录。

若 `Rejected` 后出现迟到成交，当前没有支持的重开操作命令，反复重启也不能修复：停止该账户的新策略提交和所有发送进程，等待退出后备份数据库及 WAL、脱敏日志与订单/成交证据，交由维护者审计修复。保持冻结，不能重复下单或把同一成交重复记为外部仓位与策略成交。已接受但中断的 policy 入场撤销也没有通用人工完成工具，应报告并保留证据。

停止 Hub 会停止并等待账户工作；停止前核实真实持仓、挂单和保护安排。模拟盘与单元测试只证明软件合同，不能证明真实交易所、收益、容量或滑点已经验收。
