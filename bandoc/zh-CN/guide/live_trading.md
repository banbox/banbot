# 实时模拟与实盘部署

`bot trade` 从同一份 `run_policy` 启动时序、因子或混合策略。默认生产与实时模拟通过本机 Hub 进行共享账户执行；先配置并启动 Hub，再启动机器人。当前默认路径限线性永续，不代表真实交易所端到端已验收。完整账户声明和恢复步骤见[共享账户与 Hub](./execution.md)。

::: warning 运行前核查
先验证策略、数据、账户单位、资金费和风险限额，再用实时模拟验证运行流程。生产必须通过真实会话能力检查与启动对账；软件保护依赖 Hub 和行情可用。修改环境名或 API 密钥不能代替验收。
:::

## 1. 编译
**从WebUI编译**  
您可执行`bot`直接启动WebUI，然后打开[/zh-CN/setting/](http://127.0.0.1:8000/zh-CN/setting/)，左侧选择【编译代码】，右侧选择目标系统和架构，执行编译并下载即可。

**从终端编译**  
`GOOS`的可选值：linux, darwin, windows, ...  
`GOARCH`的可选值：amd64, arm64, ...

::: code-group
```shell [Linux]
export GOARCH="amd64"
export GOOS="linux"
go build -o bot
```

```shell [MacOS]
export GOARCH="amd64"
export GOOS="darwin"
go build -o bot
```

```shell [Windows Power Shell]
# build for windows
$env:GOARCH="amd64"
$env:GOOS="windows"
go build -o bot.exe

# build for linux
$env:GOARCH="amd64"
$env:GOOS="linux"
go build -o bot
```

```shell [Windows CMD]
# build for windows
set GOARCH=amd64
set GOOS=windows
go build -o bot.exe

# build for linux
set GOARCH=amd64
set GOOS=linux
go build -o bot
```
:::

## 2. 编辑配置

上传包含已注册策略的可执行文件到 `/ban/bot`，准备 `/ban/data`：

```sh
mkdir -p /ban/data
export BanDataDir=/ban/data
/ban/bot init
```

编辑 `config.local.yml` 的市场、数据库和策略设置。默认依次加载 `config.yml` 与 `config.local.yml`，后者优先；额外覆盖文件可重复传 `--config`。账户凭据仅放在机器人配置中，Hub 自身配置提供公共市场、数据库与发送租约。

交易所网络代理读取 `exchange.<name>.proxy`：写 `no` 表示不使用代理；留空时启动阶段自动探测一次环境变量/系统代理，之后各交易所共用该结果，探测后修改系统代理需重启才生效。详见[网络代理](./configuration.md#网络代理)。

实时模拟选 `env: dry_run` 并声明 `execution.funding_policy: explicit-zero`；生产选 `env: prod`、真实账户凭据与 `required-stream`。两者都需执行单位、资本与风险声明。纯时序还要求 `execution.store` 和 `sender_lease_dir`；`leverage`（含账户级）作为账户全部品种的固定杠杆由 Hub 账户 owner 设置到交易所，策略请求里的逐单杠杆仍被拒绝；`fatal_stop` 由账户 owner 按权益回落执行。不要把旧配置直接改成 prod。详见[配置文件](./configuration.md)及[共享账户配置步骤](./execution.md#配置与启动)。

同一物理账户可由多个策略/机器人共享一个 Hub，保留各策略预算与持仓归属；不要让独立发送进程争用同一账户。独立资金边界使用不同子账户。多个机器人使用稳定、互不冲突的策略 ID，启动后核实归属和预算。

## 3. 启动 Hub 和机器人

`hub` 同时托管公共行情与私有账户执行，`spider` 是它的别名。注册的自定义时序数据源可自行回填和订阅，不依赖公共 Spider；账户执行仍按所选 Hub/provider 路径运行。公共行情默认 `127.0.0.1:6789`，私有 socket 默认 `/ban/data/execution/hub.sock`，两者不能互相替代。

先按共享账户指南准备完整 `hub.yml` 和 `live.yml`。首次启动在前台检查 Hub 监听、账户注册、恢复/对账和策略预热；通过后可转入进程管理器或后台运行并保留 stdout/stderr：

```sh
nohup /ban/bot hub --datadir /ban/data --config /ban/hub.yml > /ban/data/hub-console.log 2>&1 &
# 确认 Hub 已监听后再执行
nohup /ban/bot trade --datadir /ban/data --config /ban/live.yml > /ban/data/trade-console.log 2>&1 &
```

不要同时启动相同 socket 的 hub 和 spider。Hub 不可用、能力不完整、声明冲突或对账失败时应处理报错，不会回退本地发送或自动改用 paper。`trade --dry-run` 只对含因子的配置有效，执行历史 events 回放；实时模拟使用 `env: dry_run`，不传该标志。

停止单个机器人不会自动平仓，Hub 继续处理账户已有挂单和保护。Hub 重启后等待机器人重新认证，认证前本地保护未激活；保留执行目录和 journal，按[恢复步骤](./execution.md#重启与恢复)操作。不能把进程存在当作账户已恢复。

内嵌生产时序机器人（自定义 `execution.live_provider`）无需单独的 Hub 进程，恒使用进程内 Hub 订单管理器（`execution.ts_facade` 已移除，配置它会报错）；同账户混合因子+时序机器人的时序部分也使用它（`SharedOrderMgr` 仅用于回测）。从旧门面升级的账户（含混合账户）在首次启动时自动一次性导入旧检查点，旧数据保持不动，不能再回滚到旧门面。详见[执行](./execution.md)。

## 订单类型的实盘行为

Stop-Limit 入场、OCO 组、多级止盈和追踪止损在回测与实盘（含实时模拟）均可用，字段见[自定义策略](./strat_custom.md)，回测撮合方式见[回测指南](./backtest.md#撮合语义)。实盘需注意：

- 止损、止盈和追踪止损由 `execution.protection_mode` 决定：`software`（默认）由 execution 引擎按价格监控触发，`native_preferred` 在适配器支持时优先挂交易所条件单，不支持时回退为引擎监控；追踪止损目前仍由引擎监控。程序停止期间 Hub 继续处理账户已有保护，Hub 不可用时本地保护不生效。
- OCO 组内某挂单入场成交后，同组其余未成交挂单被撤销，出场标签 `oco_cancel`；默认 Hub 路径上，同一 tick 内同时可执行的成员只保留创建最早的一个（详见[自定义策略](./strat_custom.md)）。
- 多级止盈在某级成交后才挂下一级；`MoveSLToBreakeven` 在该级成交后移动剩余仓位止损。
- 回测专用的 `data_gap` / `delist` 强平标签与 `bt_*` 成本键在实盘不生效。

## 实盘能力支持矩阵

实盘与实时模拟一律经 execution 模块（Hub 或内嵌账户）。旧的实盘订单栈已删除，只在旧栈中存在的能力按下表处理：**支持**；**拒绝**（启动或请求时报错）；**忽略**（配置键被忽略，启动时告警，不阻止启动）。每项的迁移说明见仓库 `doc/project/05_biz_orders_live.md`「支持矩阵」。

| 能力 | 状态 | 当前行为 |
|---|---|---|
| 对冲（双向）持仓模式 | 拒绝 | Hub 账户注册体检报错；持仓需为单向模式 |
| 现货 / 保证金现货（`market_type: spot`/`margin`） | 拒绝 | 启动报 `market_type "spot" is not supported by the execution path (linear USDT-margined perpetuals only)…`；只支持 linear 永续 |
| 反向合约、交割合约 | 拒绝 | 启动或适配器拒绝 |
| 测试网（`env: test`） | 拒绝 | 启动报 `testnet live trading is not supported by the execution path; use env: dry_run or prod` |
| 单笔订单杠杆、分档杠杆自动缩量 | 拒绝 | 杠杆只能按账户声明，由 Hub 账户 owner 设置；逐单杠杆被拒绝，超出交易所档位的下单由交易所拒绝 |
| 动态 `UpdatePairs`、选币轮动（`pairlists`、`pairmgr.cron`） | 拒绝 / 忽略 | 品种在启动时固定；`UpdatePairs` 返回错误，选币键告警后忽略 |
| 追加保证金 `margin_add_rate`、`charge_on_bomb` | 忽略 | 告警；风险由 owner 的准入与亏损熔断承担 |
| 限价摆放 `put_limit_secs`、`odbook_ttl` | 忽略 | 告警；限价由 `order_mode`、`limit_vol_secs`、`near_limit_bps` 决定 |
| 账户轮询 `account_pull_secs` | 忽略 | 告警；余额与权益来自账户 owner |
| 实盘回测对比 `bt_in_live` | 忽略 | 告警；该功能已删除 |
| 钱包快照落库 | 拒绝（不写入） | 不再写入 `wallet_snapshot` |
| 止损/止盈/追踪止损、多级止盈、OCO、改单 | 支持 | 见上节；`execution.protection_mode` 选择引擎监控或优先原生条件单 |
| `fatal_stop` | 支持（语义变化） | 由账户 owner 按权益回落执行并持久化暂停 |
| `take_over_strat` | 支持（部分） | 外部接管记录恢复为策略订单；不再自动把交易所用户持仓生成跟踪单 |
| `close_on_stuck` | 支持 | 超时未收到 K 线时经账户订单管理器平仓（只覆盖该 runtime 已注册管理器的账户） |

## 4. 消息通知、DashBoard和监控
### 消息通知与远程控制

通过 `rpc_channels` 配置企业微信、邮件或 Telegram，消息模板、筛选和 SMTP 示例集中见[消息通知](./notifications.md)。`min_intv_secs` 与 `retry_delay` 的单位均为秒。共享账户机器人有多个账户时，Telegram 只在第一个可交易账户上运行（见[消息通知](./notifications.md)）。通知异步排队，收到消息不代表刚刚成交，部署时仍需检查账户和日志。

Telegram 自建模式配置 `token` 与 `chat_id`；未配置 token 的仪表盘中转模式可从启动日志中的绑定链接接入，`secret` 留空会生成，手动设置须为 UUID v4。中转模式需放通 `www.banbot.site` 的 HTTPS 和 TCP 6788；自建模式需可访问 Telegram API，可配置渠道 `proxy`。

`/orders`、`/close`、`/status`、`/disable`、`/enable` 等命令可能影响交易。多账户先确认当前选择的账户；通道授权不等同于 Dashboard 账户角色。先在测试账户验证通知和控制范围。完整配置见[通知指南](./notifications.md)及 [RPC API](../api/rpc.md)。

### DashBoard UI
您可在yml中配置`api_server`，这将会在启动机器人时同时启动一个web服务，然后您可在机器人启动后通过配置的账号密码访问机器人的DashBoard，查看交易概况并管理机器人。
```yaml
api_server:
  enable: true
  bind_ip: 127.0.0.1
  port: 8001  # 需在防火墙放行或通过nginx反向代理
  jwt_secret_key: '[very_strong_secret]'
  users:
    - user: ban
      pwd: 'replace-with-a-strong-password'
      allow_ips: []  # 允许的客户端ip
      acc_roles: 
        user1: admin  # user1对应accounts中的用户key
```
共享账户实盘中，时序、因子、混合机器人只在恰有一个账户时启动 `api_server`（多个账户时不启动并记录告警）；请求须带 `X-Authorization: Bearer <token>` 和 `X-Account`，详见[共享账户与 Hub](./execution.md#运行与排障)。`/api/bot` 使用 JWT，账户操作还需角色授权；`/api/kline`、`/api/kline/csv/upload`、`/api/ws` 不在 JWT 鉴权组内，公网部署必须在反向代理处保护这些路径。

上面的web服务是以http启动的，建议您将`bind_ip`设为服务器本地或内网ip，以避免未授权的访问。

如果您需要从本地主机访问服务器的Dashboard，您可通过ssh连接到运行机器人的服务器，然后通过端口转发将对本地端口的请求转发到服务器端口：
```shell
ssh -L [本地端口]:[服务器bind_ip]:[服务器端口] [用户名]@[服务器公网地址]
# e.g. 
ssh -L 8001:127.0.0.1:8001 your_username@remote_server_address
```
::: tip Tip 
如果您需要将dashboard暴露到互联网，强烈建议您通过nginx等配置https，并设置一个较强的账号和密码。
:::

![image](/img/dash_login_cn.png)
> 本地启动实盘，或ssh端口转发后，机器人的访问url都是127.0.0.1:[port]，其中port是`api_server.port`的值。  
> 如果您通过nginx反向代理，这两个地址应当是从nginx访问的地址，比如"https://bot.myserver.com/8001"

### 监控存活
`check_bot.sh` 只按命令行中的配置文件名判断进程存在；下面的 `live` 对应 `live.yml`，不是 `trade` 子命令。还应监控 Hub 的恢复告警、账户冻结和行情可用性；进程存活不能证明保护有效。

您可使用`crontab`定期执行[check_bot.sh](https://github.com/banbox/banbot/blob/main/doc/check_bot.sh)脚本，检查机器人存活情况，发送邮件通知。
```shell
# for centos 8
dnf install mailx
vi /etc/mail.rc
```
示例配置如下：
```text
set smtp=smtps://smtp.gmail.com:465
set smtp-auth=login
set smtp-auth-user=your_email@gmail.com
set from=your_email@gmail.com
set smtp-auth-password=your_email_password
set ssl-verify=strict
```
测试邮件发送功能：
```shell
echo "Test email body" | mail -s "Test email subject" recipient@example.com
```
打开crontab：
```shell
crontab -e
```
添加一行：
```text
*/5 * * * * /ban/check_bot.sh live user1@example.com user2@example.com
```
赋予脚本执行权限：
```shell
chmod +x /ban/check_bot.sh
```

## 多因子与截面引擎

因子/混合实时策略也默认使用 Hub，不要求另行注册名为 `verified-session` 的工厂；该名称只是自定义生产嵌入示例。缺真实执行、标准元数据、数据可见性/资金费和对账证据时明确拒绝启动。不要把 provider 注册或 YAML 校验当作真实交易所验收。

实盘移除历史 archive，预热并安装当前订阅，对账成功后接纳目标；全部 history 合成方法当前被拒绝。`InitialNAV` 不给真实账户充值。策略行为、配对注册、结果解释及限制见[多因子指南](./factor.md)和[因子 API](../api/factor.md)。

## 资源与控制接口的边界

entry 为本次实盘装配 Runtime、账户状态、交易所、数据源和服务；停止后等待 worker/回调退出再释放拥有的资源。嵌入调用者应使用显式依赖并遵守资源所有者的 Close/Stop/Join 合同，不能通过更换包级配置来切换正在运行的账户。

实盘 HTTP/WS 和 RPC 是控制与观察接口，使用当前会话的配置快照、账户、时钟、订单和钱包依赖；它们不拥有因子图或计算状态。开发 WebUI 使用任务工厂创建独立回测 Runtime，与实盘 ApiServer 的会话边界不同。见 [Web API](../api/web.md)、[RPC API](../api/rpc.md) 和 [Runtime API](../api/runtime.md)。
