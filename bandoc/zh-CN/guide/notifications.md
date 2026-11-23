# 消息通知

实盘任务可通过 `rpc_channels` 配置企业微信、Telegram 和邮件，按消息类型及账户过滤。通知由任务自己的会话发送，与对应订单、钱包和语言配置绑定。

## 邮件示例

将下面内容合并到运行配置，替换 SMTP 和收件人信息：

```yaml
name: my-bot
mail:
  enable: true
  host: smtp.example.com
  port: 465
  username: sender@example.com
  password: YOUR_SMTP_PASSWORD
rpc_channels:
  email_notice:
    type: email
    touser: receiver@example.com
    msg_types: [status, exception, entry, exit]
    accounts: []
    min_intv_secs: 5
    retry_delay: 30
webhook:
  status:
    content: '{name}: {status}'
  exception:
    content: '{name}: {status}'
  entry:
    content: '{name} {account} {pair} {strategy} {enter_tag} {price:.5f}'
  exit:
    content: '{name} {account} {pair} {strategy} {exit_tag} {profit:.2f}'
```

`webhook` 是各类消息的**内容模板**，不是通用 HTTP URL 渠道；当前 `rpc_channels.*.type` 支持 `wework`、`mail`/`email` 和 `telegram`。仅配置 `mail` 不会自动创建通知渠道，还需配置 `rpc_channels` 和对应消息模板。

## 筛选与发送

| 字段 | 含义 |
| --- | --- |
| `disable` | 禁用该渠道 |
| `msg_types` | 允许的消息类型，留空允许全部；包括 `status`、`exception`、`startup`、`entry`、`exit`、`market` |
| `accounts` | 允许的账户名，留空允许全部 |
| `keywords` | 消息关键词筛选 |
| `min_intv_secs` | 最小发送间隔，秒 |
| `retry_delay` | 失败重试间隔，秒 |
| `proxy` | 渠道代理配置 |

消息异步排队，可能合并发送；不能用收到通知的时刻代替交易所成交时间。发送受 `min_intv_secs` 限速，失败后按 `retry_delay` 重试；停止时尚未发出的消息会被丢弃。模板字段随消息类型而变，入场/出场可使用 `pair`、`strategy`、`price`、`value` 等对应字段。

## 按账户绑定渠道

多账户时，可在 `accounts.<账户名>.rpc_channels` 中引用全局渠道并覆盖部分字段，为该账户生成专属渠道：

```yaml
rpc_channels:
  tg_main:
    type: telegram
    token: "BOT_TOKEN"
    chat_id: 123456
accounts:
  acc1:
    rpc_channels:
      - name: tg_main
        msg_types: [entry, exit]
```

- 每项必须含 `name`，且须是全局 `rpc_channels` 中已有的渠道名，否则启动报错；其余字段覆盖该渠道的同名配置。
- 未写 `accounts` 字段时，默认只发送该账户的消息。
- 生成的渠道名为 `<name>_<账户名>`；全局渠道本身仍然保留，不受影响。
- `no_trade: true` 的账户会跳过。
- 未配置全局 `rpc_channels` 时不会创建任何渠道。

## 共享账户（Hub）机器人

在默认的 prod/dry-run 共享账户路径上，机器人的每个账户都有自己的通知会话。Telegram 渠道（`type: telegram`，包括账户对它的引用）只为第一个可交易账户启动（账户名最小者，排除 `no_trade` 账户；只有一个账户时即该账户），因为一个 bot token 在一个进程内只允许一个命令监听；其他账户跳过并记告警。企业微信和邮件渠道对每个账户都有效。消息异步发送：经容量 256 的有界队列，队列满时丢弃并告警。启动时发送状态消息，关停时发送 `bot stop` 消息并最多等待 3 秒，仍在队列中的消息会被丢弃。异常告警走同一队列。

## 企业微信与 Telegram

企业微信使用 `type: wework`，填写 `corp_id`、`agent_id`、`corp_secret`、`touser`。Telegram 自建机器人使用 `type: telegram`，填写 `token`、`chat_id` 等字段；全部配置形式见配置模板。

Telegram 除通知外还有 `/orders`、`/close`、`/status`、`/disable`、`/enable` 等远程命令，可能影响交易。它的身份授权依赖所选 Telegram 通道，不等同于 Dashboard 的 `api_server.users` 账户角色；部署前确认通道访问范围。

通知配置变更通常需要重启。先使用可控的测试账户核查发送结果和日志。更多字段见[配置](./configuration.md)，编程接口见 [rpc API](../api/rpc.md)。
