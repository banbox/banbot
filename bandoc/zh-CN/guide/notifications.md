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

消息异步排队，可能合并发送；不能用收到通知的时刻代替交易所成交时间。异常通知还受聚合与限速控制。模板字段随消息类型而变，入场/出场可使用 `pair`、`strategy`、`price`、`value` 等对应字段。

## 企业微信与 Telegram

企业微信使用 `type: wework`，填写 `corp_id`、`agent_id`、`corp_secret`、`touser`。Telegram 自建机器人使用 `type: telegram`，填写 `token`、`chat_id` 等字段；全部配置形式见配置模板。

Telegram 除通知外还有 `/orders`、`/close`、`/status`、`/disable`、`/enable` 等远程命令，可能影响交易。它的身份授权依赖所选 Telegram 通道，不等同于 Dashboard 的 `api_server.users` 账户角色；部署前确认通道访问范围。

通知配置变更通常需要重启。先使用可控的测试账户核查发送结果和日志。更多字段见[配置](./configuration.md)，编程接口见 [rpc API](../api/rpc.md)。
