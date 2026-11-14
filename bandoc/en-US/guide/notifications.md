# Notifications

Live tasks use `rpc_channels` for WeCom, Telegram and email, with message-type/account filtering. Each task's notification session binds to its own orders, wallets and language settings.

## Email example

Merge this into your run configuration and replace the SMTP and recipient values:

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

`webhook` defines message **content templates**, not a generic HTTP URL channel. Currently `rpc_channels.*.type` accepts `wework`, `mail`/`email` and `telegram`. Configuring `mail` alone does not create a notification channel; also configure `rpc_channels` and the corresponding templates.

## Filtering and delivery

| Field | Meaning |
| --- | --- |
| `disable` | Disable the channel |
| `msg_types` | Allowed message types; empty allows all, including `status`, `exception`, `startup`, `entry`, `exit`, `market` |
| `accounts` | Allowed account names; empty allows all |
| `keywords` | Message keyword filtering |
| `min_intv_secs` | Minimum send interval, seconds |
| `retry_delay` | Retry interval, seconds |
| `proxy` | Channel proxy configuration |

Messages queue asynchronously and may be combined. Notification arrival time is not exchange fill time. Exception notifications also aggregate and rate-limit. Template fields depend on message type; entry/exit payloads include relevant fields such as `pair`, `strategy`, `price` and `value`.

## WeCom and Telegram

WeCom uses `type: wework` with `corp_id`, `agent_id`, `corp_secret` and `touser`. A custom Telegram bot uses `type: telegram` with `token`, `chat_id` and related settings. See the configuration template for all forms.

Telegram also exposes remote commands such as `/orders`, `/close`, `/status`, `/disable` and `/enable`, which can affect trading. Authorization depends on the selected Telegram channel; it is separate from Dashboard `api_server.users` roles. Verify channel access before deployment.

Notification changes normally require a restart. Check delivery and logs with a controlled test account first. See [Configuration](./configuration.md) and [rpc API](../api/rpc.md).
