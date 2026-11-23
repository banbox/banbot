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

Messages queue asynchronously and may be combined. Notification arrival time is not exchange fill time. Sending is rate-limited by `min_intv_secs` and retried after `retry_delay` on failure; messages not yet sent when the task stops are discarded. Template fields depend on message type; entry/exit payloads include relevant fields such as `pair`, `strategy`, `price` and `value`.

## Per-account channels

With multiple accounts, `accounts.<account>.rpc_channels` can reference a global channel and override some of its fields to create a channel dedicated to that account:

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

- Each item must contain `name`, which must be an existing global `rpc_channels` name, otherwise startup fails; the other fields override the same-named settings of that channel.
- When `accounts` is not set in the item, the channel only sends that account's messages.
- The generated channel is named `<name>_<account>`; the global channel itself stays in place unchanged.
- Accounts with `no_trade: true` are skipped.
- Without a global `rpc_channels`, no channel is created.

## Shared-account (Hub) robots

On the default prod / dry-run shared-account path every account of a robot has its own notification session. Telegram channels (`type: telegram`, including account references to them) are started only for the first tradable account (smallest account name, `no_trade` accounts excluded; with a single account, that account), because one bot token allows a single command listener per process; the other accounts skip them and a warning is logged. WeCom and email channels work for every account. Messages are asynchronous: they go through a bounded queue of 256 and are dropped, with a warning, when it is full. A startup status message is sent, and a `bot stop` message is sent on shutdown with a wait of at most 3 seconds; anything still queued is discarded. Exception alerts are sent through the same queue.

## WeCom and Telegram

WeCom uses `type: wework` with `corp_id`, `agent_id`, `corp_secret` and `touser`. A custom Telegram bot uses `type: telegram` with `token`, `chat_id` and related settings. See the configuration template for all forms.

Telegram also exposes remote commands such as `/orders`, `/close`, `/status`, `/disable` and `/enable`, which can affect trading. Authorization depends on the selected Telegram channel; it is separate from Dashboard `api_server.users` roles. Verify channel access before deployment.

Notification changes normally require a restart. Check delivery and logs with a controlled test account first. See [Configuration](./configuration.md) and [rpc API](../api/rpc.md).
