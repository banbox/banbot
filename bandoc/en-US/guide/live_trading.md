# Live simulation and production deployment

`bot trade` starts time-series, factor or mixed strategies from one `run_policy`. Default production and live simulation use a local Hub for shared-account execution: configure and start Hub before the robot. The default path currently supports linear perpetuals and does not establish completed end-to-end real-venue acceptance. See [Shared accounts and Hub](./execution.md) for account declarations and recovery.

::: warning Before running
Verify strategy, data, instrument units, funding and risk limits, then validate operation in live simulation. Production requires real-session capability checks and startup reconciliation. Software protection depends on Hub and market-data availability. Changing an environment name or API key cannot replace acceptance testing.
:::

## 1. Compilation
**Compiling from WebUI**  
You can execute `bot` to start the WebUI directly, then navigate to [/en-US/setting/](http://127.0.0.1:8000/en-US/setting/). On the left side, select 【Compile Code】, and on the right side, choose the target system and architecture. Proceed with the compilation and download.

**Compiling from Terminal**  
Possible values for `GOOS`: linux, darwin, windows, ...  
Possible values for `GOARCH`: amd64, arm64, ...

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

## 2. Editing configuration

Upload the executable containing registered strategies to `/ban/bot` and prepare `/ban/data`:

```sh
mkdir -p /ban/data
export BanDataDir=/ban/data
/ban/bot init
```

Edit market, database and strategy settings in `config.local.yml`. Defaults load `config.yml` followed by `config.local.yml`, with the latter taking precedence. Repeat `--config` for additional overlays. Keep account credentials in robot configuration; Hub configuration supplies public-market/database settings and the sender lease.

The exchange network proxy comes from `exchange.<name>.proxy`: `no` disables the proxy; when empty, the environment/system proxy is detected once at startup and the result is shared by all exchanges, so a system proxy change after detection needs a restart. See [Network Proxy](./configuration.md#network-proxy).

For live simulation select `env: dry_run` and `execution.funding_policy: explicit-zero`. For production select `env: prod`, real credentials and `required-stream`. Both require instrument units, capital and risk declarations. Time-series-only entries additionally require `execution.store` and `sender_lease_dir`. `leverage` (including account-level settings) is the fixed leverage of every account instrument and is set at the venue by the Hub account owner, while a per-order leverage in a strategy request is still rejected; `fatal_stop` is enforced by the account owner on equity loss. Do not simply switch an old configuration to prod. See [Configuration](./configuration.md) and [Shared account setup](./execution.md#configure-and-start).

Several strategies/robots can share one physical account through one Hub while retaining strategy budgets and attribution. Do not run independent senders for that account. Use separate subaccounts for independent capital boundaries. Keep strategy IDs stable and distinct across robots, and check attribution and budgets after startup.

## 3. Starting Hub and the robot

`hub` hosts public market data and private account execution; `spider` is its alias. Registered custom time-series sources can backfill and subscribe independently of the public Spider. Account execution still follows the selected Hub/provider route. Public data defaults to `127.0.0.1:6789`; the private socket defaults to `/ban/data/execution/hub.sock`. They are not interchangeable.

Prepare complete `hub.yml` and `live.yml` files using the shared-account guide. Check Hub listening, account registration, recovery/reconciliation and strategy warmup in the foreground first. After verification, use a process manager or background execution while retaining stdout/stderr:

```sh
nohup /ban/bot hub --datadir /ban/data --config /ban/hub.yml > /ban/data/hub-console.log 2>&1 &
# Run only after Hub is listening
nohup /ban/bot trade --datadir /ban/data --config /ban/live.yml > /ban/data/trade-console.log 2>&1 &
```

Do not start hub and spider on the same socket. Address errors from a missing Hub, incomplete capabilities, conflicting declarations or failed reconciliation; startup does not fall back to local sending or paper. `trade --dry-run` requires a factor policy and runs historical events replay. Use `env: dry_run` without that flag for live simulation.

Stopping one robot does not automatically close positions. Hub continues existing pending orders and protection. After Hub restart, accounts await robot reauthentication and local protection is inactive until authentication. Preserve the execution directory and journals, and follow [Recovery](./execution.md#restart-and-recovery). A running process alone does not prove account recovery.

An embedded production time-series robot (custom `execution.live_provider`) runs its own account without a separate Hub process and always uses the in-process Hub order manager (`execution.ts_facade` was removed and is rejected); so does the time-series part of a mixed factor + time-series robot (`SharedOrderMgr` is backtest-only). Accounts upgraded from the legacy facade (mixed accounts included) have their legacy checkpoints imported automatically, once, on first start; legacy data is left untouched and rolling back to the old facade is no longer available. See [Execution](./execution.md).

## Order types in live trading

Stop-Limit entries, OCO groups, multi-level take-profit and trailing stops work in both backtest and live trading (including live simulation). See [custom strategies](./strat_custom.md) for the fields and the [backtesting guide](./backtest.md#matching-semantics) for backtest matching. Live caveats:

- Stop-loss, take-profit and trailing stop follow `execution.protection_mode`: `software` (default) lets the execution engine monitor prices, `native_preferred` prefers exchange conditional orders when the adapter supports them and otherwise falls back to engine monitoring; trailing stops are still monitored by the engine. While the program is stopped the Hub keeps handling the protections already on the account; local protection is inactive while the Hub is unavailable.
- When one pending entry in an OCO group fills, the unfilled siblings are cancelled with exit tag `oco_cancel`; on the default Hub path, of several members executable in the same tick only the earliest-created one is kept (see [custom strategies](./strat_custom.md)).
- Multi-level take-profit places the next level only after the previous one fills; `MoveSLToBreakeven` moves the remaining stop-loss once that level fills.
- The backtest-only `data_gap` / `delist` exit tags and the `bt_*` cost keys do not apply to live trading.

## Live capability support matrix

Live trading and live simulation always go through the execution module (Hub or embedded account). The old live order stack was deleted; capabilities that only existed in it are handled as follows: **supported**; **refused** (an error at startup or request time); **ignored** (the configuration key has no effect and a warning is logged at startup, startup is not blocked). Migration notes for each item are in the repository file `doc/project/05_biz_orders_live.md` ("support matrix").

| Capability | Status | Current behaviour |
|---|---|---|
| Hedge (dual-side) position mode | Refused | Hub account registration check fails; positions must be one-way |
| Spot / margin spot (`market_type: spot`/`margin`) | Refused | Startup fails with `market_type "spot" is not supported by the execution path (linear USDT-margined perpetuals only)…`; only linear perpetuals are supported |
| Inverse and dated contracts | Refused | Rejected at startup or by the adapter |
| Testnet (`env: test`) | Refused | Startup fails with `testnet live trading is not supported by the execution path; use env: dry_run or prod` |
| Per-order leverage, automatic tiered-leverage downsizing | Refused | Leverage is declared per account and applied by the Hub account owner; per-order leverage is rejected and orders beyond the venue tier are rejected by the venue |
| Dynamic `UpdatePairs`, pair rotation (`pairlists`, `pairmgr.cron`) | Refused / ignored | Pairs are fixed at startup; `UpdatePairs` returns an error, rotation keys are ignored with a warning |
| Margin top-up `margin_add_rate`, `charge_on_bomb` | Ignored | Warning; risk is handled by owner admission and the loss breaker |
| Limit placement `put_limit_secs`, `odbook_ttl` | Ignored | Warning; limit prices follow `order_mode`, `limit_vol_secs`, `near_limit_bps` |
| Account polling `account_pull_secs` | Ignored | Warning; balances and equity come from the account owner |
| Live-vs-backtest comparison `bt_in_live` | Ignored | Warning; the feature was removed |
| Wallet snapshot persistence | Refused (not written) | `wallet_snapshot` is no longer written |
| Stop-loss/take-profit/trailing stop, multi-level take-profit, OCO, order editing | Supported | See the previous section; `execution.protection_mode` selects engine monitoring or preferring native conditional orders |
| `fatal_stop` | Supported (semantics changed) | Enforced by the account owner on equity drawdown with a persisted pause |
| `take_over_strat` | Supported (partial) | External takeover records are restored as strategy orders; user-opened exchange positions are no longer auto-tracked |
| `close_on_stuck` | Supported | Closes through the account order manager when no candle arrives in time (only accounts whose manager is registered in this runtime) |

## 4. Notifications, DashBoard, and Monitoring
### Notifications and remote control

Configure WeCom, email or Telegram through `rpc_channels`. See [Notifications](./notifications.md) for templates, filtering and SMTP examples. Both `min_intv_secs` and `retry_delay` use seconds. On shared-account robots with several accounts, Telegram runs only on the first tradable account (see [Notifications](./notifications.md#shared-account-hub-robots)). Delivery is asynchronous: receiving a message does not establish a new fill; check account state and logs during deployment.

Custom Telegram mode uses `token` and `chat_id`. Dashboard relay mode without a token provides a binding link in startup logs. An empty `secret` is generated; a manually supplied secret must be UUID v4. Relay mode needs outbound HTTPS and TCP 6788 to `www.banbot.site`; custom mode needs Telegram API access and can use a channel `proxy`.

Commands including `/orders`, `/close`, `/status`, `/disable` and `/enable` can affect trading. Confirm the selected account in multi-account operation. Channel authorization differs from Dashboard account roles; test delivery and control scope with a test account first. See [Notifications](./notifications.md) and [RPC API](../api/rpc.md) for configuration.

### DashBoard UI
You can configure `api_server` in the yml file. This will start a web service simultaneously when the bot is launched. After the bot is started, you can access the bot's DashBoard using the configured username and password to view the trading overview and manage the bot.
```yaml
api_server:
  enable: true
  bind_ip: 127.0.0.1
  port: 8001  # Needs to be allowed in firewall or through nginx reverse proxy
  jwt_secret_key: '[very_strong_secret]'
  users:
    - user: ban
      pwd: 'replace-with-a-strong-password'
      allow_ips: []  # Allowed client IPs
      acc_roles:
        user1: admin  # user1 corresponds to the user key in accounts
```
In shared-account live trading, time-series, factor and mixed robots start `api_server` only when they have exactly one account (with several accounts it is not started and a warning is logged); requests need `X-Authorization: Bearer <token>` and `X-Account`, see [Shared accounts and Hub](./execution.md#operation-and-troubleshooting). `/api/bot` uses JWT and account operations require roles. `/api/kline`, `/api/kline/csv/upload` and `/api/ws` are outside the JWT group; protect these paths at the reverse proxy for public deployment.

The web service mentioned above is started with HTTP. It is recommended that you set the `bind_ip` to the local or intranet IP of the server to prevent unauthorized access.

If you need to access the server's Dashboard from your local machine, you can connect to the server running the bot via SSH and then use port forwarding to forward requests from a local port to the server port:
```shell
ssh -L [local_port]:[server_bind_ip]:[server_port] [username]@server_public_address
# e.g. 
ssh -L 8001:127.0.0.1:8001 your_username@remote_server_address
```

::: tip Tip
If you need to expose the dashboard to the Internet, it is strongly recommended that you configure https through nginx and set a strong account and password.
:::

![image](/img/dash_login_en.png)
> When starting a live trading locally or after SSH port forwarding, the access URL for the robot is always 127.0.0.1:[port], where [port] is the value of `api_server.port`.  
> If you use an nginx reverse proxy, these two addresses should be the ones accessed from nginx, such as "https://bot.myserver.com/8001"

### Monitoring
`check_bot.sh` checks only for a configuration filename in process arguments. The `live` argument below matches `live.yml`, not the `trade` subcommand. Also monitor Hub recovery warnings, account freezes and market-data availability; process liveness does not prove protection is active.

You can use `crontab` to periodically execute the [check_bot.sh](https://github.com/banbox/banbot/blob/main/doc/check_bot.sh) script to check the bot's status and send email notifications.
```shell
# for centos 8
dnf install mailx
vi /etc/mail.rc
```
Sample configuration:
```text
set smtp=smtps://smtp.gmail.com:465
set smtp-auth=login
set smtp-auth-user=your_email@gmail.com
set from=your_email@gmail.com
set smtp-auth-password=your_email_password
set ssl-verify=strict
```
Test the email sending function:
```shell
echo "Test email body" | mail -s "Test email subject" recipient@example.com
```
Open crontab:
```shell
crontab -e
```
Add a line:
```text
*/5 * * * * /ban/check_bot.sh live user1@example.com user2@example.com
```
Grant execution permissions to the script:
```shell
chmod +x /ban/check_bot.sh
```

## Factor and cross-sectional engine

Factor/mixed live strategies also default to Hub. They do not require a factory named `verified-session`; that name is a custom production embedding example. Missing real execution, normalized metadata, data visibility/funding or reconciliation evidence fails startup. Provider registration and YAML validation do not establish real-venue acceptance.

Remove historical archive input, warm up and install current subscriptions, then admit targets after reconciliation. Live rejects every history combination method. `InitialNAV` deposits no live funds. See [Multi-factor strategies](./factor.md) and [Factor API](../api/factor.md) for behavior, pair registration, results and limits.

## Resource and control boundaries

entry assembles the Runtime, accounts, exchange, sources and services for one live run. Shutdown waits for workers and callbacks before releasing owned resources. Embedded callers must inject dependencies and follow the owner’s Close/Stop/Join contract; changing package configuration cannot safely switch an active account.

Live HTTP/WS and RPC provide control and observation through the session’s snapshot, accounts, clock, orders and wallets. They do not own the factor graph or computation state. Development WebUI factories create separate backtest Runtimes; their task boundary differs from the live ApiServer session. See [Web API](../api/web.md), [RPC API](../api/rpc.md) and [Runtime API](../api/runtime.md).
