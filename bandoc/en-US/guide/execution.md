# Shared accounts and Hub

Default live entries for time-series, factor and mixed strategies use a local Hub account service. Robots own public inputs, strategy computation and order presentation. Hub owns account ledgers, private reports, net execution and pending/protection advancement. Stopping a robot does not close positions or stop its Hub account.

## Choose a run mode

| Goal | Configuration and command | Account location |
| --- | --- | --- |
| Historical backtest | `backtest`; weights/events for factors, events for mixed runs | In-process simulation |
| Factor/mixed historical paper replay | `trade --dry-run` | Historical events replay, without a live Hub account |
| Live simulation | `env: dry_run`; start `hub` before `trade` | Durable Hub paper account |
| Production trading | `env: prod`; start `hub` before `trade` | Durable Hub account, requiring verified real-session capabilities |

An omitted `execution.live_provider`, `hub` or `banexg` selects Hub. A missing Hub fails startup without local-sender fallback. Custom registered providers are explicit production embedding options and cannot mix with Hub in one robot. The retired `LiveOrderMgr` sender is not an alternative route around account execution.

## Configure and start

1. Prepare base market/database configuration and compiled strategies. The default Hub currently accepts only `prod`/`dry_run` linear perpetual accounts with an explicit settlement currency. Testnet, spot, inverse contracts and live options are unsupported.
2. Declare stable strategy `id`, `account` and same-account `capital_weight` values in robot configuration. Supply positive `wallet_amounts`, `execution.margin_rate` (greater than 0 and at most 1), `max_account_margin`, `max_virtual_gross` and `strategy_gross_limit`. Capital declarations do not deposit exchange funds or permit allocation beyond available account cash.
3. Configure `execution.instruments`, keyed by SID, with `id`, `version`, `valuation: linear_perpetual`, `settlement_currency`, `quantity_step`, `price_tick`, `contract_size`, `money_scale` and market-required minimum quantity/notional. Time-series instrument IDs are canonical symbols; factor SIDMap entries must match their instruments. Units, versions and precision must match normalized banexg market metadata. Do not copy another instrument's values.
4. Use the same absolute `execution.sender_lease_dir` on the host and every process capable of sending for the physical account. Production Hub requires explicit configuration. Locks in different directories do not prevent duplicate senders. Stop and migrate the old sender before cutover; do not run another owner alongside it.
5. Production robots supply account credentials and `funding_policy: required-stream`, passing capability and startup reconciliation checks. Live simulation supplies positive paper cash and `funding_policy: explicit-zero`. Hub does not currently ingest simulated funding; `required-stream` cannot substitute for that declaration. The default route does not establish complete parity with historical configurations.

This is only a path overlay for an existing complete configuration. It omits strategies, instruments and risk declarations:

```yaml
env: dry_run
execution:
  live_provider: hub
  hub_socket: /ban/data/execution/hub.sock
  sender_lease_dir: /ban/sender-leases
  store: /ban/data/execution-declaration.sqlite
  funding_policy: explicit-zero
```

Time-series live preflight still requires `execution.store` and sender-lease declarations. Hub chooses the actual account database under `<datadir>/execution` by account identity; a robot cannot redirect the remote ledger with `store`. Put account overrides directly in `accounts.<name>`, for example `accounts.default.hub_socket`, without a nested execution block.

Hub configuration supplies database/public-market settings and the sender lease. Private account credentials arrive in robot registration requests. Hub does not initialize private accounts from its own YAML or give credentials to public miners. Arbitrary exchange SDK options are not inherited; old configurations relying on custom endpoints/proxies in those options require separate checking.

Check startup logs in the foreground first. Start the robot after Hub has successfully begun listening:

```sh
/ban/bot hub --datadir /ban/data --config /ban/hub.yml
# Start the robot in a second terminal
/ban/bot trade --datadir /ban/data --config /ban/live.yml
```

`spider` is a CLI alias for `hub`; do not start a second service on the same path. Public market data uses `spider_addr`, default `127.0.0.1:6789`. The private socket defaults to `<datadir>/execution/hub.sock`. To change it, use Hub's `--execution-socket /absolute/path` and the same robot `execution.hub_socket`.

The private socket is mode 0600 and its directory must not be group/world writable. All clients under the same OS user are trusted to operate every registered account. There is no private TCP transport or strategy-level multi-user authorization; do not expose it as a remote account service.

## Multiple strategies and pending orders

One physical account should have one sending owner. TS/factor strategies retain separate budgets and virtual positions. Account execution nets opposing contributions and sends only the remainder. Exchange balances remain shared; use separate physical accounts for independent capital and risk boundaries.

First registration establishes account capital, risk and instrument baselines. Later robots can add strategies only from unassigned cash, without replacing the baseline or dynamically adding instruments not installed by the host. Preserve account/strategy IDs, declarations and local journals across restart. Renaming cannot bypass recovery; two writers using the same journal encounter file-lock/epoch conflicts.

Account-owned pending orders and protection continue when the originating robot disconnects. Software protection needs a running Hub and visible quotes. Execution uses independent REST book snapshots and cannot guarantee detection of transient crossings between snapshots. Native protection also requires explicit adapter capability evidence. `cancel_entry` cancels remaining entry without closing filled inventory; closing is a separate exit request. Command acceptance, a cancellation response or a robot display update cannot replace final fill/cancellation evidence. Callbacks may also be delivered more than once.

## Restart and recovery

- **Robot-only restart:** leave Hub and its account running, and reconnect with the original configuration and journal. Account reports, public clocks and pending protection remain active. Client disconnect does not remove an account or close positions.
- **Hub restart:** preserve the execution directory, SQLite files (including WAL sidecars), paper journals, host declarations and robot journals. `accounts.json` stores credential-free identities only. Recovered accounts enter `waiting_authentication`, with local protection inactive. A robot must register again; account operation resumes only after recovery and reconciliation succeed. There is no autonomous credential restoration guarantee when all robots are offline.
- **Paper recovery:** registration restores simulated orders, cash and costs. Missing/corrupt journals or changed initial cash, fees, slippage/model fail recovery. Do not delete the ledger to bypass a conflict.
- **Stale socket:** verify the old Hub has stopped and released sender ownership before handling its stale path. Startup does not automatically delete an existing socket or take over its owner.

Ambiguous submission/cancellation outcomes retain states such as `Unknown`/`CancelPending` and block ordinary position changes. Recover/retry with the original command ID and payload instead of inventing a replacement order. Require stable-client-ID queries, complete trade history and account reconciliation. One not-found response cannot prove zero execution. Report/recovery failures can persistently freeze an account; do not edit SQLite order states or remove dedup records.

If a late fill arrives after `Rejected`, no supported operator command can reopen that order, and repeated restarts cannot repair it. Stop new strategy submissions and all account senders, wait for them to exit, then preserve the database/WAL, redacted logs and order/trade evidence for an audited maintainer repair. Keep the freeze; do not resubmit or book the same fill as both an external position and strategy fill. An interrupted accepted policy-entry cancellation also lacks a general operator completion tool; report it and retain evidence.

Stopping Hub stops and joins account work. Check real positions, working orders and protection arrangements first. Paper runs and unit tests establish software contracts; they do not establish real-venue readiness, profitability, capacity or slippage acceptance.
