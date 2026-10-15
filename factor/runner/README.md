# Factor runners: YAML, storage, archives and live execution

Root `backtest` and `trade` consume one YAML `RunSpec` and dispatch time-series,
factor or mixed engines. Omitted `engine` keeps the existing time-series behavior.
Factor backtest mode is selected by `--mode`,
then `execution.mode`, then `events`; mixed replay requires `events`.

Root `research` uses the same unified YAML loader, default files, `--datadir`
and `--no-default` rules. To skip defaults, use
`--no-default --config /absolute/runtime.yml`; `@`/`$` paths still need a data
directory. Strategy configurations use YAML only. Archive JSONL is a data format.
Select a registered Go factor definition with `engine: factor`; arbitrary Go
nodes and portfolio builders keep explicit versions and missing-data policies.
For example, add this to the usual exchange/database/account configuration:

```yaml
data:
  pit_policy: static-approximation
execution:
  mode: weights
  funding_policy: explicit-zero
run_policy:
  - name: momentum-vol
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 10}
```

`banbot backtest --config runtime.yml` reads bounded pages directly from the
configured storage. A latest-value SQL table requires the explicit approximation
above: it cannot recover past revisions or publication times. Strict PIT needs
an immutable archive or an attested version-page provider. Events defaults to
observable one-minute execution prices, normalized Banexg instrument metadata
and capital-derived risk limits; unsupported market units fail at assembly.
Multiple strategies on one account declare their capital weights explicitly.
Research needs no execution account; ordinary events and dry-run use MemoryStore.

`banbot data archive --input records.jsonl --out chunk.gob --max-records 100000`
freezes bounded `factor.VersionRecord` JSON lines. Declare field types with the
archive schema (`--schema fields.yml`) when concrete integer widths matter.
Typed decoding preserves large integers; untyped integers outside float64's
exact range are rejected rather than silently rounded. Conflicting types fail.
Missing keys, NULLs, strings and arbitrary custom fields remain in the in-memory
`DataSeries.Values map[string]any`. Gob export preserves concrete types supported
by Gob: built-in values and registered concrete types. Custom concrete values in
interface fields require the application's existing Gob registration; arbitrary
in-memory values are not guaranteed to be Gob-exportable.

Use `banbot research --config runtime.yml`,
`banbot backtest --mode weights|events --config runtime.yml`, or
`banbot trade --dry-run --config runtime.yml`. Relative archive and
ledger paths are resolved from the field's originating configuration file.
YAML loading is read-only and needs no version marker. Factor options such as
`archive`, `expressions`, `portfolio` and `decision` live directly under each
`run_policy` item. Root `accounts.<name>` holds account execution overrides;
`execution` retains shared defaults. Panels, decisions, matured diagnostics and the final scalar summary stream as JSON
lines. `Result.Unresolved` reports labels that extend beyond the supplied data.

At least 25 closed hourly observations are needed before the default 24-period
momentum/volatility strategy becomes eligible. For complete unified YAML examples,
see the [factor guide](../../bandoc/en-US/guide/factor.md). Risk gross and margin
limits are absolute settlement-currency amounts, not leverage multipliers. Every
SID needs declared instrument units; exchange names do not select behavior.

Weights retain quantities between explicit decisions and charge actual changed
notional. Events and dry-run trade use the shared account coordinator,
MemoryStore, integer quantity constraints, price ticks, venue minima,
margin/gross limits and deterministic paper fills. The paper venue models full
market fills, directional slippage and fees; it does not model queue priority,
partial-fill liquidity or network failures. Limit-maker orders rest until a
later observable quote crosses their limit. Explicit `StorePath` together with
`SenderLeaseDir` opts into a durable diagnostic replay. Existing paper stores are refused
because their simulated venue history cannot be reconstructed safely.

Large events/dry-run replays can set `execution.history: cold/history.sqlite`
in unified YAML (`Execution.HistoryPath` for embedded callers). This optional
indexed cold archive uses the existing SQLite dependency, no sender lease, and
the same domain MemoryStore validation and rollback. Settled orders, closed
lots, plans, events, postings and paper receipts are evicted only after one
successful archive commit and remain readable for duplicate reports, delayed
funding, lagging projections and final Gob audit export. Default small replays
still create no execution database. The archive path must be new; it is not a
resume snapshot and is rejected for real trade or durable execution. Its hot
record count does not bound active strategy state or total process RSS.

Ordinary backtests also publish `resolved.json` with actual defaults and field
origins, and `account-<account>/manifest.json` plus versioned event/posting Gob
chunks. Completion follows output sync/close and resource cleanup; incomplete
runs preserve their errors. `data.page_bytes` optionally limits the decoded
logical payload of an input page or bootstrap batch. The compiled budget report
lists streams, rows, byte estimates and exclusions; it is not a heap/RSS limit.

Execution uses observable prices strictly after decision completion and before
exclusive expiry. Events requires ticks (`event`) or a declared `1m` price
stream; a completed candle's OHLC cannot reconstruct the intervening market.
The weights mode also requires an explicit observable price stream; any ideal
open-price assumption belongs to the supplied stream and run manifest.
`DecisionDelayMS` advances the archive visibility cutoff beyond the logical
bar-end grid while preserving grid cadence and sampling. Actual live receipt
time is the cutoff; completion plus `LatencyMS` sets the executable window.

Archive decisions advance publication/reception indexes and an event-time
queue within each bounded raw chunk. Each decision clones only the latest
eligible event/revision per stream, applying the round's local-reception gate
before selection even in publication replay; the raw `Next` batches deliver every
revision for source callbacks, funding, quotes and content evidence. Earlier
cutoff queries and independent reception cutoffs use the immutable raw chunk
without rewinding the advancing cursor. Public records keep owned typed Values,
including nested data and NULLs; consumer edits cannot change later snapshots.

Funding is `required-stream` with explicit source settlements, or the declared
`explicit-zero` simplification. Required streams must appear for every
investable SID in every chunk; use explicit zero-rate settlement records for
known-zero intervals. Late funding is rejected rather than charged against a
later position. Revision conflicts require explicit reconciliation. No 8-hour
venue schedule is inferred.

`runner.NewLive` consumes current provider records through `Observe` and
drained decision barriers through `Flush`. It requires a real clock and a
reconciled account sink; it never sends archived targets to a live venue.
`AccountSink` submits only its own Full/Patch target revision; the account service
merges strategies while preserving sent allocations and absolute expiry.
It checks account/strategy/currency identity and the committed checkpoint.
Live history-IC is refused until a matured-history
provider is supplied; fixed/equal combinations are supported.

Compatible consumers may borrow one `ComputationGroup` session while retaining
their own targets and account state. `Stop` cancels intake; `Join` waits for
actual callback/computation work before releasing that borrow. The last joined
consumer removes the session from the group, so repeated dynamic Universe
generations do not accumulate old sessions. Joining one borrower preserves the
session and recursive indicator state still used by the remaining consumers.

For current live trading use `banbot trade --config runtime.yml` with
`execution.live_provider: verified-session`.
Use `--live-provider verified-session` to override the configured provider;
live configurations must have no archive chunks. An embedding application registers
`entry.RegisterFactorLiveBinding` with the current session's verified Banexg
transport, canonical symbol metadata, publication/revision mapper and funding
policy verifier. A Banexg session lacking these proofs fails with an unsupported-capability error;
configuration flags cannot replace transport or account-snapshot evidence.
The entry owns the explicit Runtime, shared account service, private report
stream and current source providers, and stops/joins them on cancellation or
failure. Existing venue cash and positions require prior ledger attribution and
successful startup reconciliation; `InitialNAV` never deposits real funds.

Live required-funding records carry exact decimal strings `mark`, `rate`, and
`account_amount` (the authoritative signed venue cash delta), plus a stable
`settlement_id`. The mapped `VersionRecord.EventTime` is the settlement cutoff.
Duplicate settlements are idempotent. Differences from virtual attribution
remain explicit unassigned cash; settlements received after position changes
require historical reconciliation. An explicit-zero live policy requires
verified absence of funding obligations for that session's instruments.

The complete streaming benchmark uses synthetic immutable archives, 20 factor
outputs sharing price/return nodes, and 1 or 10 strategies on each of two active
SQLite/paper accounts. This benchmark explicitly exercises the persistent
execution backend; ordinary backtests default to MemoryStore. Configure
test-only environment variables, then run:

```powershell
$env:BANBOT_FACTOR_BENCH_ASSETS='500'
$env:BANBOT_FACTOR_BENCH_HOURS='17520'
$env:BANBOT_FACTOR_BENCH_STRATEGIES='10' # also run with 1
$env:BANBOT_FACTOR_BENCH_CHUNK_BARS='24'
go test ./factor/runner -run '^$' -bench BenchmarkSharedArchivePipeline -benchtime=1x -timeout 24h
```

The defaults are 24 assets, 32 hours, 1 strategy per account and 24-hour raw
chunks. Measurements include node updates, maximum retained raw/session/label
sizes, real fills, throughput, allocations and sampled heap. Archive generation
is outside the timed replay. This is a full runnable workload definition;
smaller measured runs do not establish full two-year throughput or memory.

## Configuration ownership when embedding

Use `runner.CloneConfig(config)` when an owner retains a configuration beyond
its caller's setup phase, such as runtime replay installation. It returns
`(Config, error)` and independently copies chunks, all snapshot maps and universe
lists, expression declarations, combination weights/columns, manifest labels and
snapshot references, and execution instrument maps. Lists retain order and
duplicates; nil and empty containers remain distinct. Decimal values retain their
immutable value semantics. This is an ownership copy, not strategy validation;
continue to call `ValidateReplayConfig` or `ValidateLiveConfig` as appropriate.

Compiled plans, computation groups, portfolio builders, historical-input factories
and observation callbacks remain borrowed handles. The owner must keep them
usable throughout the run and stop/join drivers before releasing shared services.
The copy never opens inputs or borrows accounts/sessions. It retains the former
runtime installation boundary's JSON serializability check, including rejection
of nonfinite configuration numbers, without decoding the configuration or dropping
nonserialized handles. Raw records still travel through `DataSeries.Values` and
`CloneVersionRecord`; configuration copying does not change their type/NULL rules.

## Current guides and live provider names

See the [guide](../../bandoc/en-US/guide/factor.md), [中文指南](../../bandoc/zh-CN/guide/factor.md) and [refactor record](../../doc/strategy_engine_refactor.md). verified-session above is an application registration example, not a built-in verified venue. Register entry.RegisterFactorLiveBinding("verified-session", factory) with real evidence first. Empty/banexg selects the built-in adapter; missing capabilities or unknown names fail explicitly. No live-venue acceptance or automatic paper fallback is implied.

## Historical source-event decisions

Set `SourceEventDecisions: true` for irregular source observations, such as
information bars. Set `Factor.Source` and `Factor.TimeFrame` explicitly and keep
`DecisionDelayMS: 0`. This clock supports historical `Research`, `Weights`, and
paper `Events`; `Trade` and the live driver reject it. `DecisionInterval` remains
a positive configuration/risk horizon, but does not create periodic decision
batches in this mode.

A decision uses `GridTime = EventTime` from a real **closed** record matching the
selected factor source and timeframe. `DecisionTime` is its current batch's
publication/receipt cutoff. Archive replay waits for both `AvailableAt` and
`IngestedAt`, even when reception replay was not separately selected. Every
requested SID/source must still satisfy the original complete snapshot barrier
and PIT rules. If some SIDs arrive later for an incomplete grid, that grid can
retry at the later receipt cutoff. A completed grid cannot be evaluated again
or replaced by an older grid; such later source revisions fail explicitly.

Quote, funding, expiry, other-source, and unclosed-record batches process ordinary
execution and pending proposals without creating source decisions. No fabricated
periodic ticks fill gaps between observed source records. A publication batch
containing two distinct selected source event times is ambiguous for this clock
and rejects before execution or decision processing. Split such observations
into genuinely distinct publication/receipt batches rather than silently dropping
one source grid or treating several grids as one observation.

The clock choice and source identity participate in manifest replay assumptions,
shared computation identity, and shared account timeline compatibility. Strategies
that check gaps in fixed wall-clock units must adapt their own gap contract before
using irregular source events; this runner does not reinterpret strategy history.

## YAML clock, settlement and paper fill choices

The unified YAML configuration exposes these choices through the ordinary
`backtest --mode weights|events` and `research` commands:

```yaml
execution:
  funding_policy: explicit-zero
  paper_fill_model: midpoint  # Events only: midpoint, bid-ask, mid-entry-market-exit
accounts:
  default:
    paper_fill_model: bid-ask # account override, shared by its strategies
run_policy:
  - name: MomentumVol
    engine: factor
    run_timeframes: [1h]      # positive calculation/risk horizon, also for labels
    params: {window: 24, k: 10}
    archive: information-bars.gob
    decision:
      source_event_decisions: true
      source: information
      timeframe: event       # exact archive stream identity, not a duration
      delay_ms: 0
    prices: {source: tick, timeframe: event, field: price}
    terminal_settlement_source: terminal  # Weights or paper Events
    # option_expiry_source: option-expiry # paper Events only
```

`decision.source` and `decision.timeframe` are required when source-event decisions
are enabled, and rejected when the option is disabled. They select the runtime
`Factor.Source/TimeFrame`; the built-in MomentumVol definition consequently reads
its `close` values from that same stream. Expression bindings continue to select
their own inputs, while `expressions.timeframe` must match `run_timeframes`.
The source clock must contain real closed records and complete PIT snapshots;
selecting a source does not manufacture observations. `event` is allowed as the
clock timeframe; `run_timeframes` or `decision.interval_ms` still provides the
positive risk horizon. Source-event decisions reject live Trade and nonzero delay.

Settlement sources must differ from prices, funding and one another. Records and
instrument contracts must satisfy the existing expiry/terminal validation rules.
Explicit `execution.instruments` can carry a nested `option` contract using
numeric SID keys; cash European options require paper Events accounting. Weights,
live Trade and Banexg live execution reject unsupported option capability.
The configuration does not imply that an exchange adapter can derive option
contracts automatically. The spread-dependent fill models require actual visible
bid/ask quotes; a missing spread is rejected, not inferred from candles.

Canonical configuration export retains these YAML fields. `resolved.json` reports
the selected clock, settlement sources, fill model and their file/account origins.
Omitting `paper_fill_model` retains the existing midpoint default.

## Stateful portfolio factories and statistical research

Use `RegisterPortfolioFactory(name, factory, validator)` for private mutable
state per run. The optional validator is a pure preflight check;
`ValidateStaticConfig`, `ValidateReplayConfig` and `ValidateLiveConfig` never
construct factory state. `PreparePortfolioConfig` constructs once after
preflight and before execution resources are assembled. Replay/live entry points
perform this preparation; embedded callers must pass the returned configuration
through one run only. Legacy factories without a validator report constructor
errors at preparation. State coupled to committed fills belongs in a
`PortfolioPolicy`. See [definition.go](definition.go) for the API, the
[statistical arbitrage guide](../../doc/statarb_guide.md) for the supported paths,
and the [statistics index](../statistics/README.md) for research primitives.
