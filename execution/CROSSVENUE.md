# Durable cross-venue spot paper execution

`OpenCrossVenuePaper` models venue-owned cash and actual spot asset units in one
settlement currency. It is a bounded research/paper API, separate from `Store`
perpetual/option lots and the factor runner's single-account adapter. No exchange
adapter, live submission or distributed execution guarantee is provided.

`EstimateCrossVenue` prices a requested equal-unit pair against sorted executable
book depth, charging each book's cash commission on its depth-weighted notional.
Books must be internally uncrossed, nonempty, observed before publication,
published before reception, received before processing and inside the declared
maximum source age. Different asset/currency pairs are rejected; translated
cross-border prices do not provide an FX hedge.

`Begin` checks prepositioned buy-venue cash and sell-venue spot inventory, checks
the estimated net edge after the explicit rebalance allowance, and commits the
buy leg only. It leaves a durable `bought` trade and actual buy fill. `Exit` uses
a caller-supplied visible sell-venue book to complete the equal-unit sell, or a
fresh buy-venue bid book to unwind the purchased inventory. A failed second leg
does not roll back the first fill. The book blocks further trades and outgoing
transfers until that exposure is completed or unwound. A successful sell may
realize less than the initial estimate, including a loss. Incoming transfer
completion remains possible while recovering an exposure.

Each fill and its balance changes commit together in SQLite. Request IDs,
terminal exit payloads, genesis balances and quote vintages reject conflicting
replays. Exact retries across reopen do not duplicate balances, fills or fees.
Displayed size is consumed durably by venue and market-observation timestamp;
changing receipt/publication metadata cannot replenish the same observed book.
Explicit later observations replenish depth. The local per-operation transaction
is atomic; the two exchange legs are sequential and are **not** an atomic trade.
SQLite contention returns an error without a partial local transaction; this is
not a live-venue account-ownership or distributed lease implementation.

`StartTransfer` deducts source cash/units and actual cash/asset fees immediately.
The destination receives nothing before `CompleteTransfer` at its declared
arrival time. In-transit cash/units contribute to aggregate `Equity(commonMark)`
but cannot fund trades. Charged fees do not contribute to equity. The estimator's
rebalance allowance is not charged a second time; actual transfers determine
realized costs. Full restoration of one destination coin costs more than a
one-coin withdrawal when the withdrawal consumes some coins as its fee.

Decimal amounts have exact paper arithmetic; this API does not invent venue tick,
lot, currency-rounding rules or withdrawal schedules absent from the research
inputs. Experiments permit 2–1000 venues, 1024 depth levels per side and 100000
trade/transfer identities. Journal state is retained for audit. Source credit
risk, latency-sensitive fills, exchange defaults and historical profitability
remain outside this deterministic paper model.

The strategy example in `banstrats` is runnable with
`go run ./cmd/statarb-crossvenue -output .omx/ultragoal/crossvenue`.
Chan's source BBO gives gross cash edge `5.644`, commissions `.945472` and
trade cash profit `4.698528`. Charging `.01 BTC` withdrawal at explicit mark
`233.546` gives aggregate PnL `2.363068`; the source's approximately `$2.6`
uses rounded fees/withdrawal cost. The example's depth and transfer latency are
explicit synthetic assumptions, not recovered historical observations.
