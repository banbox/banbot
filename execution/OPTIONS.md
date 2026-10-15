# Cash European options in research and paper execution

Declare `Instrument.Valuation = "cash_european_option"` and an `Option`
descriptor: `UnderlyingID`, `Right` (`call` or `put`), positive `Strike`,
`ExpiryMS`, and positive `UnderlyingStressMax`. Premium, strike and underlying
fixing use settlement currency per underlying unit. `QuantityStep * ContractSize`
is the underlying units represented by one ledger step. These contracts have
cash settlement at expiry, with no early exercise or physical delivery.

Purchases debit the entire signed premium and fees immediately; sales credit
the premium less fees. Cost basis and realized PnL remain available for reporting.
Account and strategy equity add signed option market value to cash. Perpetuals
continue adding unrealized PnL. Internal matches transfer synthetic premiums
between strategies without changing venue cash. External trades remain in the
unassigned book. Option expiry closes actual and attributed books in one ledger
transaction, including zero payoff, and preserves perpetual positions.

The risk contract is explicit and confined to a research scenario:
`0 <= underlying fixing <= UnderlyingStressMax`. A long reserves its marked
premium; a short put reserves its entire strike payoff and a short call reserves
`max(UnderlyingStressMax - Strike, 0)`, multiplied by underlying units. This is
independent of premium and perpetual margin rate. Virtual strategies reserve
their own exposures; the actual account nets identical contracts only. There is
no netting across strikes or expiries. Gross caps use the greater of premium
notional and stress collateral. Fixings above the declared bound are rejected.
This contract supplies **no live solvency guarantee**: call payoffs are unbounded
outside the declared scenario. `BanexgAdapter` rejects all cash option instruments.

`OptionExpiry` must contain the exact-expiry `FixingAtMS`, observed nonnegative
`Fixing`, its `PublishedMS`, and processing `AtMS >= PublishedMS`. Its semantic
identity derives from contract ID and expiry. Processing-clock retries deduplicate
across SQLite reopen and memory cold-history eviction. Changed fixing,
publication metadata or descriptor rejects. Pending and uncertain orders block
expiry. New fills, external activity or internal matches cannot resurrect a
settled contract, even with a backdated execution timestamp. New executions at
or after expiry reject. Premium execution quotes must be positive; worthless
contracts settle through expiry rather than a fabricated positive exit fill.

`SharedAccountBorrow.ApplyOptionExpiry` serializes durable ledger settlement and
paper cash/position settlement under the account gate and the paper venue lock.
Paper venue state remains process-local; a diagnostic ledger is not a restart
snapshot and `NewPaperSink` continues rejecting an existing ledger path.

Historical factor runner `Events` mode routes an explicit
`Config.OptionExpirySource`. Each settlement `VersionRecord` uses the option SID,
`EventTime` equal to expiry, `AvailableAt` equal to publication, and
`Values["fixing"]` as a decimal string. Reception replay also waits for
`IngestedAt`. Settlement precedes decisions at the same visible timestamp.
Settled inherited targets retire before subsequent plans; a zero expired
allocation needs no further quote, and nonzero resurrection rejects.
`Weights` and live `Trade` modes reject declared cash options because their
legacy book/live transport do not implement this accounting contract.
