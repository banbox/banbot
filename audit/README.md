# Read-only quality and funding review

These tools implement warning/review mode, not a data rejection or ledger repair
policy. They have no database writer, execution sender or apply command.

## Market refresh review

Capture the existing rows and freshly re-fetched rows as separate typed
`factor.VersionStore` archives, with the same SID registry and the same selected
venue/market/source/time range. Fetch into the candidate archive, not the live
series tables. Archive creation is available through the existing `data archive`
command and the `factor.VersionStore` API; file presence does not establish that
all expected symbols or bars were captured.

```
banbot tool data-audit refresh-review \
  --baseline baseline.gob --candidate downloaded.gob --stage proposed.gob \
  --min-symbols 20 --min-fraction 0.8
```

The thresholds are illustrative, not defaults or exchange-specific facts. Both
thresholds must be provided explicitly. An alert means that at least that many
observed symbols, and at least that fraction of observed symbols, have equal
open/high/low/close and exactly zero volume in the same source/timeframe/time
bucket. It does not prove an exchange outage, false data, or tradability. The
fraction's denominator is the supplied snapshot, not the exchange's entire
listed universe. Do not mix unrelated venues/markets in a review. Missing,
NULL, non-numeric and non-finite OHLCV values do not become zero-valued bars.

The JSON output includes baseline/candidate warnings, typed row content hashes,
changed fields, retained fields and baseline-only counts. The staged archive
contains proposed candidate rows; it never requests deletion of baseline-only
rows. A field absent from a candidate retains its baseline value, including
custom fields. An explicit candidate NULL replaces the old value with NULL.
Concrete types, large integers and nested values use the existing archive codec,
not a float-only OHLCV projection or JSON round trip. Unsupported archive types
fail rather than silently coerce values. Source archives remain unchanged, and
an existing different stage file cannot be overwritten.

A human must review the warnings and differences before any future application
workflow. There is deliberately no automatic import/apply step. In particular,
do not run `kline purge` as a substitute for a bounded refresh, or assume
`repair-ranges` refreshes bar contents. A future apply path must preserve all
extended columns and validate visible replacement contents before modifying old
data; QuestDB WAL visibility must be checked explicitly.

## Historical funding audit

```
banbot tool data-audit funding --venue venue.json --ledger ledger.json > audit.json
```

`venue.json` is an `audit.FundingSnapshot`: account identity, half-open
`start_ms`/`stop_ms`, a symbol-to-execution-instrument-ID map named `instruments`,
and `rows` containing the authoritative `banexg.FundingCash` records. Obtain them
through the exchange's `FundingCashCapability`, including every supported
funding type and complete bounded windows. The inclusive API end timestamp must
be `stop_ms - 1`. This tool does not claim that a user-supplied file is a complete
venue snapshot.

`ledger.json` is an `audit.FundingLedgerSnapshot`: the exact same `account` and
`events` containing the relevant execution `CommittedEvent` exports, including
all Funding source events covering the requested interval. Obtain events from
the existing `Store.EventsAfter` read API on a consistent backup or a controlled
read snapshot. Never hand-edit a live execution database to produce this input.
Use one account/session/settlement domain per pair of files; cross-account
snapshots, duplicate IDs, unknown instrument mappings and invalid/out-of-window
venue rows are rejected.

Example snapshot envelopes (record arrays omitted here):

```json
{
  "account": {"VenueSessionIdentity": "venue-production", "Account": "main", "SettlementDomain": "USDT"},
  "start_ms": 1793059200000,
  "stop_ms": 1793145600000,
  "instruments": {"BTC/USDT:USDT": "btc-perpetual"},
  "rows": []
}
```

```json
{
  "account": {"VenueSessionIdentity": "venue-production", "Account": "main", "SettlementDomain": "USDT"},
  "events": []
}
```

The report distinguishes missing ledger settlements, conflicts in immutable
settlement contents, and ledger-only IDs in the audit interval. Comparison uses
exact decimal values and independent settlement IDs, including ordinary and
special settlements at the same timestamp. Missing rows are **not** approval to
backfill: historical strategy attribution, publication delays and completeness
still require investigation. The tool does not call `ApplyFunding`, replay any
event, advance a funding watermark, change balances, or release any account
freeze. It reports evidence discrepancies only.
