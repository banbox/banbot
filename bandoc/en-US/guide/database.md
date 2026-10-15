# Database

banbot stores market data, time-series data, and related metadata in QuestDB or TimescaleDB. Both use the PostgreSQL protocol; strategies, backtests, K-line tools, and custom time-series data work the same way with either backend. Select the backend in the configuration.

## Choosing a Backend

| Backend | Suitable for | Deployment |
| --- | --- | --- |
| QuestDB | Quick local setup focused on market-data writes and reads | When the default local port is unavailable, banbot can automatically download and start QuestDB |
| TimescaleDB | Existing PostgreSQL infrastructure managed by operations teams | Deploy PostgreSQL and the TimescaleDB extension yourself first |

Do not alternate between backends for different bots using the same `BanDataDir`. The table structures and data files are not directly interchangeable; plan export, conversion, and import separately before migrating.

## QuestDB Configuration

QuestDB's default PGWire port is `8812`. Explicitly setting `db_type` avoids incorrect detection when the port changes.

```yaml
database:
  db_type: questdb
  url: postgresql://admin:quest@127.0.0.1:8812/qdb?sslmode=disable
  max_pool_size: 50
  auto_create: true
  qdb_mem_pct: 0.3       # Usage ratio of available memory, range 0 to 1
  qdb_max_mem_mb: 16384  # Maximum memory usage in MB
```

When no QuestDB is reachable locally, banbot attempts to install and start it. `qdb_mem_pct` and `qdb_max_mem_mb` apply only to this backend. QuestDB WAL writes become visible asynchronously: relevant framework read-after-write paths use targeted visibility waits or owner-local caches/locks. Successful INSERT or CTAS does not guarantee an immediate read sees it. Wait for expected rows, timestamps, ranges, or counts. Before DROP/RENAME, verify the replacement matches its expected snapshot. Timeouts retain recovery markers; an empty read is not evidence that recovery is unnecessary. External SQL tools must follow the same rules.

## TimescaleDB Configuration

TimescaleDB must be deployed first, and the target database must have the TimescaleDB extension installed. banbot does not download or start PostgreSQL/TimescaleDB. The default PostgreSQL port is `5432`.

```yaml
database:
  db_type: timescale
  url: postgresql://banbot:your-password@127.0.0.1:5432/banbot?sslmode=disable
  max_pool_size: 50
  auto_create: true
```

`auto_create: true` initializes or upgrades the banbot schema after a successful connection; it does not create a PostgreSQL service, user, or database. In production, use a dedicated database account for banbot and configure TLS, passwords, and network access controls according to your deployment requirements.

## Automatic Detection

When `db_type` is not set, banbot detects the backend from the connection port: `8812` is treated as QuestDB, and `5432` as PostgreSQL/TimescaleDB; other ports are probed further. New configurations should always set `db_type` for readable and stable behavior.

## Time-Series Semantics

Regardless of the backend, banbot uses a unified time-series interface to manage K-lines and custom data, including table creation, writes, reads, coverage, and gap backfilling. K-line coverage is maintained by the internal `sranges` mechanism; do not treat legacy `KInfo` or `KHole` tables as an external integration contract.

Time-series schemas store fixed float/int/string/bool/json columns; default K-lines and extensions use the same DataSeries.Values runtime map. Database integers are 64-bit. Database JSON encoding is not the same contract as arbitrary Go values; fixed-column round trips cannot restore absent-key versus explicit-NULL distinctions.

QuestDB custom-series deletion updates effective sranges coverage first, then rewrites physical data only when the current compaction threshold is reached, verifying the replacement snapshot. TimescaleDB uses transactions and physical deletion. Do not bypass the repository to alter coverage metadata. ormo order state and ormu UI tasks use separate SQLite files; shared execution ledgers/stores also have their own persistence responsibility, separate from time-series tables.

See [Custom Time-Series Data](./custom_data.md) for integration, storage, and consumption.
