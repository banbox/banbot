# Architecture performance evidence

These tools cover `doc/better_arch.md` sections 9.5 and 12. They do not reconstruct
the missing pre-optimization dirty tree, prove physical power-loss durability, or
replace PG/QuestDB/provider/account acceptance. No new project dependencies.

## Repeatable workloads

`strat/architecture_dualma_benchmark_test.go` mirrors the SMA(5)/SMA(20)/Cross
calculation in `banstrats/ma/demo.go`. It drives real banta bar environments,
map-based `StratJob.SetData` and `OnData`, checks large int64/NULL/missing fields,
and measures 1/10 consumer fanout with shared bar environments. It observes
signals without sending orders. The same public-API-only test file runs on the
preserved v0.5.7 checkout; copy only this harness to an isolated baseline and
use managed `freeze --build-package ./strat --benchmark-cwd strat` on both.
`BANBOT_ARCH_BENCH_ASSETS` and `HOURS` select its scale (defaults 24/128).
This comparison measures the TS computation/callback path, not provider,
execution, reports, or the CS/mixed full-chain acceptance gate.

`BenchmarkReplayOptionalResearch` in `factor/runner` compares the same immutable
24-asset/28-hour input and weights execution with research labels enabled and
disabled. Disabled research reports zero retained pending frames. This is a
configuration comparison within the candidate, not an old/new regression
certificate; archive decoding is included, input generation excluded.

`factor/architecture_benchmark_test.go` runs the same deterministic map-based feed
through TS `StratJob.SetData → DataHub → OnBar`, CS immutable snapshots and a real
20-output Session, and both paths together. It covers 8/32/128 columns and 1/10
consumers, checking int64 above JavaScript precision, string, bool, nested JSON,
explicit NULL and missing. This is a computation hot path, not a complete
backtest: it excludes provider/DB/network I/O, order execution, and outputs.

Each benchmark operation replays the configured asset/history size. Defaults
are 24 assets and 32 hours. Run one scope before selecting a two-year workload:

```powershell
$env:GOMAXPROCS = '4'
$env:BANBOT_ARCH_BENCH_ASSETS = '500'
$env:BANBOT_ARCH_BENCH_HOURS = '48' # 17520 selects 2 years
go test ./factor -run '^$' -bench 'BenchmarkArchitectureHotPath/CS/columns-8/consumers-1$' -benchmem -benchtime=1x -count=1 -timeout=30m
```

`TestArchitectureHotPathBoundedHistory` checks the actual Session indicator
arrays against their retention bound at 512 and 1024 hours. Retained-value
counts are not process memory. The benchmark reports asset-events and node
updates per operation and sampled heap (every 64 hours); Go reports bytes and allocations. Select `TS` and `mixed`
explicitly and repeat 1/10 consumers for their independent evidence.

`BenchmarkSharedArchivePipeline` separately includes immutable archive input,
barrier, diagnostics, two accounts and actual SQLite/coordinator/paper execution.
Its existing `BANBOT_FACTOR_BENCH_ASSETS`, `HOURS`, `STRATEGIES` and `CHUNK_BARS`
settings keep archive chunks bounded. It excludes PG/QuestDB and genuine venue
I/O. Do not extrapolate its 48-hour results to two years.

## Dirty provenance and paired comparison

`architecture_performance.py` needs Python 3.11+, uses the standard library, and
has `test_architecture_performance.py`. On this Windows machine it can run with
the existing uv-managed Python; `uv run --no-project --python 3.11` is another
option. It freezes every non-ignored tracked and untracked workspace file,
including Go/module files, embedded SQL/HTML, assembly and the harness;
YAML hashes remain separate. The selected executable is excluded from source hashes.
It records explicit fixture digests, dirty Git status, Go/build environment and host.
Only hashes are recorded for inputs/config files, not their contents. Record
database versions, cold/warm cache, width, timeframe, warmup/page budgets and
fixture generator identity using `--environment '@environment.json'`.

Use two separately preserved source checkouts, let `freeze --build-package`
compile each test executable between matching source/config snapshots, and supply identical
input fixtures. Make the benchmark fixture available in both source trees;
copying new production implementation to the baseline invalidates that baseline.
An old Git HEAD is insufficient when relevant uncommitted implementation exists.

```powershell
# Run in each preserved checkout with identical GOMAXPROCS/BANBOT_* settings:
python scripts/architecture_performance.py freeze --root . --go go --build-package ./factor --input fixture.gob --executable tmp/architecture-bench.exe --benchmark-cwd factor --environment '@environment.json' --output tmp/provenance.json

# commands-old.json and commands-new.json contain absolute executable argv:
# ["D:/old/tmp/architecture-bench.exe", "-test.run=^$",
#  "-test.bench=BenchmarkArchitectureHotPath", "-test.benchtime=2s", "-test.count=1"]
python scripts/architecture_performance.py compare --baseline-manifest D:/old/tmp/provenance.json --candidate-manifest D:/new/tmp/provenance.json --baseline-command '@commands-old.json' --candidate-command '@commands-new.json' --rounds 10 --output tmp/architecture-comparison
```

JSON `@file` arguments avoid PowerShell 5 stripping native argument quotes.
Commands use argv directly without a shell. Prebuilding excludes compiler time
from the compared `ns/op`. Rounds alternate AB and BA; at least 10 matched pairs
are required. Paired log ratios receive a conservative Student t interval with
at least 95% coverage under the normal-log assumption. The benchmark passes
only when the upper CI bound is ≤1.05; lower >1.05 fails; crossing 1.05 is
inconclusive. Individual metrics and raw logs remain available. This interval
is per benchmark, not a simultaneous guarantee across the entire matrix.

The tool rejects mismatched host/Go/input/config/benchmark environments,
different benchmark argv after the executable path, and changing executables.
Managed freeze runs `go test -c`, verifies unchanged source/config hashes before
and after compilation, and records the build command and binary hash. Acceptance
requires this recorded build chain for the selected executable and the same build
package on both sides. A plain freeze of a prebuilt executable records only a
source/binary pair; it has no build-origin evidence and is allowed only with `--smoke`.
These local artifacts depend on retaining the actual checkout and compiler;
they are not signed attestations against deliberate manifest editing.
Fixtures and captured YAML files are rehashed before and after every command,
including the final pair. Changed/missing files reject the comparison. Rejected
runs leave `comparison.json` explicitly incomplete, so a previous passing result
cannot masquerade as the new result. Declare ignored runtime configs, external
fixtures and other runtime files with `--input`; the tool cannot infer unlisted
runtime file dependencies. Hashing reads fixtures and can warm filesystem caches;
cold-cache workloads must reset the relevant caches inside their driver after
verification and before the timed workload.
Use `--benchmark-cwd factor/runner` for runner fixtures, or another identical
package-relative path in both checkouts; it is part of comparison identity.
Freeze `environment` faithfully: DB/cache information
cannot be discovered safely by guessing credentials. RSS sampling measures the
direct command process peak working set/VmHWM and excludes child processes, so
use prebuilt binaries rather than `go test` to measure process memory. It is
sampled evidence, not a heap/RSS hard ceiling.

`--smoke` permits identical source/binary and unmanaged prebuilt comparisons to validate parsing,
ordering and report generation. Such reports always set
`acceptance_eligible=false` and `passed=false`, even if a statistical metric
passes. They cannot prove old/new ≤5% acceptance.

## Windows race execution

Latest local evidence is in [the validation report](architecture_validation_report.md):
the 24-asset/128-hour dualMA comparison against v0.5.7 ran 10 AB/BA pairs,
with 1/10 consumer ratios 0.97854/0.99139 and inconclusive 95% intervals.
Candidate-only optional research replay retained 0 pending frames when disabled;
the 18-case three-mode computation matrix passed. These scopes exclude the
ordinary provider/database/account/report full-chain acceptance requirement.

The normal Windows `go test -race` requires CGO and a C toolchain. When gcc is
unavailable, an official temporary Zig 0.14.1 archive can supply it without
changing `go.mod` or global environment. Verify the download independently.

```powershell
$env:GOMAXPROCS = '4'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ./scripts/run_architecture_race.ps1 -Go D:/ban/tools/go/bin/go.exe -Zig D:/path/to/zig.exe
```

The script compiles each package sequentially with race instrumentation, links
Windows synchronization imports, and fixes the temporary test binary's PE image
base because Zig/LLD ASLR can exceed the Windows TSan shadow address range. It
does not change system policy or production binaries. Logs and test binaries go
to ignored `tmp/architecture-race`; previous `CGO_ENABLED` and `CC` are restored.
`-Packages @('./factor') -Tests TestArchitectureHotPathBoundedHistory` selects a
small probe. Run memory/benchmark acceptance separately from race and unrelated
long tests to reduce contention. PASS means no race was detected on exercised
paths; it does not prove all possible schedules or venue behavior.
