# config package

See [configuration](../guide/configuration.md) for user workflows. Embedded programs should load unified configuration with `LoadRunSpec` and then assemble the selected engines. Legacy package globals are not configuration containers for concurrent tasks.

## Load unified configuration

```go
args := &config.CmdArgs{
    DataDir: "/path/to/data",
    Configs: config.ArrString{"experiment.yml"},
}
spec, err := config.LoadRunSpec(args, false)
if err != nil {
    return err
}
effective, yamlErr := spec.EffectiveYAML(true)
if yamlErr != nil {
    return yamlErr
}
_ = effective
```

`LoadRunSpec(args *CmdArgs, showLog bool) (*RunSpec, *errs.Error)` reads default files, explicit files, inline YAML, and explicit CLI overrides. `LoadUnifiedConfigs(paths []string, showLog bool)` loads a known list of YAML files; it does not replace CLI layering or task assembly. Ordinary loading does not migrate, back up, or rewrite user files.

| Type | Contract |
| --- | --- |
| `CmdArgs` | CLI inputs including `Configs`, `ConfigData`, `DataDir`, `NoDefault`, time/symbol and optimization options; a field does not imply every subcommand exposes its flag |
| `UnifiedConfig` | `Root *Config` holds shared root fields; the single policy list is `RunPolicy []*PolicyV2`, not `Root.RunPolicy` |
| `PolicyV2` | Embeds `RunPolicyConfig`, with `Engine`, `ID`, `Account`, `CapitalWeight *float64`, `Factor`, and `ExplicitEngine` |
| `RunPolicyConfig` | Time-series name, timeframes, `refine_tf`, direction, pairs, `Params map[string]float64`, `PairParams`, sizing/filter options; `Def` / `DefInt` declare search parameters |
| `RunSpec` | Merged values, origins, and path bases; exported configuration/origin accessors return copies |
| `Snapshot` | Task-owned read-only configuration and explicit directories; `View()` is read-only, `Clone()` copies mutable containers |

`CmdArgs.ExplicitFlags` records flags actually supplied. Go callers overriding YAML with zero/empty values must also set the relevant marker. `RawPairs` and `RawTimeFrames` are comma-separated CLI strings corresponding to parsed `Pairs` and `TimeFrames`.

## RunSpec API

| API | Result/purpose |
| --- | --- |
| `Config() *UnifiedConfig` | Return an owned configuration copy |
| `Engines() []string` | Engines required by current policies |
| `Origin(field) (FieldOrigin, bool)` / `Origins()` | Field origins including `Source` and `Kind` |
| `EffectiveYAML(redact bool) ([]byte, error)` | Effective YAML; true redacts secrets, and advanced paths resolve for independently loadable output |
| `Hash() (string, error)` | Effective configuration identity hash |
| `ResolvePath(field string) (string, error)` | Resolve relative to the final supplying file; `@` / `$` needs a data directory, `:memory:` keeps its meaning |
| `WithExecutionMode(mode, source string) *RunSpec` | Return a copy recording execution-mode override and origin |
| `RuntimeSnapshot() (*Snapshot, *errs.Error)` | Normalize common settings and project only time-series policies; does not run factor tasks |

For example, `spec.ResolvePath("data.archive")` follows the file actually supplying that field. Inline/CLI layers use the data directory or, when absent, the working directory. Missing fields, empty paths, or required path bases return errors.

`LoadRuntimeSnapshot(args *CmdArgs)` retains time-series loading. `UnifiedConfig.TimeSeriesConfig()` is a restricted legacy projection: it rejects factors, advanced overrides, and new account/identity/budget fields. It is not the unified engine entry point.

## Structure and validation

Omitting `engine` retains legacy time-series `More` parameters. Explicit `time_series` / `factor` enables strategy identity, account, and capital allocation. Referenced accounts must exist, IDs must be unique within an account, and multiple strategies need explicit budgets totaling at most 1. `CapitalWeight` is distinct from `stake_rate` sizing.

Canonical YAML puts factor fields directly in `run_policy[]`, shared execution defaults in `execution`, and account overrides in `accounts.<name>`. `config_version` is optional; old 1/2 markers, `factor` wrappers, and `execution.accounts` remain readable. Duplicate spellings for the same field within one layer conflict. Advanced fields validate allowed keys and types.

Configuration must be single-document YAML with a mapping root and no duplicate keys. Later layers wholly replace `run_policy`, `wallet_amounts`, `fatal_stop`, `watch_jobs`, and `historical_coverage`; other mappings normally deep-merge. See [configuration](../guide/configuration.md) for time representation changes and environment expansion.

Common root structures include `DatabaseConfig` (`DbType`, `Url`, `SIDRegistryURL`, etc.), `APIServerConfig`, `UserConfig`, `PairMgrConfig`, `StratPerfConfig`, and `AccountConfig`. The configuration template is the complete YAML reference. Credentials remain in root `accounts`; execution settings do not replace actual trading authorization. `RPCChannels` maps channel configuration and supports `wework`, `mail`/`email`, and `telegram`; root `Webhook` holds message templates. See [notifications](../guide/notifications.md).

## Compatibility APIs and writing

| API | Boundary |
| --- | --- |
| `GetConfig(args, showLog)` / `ParseConfig(path)` / `ParseConfigs(paths, showLog)` | Return legacy time-series `Config`; not unified factor/mixed dispatch |
| `LoadConfig(args)` / `ApplyConfig(args, cfg)` | Install legacy process globals; unsuitable for switching concurrent tasks; `LoadConfig` does not reinstall after Loaded |
| `GetDataDir()` / `GetDataDirSafe()` | Legacy global directory access; the former raises a configuration error when absent, the latter allows empty |
| `ResolveDataDir(explicit)` / `GetStratDir()` | Explicit data directory precedes `BanDataDir`; strategy directory comes from `BanStratDir` |
| `ParsePath(path)` | Legacy data-directory prefix resolution; new fields use `RunSpec.ResolvePath` |
| `GetTakeOverTF(pair, defTF)` / `GetAccLeverage(account)` / `GetStakeAmount(account)` | Compatibility lookups depending on legacy state |
| `DumpYaml(desensitize bool)` / `(*Config).DumpYaml()` | Export legacy configuration; the package function requires a redaction argument |
| `ParseTimeRange(raw)` | Return start/end milliseconds and an error |
| `GetExportConfig(path)` / `LoadPerfs(inDir)` | Export configuration / legacy performance loading |

Editors use `WriteConfigAtomic(path string, expected, candidate []byte) *errs.Error`. `expected` is the original read bytes; concurrent changes reject the write. Validated atomic replacement preserves permissions. Loading does not automatically write. `GetExgConfig` is no longer exported; use read-only `Snapshot.View().Exchange`. See [Runtime API](./runtime.md) for lifecycle contracts.
