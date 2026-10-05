# Factor and cross-sectional API


This page covers factor, factor/expr, factor/research and factor/runner. See [Multi-factor strategies](../guide/factor.md) for configuration and workflows.

CLI startup is unified: root `backtest` / `trade` dispatch factor, time-series or mixed policies from one YAML `RunSpec`. Root `research`, `validate --spec`, `explain --spec` and `data archive` provide research, standalone compilation and archive conversion. Embedding APIs below retain their existing contracts.

| Package | Components | Responsibility |
| --- | --- | --- |
| factor | Plan, Session, Batch, VersionStore, Snapshot, Universe, RoundBarrier, TargetPortfolio | Visibility snapshots and native computation |
| factor/expr | Spec, Compile, CloneSpec | Startup compilation and complete declaration validation |
| factor/research | ComboSpec, ICHistory, LabelQueue, ManifestSpec | Matured research separated from inference |
| factor/backtest | Book | Approximate weights accounting |
| factor/runner | Config, Run, NewLive, ComputationGroup, AccountSink | Replay/live drivers and account targets |

## Registration and compilation

runner.RegisterDefinition(name string, builder runner.DefinitionBuilder) error registers a Go graph. DefinitionBuilder is func(runner.Config) (*factor.Plan, research.ComboSpec, error). Duplicate names fail; momentum-vol is built in.

runner.RegisterPortfolioBuilder registers a uniquely versioned PortfolioBuilder accepting Frame, Universe, PortfolioSpec and PortfolioDefinition and returning a target, diagnostics and error. top-bottom-k-v1 is reserved.

runner.CompileDefinition uses the same definition resolution as replay/live. Config.Expressions is exclusive with Plan/Definition. Explicit schema, missing-data policy and operator versions define reproducible graph identity.

## A compilable Go definition

Compile this entry in your strategy project before selecting CodeMomentumV1. It declares two native factor outputs and handles registration failure:

```go
package main

import (
    "fmt"
    "github.com/banbox/banbot/entry"
    "github.com/banbox/banbot/factor"
    "github.com/banbox/banbot/factor/research"
    "github.com/banbox/banbot/factor/runner"
)

func buildMomentum(c runner.Config) (*factor.Plan, research.ComboSpec, error) {
    if c.Factor.Source == "" || c.Factor.Field == "" || c.Factor.TimeFrame == "" || c.Factor.Window < 2 {
        return nil, research.ComboSpec{}, fmt.Errorf("source, field, timeframe and window >= 2 are required")
    }
    price := factor.Positive(factor.Field(c.Factor.Source, c.Factor.Field, c.Factor.TimeFrame))
    returns := factor.Return(price, c.Factor.Window)
    plan, err := factor.New().
        Add("momentum", factor.ZScore(returns)).
        Add("momentum_rank", factor.Rank(returns)).
        Compile()
    return plan, research.ComboSpec{
        Method: research.Equal,
        Columns: []string{"momentum", "momentum_rank"},
    }, err
}

func main() {
    if err := runner.RegisterDefinition("CodeMomentumV1", buildMomentum); err != nil {
        panic(err)
    }
    entry.RunCmd()
}
```

Replace the built-in policy with this declaration; retain the market/history/execution settings. definition and expressions are exclusive:

```yaml
run_policy:
  - name: custom_momentum
    engine: factor
    run_timeframes: [1h]
    params: {window: 24, k: 1}
    definition: CodeMomentumV1
```

## Configuration ownership

runner.CloneConfig(c Config) (Config, error) owns chunks, snapshot Universe lists/maps, expressions, combo, manifest and execution instruments. It preserves list order, duplicates and nil/empty distinctions. One JSON serialization check retains NaN/Inf rejection.

Plan, ComputationGroup, PortfolioBuilder, HistoricalInput, ObserveBatch and the internal timeline remain borrowed. Copying opens no input/account and does not replace ValidateReplayConfig/ValidateLiveConfig. Owners keep borrowed services usable until all drivers Join. expr.CloneSpec, research.CloneComboSpec and research.CloneManifestSpec expose the corresponding container copies.

## Replay and live

Run drives history. NewLive requires a real clock and reconciled sink; Observe accepts current records and Flush drains decision barriers. Stop rejects intake; Join waits for accepted callbacks/computation and releases its session borrow. Shared computation does not share portfolios, budgets or research state.

Embedding uses runtime.CompileFactorsLivePlan, SubscribeFactorsLive, InstallFactorsLive and BindFactorsLive. entry.RegisterFactorLiveBinding registers verified current-session evidence. A missing transport, metadata, revision/funding or account capability fails startup; a YAML flag does not supply it. Full real-venue acceptance remains unverified.

## Data and output

VersionRecord retains event/visibility/reception times, revisions and typed Values. Snapshot getter copies are ownership boundaries; arbitrary columns and NULL must remain. Session and Batch differ in recursive-history semantics.

Manifest records code, plan, Universe, visibility, input references, labels and execution assumptions. It is not a raw-record clone. Result.Unresolved retains immature labels. Account event/posting Gob and legacy time-series orders.gob have different audit/report contracts.
