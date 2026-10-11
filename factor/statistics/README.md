# Statistics: task index and contracts

This package supplies Go research primitives. Choose an estimator by its input,
sampling assumptions and output contract; a book prefix is provenance, not a
second execution engine. The [statistical arbitrage guide](../../doc/statarb_guide.md)
connects these functions to training, factor runners and execution.

| Task | Entry points | Implementation / contract |
| --- | --- | --- |
| Stationarity and cointegration | `AugmentedDickeyFuller`, `EngleGranger`, `Johansen` | [stationarity.go](stationarity.go), [johansen.go](johansen.go): test statistics require the appropriate critical-value distribution |
| Pair hedge, spread and duration | `Regress`, `FitOU`, `EmpiricalThreshold`, `CrossingDurations`, `BootstrapDurations` | [pairs.go](pairs.go), [spreads.go](spreads.go), [vidyamurthy.go](vidyamurthy.go): intercept, half-life convention and observation clock are explicit |
| Online filtering and temporal state | `KalmanPair`, `FitKalmanNoise`, `LocalLevel`, `NewLaggedSpreadFilter`, `NewSourceLaggedSpreadFilter`, `FitAR`, `FitARIMA`, `FitBinaryHMM` | [pairs.go](pairs.go), [kalman_fit.go](kalman_fit.go), [temporal.go](temporal.go), [vidyamurthy.go](vidyamurthy.go), [chan_arima.go](chan_arima.go), [chan_hmm.go](chan_hmm.go): keep one mutable filter per run |
| Exposures, selection and allocation | `FitPCA`, `FitFactorAnalysis`, `FactorNeutralWeights`, `HierarchicalRiskParity`, `LongOnlyMinimumVariance` | [pca.go](pca.go), [factor_analysis.go](factor_analysis.go), [neutral_allocation.go](neutral_allocation.go), [afml_hrp.go](afml_hrp.go), [afml_minvariance.go](afml_minvariance.go): retain fitted feature/asset order |
| Information bars and sampling | `StandardActivityBars`, `InformationActivityBars`, `CUSUMEvents`, `SelectStationaryFFD` | [afml_bars.go](afml_bars.go), [afml.go](afml.go), [afml_ffd.go](afml_ffd.go): event index and elapsed wall time have different units |
| Labels, overlap and validation | `TripleBarrierLabel`, `CalendarBarrierLabels`, `EventUniqueness`, `PurgedKFold`, `CombinatorialPurgedCV`, `RunCPCVResearch` | [afml.go](afml.go), [afml_calendar_labels.go](afml_calendar_labels.go), [afml_validation.go](afml_validation.go), [afml_cpcv_research.go](afml_cpcv_research.go): label end/maturity determines exclusion, not just row order |
| Models, tuning and importance | `TrainRandomForest`, `TrainSequentialBaggedClassifier`, `PurgedParameterSearch`, `RefitPurgedSearch`, `PurgedFeatureImportance` | [afml_forest.go](afml_forest.go), [afml_bagging.go](afml_bagging.go), [afml_search.go](afml_search.go), [afml_completion_refit.go](afml_completion_refit.go), [afml_features_importance.go](afml_features_importance.go): fits belong inside each training fold |
| Options | `PriceEuropeanOption`, `EuropeanImpliedVolatility`, `AmericanOptionPrice`, `AmericanImpliedVolatility`, `VolatilitySmirk` | [options.go](options.go), [options_american.go](options_american.go), [option_surface.go](option_surface.go): pricing functions do not establish runner settlement capability |
| Order flow and microstructure | `BulkVolumeFractions`, `VolumeSynchronizedPIN`, `KyleLambda`, `HasbrouckImpactGibbs`, `OrderFlowWindow`, `CumulativeMicroInformation` | [orderflow.go](orderflow.go), [afml_orderflow_features.go](afml_orderflow_features.go), [afml_micro_information.go](afml_micro_information.go): preserve actual quote/flow visibility and units |
| Cross-venue opportunity | `CrossVenueArbitrage` | [chan_crossvenue.go](chan_crossvenue.go): same asset/currency, executable BBO, displayed size and pre-funded balances; independent [paper execution](../../execution/crossvenue.go) handles the experiment lifecycle |
| Lifecycle and capacity evidence | `BoundaryFlowTWRR`, `LifecycleConcentration`, `ExecutionCostMetrics`, `LifecycleGeneralCharacteristics`, `EvaluatedCapacity` | [afml_lifecycle_report.go](afml_lifecycle_report.go), [afml_general_characteristics.go](afml_general_characteristics.go): use observable execution evidence and matured results |

## Numerical boundaries

Read each function's Go comment and its adjacent `_test.go` before combining it
with another estimator. Regressions use rows of observations; covariance and
feature columns must preserve a stable order. Nonfinite inputs, unidentified fits,
invalid dimensions and unsupported domains return errors. Warmup/NULL values
must be excluded deliberately rather than converted to zero.

Examples of contracts that must remain distinct: `FitOU` accepts only
nonoscillating mean reversion (`-1 < Slope < 0`); discrete and Euler half-lives
are separate outputs. ADF and Engle–Granger statistics are not Gaussian p-values.
`CrossVenueArbitrage` accepts 2–1000 quotes and fee/transfer rates in `[0,1)`.
Iterative fits and combinatorial experiments expose iteration/work/trial limits;
set them for the actual dataset rather than treating defaults as an unlimited
production workload. Passing validation proves the declared numerical domain,
not economic validity or profitability.

Run the package checks with `go test ./factor/statistics` and
`go vet ./factor/statistics`. Synthetic and deterministic tests validate contracts;
research conclusions still require independent out-of-sample evidence.
