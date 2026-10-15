# Ulcer Index and UPI formula review

Research date: 2026-10-13. Upstream references use `braverock/PerformanceAnalytics` commit `c079aea14727f68b5bfcc1e90292ceeb8a1f22e7`.
## Canonical definitions

For a positive NAV/equity series over the full assessment window:

```text
peak[t] = max(value[0:t])
drawdown[t] = value[t] / peak[t] - 1       # <= 0
UI  = sqrt(sum(drawdown[t]^2) / N)
UPI = (annualized_compound_return - annual_effective_Rf) / UI
```

UI is the RMS of percentage drawdowns from the cumulative high-water mark; it
captures depth and time below a previous high. It is not a rolling-14/30/60
indicator unless separately labelled. Canonical banbot UI uses the cumulative
peak over the full window and unsmoothed daily UTC equity observations. Decimal
drawdowns produce fractional UI (`0.0224 = 2.24%`); percent drawdowns produce
percentage-point UI (`2.24`).
## Source evidence

* [Peter Martin: Ulcer Index and UPI](http://www.tangotools.com/ui/ui.htm)
  defines UI as RMS percentage drawdown from earlier highs, gives
  `UI = sqrt(SumSq / NumOfPeriods)`, and defines `UPI = (Total return - Risk-free
  return) / UI`. It recommends compounded annual returns, reinvested
  distributions, net fees/costs/slippage, daily/weekly data, and warns that
  quarterly data can miss intra-period drawdown/recovery.
* [Wallible (2025)](https://www.wallible.com/zh/blog/2025-11-08-ulcer-performance-index/)
  gives the same formula and requires consistent frequency/Rf conventions. Its
  thresholds are editorial, not a Martin standard. [Zhihu](https://zhuanlan.zhihu.com/p/1941762146561160491)
  explains depth plus duration and presents `annual return / UI` as a useful
  zero-Rf variant of UPI.
* [PerformanceAnalytics UlcerIndex docs](https://rdrr.io/cran/PerformanceAnalytics/man/UlcerIndex.html)
  specify `sqrt(sum(D'_i^2 / n))` and state that frequency affects the result.
  Pinned [`R/UlcerIndex.R`](https://github.com/braverock/PerformanceAnalytics/blob/c079aea14727f68b5bfcc1e90292ceeb8a1f22e7/R/UlcerIndex.R)
  uses denominator `N`, never `N-1`; [`R/DrawdownPeak.R`](https://github.com/braverock/PerformanceAnalytics/blob/c079aea14727f68b5bfcc1e90292ceeb8a1f22e7/R/DrawdownPeak.R)
  compounds returns from prior peaks and assigns zero at new highs.
* Pinned [`R/MartinRatio.R`](https://github.com/braverock/PerformanceAnalytics/blob/c079aea14727f68b5bfcc1e90292ceeb8a1f22e7/R/MartinRatio.R)
  computes `Rp = (prod(1 + R))^(period / length(R)) - 1`, then `(Rp - Rf) / UI`;
  code defaults `Rf` to zero.
## Implementation decisions

Use `N`, the population RMS denominator. `N-1` is an unbiased sample-variance
correction and is not part of UI.

For banbot's explicit NAV stream, require `nav[0]` to be initial capital and
count every accepted NAV observation, including that first zero-drawdown point.
Do not add a synthetic baseline while changing the denominator. A returns helper
may compound returns into post-return wealth observations, but its count and
baseline convention must be documented separately; different observation sets
produce different UI values.

UI is not annualized. For daily 24/7 crypto data, use normalized daily UTC
endpoints and annualize the return numerator with elapsed years using `365.2425`
days/year. Use `252` only for a trading-day series. Do not smooth NAV before
canonical UI/UPI; smoothing changes the path and can hide sharp losses.

Use an annual effective risk-free rate with annualized CAGR. Convert a period or
cumulative risk-free return to that annual basis before subtraction. The docs say
`Rf` is "in same period as your returns", while code subtracts it from annualized
`Rp`; this is ambiguous for daily returns. Banbot should use explicit annual
`Rf`; current backtest reports use `Rf=0`.

Reject zero, negative, NaN, and infinite NAV values. For an all-new-high path,
UI is zero and UPI undefined; return JSON null rather than `+Inf`. Preserve
negative UPI when excess return is negative.
## Banbot API

```text
CalcUlcerPerformance(nav, elapsedYears, annualRiskFree)
    -> uiPercentPoints, upi, error
```
The Go accumulator counts
explicit initial NAV, uses `sqrt(sum / count)`, annualizes using elapsed years,
and converts CAGR to percentage points before dividing by UI. Report source
`daily_equity` means daily UTC closes plus explicit initial NAV; an exact
midnight mark closes the preceding day, missing days carry the latest mark,
and a final partial day uses the terminal mark. `daily_order_equity` uses
reconstructed daily order equity over that group's active span and initial
wallet capital. Group values are diagnostics, not additive portfolio metrics.
`CalcUlcerPerformanceByReturns` counts only post-return observations while
referencing an implicit initial NAV of one. UI is percentage points in JSON
and CSV, UPI is dimensionless. Historical reports without full observations
remain unavailable. Keep maximum drawdown, worst daily loss,
and recovery duration separate; UI/UPI measures path comfort and does not
replace shock limits.
