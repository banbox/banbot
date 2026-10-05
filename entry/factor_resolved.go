package entry

import (
	"encoding/json"
	"fmt"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
)

// Resolved values are a separate output: user YAML remains concise, and this
// allowlist cannot accidentally serialize credentials or runtime resources.
type resolvedFactorValue struct {
	Value  any                `json:"value"`
	Origin config.FieldOrigin `json:"origin"`
}

func writeResolvedFactorConfig(path string, spec *config.RunSpec, configs []runner.Config) error {
	u := spec.Config()
	accountCapital, declaredCapital := map[string]float64{}, map[string]float64{}
	for _, c := range configs {
		accountCapital[c.AccountID] += c.InitialNAV
		if c.AccountInitialNAV > 0 {
			declaredCapital[c.AccountID] = c.AccountInitialNAV
		}
	}
	strategies := make([]map[string]resolvedFactorValue, 0, len(configs))
	position := 0
	for policyIndex, policy := range u.RunPolicy {
		if policy.Engine != config.EngineFactor {
			continue
		}
		if position >= len(configs) {
			return fmt.Errorf("resolved factor configuration does not match RunSpec")
		}
		c := configs[position]
		position++
		accountNAV := c.AccountInitialNAV
		if accountNAV == 0 {
			accountNAV = c.InitialNAV
			if c.Mode == runner.Events {
				accountNAV = accountCapital[c.AccountID]
				if declaredCapital[c.AccountID] > 0 {
					accountNAV = declaredCapital[c.AccountID]
				}
			}
		}
		namespace := c.ComputationContext.DataNamespace
		if namespace == "" {
			namespace = "archive"
			if len(spec.Engines()) > 1 {
				namespace = "mixed-replay"
			}
		}
		prefix := fmt.Sprintf("run_policy[%d].", policyIndex)
		origin := func(fallback string, fields ...string) config.FieldOrigin {
			for _, field := range fields {
				if value, ok := spec.Origin(field); ok {
					return value
				}
			}
			return config.FieldOrigin{Kind: "derived", Source: fallback}
		}
		values := map[string]resolvedFactorValue{}
		set := func(key string, value any, fallback string, fields ...string) {
			values[key] = resolvedFactorValue{value, origin(fallback, fields...)}
		}
		set("strategy_id", c.StrategyID, "registered strategy name", prefix+"id", prefix+"name")
		set("account_id", c.AccountID, "configured default trading account", prefix+"account")
		set("definition", c.Definition, "registered Go builder", prefix+"definition", prefix+"name")
		if c.Expressions != nil {
			set("expressions", c.Expressions, "declarative factor expressions", prefix+"expressions")
			plan, combo, err := runner.CompileDefinition(c)
			if err != nil {
				return err
			}
			set("factor_plan_hash", plan.Hash(), "compiled expression semantics")
			set("combine", combo, "resolved expression combination", prefix+"combo", prefix+"expressions.combine")
		}
		set("execution_mode", c.Mode, "ordinary backtest events default", "execution.mode")
		set("currency", c.Manifest.Currency, "USD default", prefix+"manifest.currency", "stake_currency")
		set("initial_nav", c.InitialNAV, "wallet capital times policy capital_weight; otherwise 10000", prefix+"capital_weight", prefix+"initial_nav", "wallet_amounts."+c.Manifest.Currency)
		set("account_initial_nav", accountNAV, "unallocated account capital", "wallet_amounts."+c.Manifest.Currency)
		set("max_records", c.MaxRecords, "100000 replay row default", "data.max_records", prefix+"max_records")
		set("max_pending", c.MaxPending, "64 pending evaluation default", prefix+"decision.max_pending", prefix+"max_pending")
		set("decision_interval_ms", c.DecisionInterval, "1h decision default", prefix+"decision.interval_ms", prefix+"run_timeframes", "run_timeframes")
		set("decision_delay_ms", c.DecisionDelayMS, "zero publication delay", prefix+"decision.delay_ms", prefix+"decision_delay_ms")
		set("latency_ms", c.LatencyMS, "1ms observable-event delay", prefix+"decision.latency_ms", prefix+"latency_ms")
		set("expiry_ms", c.ExpiryMS, "60000ms target expiry", prefix+"decision.expiry_ms", prefix+"expiry_ms")
		set("price_source", c.Prices.Source, "archive declared source or storage kline", prefix+"prices.source")
		set("price_timeframe", c.Prices.TimeFrame, "archive declared timeframe or events storage 1m", prefix+"prices.timeframe")
		set("price_field", c.Prices.Field, "archive declared field or storage close", prefix+"prices.field")
		set("funding_source", c.FundingSource, "explicit required funding source", prefix+"funding_source")
		set("funding_policy", c.Manifest.Costs.FundingPolicy, "explicit simulation policy", "accounts."+c.AccountID+".funding_policy", "execution.funding_policy", prefix+"manifest.costs.funding_policy")
		set("visibility_policy", c.Snapshot.VisibilityPolicy, "archive available-at identity", "data.pit_policy", prefix+"snapshot.visibility_policy")
		set("data_namespace", namespace, "storage namespace or immutable archive identity", "data.namespace")
		set("execution_history", c.Execution.HistoryPath, "file-free memory execution", "accounts."+c.AccountID+".history", "execution.history")
		if input, ok := c.HistoricalInput.(interface{ InputBudgetReport() any }); ok {
			set("input_budget", input.InputBudgetReport(), "compiled subscription input budget", "data.page_bytes", "data.page_rows", "data.prefetch_rows")
		}
		for name, value := range map[string]any{"margin_rate": c.Execution.MarginRate, "max_account_margin": c.Execution.MaxAccountMargin, "max_virtual_gross": c.Execution.MaxVirtualGross, "strategy_gross_limit": c.Execution.StrategyGrossLimit} {
			set(name, value, "validated capital/leverage-derived risk", "accounts."+c.AccountID+"."+name, "execution."+name, prefix+"execution."+name)
		}
		strategies = append(strategies, values)
	}
	if position != len(configs) {
		return fmt.Errorf("resolved factor configuration has undeclared strategies")
	}
	body, err := json.MarshalIndent(struct {
		Version    int                              `json:"version"`
		Strategies []map[string]resolvedFactorValue `json:"strategies"`
	}{1, strategies}, "", "  ")
	if err != nil {
		return err
	}
	if err := config.WriteConfigAtomic(path, nil, append(body, '\n')); err != nil {
		return err
	}
	return nil
}
