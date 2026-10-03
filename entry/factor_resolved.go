package entry

import (
	"encoding/json"
	"fmt"
	"slices"

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
		importedFields := map[string]string{
			"strategy_id": "StrategyID", "account_id": "AccountID", "definition": "Definition",
			"currency": "Manifest.Currency", "initial_nav": "InitialNAV", "account_initial_nav": "AccountInitialNAV",
			"max_records": "MaxRecords", "max_pending": "MaxPending", "decision_interval_ms": "DecisionInterval",
			"decision_delay_ms": "DecisionDelayMS", "latency_ms": "LatencyMS", "expiry_ms": "ExpiryMS",
			"price_source": "Prices.Source", "price_frequency": "Prices.Frequency", "price_field": "Prices.Field",
			"funding_source": "FundingSource", "funding_policy": "Manifest.Costs.FundingPolicy",
			"visibility_policy": "Snapshot.VisibilityPolicy", "data_namespace": "ComputationContext.DataNamespace",
			"margin_rate": "Execution.MarginRate", "max_account_margin": "Execution.MaxAccountMargin",
			"max_virtual_gross": "Execution.MaxVirtualGross", "strategy_gross_limit": "Execution.StrategyGrossLimit",
		}
		set := func(key string, value any, fallback string, fields ...string) {
			if importedFields[key] != "" {
				field := prefix + "factor.config." + importedFields[key]
				if _, ok := spec.Origin(field); ok && (key != "account_initial_nav" || c.AccountInitialNAV != 0) {
					// JSON overrides common defaults, then explicit policy IDs,
					// data and account execution settings override JSON.
					before := 0
					switch key {
					case "strategy_id", "account_id", "initial_nav", "max_records":
						before = 1
					case "margin_rate", "max_account_margin", "max_virtual_gross", "strategy_gross_limit", "funding_policy":
						before = 2
					}
					fields = slices.Insert(fields, min(before, len(fields)), field)
				}
			}
			values[key] = resolvedFactorValue{value, origin(fallback, fields...)}
		}
		set("strategy_id", c.StrategyID, "registered strategy name", prefix+"id", prefix+"name")
		set("account_id", c.AccountID, "configured default trading account", prefix+"account")
		set("definition", c.Definition, "registered Go builder", prefix+"factor.definition", prefix+"name")
		if c.Expressions != nil {
			set("expressions", c.Expressions, "declarative factor expressions", prefix+"factor.expressions", prefix+"factor.config.expressions")
			plan, combo, err := runner.CompileDefinition(c)
			if err != nil {
				return err
			}
			set("factor_plan_hash", plan.Hash(), "compiled expression semantics")
			set("combine", combo, "resolved expression combination", prefix+"factor.combo", prefix+"factor.expressions.combine")
		}
		set("execution_mode", c.Mode, "ordinary backtest events default", "execution.mode")
		set("currency", c.Manifest.Currency, "USD default", prefix+"factor.manifest.currency", "stake_currency")
		set("initial_nav", c.InitialNAV, "wallet capital times policy capital_weight; otherwise 10000", prefix+"capital_weight", prefix+"factor.initial_nav", "wallet_amounts."+c.Manifest.Currency)
		set("account_initial_nav", accountNAV, "unallocated account capital", "wallet_amounts."+c.Manifest.Currency)
		set("max_records", c.MaxRecords, "100000 replay row default", "data.max_records", prefix+"factor.max_records")
		set("max_pending", c.MaxPending, "64 pending evaluation default", prefix+"factor.decision.max_pending", prefix+"factor.max_pending")
		set("decision_interval_ms", c.DecisionInterval, "1h decision default", prefix+"factor.decision.interval_ms", prefix+"run_timeframes", "run_timeframes")
		set("decision_delay_ms", c.DecisionDelayMS, "zero publication delay", prefix+"factor.decision.delay_ms", prefix+"factor.decision_delay_ms")
		set("latency_ms", c.LatencyMS, "1ms observable-event delay", prefix+"factor.decision.latency_ms", prefix+"factor.latency_ms")
		set("expiry_ms", c.ExpiryMS, "60000ms target expiry", prefix+"factor.decision.expiry_ms", prefix+"factor.expiry_ms")
		set("price_source", c.Prices.Source, "archive declared source or storage kline", prefix+"factor.prices.source")
		set("price_frequency", c.Prices.Frequency, "archive declared frequency or events storage 1m", prefix+"factor.prices.frequency")
		set("price_field", c.Prices.Field, "archive declared field or storage close", prefix+"factor.prices.field")
		set("funding_source", c.FundingSource, "explicit required funding source", prefix+"factor.funding_source")
		set("funding_policy", c.Manifest.Costs.FundingPolicy, "explicit simulation policy", "execution.accounts."+c.AccountID+".funding_policy", "execution.funding_policy", prefix+"factor.manifest.costs.funding_policy")
		set("visibility_policy", c.Snapshot.VisibilityPolicy, "archive available-at identity", "data.pit_policy", prefix+"factor.snapshot.visibility_policy")
		set("data_namespace", namespace, "storage namespace or immutable archive identity", "data.namespace")
		set("execution_history", c.Execution.HistoryPath, "file-free memory execution", "execution.accounts."+c.AccountID+".history", "execution.history", prefix+"factor.config.Execution.HistoryPath")
		if input, ok := c.HistoricalInput.(interface{ InputBudgetReport() any }); ok {
			set("input_budget", input.InputBudgetReport(), "compiled subscription input budget", "data.page_bytes", "data.page_rows", "data.prefetch_rows")
		}
		for name, value := range map[string]any{"margin_rate": c.Execution.MarginRate, "max_account_margin": c.Execution.MaxAccountMargin, "max_virtual_gross": c.Execution.MaxVirtualGross, "strategy_gross_limit": c.Execution.StrategyGrossLimit} {
			set(name, value, "validated capital/leverage-derived risk", "execution.accounts."+c.AccountID+"."+name, "execution."+name, prefix+"factor.execution."+name)
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
