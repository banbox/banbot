package config

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// Advanced fields are configuration choices rather than runner structs.
// Engine adapters additionally validate source capabilities and strategy DAGs.
func validateAdvanced(path string, fields map[string]any) error {
	var allowed string
	switch {
	case path == "data":
		allowed = "namespace page_rows prefetch_rows page_bytes archive max_records pit_policy"
	case path == "execution":
		allowed = "mode store history sender_lease_dir live_provider funding_policy accounts instruments margin_rate max_account_margin max_virtual_gross strategy_gross_limit"
	case strings.HasPrefix(path, "execution.accounts."), strings.HasPrefix(path, "accounts.") && strings.Count(path, ".") == 1:
		allowed = "mode store history sender_lease_dir live_provider funding_policy instruments margin_rate max_account_margin max_virtual_gross strategy_gross_limit"
	case strings.HasSuffix(path, ".factor"), strings.HasPrefix(path, "run_policy[") && strings.HasSuffix(path, "]") && !strings.Contains(path, "."):
		allowed = "archive chunks snapshot combo portfolio decision research manifest prices funding_source initial_nav max_records definition expressions"
	case strings.HasSuffix(path, ".expressions"):
		allowed = "schema_version timeframe bindings params lets outputs combine"
	case strings.Contains(path, ".expressions.bindings."):
		allowed = "source timeframe sampling max_age_ms"
	case strings.HasSuffix(path, ".decision"):
		allowed = "interval_ms delay_ms latency_ms expiry_ms max_pending"
	case strings.HasSuffix(path, ".prices"):
		allowed = "source timeframe field"
	case strings.HasSuffix(path, ".combo"), strings.HasSuffix(path, ".expressions.combine"):
		allowed = "method columns weights min_samples min_pairs min_confidence decay direction fallback label"
	case strings.HasSuffix(path, ".portfolio"):
		allowed = "builder k long_notional short_notional mode policy policy_params rebalance selection holding transition allocation"
	case strings.HasSuffix(path, ".rebalance"):
		allowed = "every_bars anchor phase duration calendar calendar_version timezone"
	case strings.HasSuffix(path, ".selection"):
		allowed = "long_k short_k long_quantile short_quantile retain_rank dropout group_quota missing_scores"
	case strings.HasSuffix(path, ".holding"):
		allowed = "min_bars max_bars min_duration max_duration by_asset adopt cooldown_bars"
	case strings.Contains(path, ".holding.by_asset."):
		allowed = "min_bars max_bars min_duration max_duration"
	case strings.HasSuffix(path, ".transition"):
		allowed = "mode exit_steps basis period_bars startup sizing on_reselect ratio final_threshold alpha entry_window_bars by_asset"
	case strings.Contains(path, ".transition.by_asset."):
		allowed = "exit_steps ratio"
	case strings.HasSuffix(path, ".allocation"):
		allowed = "method reserve_ratio fixed_notional vol_target asset_cap group_caps turnover_limit net_cap beta_cap"
	case strings.HasSuffix(path, ".research"):
		allowed = "labels label_wait_ms"
	case strings.HasSuffix(path, ".snapshot"):
		allowed = "grid_time decision_time replay_time universe sid_map schemas source_versions adjustment_version visibility_policy"
	case strings.HasSuffix(path, ".universe"):
		allowed = "version investable reference tradable evaluation tracked static"
	case strings.HasSuffix(path, ".manifest"):
		allowed = "currency code_revision factor_plan_hash universe_version visibility_policy execution_mode latency_assumption static_universe combo portfolio labels parameters costs snapshots"
	case strings.HasSuffix(path, ".costs"):
		allowed = "fee_rate slippage_rate funding_policy"
	case strings.Contains(path, ".chunks["):
		allowed = "path from to"
	case strings.Contains(path, ".labels["):
		allowed = "name kind horizon overlapping periods_per_year"
	default:
		return nil // Explicitly open source/SID/parameter maps are adapter validated.
	}
	for key, value := range fields {
		field := path + "." + key
		if !strings.Contains(" "+allowed+" ", " "+key+" ") {
			return fmt.Errorf("%s: unknown advanced field", field)
		}
		if value == nil {
			return fmt.Errorf("%s cannot be null", field)
		}
		switch key {
		case "archive", "store", "history", "sender_lease_dir", "path", "namespace", "live_provider", "funding_policy", "pit_policy", "source", "timeframe", "field", "version", "adjustment_version", "visibility_policy", "currency", "code_revision", "factor_plan_hash", "universe_version", "execution_mode", "latency_assumption", "name", "kind", "builder", "definition", "sampling":
			if text, ok := value.(string); !ok || strings.TrimSpace(text) == "" {
				return fmt.Errorf("%s must be a nonempty string", field)
			}
		case "mode":
			text, ok := value.(string)
			valid := "research weights events paper live trade backtest"
			if strings.HasSuffix(path, ".portfolio") {
				valid = "full patch"
			}
			if strings.HasSuffix(path, ".transition") {
				valid = "direct linear-exit cohort geometric target-step"
			}
			if !ok || !strings.Contains(" "+valid+" ", " "+text+" ") {
				return fmt.Errorf("%s has unsupported mode %v", field, value)
			}
		case "method":
			if text, ok := value.(string); (!strings.HasSuffix(path, ".allocation")) && (!ok || !strings.Contains(" equal fixed history-ic history-rank-ic history-icir history-rank-icir history-ewma ", " "+text+" ")) {
				return fmt.Errorf("%s has unsupported combo method %v", field, value)
			}
		case "page_bytes":
			if !nonnegativeInteger(value) {
				return fmt.Errorf("%s must be a nonnegative integer", field)
			}
			item := reflect.ValueOf(value)
			if item.Kind() >= reflect.Uint && item.Kind() <= reflect.Uint64 && item.Uint() > math.MaxInt64 {
				return fmt.Errorf("%s exceeds int64", field)
			}
		case "every_bars", "exit_steps", "period_bars", "entry_window_bars":
			if !nonnegativeInteger(value) || numeric(value) <= 0 {
				return fmt.Errorf("%s must be a positive integer", field)
			}
		case "long_k", "short_k", "retain_rank", "dropout", "min_bars", "max_bars", "cooldown_bars", "phase", "anchor", "min_samples", "min_pairs":
			if !nonnegativeInteger(value) {
				return fmt.Errorf("%s must be a nonnegative integer", field)
			}
		case "duration", "min_duration", "max_duration":
			if _, err := ParseDurationOverride(value); err != nil {
				return fmt.Errorf("%s: %w", field, err)
			}
		case "page_rows", "prefetch_rows", "max_records", "max_pending", "k", "interval_ms", "expiry_ms", "horizon", "schema_version", "max_age_ms":
			if !nonnegativeInteger(value) || numeric(value) <= 0 {
				return fmt.Errorf("%s must be a positive integer", field)
			}
		case "from", "to", "delay_ms", "latency_ms", "label_wait_ms", "grid_time", "decision_time", "replay_time":
			if !nonnegativeInteger(value) {
				return fmt.Errorf("%s must be a nonnegative integer", field)
			}
		case "initial_nav", "long_notional", "short_notional", "fee_rate", "slippage_rate", "periods_per_year":
			number := numeric(value)
			if math.IsNaN(number) || math.IsInf(number, 0) || number < 0 {
				return fmt.Errorf("%s must be a finite nonnegative number", field)
			}
		case "margin_rate", "max_account_margin", "max_virtual_gross", "strategy_gross_limit":
			switch value.(type) {
			case string, int, int64, uint64, float64:
			default:
				return fmt.Errorf("%s must be a decimal", field)
			}
			number, err := decimal.NewFromString(fmt.Sprint(value))
			if err != nil || number.IsNegative() {
				return fmt.Errorf("%s must be a nonnegative decimal", field)
			}
		case "static", "static_universe", "overlapping":
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s must be a boolean", field)
			}
		case "columns":
			if reflect.TypeOf(value).Kind() != reflect.Slice {
				return fmt.Errorf("%s must be a list", field)
			}
			list := reflect.ValueOf(value)
			for i := 0; i < list.Len(); i++ {
				if text, ok := list.Index(i).Interface().(string); !ok || text == "" {
					return fmt.Errorf("%s[%d] must be a nonempty string", field, i)
				}
			}
		case "chunks", "labels", "snapshots", "investable", "reference", "tradable", "evaluation", "tracked":
			if reflect.TypeOf(value).Kind() != reflect.Slice {
				return fmt.Errorf("%s must be a list", field)
			}
		case "snapshot", "combo", "portfolio", "decision", "research", "manifest", "prices", "universe", "costs", "accounts", "weights", "parameters", "schemas", "source_versions", "sid_map", "instruments", "expressions", "bindings", "params", "lets", "outputs", "combine", "policy_params", "rebalance", "selection", "holding", "transition", "allocation", "by_asset":
			if reflect.TypeOf(value).Kind() != reflect.Map {
				return fmt.Errorf("%s must be a mapping", field)
			}
		}
		if nested, ok := value.(map[string]any); ok {
			if key == "accounts" || key == "by_asset" {
				for account, item := range nested {
					overrides, ok := item.(map[string]any)
					if !ok {
						return fmt.Errorf("%s.%s must be a mapping", field, account)
					}
					if err := validateAdvanced(field+"."+account, overrides); err != nil {
						return err
					}
				}
			} else if err := validateAdvanced(field, nested); err != nil {
				return err
			}
		}
		if list, ok := value.([]any); ok && (key == "chunks" || key == "labels") {
			for i, item := range list {
				nested, ok := item.(map[string]any)
				if !ok {
					return fmt.Errorf("%s[%d] must be a mapping", field, i)
				}
				if err := validateAdvanced(fmt.Sprintf("%s[%d]", field, i), nested); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func numeric(value any) float64 {
	item := reflect.ValueOf(value)
	if !item.IsValid() {
		return math.NaN()
	}
	switch item.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(item.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(item.Uint())
	case reflect.Float32, reflect.Float64:
		return item.Float()
	}
	return math.NaN()
}
func nonnegativeInteger(value any) bool {
	if value == nil {
		return false
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return numeric(value) >= 0
	}
	return false
}

// ParseDurationOverride accepts explicit duration strings for adapters that
// use Go durations; integer *_ms configuration fields remain strict integers.
func ParseDurationOverride(value any) (time.Duration, error) {
	text, ok := value.(string)
	if !ok {
		return 0, fmt.Errorf("duration must be a string")
	}
	duration, err := time.ParseDuration(text)
	if err != nil || duration < 0 {
		return 0, fmt.Errorf("invalid nonnegative duration %q", text)
	}
	return duration, nil
}
