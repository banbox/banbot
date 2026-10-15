package entry

import "github.com/banbox/banbot/data"

// Only typed subscription choices cross into Runtime. PIT/archive settings and
// factor retention stay at their own assembly boundaries.
func factorSourcePlanOptions(fields map[string]any) (data.SubscriptionPlanOptions, error) {
	var options data.SubscriptionPlanOptions
	selected := make(map[string]any)
	for _, key := range []string{"namespace", "page_rows", "prefetch_rows", "page_bytes"} {
		if value, present := fields[key]; present {
			selected[key] = value
		}
	}
	err := decodeFactorFields(selected, &options)
	return options, err
}
