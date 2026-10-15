package entry

import "testing"

func TestLiveSourcePlanOptionsCarryDeclaredBudget(t *testing.T) {
	options, err := factorSourcePlanOptions(map[string]any{"namespace": "my-data", "page_rows": 17, "prefetch_rows": 100, "page_bytes": int64(8000), "max_records": 9999, "pit_policy": "strict"})
	if err != nil || options.Namespace != "my-data" || options.PageRows != 17 || options.PrefetchRows != 100 || options.PageBytes != 8000 || options.AnchorMS != 0 || options.EndMS != 0 {
		t.Fatalf("live runtime lost declared subscription choices: %+v %v", options, err)
	}
}
