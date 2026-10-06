package research

import (
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestComboSpecYAMLConfigurationKeys(t *testing.T) {
	spec := ComboSpec{Method: HistoryRankIC, Columns: []string{"momentum"},
		Label: "forward", MinSamples: 8, MinPairs: 10, MinConfidence: .2,
		Decay: .9, Direction: "positive", Fallback: "equal"}
	data, err := yaml.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err = yaml.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"min_samples", "min_pairs", "min_confidence"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing configuration key %q in %s", key, data)
		}
	}
	var restored ComboSpec
	if err = yaml.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored, spec) {
		t.Fatalf("configuration changed on YAML round trip: %+v", restored)
	}
	data, err = yaml.Marshal(ComboSpec{Method: Equal, Columns: []string{"momentum"}})
	if err != nil {
		t.Fatal(err)
	}
	var defaults map[string]any
	if err = yaml.Unmarshal(data, &defaults); err != nil {
		t.Fatal(err)
	}
	if len(defaults) != 2 {
		t.Fatalf("unused quality settings emitted: %s", data)
	}
}
