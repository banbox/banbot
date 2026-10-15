package config

import "testing"

type snapshotTypedMap map[string][]int
type snapshotTypedSlice []map[string]int
type snapshotTypedArray [2]*snapshotTypedNode
type snapshotTypedNode struct {
	Values []int
	Extra  interface{}
	Next   *snapshotTypedNode
}

func TestSnapshotOwnsMutableConfig(t *testing.T) {
	source := &Config{
		Pairs:     []string{"BTC/USDT"},
		TimeRange: &TimeTuple{StartMS: 100, EndMS: 200},
		WalletAmounts: map[string]float64{
			"USDT": 1000,
		},
		PairFilters: []*CommonPairFilter{{Items: map[string]interface{}{
			"nested": map[string]interface{}{"limit": 3},
		}}},
		Exchange: &ExchangeConfig{Items: map[string]map[string]interface{}{
			"binance": {"proxy": "no"},
		}},
	}

	snapshot := NewSnapshot(source)
	view := snapshot.View()
	if view == nil || view.TimeRange == nil || view.TimeRange.StartMS != 100 {
		t.Fatalf("snapshot lost scalar state: %#v", view)
	}

	view.Pairs[0] = "ETH/USDT"
	view.TimeRange.StartMS = 150
	view.WalletAmounts["USDT"] = 500
	view.PairFilters[0].Items["nested"].(map[string]interface{})["limit"] = 5
	view.Exchange.Items["binance"]["proxy"] = "changed"
	if source.Pairs[0] != "BTC/USDT" || source.TimeRange.StartMS != 100 ||
		source.WalletAmounts["USDT"] != 1000 ||
		source.PairFilters[0].Items["nested"].(map[string]interface{})["limit"] != 3 ||
		source.Exchange.Items["binance"]["proxy"] != "no" {
		t.Fatal("snapshot shares mutable configuration state")
	}

	if NewSnapshot(nil).View() != nil {
		t.Fatal("nil config snapshot should have a nil view")
	}
}

func TestSnapshotOwnsConfigDirectories(t *testing.T) {
	oldDataDir, oldStratDir := DataDir, stratDir
	DataDir, stratDir = "/snapshot/data", "/snapshot/strategy"
	t.Cleanup(func() { DataDir, stratDir = oldDataDir, oldStratDir })

	snapshot := NewSnapshot(&Config{})
	if snapshot.DataDir != DataDir || snapshot.StrategyDir != stratDir {
		t.Fatalf("snapshot directories = %q/%q, want %q/%q", snapshot.DataDir, snapshot.StrategyDir, DataDir, stratDir)
	}

	DataDir, stratDir = "/changed/data", "/changed/strategy"
	clone := snapshot.Clone()
	if clone.DataDir != "/snapshot/data" || clone.StrategyDir != "/snapshot/strategy" {
		t.Fatalf("cloned snapshot directories = %q/%q", clone.DataDir, clone.StrategyDir)
	}
}

func TestSnapshotAcceptsExplicitDirectories(t *testing.T) {
	oldDataDir, oldStratDir := DataDir, stratDir
	DataDir, stratDir = "/global/data", "/global/strategy"
	t.Cleanup(func() { DataDir, stratDir = oldDataDir, oldStratDir })

	snapshot := NewSnapshotWithDirs(&Config{}, "/runtime/data", "/runtime/strategy")
	if snapshot.DataDir != "/runtime/data" || snapshot.StrategyDir != "/runtime/strategy" {
		t.Fatalf("explicit snapshot directories = %q/%q", snapshot.DataDir, snapshot.StrategyDir)
	}
	partial := NewSnapshotWithDirs(&Config{}, "/runtime/data", "")
	if partial.DataDir != "/runtime/data" || partial.StrategyDir != "" {
		t.Fatalf("partial snapshot directories = %q/%q", partial.DataDir, partial.StrategyDir)
	}
}

func TestConfigCloneAndSnapshotPreserveDatabase(t *testing.T) {
	source := &Config{Database: &DatabaseConfig{
		Url:       "http://questdb.example:9000/runtime_namespace",
		Retention: "30d", MaxPoolSize: 4, AutoCreate: true, DbType: "questdb",
		SIDRegistryURL: "postgresql://registry.example/banbot",
	}}

	clone := source.Clone()
	if clone.Database == nil || clone.Database.Url != source.Database.Url ||
		clone.Database == source.Database {
		t.Fatalf("config clone lost or shared database config: %#v", clone.Database)
	}

	snapshot := NewSnapshotWithDirs(source, "/runtime/data", "/runtime/strategy")
	view := snapshot.View()
	if view == nil || view.Database == nil || view.Database.Url != source.Database.Url ||
		view.Database.SIDRegistryURL != source.Database.SIDRegistryURL {
		t.Fatalf("snapshot lost database URL: %#v", view)
	}
	view.Database.Url = "http://changed.example:9000/other_namespace"
	view.Database.SIDRegistryURL = "postgresql://changed.example/other"
	if source.Database.Url != "http://questdb.example:9000/runtime_namespace" {
		t.Fatal("snapshot database mutation reached source")
	}
	if source.Database.SIDRegistryURL != "postgresql://registry.example/banbot" {
		t.Fatal("snapshot SID registry mutation reached source")
	}
}

func TestCloneConfigValuePreservesTypedContainers(t *testing.T) {
	sourceMap := snapshotTypedMap{"numbers": {1, 2}}
	sourceSlice := snapshotTypedSlice{{"value": 3}}
	sourceArray := snapshotTypedArray{
		&snapshotTypedNode{Values: []int{4}},
		&snapshotTypedNode{Values: []int{5}},
	}
	sourceNode := &snapshotTypedNode{
		Values: []int{6},
		Extra:  sourceMap,
		Next:   &snapshotTypedNode{Values: []int{7}},
	}
	var nilMap snapshotTypedMap
	var nilSlice snapshotTypedSlice
	var nilNode *snapshotTypedNode

	source := map[string]interface{}{
		"map":       sourceMap,
		"slice":     sourceSlice,
		"array":     sourceArray,
		"node":      sourceNode,
		"nil_map":   nilMap,
		"nil_slice": nilSlice,
		"nil_node":  nilNode,
	}
	clone := cloneConfigValue(source).(map[string]interface{})

	clonedMap, ok := clone["map"].(snapshotTypedMap)
	if !ok {
		t.Fatalf("typed map changed type: %T", clone["map"])
	}
	clonedSlice, ok := clone["slice"].(snapshotTypedSlice)
	if !ok {
		t.Fatalf("typed slice changed type: %T", clone["slice"])
	}
	clonedArray, ok := clone["array"].(snapshotTypedArray)
	if !ok {
		t.Fatalf("typed array changed type: %T", clone["array"])
	}
	clonedNode, ok := clone["node"].(*snapshotTypedNode)
	if !ok {
		t.Fatalf("typed pointer changed type: %T", clone["node"])
	}

	clonedMap["numbers"][0] = 20
	clonedSlice[0]["value"] = 30
	clonedArray[0].Values[0] = 40
	clonedNode.Values[0] = 60
	clonedNode.Next.Values[0] = 70
	clonedNode.Extra.(snapshotTypedMap)["numbers"][1] = 80

	if sourceMap["numbers"][0] != 1 || sourceSlice[0]["value"] != 3 ||
		sourceArray[0].Values[0] != 4 || sourceNode.Values[0] != 6 ||
		sourceNode.Next.Values[0] != 7 || sourceMap["numbers"][1] != 2 {
		t.Fatal("typed configuration containers share mutable state")
	}
	if clonedMap == nil || clonedSlice == nil || clonedNode == sourceNode {
		t.Fatal("typed container clone lost ownership")
	}
	if got, ok := clone["nil_map"].(snapshotTypedMap); !ok || got != nil {
		t.Fatalf("nil typed map clone = %#v (%T)", clone["nil_map"], clone["nil_map"])
	}
	if got, ok := clone["nil_slice"].(snapshotTypedSlice); !ok || got != nil {
		t.Fatalf("nil typed slice clone = %#v (%T)", clone["nil_slice"], clone["nil_slice"])
	}
	if got, ok := clone["nil_node"].(*snapshotTypedNode); !ok || got != nil {
		t.Fatalf("nil typed pointer clone = %#v (%T)", clone["nil_node"], clone["nil_node"])
	}
}
