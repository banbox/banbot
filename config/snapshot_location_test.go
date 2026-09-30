package config

import (
	"testing"
	"time"
)

func TestSnapshotRetainsLocationWhenRebuiltAndCloned(t *testing.T) {
	location := time.FixedZone("display", 8*60*60)
	parent := NewSnapshotWithDirs(&Config{Pairs: []string{"BTC/USDT"}}, "data", "strategies", location)
	child := NewSnapshotWithDirs(parent.View(), parent.DataDir, parent.StrategyDir, parent.Location())
	clone := child.Clone()
	for _, snapshot := range []*Snapshot{parent, child, clone} {
		if snapshot.Location() != location || snapshot.DataDir != "data" || snapshot.StrategyDir != "strategies" {
			t.Fatalf("snapshot lost display metadata: %#v", snapshot)
		}
	}
	clone.View().Pairs[0] = "ETH/USDT"
	if parent.View().Pairs[0] != "BTC/USDT" || child.View().Pairs[0] != "BTC/USDT" {
		t.Fatal("location propagation shared mutable configuration")
	}
}
