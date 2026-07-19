package orm

import (
	"context"
	"testing"
)

func TestEnsureTimescaleCompressionSkipsQuestDB(t *testing.T) {
	oldQuestDB := IsQuestDB
	IsQuestDB = true
	t.Cleanup(func() { IsQuestDB = oldQuestDB })

	if err := EnsureTimescaleCompression(context.Background()); err != nil {
		t.Fatalf("expected QuestDB compression check to be skipped: %v", err)
	}
}

func TestCompressionPolicies(t *testing.T) {
	want := map[string]int64{
		"kline_1m":  5_184_000_000,
		"kline_5m":  5_184_000_000,
		"kline_15m": 7_776_000_000,
		"kline_1h":  15_552_000_000,
		"kline_1d":  94_608_000_000,
	}
	if len(compressionPolicies) != len(want) {
		t.Fatalf("expected %d compression policies, got %d", len(want), len(compressionPolicies))
	}
	for _, policy := range compressionPolicies {
		if policy.compressAfter != want[policy.table] {
			t.Fatalf("unexpected compression policy for %s: %d", policy.table, policy.compressAfter)
		}
	}
}

func TestIsBanbotNowFunc(t *testing.T) {
	for _, name := range []string{"banbot_now_ms", "public.banbot_now_ms", `"public"."banbot_now_ms"`} {
		if !isBanbotNowFunc(name) {
			t.Fatalf("expected %q to be recognized", name)
		}
	}
	if isBanbotNowFunc("public.other_now_ms") {
		t.Fatal("unexpected integer_now function match")
	}
}
