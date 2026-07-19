package orm

import (
	"strconv"
	"strings"
	"testing"
)

func TestPgMigrationReconcilesAggRulesAfterVersion4(t *testing.T) {
	version5 := pgMigrationBody(5)
	if !strings.Contains(version5, "add column if not exists agg_rules") {
		t.Fatal("version 5 must reconcile a missing exsymbol.agg_rules column")
	}
}

func TestPgMigrationReconcilesLegacyMetadataSchema(t *testing.T) {
	version6 := pgMigrationBody(6)
	for _, want := range []string{
		"rename column name to market",
		"create unique index if not exists idx_calendars_market_start",
		"create unique index if not exists idx_adj_factors_sid_sub_start",
		"create unique index if not exists ins_kline_sid_tf_pkey",
	} {
		if !strings.Contains(version6, want) {
			t.Fatalf("version 6 migration is missing %q", want)
		}
	}
}

func TestPgMigrationReconcilesLegacyKlineSchema(t *testing.T) {
	version7 := pgMigrationBody(7)
	for _, want := range []string{
		"'kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_1d', 'kline_un'",
		"rename column info to buy_volume",
		"add column if not exists quote",
		"add column if not exists buy_volume",
		"add column if not exists trade_num",
		"add column if not exists expire_ms",
	} {
		if !strings.Contains(version7, want) {
			t.Fatalf("version 7 migration is missing %q", want)
		}
	}
}

func TestPgMigrationReconcilesLegacyKlineDefaults(t *testing.T) {
	version8 := pgMigrationBody(8)
	for _, want := range []string{
		"'kline_1m', 'kline_5m', 'kline_15m', 'kline_1h', 'kline_1d', 'kline_un'",
		"alter column buy_volume set default 0",
	} {
		if !strings.Contains(version8, want) {
			t.Fatalf("version 8 migration is missing %q", want)
		}
	}
	version7Pos := strings.Index(ddlPgMigrations, "-- version 7")
	version8Pos := strings.Index(ddlPgMigrations, "-- version 8")
	if version7Pos < 0 || version8Pos <= version7Pos {
		t.Fatal("version 8 must run after the migration that creates buy_volume")
	}
}

func pgMigrationBody(target int) string {
	for _, migration := range strings.Split(ddlPgMigrations, "-- version") {
		lines := strings.SplitN(strings.TrimSpace(migration), "\n", 2)
		if len(lines) != 2 {
			continue
		}
		version, err := strconv.Atoi(strings.TrimSpace(lines[0]))
		if err == nil && version == target {
			return strings.ToLower(lines[1])
		}
	}
	return ""
}
