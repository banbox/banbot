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
