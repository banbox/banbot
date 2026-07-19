package orm

import (
	"strconv"
	"strings"
	"testing"
)

func TestPgMigrationReconcilesAggRulesAfterVersion4(t *testing.T) {
	var version5 string
	for _, migration := range strings.Split(ddlPgMigrations, "-- version") {
		lines := strings.SplitN(strings.TrimSpace(migration), "\n", 2)
		if len(lines) != 2 {
			continue
		}
		version, err := strconv.Atoi(strings.TrimSpace(lines[0]))
		if err == nil && version == 5 {
			version5 = strings.ToLower(lines[1])
			break
		}
	}
	if !strings.Contains(version5, "add column if not exists agg_rules") {
		t.Fatal("version 5 must reconcile a missing exsymbol.agg_rules column")
	}
}
