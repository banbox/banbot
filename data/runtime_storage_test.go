package data

import "testing"

func TestExplicitRuntimeRequiresStorage(t *testing.T) {
	deps := &RuntimeDeps{}
	queries, conn, err := deps.conn()
	if err == nil || queries != nil || conn != nil {
		t.Fatal("explicit data dependencies must reject missing storage")
	}
}
