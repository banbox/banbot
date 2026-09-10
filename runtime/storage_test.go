package runtime

import (
	"testing"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/orm"
)

func TestRuntimeRequiresStorageForConfiguredDatabase(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	_, err := process.NewRuntime(Options{Config: &config.Config{
		Database: &config.DatabaseConfig{Url: "postgresql://db.example/banbot"},
	}})
	if err == nil {
		t.Fatal("runtime accepted database configuration without explicit storage")
	}
}

func TestRuntimeRejectsStorageIdentityAndBackendMismatch(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	cfg := &config.Config{Database: &config.DatabaseConfig{
		Url: "postgresql://db.example/banbot", DbType: "timescale",
	}}
	for name, storage := range map[string]*orm.Storage{
		"identity": orm.NewStorage(nil, false, "database:other.example:5432/banbot"),
		"backend":  orm.NewStorage(nil, true, "database:db.example:5432/banbot"),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := process.NewRuntime(Options{Config: cfg, Storage: storage}); err == nil {
				t.Fatalf("runtime accepted %s mismatch", name)
			}
		})
	}
}

func TestRuntimeBindsActualStorageIdentity(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	storage := orm.NewStorage(nil, true, "database:first")
	first, err := process.NewRuntime(Options{Storage: storage})
	if err != nil {
		t.Fatal(err)
	}
	second, err := process.NewRuntime(Options{Storage: storage})
	if err != nil {
		t.Fatal(err)
	}
	if first.Storage != storage || first.Symbols.Storage() != storage || second.Symbols.Storage() != storage {
		t.Fatal("runtime and symbol catalog must bind the supplied storage")
	}
	if _, err := process.NewRuntime(Options{Storage: storage, StorageNamespace: "unrelated"}); err == nil {
		t.Fatal("conflicting allocator namespace accepted")
	}
}

func TestRuntimeBindsOrderStoragePathAtConstruction(t *testing.T) {
	process := NewProcess()
	defer process.Close()
	rt, err := process.NewRuntime(Options{
		Config:  &config.Config{Name: "runtime-orders"},
		DataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()

	_, conn, connErr := rt.Orders.Conn(true)
	if connErr != nil {
		t.Fatalf("runtime order state was not bound to storage path: %v", connErr)
	}
	conn.Close()
}
