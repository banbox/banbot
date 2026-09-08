package orm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/banbox/banbot/config"
)

func TestCanonicalStorageIdentityPrefersExplicitIdentity(t *testing.T) {
	explicit := "explicit:catalog-a"
	if got := CanonicalStorageIdentity("  "+explicit+"  ", "postgresql://legacy/old", t.TempDir()); got != explicit {
		t.Fatalf("explicit storage identity = %q, want %q", got, explicit)
	}

	wantDatabase := CanonicalDatabaseIdentity("postgresql://user:pass@quest.example:8812/banbot", t.TempDir())
	if got := CanonicalStorageIdentity("", "postgresql://user:pass@quest.example:8812/banbot", t.TempDir()); got == "" || got != wantDatabase {
		t.Fatalf("empty explicit identity = %q, want canonical database identity %q", got, wantDatabase)
	}
}

func TestSymbolStateStorageIdentityUsesAllocatorNamespace(t *testing.T) {
	state := NewSymbolStateWithAllocator(NewSIDAllocatorForNamespace("explicit:catalog-a"))
	if got := state.StorageIdentity(); got != "explicit:catalog-a" {
		t.Fatalf("symbol state storage identity = %q, want %q", got, "explicit:catalog-a")
	}
	var nilState *SymbolState
	if got := nilState.StorageIdentity(); got != "" {
		t.Fatalf("nil symbol state storage identity = %q, want empty", got)
	}
}

func TestCompactProcessLockRootForIdentityIgnoresLegacyConfig(t *testing.T) {
	oldDataDir, oldDatabase := config.DataDir, config.Database
	t.Cleanup(func() {
		config.DataDir = oldDataDir
		config.Database = oldDatabase
	})

	config.DataDir = t.TempDir()
	config.Database = &config.DatabaseConfig{Url: "postgresql://legacy:secret@quest.example:8812/legacy"}
	identity := "explicit:runtime-a"
	digest := sha256.Sum256([]byte(identity))
	want := filepath.Join(os.TempDir(), "banbot", "compact", hex.EncodeToString(digest[:]))

	if got := CompactProcessLockRootForIdentity(identity); got != want {
		t.Fatalf("explicit lock root = %q, want %q", got, want)
	}

	config.DataDir = t.TempDir()
	config.Database.Url = "postgresql://changed:credentials@other.example:5432/other"
	if got := CompactProcessLockRootForIdentity(identity); got != want {
		t.Fatalf("legacy config changed explicit lock root to %q", got)
	}
	if got := CompactProcessLockRootForIdentity("explicit:runtime-b"); got == want {
		t.Fatal("different explicit storage identities reused a lock root")
	}
}

func TestCompactProcessLockRootForIdentityPartitionsFileLeases(t *testing.T) {
	firstRoot := CompactProcessLockRootForIdentity("explicit:runtime-a")
	secondRoot := CompactProcessLockRootForIdentity("explicit:runtime-b")
	if firstRoot == secondRoot {
		t.Fatal("different explicit storage identities reused a file-lock root")
	}

	release, acquired, err := tryAcquireCompactProcessExclusiveLock(firstRoot, "rewrite_identity_test")
	if err != nil || !acquired {
		t.Fatalf("first identity lease: acquired=%v err=%v", acquired, err)
	}
	defer func() { _ = release() }()

	if _, acquired, err := tryAcquireCompactProcessExclusiveLock(
		CompactProcessLockRootForIdentity("explicit:runtime-a"), "rewrite_identity_test"); err != nil || acquired {
		t.Fatalf("same identity lease: acquired=%v err=%v, want blocked", acquired, err)
	}

	otherRelease, acquired, err := tryAcquireCompactProcessExclusiveLock(secondRoot, "rewrite_identity_test")
	if err != nil || !acquired {
		t.Fatalf("different identity lease: acquired=%v err=%v", acquired, err)
	}
	if err := otherRelease(); err != nil {
		t.Fatalf("release different identity lease: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := acquireCompactProcessSharedLock(ctx, firstRoot, "rewrite_identity_test"); err == nil {
		t.Fatal("cancelled lock acquisition unexpectedly succeeded")
	}
}

func TestCompactProcessLockRootForAllocatorUsesExplicitIdentity(t *testing.T) {
	oldRoot := compactProcessLockRootFn
	legacyRoot := filepath.Join(t.TempDir(), "legacy")
	compactProcessLockRootFn = func() string { return legacyRoot }
	t.Cleanup(func() { compactProcessLockRootFn = oldRoot })

	explicit := NewSIDAllocatorForStorage("explicit:runtime-lock", t.TempDir())
	if got := compactProcessLockRootForAllocator(explicit); got != explicit.sharedReservationRoot() {
		t.Fatalf("explicit allocator lock root = %q, want %q", got, explicit.sharedReservationRoot())
	}
	legacy := NewSIDAllocator()
	if got := compactProcessLockRootForAllocator(legacy); got != legacyRoot {
		t.Fatalf("legacy allocator lock root = %q, want %q", got, legacyRoot)
	}
}
