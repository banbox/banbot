package orm

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/banbox/banbot/config"
)

func TestCanonicalDatabaseIdentityNormalizesEquivalentLoopbackURLs(t *testing.T) {
	dataDir := t.TempDir()
	want := "database:<loopback>:8812/qdb|qdbdata:" +
		absoluteStoragePath(filepath.Join(dataDir, "qdbdata"))

	for _, rawURL := range []string{
		" postgresql://user:pass@LOCALHOST:08812/qdb ",
		"postgres://another:credential@127.0.0.1:8812/qdb",
		"postgresql://user:pass@[::1]:8812/qdb",
		"postgresql://user:pass@[::FFFF:127.0.0.1]:8812/qdb",
	} {
		if got := CanonicalDatabaseIdentity(rawURL, dataDir); got != want {
			t.Fatalf("CanonicalDatabaseIdentity(%q) = %q, want %q", rawURL, got, want)
		}
	}
}

func TestCanonicalDatabaseIdentityNormalizesDefaultPortAndIgnoresCredentials(t *testing.T) {
	dataDir := t.TempDir()
	withoutPort := CanonicalDatabaseIdentity("postgresql://first:secret@QUEST.EXAMPLE/qdb", dataDir)
	withDefaultPort := CanonicalDatabaseIdentity("postgres://second:other@quest.example:5432/qdb", t.TempDir())
	if withoutPort != withDefaultPort {
		t.Fatalf("default port or credentials changed identity: %q != %q", withoutPort, withDefaultPort)
	}

	if got := CanonicalDatabaseIdentity("postgresql://user:pass@quest.example:5433/qdb", dataDir); got == withoutPort {
		t.Fatalf("different database port reused identity %q", got)
	}
	if got := CanonicalDatabaseIdentity("postgresql://user:pass@quest.example:5432/other", dataDir); got == withoutPort {
		t.Fatalf("different database reused identity %q", got)
	}
}

func TestCanonicalDatabaseIdentitySeparatesLoopbackQDBDataDirectories(t *testing.T) {
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	url := "postgresql://user:pass@localhost:8812/qdb"

	first := CanonicalDatabaseIdentity(url, firstDir)
	second := CanonicalDatabaseIdentity(url, secondDir)
	if first == second {
		t.Fatalf("different loopback qdbdata directories reused identity %q", first)
	}

	remoteFirst := CanonicalDatabaseIdentity("postgresql://user:pass@quest.example:8812/qdb", firstDir)
	remoteSecond := CanonicalDatabaseIdentity("postgresql://other:credentials@QUEST.EXAMPLE:8812/qdb", secondDir)
	if remoteFirst != remoteSecond {
		t.Fatalf("remote database identity was split by recovery data directory: %q != %q", remoteFirst, remoteSecond)
	}
}

func TestCanonicalDatabaseIdentityDoesNotSplitLoopbackTimescaleByDataDir(t *testing.T) {
	first := CanonicalDatabaseIdentityForType(
		"postgresql://user:pass@localhost:5432/banbot", t.TempDir(), "timescale")
	second := CanonicalDatabaseIdentityForType(
		"postgres://other:credentials@127.0.0.1/banbot", t.TempDir(), "timescaledb")
	if first != second {
		t.Fatalf("loopback Timescale identity was split by recovery directory: %q != %q", first, second)
	}
	if strings.Contains(first, "qdbdata:") {
		t.Fatalf("Timescale identity contains QuestDB storage path: %q", first)
	}
}

func TestCanonicalDatabaseIdentityExplicitTypeOverridesPortInference(t *testing.T) {
	dataDir := t.TempDir()
	timescale := CanonicalDatabaseIdentityForType(
		"postgresql://user:pass@localhost:8812/qdb", dataDir, "timescale")
	quest := CanonicalDatabaseIdentityForType(
		"postgresql://user:pass@localhost:5432/qdb", dataDir, "questdb")
	if strings.Contains(timescale, "qdbdata:") {
		t.Fatalf("explicit Timescale type used QuestDB directory: %q", timescale)
	}
	if !strings.Contains(quest, "qdbdata:") {
		t.Fatalf("explicit QuestDB type omitted loopback storage directory: %q", quest)
	}
}

func TestCompactProcessLockRootUsesCanonicalDatabaseIdentityDigest(t *testing.T) {
	oldDataDir, oldDatabase := config.DataDir, config.Database
	t.Cleanup(func() {
		config.DataDir = oldDataDir
		config.Database = oldDatabase
	})

	dataDir := t.TempDir()
	url := "postgresql://user:pass@[::1]:8812/qdb"
	config.DataDir = dataDir
	config.Database = &config.DatabaseConfig{Url: url}

	identity := CanonicalDatabaseIdentity(url, dataDir)
	digest := sha256.Sum256([]byte(identity))
	want := filepath.Join(os.TempDir(), "banbot", "compact", hex.EncodeToString(digest[:]))
	if got := compactProcessLockRoot(); got != want {
		t.Fatalf("compact process lock root = %q, want identity digest root %q", got, want)
	}

	for _, equivalentURL := range []string{
		"postgres://other:credentials@127.0.0.1:8812/qdb",
		"postgresql://USER:PASSWORD@LOCALHOST:8812/qdb",
	} {
		config.Database.Url = equivalentURL
		if got := compactProcessLockRoot(); got != want {
			t.Fatalf("equivalent database URL %q changed compact process lock root to %q", equivalentURL, got)
		}
	}

	config.DataDir = t.TempDir()
	if got := compactProcessLockRoot(); got == want {
		t.Fatal("different loopback qdbdata directory reused compact process lock root")
	}
}
