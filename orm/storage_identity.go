package orm

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CanonicalDatabaseIdentity returns the storage identity used by process-local
// allocators and rewrite locks. It preserves the historical two-argument API;
// the database kind is inferred from the URL when callers do not have config
// metadata available.
func CanonicalDatabaseIdentity(rawURL, dataDir string) string {
	return CanonicalDatabaseIdentityForType(rawURL, dataDir, "")
}

// CanonicalDatabaseIdentityForType canonicalizes one database endpoint. DataDir
// is part of the identity only for a loopback QuestDB instance, whose embedded
// qdbdata directory is the actual storage boundary. TimescaleDB/PostgreSQL
// instances use their endpoint/database identity even when their process also
// has a local recovery directory.
func CanonicalDatabaseIdentityForType(rawURL, dataDir, dbType string) string {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return ""
	}
	poolCfg, err := pgxpool.ParseConfig(normalizeDatabaseURL(rawURL))
	if err != nil || poolCfg == nil || poolCfg.ConnConfig == nil {
		// Keep malformed URLs isolated instead of silently merging them into a
		// default allocator. The actual connection path will report the parse
		// error later.
		return "database-url:" + rawURL
	}
	connCfg := poolCfg.ConnConfig
	host := canonicalDatabaseHost(connCfg.Host)
	identity := fmt.Sprintf("database:%s:%d/%s", host, connCfg.Port, connCfg.Database)
	if host == "<loopback>" && isQuestDBType(dbType, connCfg.Port) && strings.TrimSpace(dataDir) != "" {
		identity += "|qdbdata:" + absoluteStoragePath(filepath.Join(dataDir, "qdbdata"))
	}
	return identity
}

func isQuestDBType(dbType string, port uint16) bool {
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "questdb", "quest":
		return true
	case "timescale", "timescaledb", "postgres", "postgresql", "postgresql+timescale":
		return false
	default:
		return port == 8812
	}
}

// CanonicalStorageIdentity prefers an explicit runtime/storage identity over
// database configuration. The explicit value is already a caller-owned
// namespace and is returned unchanged; an empty value falls back to the
// canonical database identity.
func CanonicalStorageIdentity(explicit, rawURL, dataDir string) string {
	if explicit = strings.TrimSpace(explicit); explicit != "" {
		return explicit
	}
	return CanonicalDatabaseIdentity(rawURL, dataDir)
}

// CanonicalStorageIdentityForType is the config-aware form used by runtime
// composition. Explicit namespaces remain authoritative; otherwise the
// database type controls whether a loopback data directory is meaningful.
func CanonicalStorageIdentityForType(explicit, rawURL, dataDir, dbType string) string {
	if explicit = strings.TrimSpace(explicit); explicit != "" {
		return explicit
	}
	return CanonicalDatabaseIdentityForType(rawURL, dataDir, dbType)
}

// sharedStorageCoordinationRoot returns the process-shared lease namespace for
// one storage identity. Runtime recovery markers stay below each runtime's
// DataDir, but SID allocation must not do so: sibling runtimes that connect to
// the same remote database need the same lock and pending ledger.
func sharedStorageCoordinationRoot(identity string) string {
	identity = strings.TrimSpace(identity)
	if identity == "" {
		return ""
	}
	return CompactProcessLockRootForIdentity(identity)
}

// StorageIdentity returns the namespace owned by this symbol state. Runtime
// composition stores its explicit namespace on the shared SID allocator; an
// unbound legacy state returns an empty identity.
func (s *SymbolState) StorageIdentity() string {
	if s == nil {
		return ""
	}
	s.mu.RLock()
	allocator := s.allocator
	s.mu.RUnlock()
	return allocator.Namespace()
}

func canonicalDatabaseHost(rawHost string) string {
	host := strings.ToLower(strings.Trim(strings.TrimSpace(rawHost), "[]"))
	if isLoopbackDatabaseHost(host) {
		return "<loopback>"
	}
	return host
}

func isLoopbackDatabaseHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func absoluteStoragePath(path string) string {
	path = filepath.Clean(path)
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}
