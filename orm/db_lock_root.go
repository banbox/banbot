package orm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/banbox/banbot/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var dbIdentLockRoots sync.Map // "<kind>|<database url>" -> resolved shared root

// dbIdentLockRoot resolves the root directory for a cross-process file lock that
// guards a DATABASE-scoped resource (table compaction swaps, kline inserts).
//
// The legacy roots live under the data dir (<dataDir>/locks/<kind>), which only
// serializes processes that resolve the same BanDataDir — two processes with
// different data dirs sharing ONE database take their leases in DIFFERENT files:
// no contention, no protection. The shared resource is the database, so the key
// must be the database identity: normalized host:port/dbname, hashed, rooted in a
// per-machine location outside any data dir.
//
// Whenever the database identity cannot be resolved (config not loaded yet,
// unparsable URL), fall back to the legacy data-dir root — never to "no lock".
func dbIdentLockRoot(kind string, fallback func() string) string {
	db := config.Database
	if db == nil || db.Url == "" {
		return fallback()
	}
	key := kind + "|" + db.Url
	if v, ok := dbIdentLockRoots.Load(key); ok {
		return v.(string)
	}
	root := sharedLockRootForDB(db.Url, kind)
	if root == "" {
		return fallback()
	}
	dbIdentLockRoots.Store(key, root)
	return root
}

// sharedLockRootForDB maps a database URL to a per-machine lock root shared by
// every process pointing at the same server, regardless of which data dir each
// runs with. Parsing goes through the same pgxpool config the real pool uses, so
// the identity equals the actual connection target. The path carries only a hash —
// the database URL holds credentials and must never leak into file system paths.
// The root lives under the per-user cache dir; no world-writable location is ever
// used — when os.UserCacheDir is unavailable (e.g. HOME unset) this returns ""
// and the caller falls back to the legacy data-dir root.
func sharedLockRootForDB(url, kind string) string {
	cfg, err := pgxpool.ParseConfig(normalizeDatabaseURL(url))
	if err != nil || cfg.ConnConfig == nil {
		return ""
	}
	cc := cfg.ConnConfig
	ident := fmt.Sprintf("%s:%d/%s", strings.ToLower(cc.Host), cc.Port, cc.Database)
	sum := sha256.Sum256([]byte(ident))
	tag := hex.EncodeToString(sum[:])[:12]
	base, err := os.UserCacheDir()
	if err != nil || base == "" {
		return ""
	}
	return filepath.Join(base, "banbot", "locks", kind+"-"+tag)
}
