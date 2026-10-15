package execution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

// SenderIdentity is physical account ownership, independent of settlement
// partitions. Ledger account IDs continue to include SettlementDomain.
func SenderIdentity(key AccountKey) string {
	body, _ := json.Marshal(struct{ VenueSessionIdentity, Account string }{key.VenueSessionIdentity, key.Account})
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func acquireSenderLeases(key AccountKey, domains []string, dir string) (func() error, error) {
	physical, err := acquireStoreLease(SenderIdentity(key), dir)
	if err != nil {
		return nil, err
	}
	releases := []func() error{physical}
	release := func() error {
		var err error
		for n := len(releases) - 1; n >= 0; n-- {
			err = errors.Join(err, releases[n]())
		}
		return err
	}
	seen := make(map[string]bool)
	for _, domain := range domains {
		if !canonicalID(domain) {
			release()
			return nil, errors.New("execution: sender settlement domain must be canonical")
		}
		if seen[domain] {
			continue
		}
		seen[domain] = true
		key.SettlementDomain = domain
		body, _ := json.Marshal(key)
		hash := sha256.Sum256(body)
		// Also hold the former full-key lease: an old executable does not know
		// the physical key. Unknown old domains still require a stopped upgrade.
		legacy, err := acquireStoreLease(hex.EncodeToString(hash[:]), dir)
		if err != nil {
			release()
			return nil, err
		}
		releases = append(releases, legacy)
	}
	return release, nil
}

// A kernel-held account lease has no TTL or stealing path. Store.Close joins
// gated operations before releasing it; process exit releases its OS handle.
func acquireStoreLease(accountID, dir string) (func() error, error) {
	if !filepath.IsAbs(dir) {
		return nil, errors.New("execution: shared sender lease directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(dir, accountID+".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := lockStoreFile(file); err != nil {
		file.Close()
		return nil, err
	}
	return func() error { return errors.Join(unlockStoreFile(file), file.Close()) }, nil
}
