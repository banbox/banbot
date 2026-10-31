package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// HubRecoveryStatus is deliberately credential-free. A known account is not
// protected merely because its ledger exists: authentication must be restored.
type HubRecoveryStatus struct {
	Account AccountKey
	Env     string
	State   string
}

const (
	HubWaitingAuthentication = "waiting_authentication"
	HubAccountInitializing   = "initializing"
	HubAccountActive         = "active"
	HubAccountStopped        = "stopped"
)

type hubRecoveryRegistry struct {
	path    string
	records map[AccountKey]HubRecoveryStatus
}

// NewHubWithRecovery remembers recovery obligations, not secrets. On restart
// each account is explicitly waiting for a robot (or a credential provider) to
// authenticate with Register. No private connection is opened from YAML.
func NewHubWithRecovery(factory HubAccountFactory, path string) (*Hub, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("execution: recovery registry requires an absolute path")
	}
	parent, err := os.Stat(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	if !parent.IsDir() || parent.Mode().Perm()&0077 != 0 {
		return nil, errors.New("execution: recovery registry requires a private directory")
	}
	h, err := NewHub(factory)
	if err != nil {
		return nil, err
	}
	r := &hubRecoveryRegistry{path: path, records: map[AccountKey]HubRecoveryStatus{}}
	if info, statErr := os.Lstat(path); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			_ = h.Close()
			return nil, errors.New("execution: recovery registry must be a private regular file")
		}
		data, readErr := os.ReadFile(path)
		var records []HubRecoveryStatus
		if readErr != nil {
			_ = h.Close()
			return nil, readErr
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&records) != nil || records == nil || decoder.Decode(new(any)) != io.EOF {
			_ = h.Close()
			return nil, errors.New("execution: invalid recovery registry")
		}
		for _, record := range records {
			if record.Account.Validate() != nil || (record.Env != "prod" && record.Env != "dry_run") || r.records[record.Account].Env != "" {
				_ = h.Close()
				return nil, errors.New("execution: invalid recovery account identity")
			}
			record.State = HubWaitingAuthentication
			r.records[record.Account] = record
		}
	} else if !os.IsNotExist(statErr) {
		_ = h.Close()
		return nil, statErr
	}
	h.recovery = r
	return h, nil
}

// rememberAccountLocked runs before account initialization while h.mu is held.
func (h *Hub) rememberAccountLocked(request HubAccountRegistration) error {
	r := h.recovery
	if r == nil {
		return nil
	}
	if prior, ok := r.records[request.Account]; ok {
		if prior.Env != request.Env {
			return errors.New("execution: recovery account environment cannot change")
		}
		// A preceding rename may have succeeded while its directory fsync
		// failed. Recheck durability before allowing a retry to start trading.
		return syncHubRecoveryDirectory(r.path)
	}
	record := HubRecoveryStatus{Account: request.Account, Env: request.Env, State: HubWaitingAuthentication}
	rows := make([]HubRecoveryStatus, 0, len(r.records)+1)
	for _, row := range r.records {
		rows = append(rows, row)
	}
	rows = append(rows, record)
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(r.path), ".hub-recovery-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	err = errors.Join(writeErr, f.Close())
	if err != nil {
		return err
	}
	if err := os.Rename(f.Name(), r.path); err != nil {
		return err
	}
	// Keep the in-memory obligation even if directory fsync fails after rename.
	r.records[request.Account] = record
	return syncHubRecoveryDirectory(r.path)
}

func syncHubRecoveryDirectory(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func (h *Hub) RecoveryStatuses(ctx context.Context) ([]HubRecoveryStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	rows := map[AccountKey]HubRecoveryStatus{}
	if h.recovery != nil {
		for key, row := range h.recovery.records {
			rows[key] = row
		}
	}
	for key, entry := range h.accounts {
		row := rows[key]
		row.Account = key
		row.Env = entry.env
		row.State = HubAccountInitializing
		select {
		case <-entry.done:
			if entry.err == nil {
				row.State = HubAccountActive
			} else {
				row.State = HubWaitingAuthentication
			}
		default:
		}
		rows[key] = row
	}
	result := make([]HubRecoveryStatus, 0, len(rows))
	for _, row := range rows {
		if h.closed {
			row.State = HubAccountStopped
		}
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		a, _ := json.Marshal(result[i].Account)
		b, _ := json.Marshal(result[j].Account)
		return string(a) < string(b)
	})
	return result, nil
}
