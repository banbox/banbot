package orm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/banbox/banbot/config"
)

const (
	klineInsertWaitTimeout = 5 * time.Minute
	klineInsertWaitPoll    = 100 * time.Millisecond
	klineInsertMaxRetry    = 10
)

var klineInsertQuestVisibilityGrace = 3 * time.Second

type klineInsertLockOwner struct {
	PID   int    `json:"pid"`
	Token string `json:"token"`
}

type heldKlineInsertLock struct {
	file  *os.File
	token string
}

var (
	heldKlineInsertLocks   = make(map[string]heldKlineInsertLock)
	heldKlineInsertLocksMu sync.Mutex
)

func klineInsertLockRoot() string {
	dataDir := config.GetDataDirSafe()
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, "locks", "kline_insert")
}

func klineInsertLockPath(root string, sid int32, timeframe string) string {
	tfKey := base64.RawURLEncoding.EncodeToString([]byte(timeframe))
	return filepath.Join(root, strconv.FormatInt(int64(sid), 10)+"_"+tfKey+".lock")
}

func acquireKlineInsertFileLock(root string, sid int32, timeframe string, ts time.Time) (bool, error) {
	if root == "" {
		return true, nil
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return false, err
	}
	path := klineInsertLockPath(root, sid, timeframe)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return false, err
	}
	locked, err := tryKlineInsertOSLock(f)
	if err != nil || !locked {
		_ = f.Close()
		return false, err
	}
	token := normalizeQuestTimestamp(ts).Format(time.RFC3339Nano)
	owner := klineInsertLockOwner{PID: os.Getpid(), Token: token}
	data, err := json.Marshal(owner)
	if err == nil {
		err = f.Truncate(0)
	}
	if err == nil {
		_, err = f.Seek(0, 0)
	}
	if err == nil {
		_, err = f.Write(data)
	}
	if err != nil {
		_ = unlockKlineInsertOSLock(f)
		_ = f.Close()
		return false, err
	}
	heldKlineInsertLocksMu.Lock()
	heldKlineInsertLocks[path] = heldKlineInsertLock{file: f, token: token}
	heldKlineInsertLocksMu.Unlock()
	return true, nil
}

func releaseKlineInsertFileLock(root string, sid int32, timeframe string, ts time.Time) error {
	if root == "" {
		return nil
	}
	path := klineInsertLockPath(root, sid, timeframe)
	token := normalizeQuestTimestamp(ts).Format(time.RFC3339Nano)
	heldKlineInsertLocksMu.Lock()
	held, ok := heldKlineInsertLocks[path]
	if ok && held.token == token {
		delete(heldKlineInsertLocks, path)
	}
	heldKlineInsertLocksMu.Unlock()
	if !ok || held.token != token {
		return nil
	}
	unlockErr := unlockKlineInsertOSLock(held.file)
	closeErr := held.file.Close()
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func klineInsertFileLockActive(root string, sid int32, timeframe string) (bool, error) {
	if root == "" {
		return false, nil
	}
	path := klineInsertLockPath(root, sid, timeframe)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return false, err
	}
	locked, err := tryKlineInsertOSLock(f)
	if err != nil {
		_ = f.Close()
		return false, err
	}
	if !locked {
		_ = f.Close()
		return true, nil
	}
	unlockErr := unlockKlineInsertOSLock(f)
	closeErr := f.Close()
	if unlockErr != nil {
		return false, unlockErr
	}
	return false, closeErr
}

func waitForKlineInsertCompletion(ctx context.Context, poll, inactiveGrace time.Duration,
	covered func() (bool, error), active func() (bool, error)) (bool, error) {
	var inactiveAt time.Time
	for {
		complete, err := covered()
		if err != nil || complete {
			return complete, err
		}
		isActive, err := active()
		if err != nil {
			return false, err
		}
		if isActive {
			inactiveAt = time.Time{}
		} else if inactiveGrace <= 0 {
			return false, nil
		} else if inactiveAt.IsZero() {
			inactiveAt = time.Now()
		} else if time.Since(inactiveAt) >= inactiveGrace {
			return false, nil
		}

		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}
