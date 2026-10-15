//go:build !windows

package orm

import (
	"errors"
	"fmt"
	"os"
)

func publishRecoveryFile(source, target string) error { return os.Rename(source, target) }

func syncDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("recovery directory %s is not a directory", path)
	}
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for fsync: %w", path, err)
	}
	return errors.Join(dir.Sync(), dir.Close())
}
