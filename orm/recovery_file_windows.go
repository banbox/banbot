//go:build windows

package orm

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

// Windows does not expose Unix directory fsync. Publish a previously flushed
// file with a write-through rename instead. This still depends on the storage
// device honoring flushes; it is not a proof of survival under physical power loss.
func publishRecoveryFile(source, target string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH)
}

func syncDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("recovery directory %s is not a directory", path)
	}
	return nil
}
