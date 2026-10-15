//go:build !windows

package config

import (
	"golang.org/x/sys/unix"
	"os"
)

func acquireConfigFileLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path+".migration.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}
func releaseConfigFileLock(file *os.File)                   { file.Close() }
func preserveConfigPermissions(source, target string) error { return nil }
func syncConfigDir(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
