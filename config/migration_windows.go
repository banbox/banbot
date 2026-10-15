//go:build windows

package config

import (
	"golang.org/x/sys/windows"
	"os"
)

func acquireConfigFileLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path+".migration.lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	err = windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &windows.Overlapped{})
	if err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}
func releaseConfigFileLock(file *os.File) { file.Close() }
func syncConfigDir(path string) error     { return nil }

func preserveConfigPermissions(source, target string) error {
	descriptor, err := windows.GetNamedSecurityInfo(source, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	dacl, _, err := descriptor.DACL()
	if err != nil {
		return err
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return err
	}
	flags := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION)
	if control&windows.SE_DACL_PROTECTED != 0 {
		flags |= windows.PROTECTED_DACL_SECURITY_INFORMATION
	}
	return windows.SetNamedSecurityInfo(target, windows.SE_FILE_OBJECT, flags, nil, nil, dacl, nil)
}
