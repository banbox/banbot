package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// AllocateOutputDir reserves a directory for a run. The first run uses base;
// subsequent runs use base_1, base_2, and so on. Mkdir is deliberately used
// as the reservation operation so two processes cannot select the same path
// between an existence check and directory creation.
func AllocateOutputDir(base string) (string, error) {
	if base == "" {
		return "", fmt.Errorf("output directory is required")
	}
	base = filepath.Clean(base)
	if err := os.MkdirAll(filepath.Dir(base), 0755); err != nil {
		return "", err
	}
	for index := 0; ; index++ {
		candidate := base
		if index > 0 {
			candidate = fmt.Sprintf("%s_%d", base, index)
		}
		err := os.Mkdir(candidate, 0755)
		if err == nil {
			return candidate, nil
		}
		if !os.IsExist(err) {
			return "", err
		}
	}
}
