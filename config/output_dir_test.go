package config

import (
	"path/filepath"
	"sync"
	"testing"
)

func TestAllocateOutputDirUsesSuffixForExistingDirectory(t *testing.T) {
	base := filepath.Join(t.TempDir(), "backtest", "abc")
	first, err := AllocateOutputDir(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AllocateOutputDir(base)
	if err != nil {
		t.Fatal(err)
	}
	if first != base || second != base+"_1" {
		t.Fatalf("allocated paths = %q, %q", first, second)
	}
}

func TestAllocateOutputDirIsAtomicAcrossCallers(t *testing.T) {
	base := filepath.Join(t.TempDir(), "backtest", "abc")
	const callers = 8
	paths := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path, err := AllocateOutputDir(base)
			if err != nil {
				errs <- err
				return
			}
			paths <- path
		}()
	}
	wg.Wait()
	close(paths)
	close(errs)
	seen := make(map[string]bool, callers)
	for path := range paths {
		if seen[path] {
			t.Fatalf("duplicate allocated path %q", path)
		}
		seen[path] = true
	}
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != callers {
		t.Fatalf("allocated %d paths, want %d", len(seen), callers)
	}
}
