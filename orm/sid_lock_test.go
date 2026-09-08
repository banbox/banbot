package orm

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
)

const (
	sidLockHelperRoot = "BANBOT_TEST_SID_LOCK_ROOT"
	sidLockHelperWant = "BANBOT_TEST_SID_LOCK_WANT"
)

func TestSIDAllocationProcessLeaseHelper(t *testing.T) {
	root := os.Getenv(sidLockHelperRoot)
	if root == "" {
		t.Skip("subprocess helper")
	}
	want, err := strconv.ParseBool(os.Getenv(sidLockHelperWant))
	if err != nil {
		t.Fatal(err)
	}
	release, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "exsymbol_sid")
	if err != nil {
		t.Fatal(err)
	}
	if acquired != want {
		if acquired {
			_ = release()
		}
		t.Fatalf("SID lease acquired=%v, want=%v", acquired, want)
	}
	if acquired {
		if err := release(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSIDAllocationProcessLeaseAcrossProcesses(t *testing.T) {
	root := t.TempDir()
	release, acquired, err := tryAcquireCompactProcessExclusiveLock(root, "exsymbol_sid")
	if err != nil || !acquired {
		t.Fatalf("parent SID lease failed: acquired=%v err=%v", acquired, err)
	}

	runHelper := func(want bool) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestSIDAllocationProcessLeaseHelper$")
		cmd.Env = append(os.Environ(),
			sidLockHelperRoot+"="+root,
			sidLockHelperWant+"="+strconv.FormatBool(want),
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("SID lease helper failed: %v\n%s", err, out)
		}
	}

	runHelper(false)
	if err := release(); err != nil {
		t.Fatal(err)
	}
	runHelper(true)
}
