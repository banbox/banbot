package entry

import (
	"errors"
	"testing"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
)

func TestExecuteBackTestPropagatesRunFailure(t *testing.T) {
	want := errs.New(core.ErrRunTime, errors.New("prediction loop failed"))

	outDir, got := executeBackTest("report", func() *errs.Error { return want })

	if got != want {
		t.Fatalf("executeBackTest() error = %v, want %v", got, want)
	}
	if outDir != "" {
		t.Fatalf("executeBackTest() output = %q on failure, want empty", outDir)
	}
}

func TestExecuteBackTestReturnsReportPath(t *testing.T) {
	outDir, err := executeBackTest("report", func() *errs.Error { return nil })

	if err != nil {
		t.Fatalf("executeBackTest() error = %v, want nil", err)
	}
	if outDir != "report" {
		t.Fatalf("executeBackTest() output = %q, want report", outDir)
	}
}
