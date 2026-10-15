package runner

import (
	"encoding/json"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/backtest"
	"os"
	"path/filepath"
)

// TargetAcceptedOutput reports successful admission, without claiming a fill.
// Executed remains the legacy callback alias for consumers implementing Output.
type TargetAcceptedOutput interface {
	TargetAccepted(*factor.TargetPortfolio, backtest.State, int64) error
}

func emitTargetAccepted(out Output, p *factor.TargetPortfolio, state backtest.State, at int64) error {
	if accepted, ok := out.(TargetAcceptedOutput); ok {
		return accepted.TargetAccepted(p, state, at)
	}
	return out.Executed(p, state, at)
}

type RunArtifact struct {
	Version int      `json:"version"`
	Status  string   `json:"status"`
	Errors  []string `json:"errors,omitempty"`
	Result  Result   `json:"result"`
}
type RunArtifactOutput interface{ RunFinished(RunArtifact) error }

func makeRunArtifact(result Result, err error) RunArtifact {
	artifact := RunArtifact{Version: 1, Status: "complete", Result: result}
	if err != nil {
		artifact.Status = "incomplete"
		artifact.Errors = []string{err.Error()}
	} else if result.Unresolved > 0 {
		artifact.Status = "incomplete"
	}
	return artifact
}
func (o *JSONOutput) RunFinished(artifact RunArtifact) error {
	return json.NewEncoder(o.Writer).Encode(struct {
		Kind string
		RunArtifact
	}{"run-completed", artifact})
}

// WriteRunArtifact publishes only a fully written/synced versioned JSON file.
func WriteRunArtifact(path string, artifact RunArtifact) (err error) {
	if err = os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".run-*.json")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer func() { _ = file.Close(); _ = os.Remove(tmp) }()
	if err = json.NewEncoder(file).Encode(artifact); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
