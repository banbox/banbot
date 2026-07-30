package entry

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/banbox/banbot/runtimeplan"
	"github.com/spf13/cobra"
)

func newInternalCommand() *cobra.Command {
	command := &cobra.Command{
		Use:    "internal",
		Hidden: true,
		Args:   cobra.NoArgs,
	}
	command.AddCommand(newInspectDataPlanCommand())
	return command
}

func newInspectDataPlanCommand() *cobra.Command {
	var requestPath, outputPath string
	command := &cobra.Command{
		Use:   "inspect-data-plan",
		Short: "inspect a compiled strategy's canonical runtime data plan",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if requestPath == "" || outputPath == "" {
				return fmt.Errorf("--request and --output are required")
			}
			requestData, err := os.ReadFile(requestPath)
			if err != nil {
				return fmt.Errorf("read runtime data plan request: %w", err)
			}
			request, err := runtimeplan.DecodeRequest(requestData)
			if err != nil {
				return err
			}
			output, inspectErr := runtimeplan.Inspect(request)
			if output == nil {
				return inspectErr
			}
			outputData, err := runtimeplan.MarshalOutput(output)
			if err != nil {
				return err
			}
			if err = writeRuntimePlanOutput(outputPath, outputData); err != nil {
				return err
			}
			return inspectErr
		},
	}
	command.Flags().StringVar(&requestPath, "request", "", "canonical request JSON path")
	command.Flags().StringVar(&outputPath, "output", "", "canonical output JSON path")
	return command
}

func writeRuntimePlanOutput(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".runtime-plan-*")
	if err != nil {
		return fmt.Errorf("create runtime data plan output: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(data)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write runtime data plan output: %w", err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish runtime data plan output: %w", err)
	}
	return nil
}
