package entry

import (
	"fmt"
	"os"

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
			dataDir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("get runtime data plan working directory: %w", err)
			}
			output, inspectErr := runtimeplan.Inspect(request, dataDir)
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
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return fmt.Errorf("open runtime data plan output: %w", err)
	}
	_, err = output.Write(data)
	if closeErr := output.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write runtime data plan output: %w", err)
	}
	return nil
}
