package entry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/banbox/banbot/factor/expr"
	"github.com/banbox/banbot/factor/runner"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// These commands compile a standalone expression spec without opening market
// data, execution accounts or a database. Research uses the normal factor command.
func addExpressionCommands(root *cobra.Command) {
	for _, name := range []string{"validate", "explain"} {
		var path string
		command := &cobra.Command{Use: name, Short: "compile a standalone YAML expression spec", Args: cobra.NoArgs}
		command.RunE = func(cmd *cobra.Command, _ []string) error {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()
			raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
			if err != nil {
				return err
			}
			if len(raw) > 1<<20 {
				return fmt.Errorf("expression spec exceeds 1 MiB")
			}
			decoder := yaml.NewDecoder(bytes.NewReader(raw))
			decoder.KnownFields(true)
			var spec expr.Spec
			if err := decoder.Decode(&spec); err != nil {
				return err
			}
			var trailing any
			if err := decoder.Decode(&trailing); err != io.EOF {
				return fmt.Errorf("expression spec must contain one YAML document")
			}
			plan, combo, err := runner.CompileDefinition(runner.Config{Expressions: &spec})
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{
				"hash": plan.Hash(), "timeframe": plan.TimeFrame(), "outputs": plan.Outputs(),
				"nodes": plan.NodeCount(), "warmup": plan.WarmupLength(), "retention": plan.StateRetention(),
				"inputs": plan.Inputs(), "combine": combo,
			})
		}
		command.Flags().StringVar(&path, "spec", "", "standalone YAML expressions mapping")
		_ = command.MarkFlagRequired("spec")
		root.AddCommand(command)
	}
}
