package entry

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/banbox/banbot/factor"
	"github.com/banbox/banbot/factor/runner"
	"github.com/spf13/cobra"
	"io"
	"os"
)

// FactorSinkFactory lets embedded runtime owners attach reconciled account
// execution without giving archival research an exchange/global dependency.
type FactorSinkFactory func(context.Context, runner.Config, bool) (runner.Sink, func() error, error)

func newFactorArchiveCommand() *cobra.Command {
	var input, path, schemaPath string
	var maxRows int
	archive := &cobra.Command{Use: "archive", Short: "freeze version-record JSON lines into an immutable typed archive", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		schema, err := readArchiveFieldTypes(schemaPath)
		if err != nil {
			return err
		}
		store, err := factor.NewVersionStore(maxRows)
		if err != nil {
			return err
		}
		f, err := os.Open(input)
		if err != nil {
			return err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		dec.DisallowUnknownFields()
		dec.UseNumber()
		for {
			if err = cmd.Context().Err(); err != nil {
				return err
			}
			var row factor.VersionRecord
			err = dec.Decode(&row)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			if err = restoreArchiveRecordTypes(&row, schema); err != nil {
				return err
			}
			if err = store.Put(row); err != nil {
				return err
			}
		}
		digest, err := store.Export(path)
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]string{"archive": path, "digest": digest})
	}}
	archive.Flags().StringVar(&input, "input", "", "version records JSON lines")
	archive.Flags().StringVar(&schemaPath, "schema", "", "YAML source-to-field type map; required for exact large integers")
	archive.Flags().StringVar(&path, "out", "", "new immutable gob archive path")
	archive.Flags().IntVar(&maxRows, "max-records", 100000, "hard archive record limit")
	_ = archive.MarkFlagRequired("input")
	_ = archive.MarkFlagRequired("out")
	return archive
}
