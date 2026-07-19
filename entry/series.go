package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/banbox/banbot/biz"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/goods"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banbot/strat"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
	"github.com/spf13/cobra"
)

type seriesFieldDefinition struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Role string `json:"role,omitempty"`
}

type seriesDefinition struct {
	Name      string                  `json:"name"`
	TimeFrame string                  `json:"timeframe"`
	Table     string                  `json:"table"`
	Fields    []seriesFieldDefinition `json:"fields"`
}

func newSeriesListCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "list registered custom series as JSON",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return writeSeriesDefinitions(command.OutOrStdout(), registeredSeriesSources())
		},
	}
}

func writeSeriesDefinitions(out io.Writer, sources []data.DataSource) error {
	items := make([]seriesDefinition, 0, len(sources))
	for _, source := range sources {
		info := source.Info()
		fields := make([]seriesFieldDefinition, 0, len(info.Binding.Fields))
		for _, field := range info.Binding.Fields {
			fields = append(fields, seriesFieldDefinition{Name: field.Name, Type: field.Type, Role: field.Role})
		}
		items = append(items, seriesDefinition{
			Name: info.Name, TimeFrame: info.TimeFrame, Table: info.Binding.Table, Fields: fields,
		})
	}
	return json.NewEncoder(out).Encode(items)
}

func RunSeriesDown(args *config.CmdArgs) *errs.Error {
	sources, err := resolveSeriesSources(args.Tables)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no custom series sources are registered")
	}
	if err := biz.SetupComsExg(args); err != nil {
		return err
	}
	pairs, err := goods.RefreshPairList(btime.TimeMS())
	if err != nil {
		return err
	}
	targets := make([]*orm.ExSymbol, 0, len(pairs))
	for _, pair := range pairs {
		target, targetErr := orm.GetExSymbolCur(pair)
		if targetErr != nil {
			return targetErr
		}
		targets = append(targets, target)
	}
	return ensureSeriesRanges(core.Ctx, orm.DefaultSeriesRepo(), sources, targets,
		config.TimeRange.StartMS, config.TimeRange.EndMS, btime.UTCStamp())
}

func resolveSeriesSources(names []string) ([]data.DataSource, *errs.Error) {
	if len(names) == 0 {
		return registeredSeriesSources(), nil
	}
	sources := make([]data.DataSource, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		source := data.GetDataSource(name)
		if source == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "custom series source %q is not registered", name)
		}
		seen[name] = true
		sources = append(sources, source)
	}
	return sources, nil
}

func registeredSeriesSources() []data.DataSource {
	names := data.ListDataSources()
	sources := make([]data.DataSource, 0, len(names))
	for _, name := range names {
		if source := data.GetDataSource(name); source != nil {
			sources = append(sources, source)
		}
	}
	return sources
}

func ensureSeriesRanges(ctx context.Context, repo orm.SeriesRepo, sources []data.DataSource, targets []*orm.ExSymbol,
	startMS, endMS, nowMS int64) *errs.Error {
	for _, source := range sources {
		info := source.Info()
		closedEnd, err := lastClosedSeriesEnd(info.TimeFrame, endMS, nowMS)
		if err != nil {
			return errs.NewMsg(core.ErrBadConfig, "custom series source=%s: %v", info.Name, err)
		}
		if closedEnd <= startMS {
			continue
		}
		for _, target := range targets {
			sub := &strat.DataSub{Source: info.Name, ExSymbol: target, TimeFrame: info.TimeFrame}
			if err := data.EnsureSeriesRangeWithRepo(ctx, repo, source, sub, startMS, closedEnd); err != nil {
				return errs.NewMsg(err.Code, "download custom series source=%s pair=%s: %s", info.Name, target.Symbol, err.Short())
			}
			if err := orm.WaitForSeriesCoverageVisible(ctx, info, target.ID, startMS, closedEnd); err != nil {
				return errs.NewMsg(err.Code, "verify custom series source=%s pair=%s: %s", info.Name, target.Symbol, err.Short())
			}
		}
	}
	return nil
}

func lastClosedSeriesEnd(timeFrame string, requestedEndMS, nowMS int64) (int64, error) {
	tfSecs, err := utils2.TFToSecSafe(timeFrame)
	if err != nil || tfSecs <= 0 {
		return 0, fmt.Errorf("invalid timeframe %q", timeFrame)
	}
	return utils2.AlignTfMSecs(min(requestedEndMS, nowMS), int64(tfSecs)*1000), nil
}
