package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

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
	return runExplicitSeriesDown(args)
}

func runExplicitSeriesDown(args *config.CmdArgs) *errs.Error {
	session, snapshot, err := openExplicitEntrySession(args)
	if err != nil {
		return err
	}
	defer session.close()
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	cfg := snapshot.View()
	sources, err := resolveSeriesSourcesWithCatalog(rt.Catalog, args.Tables)
	if err != nil {
		return err
	}
	if len(sources) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "no custom series sources are registered")
	}
	pairs, err := goods.RefreshPairListWithRuntimeDeps(&goods.RuntimeDeps{
		Core: rt.Core, Clock: rt.Clock, Config: cfg, DataDir: snapshot.DataDir,
		Symbols: rt.Symbols, Storage: rt.Storage, Exchange: rt.Exchange,
	}, rt.Clock.TimeMS())
	if err != nil {
		return err
	}
	targets := make([]*orm.ExSymbol, 0, len(pairs))
	for _, pair := range pairs {
		target, targetErr := rt.Symbols.GetExSymbolCur(pair)
		if targetErr != nil {
			return targetErr
		}
		targets = append(targets, target)
	}
	var startMS, endMS int64
	if cfg.TimeRange != nil {
		startMS, endMS = cfg.TimeRange.StartMS, cfg.TimeRange.EndMS
	}
	return ensureSeriesRanges(rt.Context(), orm.NewSeriesRepo(rt.Storage), sources, targets,
		startMS, endMS, rt.Clock.TimeMS())
}

func resolveSeriesSources(names []string) ([]data.DataSource, *errs.Error) {
	return resolveSeriesSourcesWithCatalog(data.LegacyDataSourceCatalog(), names)
}

func resolveSeriesSourcesWithCatalog(catalog *data.DataSourceCatalog, names []string) ([]data.DataSource, *errs.Error) {
	if catalog == nil {
		return nil, errs.NewMsg(core.ErrBadConfig, "custom series catalog is not configured")
	}
	if len(names) == 0 {
		return registeredSeriesSourcesWithCatalog(catalog), nil
	}
	sources := make([]data.DataSource, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		source := catalog.GetDataSource(name)
		if source == nil {
			return nil, errs.NewMsg(core.ErrBadConfig, "custom series source %q is not registered", name)
		}
		seen[name] = true
		sources = append(sources, source)
	}
	return sources, nil
}

func registeredSeriesSources() []data.DataSource {
	return registeredSeriesSourcesWithCatalog(data.LegacyDataSourceCatalog())
}

func registeredSeriesSourcesWithCatalog(catalog *data.DataSourceCatalog) []data.DataSource {
	if catalog == nil {
		return nil
	}
	names := catalog.ListDataSources()
	sources := make([]data.DataSource, 0, len(names))
	for _, name := range names {
		if source := catalog.GetDataSource(name); source != nil {
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
