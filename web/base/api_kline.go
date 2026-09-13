package base

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/orm"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/gofiber/fiber/v2"
)

func RegApiKline(api fiber.Router) {
	api.Get("/symbols", getSymbols)
	api.Get("/data_sources", getDataSources)
	api.Get("/series", getSeries)
	api.Get("/hist", getHist)
	api.Get("/all_inds", getTaInds)
	api.Post("/calc_ind", postCalcInd)
}

// RegApiKlineWithRuntimeDeps binds the read-only K-line routes to one runtime.
// Indicator routes are pure and share the existing implementation.
func RegApiKlineWithRuntimeDeps(api fiber.Router, deps data.RuntimeDeps) {
	h := klineHandlers{deps: &deps}
	api.Get("/symbols", h.getSymbols)
	api.Get("/data_sources", h.getDataSources)
	api.Get("/series", h.getSeries)
	api.Get("/hist", h.getHist)
	api.Get("/all_inds", getTaInds)
	api.Post("/calc_ind", postCalcInd)
}

type klineHandlers struct{ deps *data.RuntimeDeps }

func (h klineHandlers) getDataSources(c *fiber.Ctx) error {
	if h.deps.Catalog == nil {
		return fmt.Errorf("runtime data-source catalog is required")
	}
	return c.JSON(fiber.Map{"data": h.deps.Catalog.ListDataSourceStatus()})
}

func (h klineHandlers) getSymbols(c *fiber.Ctx) error {
	if h.deps.Symbols == nil || h.deps.Core == nil {
		return fmt.Errorf("runtime symbols and core are required")
	}
	exsList := h.deps.Symbols.GetExSymbols(h.deps.Core.ExgName, h.deps.Core.Market)
	res := make([]map[string]interface{}, 0, len(exsList))
	for _, exs := range exsList {
		res = append(res, map[string]interface{}{"exchange": exs.Exchange, "market": exs.Market, "symbol": exs.Symbol, "short_name": exs.ToShort()})
	}
	return c.JSON(fiber.Map{"data": res})
}

func getDataSources(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"data": data.ListDataSourceStatus()})
}

// getSeries reads a registered custom series, or the built-in kline series,
// without requiring the caller to know the physical storage backend.
func getSeries(c *fiber.Ctx) error { return (klineHandlers{}).getSeries(c) }

func (h klineHandlers) getSeries(c *fiber.Ctx) error {
	if err := h.validate(); err != nil {
		return err
	}
	type SeriesArgs struct {
		Source    string `query:"source" validate:"required"`
		SID       int32  `query:"sid" validate:"required"`
		TimeFrame string `query:"timeframe"`
		Fields    string `query:"fields"`
		StartMS   int64  `query:"start"`
		EndMS     int64  `query:"end"`
		Limit     int    `query:"limit"`
	}
	args := new(SeriesArgs)
	if err := VerifyArg(c, args, ArgQuery); err != nil {
		return err
	}
	if args.Limit <= 0 {
		args.Limit = 100
	}
	if args.Limit > 1000 {
		args.Limit = 1000
	}
	if args.EndMS <= 0 {
		args.EndMS = time.Now().UnixMilli()
		if h.deps != nil {
			args.EndMS = h.deps.Clock.TimeMS()
		}
	}
	if args.StartMS <= 0 {
		args.StartMS = args.EndMS - 30*24*time.Hour.Milliseconds()
	}
	if args.StartMS >= args.EndMS {
		return fmt.Errorf("`start` must be less than `end`")
	}

	var target *orm.ExSymbol
	if h.deps == nil {
		target = orm.GetSymbolByID(args.SID)
	} else {
		target = h.deps.Symbols.GetSymbolByID(args.SID)
	}
	if target == nil {
		return fmt.Errorf("symbol sid %d not found", args.SID)
	}
	info, queryFields, err := resolveSeriesQueryInfoWithCatalog(h.catalog(), args.Source, args.TimeFrame, splitSeriesFields(args.Fields))
	if err != nil {
		return err
	}

	var rows []*orm.DataSeries
	if info.Name == orm.SeriesSourceKline {
		sess, release, err := h.queries()
		if err != nil {
			return err
		}
		defer release()
		rows, err = sess.QuerySeriesFields(target, info.TimeFrame, queryFields, args.StartMS, args.EndMS, args.Limit, false)
		if err != nil {
			return err
		}
	} else {
		rows, err = h.seriesStore().Read(h.context(), info, target, args.StartMS, args.EndMS, args.Limit)
		if err != nil {
			return err
		}
	}
	return c.JSON(fiber.Map{
		"source":    info.Name,
		"timeframe": info.TimeFrame,
		"table":     info.Binding.Table,
		"fields":    info.Binding.Fields,
		"data":      rows,
	})
}

func resolveSeriesQueryInfo(source, timeFrame string, fields []string) (*orm.SeriesInfo, []string, error) {
	return resolveSeriesQueryInfoWithCatalog(data.LegacyDataSourceCatalog(), source, timeFrame, fields)
}

func resolveSeriesQueryInfoWithCatalog(catalog *data.DataSourceCatalog, source, timeFrame string, fields []string) (*orm.SeriesInfo, []string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, nil, fmt.Errorf("series source is required")
	}
	source = orm.NormalizeSeriesSource(source)
	if source == orm.SeriesSourceKline {
		if timeFrame == "" {
			return nil, nil, fmt.Errorf("kline timeframe is required")
		}
		fields = orm.NormalizeSeriesFields(source, fields)
		return &orm.SeriesInfo{
			Name:      source,
			TimeFrame: timeFrame,
			Binding: orm.SeriesBinding{
				Table:  orm.SeriesTableName(source, timeFrame),
				Fields: klineQueryFields(fields),
			},
		}, fields, nil
	}

	src := catalog.GetDataSource(source)
	if src == nil || src.Info() == nil {
		return nil, nil, fmt.Errorf("data source %q is not registered", source)
	}
	info := src.Info()
	if timeFrame != "" && timeFrame != info.TimeFrame {
		return nil, nil, fmt.Errorf("timeframe %q does not match data source %q timeframe %q", timeFrame, source, info.TimeFrame)
	}
	copyInfo := *info
	copyInfo.Binding = info.Binding
	copyInfo.Binding.Fields = append([]orm.SeriesField(nil), info.Binding.Fields...)
	if len(fields) == 0 {
		return &copyInfo, nil, nil
	}

	available := make(map[string]orm.SeriesField, len(copyInfo.Binding.Fields))
	for _, field := range copyInfo.Binding.Fields {
		available[field.Name] = field
	}
	requested := orm.NormalizeSeriesFields(source, fields)
	copyInfo.Binding.Fields = make([]orm.SeriesField, 0, len(requested))
	for _, name := range requested {
		field, ok := available[name]
		if !ok {
			return nil, nil, fmt.Errorf("field %q is not defined by data source %q", name, source)
		}
		copyInfo.Binding.Fields = append(copyInfo.Binding.Fields, field)
	}
	return &copyInfo, requested, nil
}

func splitSeriesFields(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.Split(raw, ",")
}

func klineQueryFields(fields []string) []orm.SeriesField {
	items := make([]orm.SeriesField, 0, len(fields))
	for _, name := range fields {
		fieldType := "float"
		if name == "trade_num" {
			fieldType = "int"
		}
		items = append(items, orm.SeriesField{Name: name, Type: fieldType})
	}
	return items
}

func getSymbols(c *fiber.Ctx) error {
	exsList := orm.GetAllExSymbols()
	res := make([]map[string]interface{}, 0)
	for _, exs := range exsList {
		res = append(res, map[string]interface{}{
			"exchange":   exs.Exchange,
			"market":     exs.Market,
			"symbol":     exs.Symbol,
			"short_name": exs.ToShort(),
		})
	}
	return c.JSON(fiber.Map{"data": res})
}

func getHist(c *fiber.Ctx) error { return (klineHandlers{}).getHist(c) }

func (h klineHandlers) getHist(c *fiber.Ctx) error {
	if err := h.validate(); err != nil {
		return err
	}
	type HistArgs struct {
		Exchange  string `query:"exchange" validate:"required"`
		Symbol    string `query:"symbol" validate:"required"`
		TimeFrame string `query:"timeframe" validate:"required"`
		FromMS    int64  `query:"from" validate:"required"`
		ToMS      int64  `query:"to" validate:"required"`
	}
	var data = new(HistArgs)
	if err := VerifyArg(c, data, ArgQuery); err != nil {
		return err
	}
	if data.ToMS <= data.FromMS {
		return errors.New("`from` must less than `to`")
	}
	exs, err2 := h.parseShort(data.Exchange, data.Symbol)
	if err2 != nil {
		return err2
	}
	exchange, err2 := h.exchange(exs.Exchange, exs.Market)
	if err2 != nil {
		return err2
	}
	startMS, stopMS, tf := data.FromMS, data.ToMS, data.TimeFrame
	var adjs []*orm.AdjInfo
	var rows []*orm.DataSeries
	if h.deps == nil {
		adjs, rows, err2 = orm.AutoFetchSeries(exchange, exs, tf, startMS, stopMS, 0, true, nil)
	} else {
		query, release, err := h.queries()
		if err != nil {
			return err
		}
		defer release()
		adjs, rows, err2 = query.AutoFetchSeries(exchange, exs, tf, startMS, stopMS, 0, true, nil)
	}
	if err2 != nil {
		return err2
	}
	dataRows, err2 := ArrSeriesRows(rows)
	if err2 != nil {
		return err2
	}
	return c.JSON(fiber.Map{
		"adjs": adjs,
		"data": dataRows,
	})
}

/*
getTaInds 获取云端指标列表
*/
func getTaInds(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"data": IndsCache,
	})
}

/*
postCalcInd 计算云端指标
*/
func postCalcInd(c *fiber.Ctx) error {
	type CalcArgs struct {
		Name   string      `json:"name" validate:"required"`
		Kline  [][]float64 `json:"kline" validate:"required"`
		Params []float64   `json:"params" validate:"required"`
	}
	var data = new(CalcArgs)
	if err := VerifyArg(c, data, ArgBody); err != nil {
		return err
	}
	res, err := CalcInd(data.Name, data.Kline, data.Params)
	if err != nil {
		return err
	}
	return c.JSON(fiber.Map{
		"code": 200,
		"data": res,
	})
}

func (h klineHandlers) context() context.Context {
	if h.deps != nil {
		return h.deps.Core.Context()
	}
	return context.Background()
}
func (h klineHandlers) catalog() *data.DataSourceCatalog {
	if h.deps != nil {
		return h.deps.Catalog
	}
	return data.LegacyDataSourceCatalog()
}
func (h klineHandlers) seriesStore() *orm.SeriesStore {
	if h.deps != nil {
		return orm.NewSeriesStore(orm.NewSeriesRepo(h.deps.Storage))
	}
	return orm.DefaultSeriesStore()
}
func (h klineHandlers) queries() (*orm.Queries, func(), *errs.Error) {
	if h.deps == nil {
		q, conn, err := orm.Conn(nil)
		if err != nil {
			return nil, nil, err
		}
		return q, conn.Release, nil
	}
	q, conn, err := h.deps.Storage.Conn(h.context())
	if err != nil {
		return nil, nil, err
	}
	q = q.WithSeriesSymbolState(h.deps.Symbols).WithKlineRuntimeOptions(h.deps.KlineOptions())
	return q, conn.Release, nil
}
func (h klineHandlers) parseShort(exchange, symbol string) (*orm.ExSymbol, *errs.Error) {
	if h.deps != nil {
		return h.deps.Symbols.ParseShort(exchange, symbol)
	}
	return orm.ParseShort(exchange, symbol)
}
func (h klineHandlers) exchange(name, market string) (banexg.BanExchange, *errs.Error) {
	if h.deps == nil {
		return GetExg(name, market, "", true)
	}
	if h.deps.Core.ExgName != name || h.deps.Core.Market != market {
		return nil, errs.NewMsg(errs.CodeParamInvalid, "exchange/market does not belong to this runtime")
	}
	return h.deps.Exchange, nil
}

func (h klineHandlers) validate() error {
	if h.deps != nil && (h.deps.Core == nil || h.deps.Clock == nil || h.deps.Symbols == nil || h.deps.Storage == nil || h.deps.Catalog == nil || h.deps.Exchange == nil || h.deps.Config == nil) {
		return fmt.Errorf("runtime K-line routes require core, clock, symbols, storage, catalog, exchange and config")
	}
	return nil
}
