package orm

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	utils2 "github.com/banbox/banexg/utils"
)

type KLineSeriesStore struct {
	Info *SeriesInfo
}

func NewKLineSeriesStore(info *SeriesInfo) *KLineSeriesStore {
	return &KLineSeriesStore{Info: info}
}

func NewKLineSeriesInfo(name, timeFrame string, fields []SeriesField) *SeriesInfo {
	return &SeriesInfo{
		Name:      name,
		TimeFrame: timeFrame,
		Binding: SeriesBinding{
			Table:     SeriesTableName(SeriesSourceKline, timeFrame),
			SIDColumn: "sid",
			Fields:    fields,
		},
	}
}

func (s *KLineSeriesStore) Ensure(ctx context.Context) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateKLineSeriesInfo(s.info()); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	binding := resolveKLineSeriesBinding(s.info())
	for _, field := range binding.Fields {
		if _, err_ := q.db.Exec(ctx, buildKLineSeriesAddColumnSQL(binding.Table, field)); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	return nil
}

func (s *KLineSeriesStore) Write(ctx context.Context, target *ExSymbol, rows []*DataRecord) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if target == nil || target.ID <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "kline series target exsymbol is required")
	}
	if err := validateKLineSeriesInfo(s.info()); err != nil {
		return err
	}
	if err := s.Ensure(ctx); err != nil {
		return err
	}
	info := s.info()
	binding := resolveKLineSeriesBinding(info)
	items, err := normalizeKLineSeriesRows(target.ID, binding.Fields, rows)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	sqlText := buildKLineSeriesUpdateSQL(binding)
	for _, row := range items {
		args := make([]any, 0, len(binding.Fields)+2)
		for _, field := range binding.Fields {
			val, ok := row.Values[field.Name]
			if !ok {
				return errs.NewMsg(core.ErrBadConfig, "kline series row missing field %q", field.Name)
			}
			normVal, err_ := normalizeSeriesFieldValue(field.Type, val)
			if err_ != nil {
				return errs.NewMsg(core.ErrBadConfig, "kline series row field %q invalid: %v", field.Name, err_)
			}
			args = append(args, normVal)
		}
		args = append(args, row.Sid, kLineSeriesTimeArg(binding.TimeColumn, row.TimeMS))
		tag, err_ := q.db.Exec(ctx, sqlText, args...)
		if err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
		if tag.RowsAffected() == 0 {
			return errs.NewMsg(core.ErrDbExecFail, "kline series target row not found table=%s sid=%d timeframe=%s time_ms=%d",
				binding.Table, row.Sid, info.TimeFrame, row.TimeMS)
		}
	}
	if IsQuestDB {
		last := items[len(items)-1]
		if err := waitForQuestKLineSeriesVisible(ctx, q, info, last); err != nil {
			return err
		}
	}
	return nil
}

func (s *KLineSeriesStore) Read(ctx context.Context, target *ExSymbol, startMS, endMS int64, limit int) ([]*DataSeries, *errs.Error) {
	if startMS >= endMS {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if target == nil || target.ID <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "kline series target exsymbol is required")
	}
	info := s.info()
	if err := validateKLineSeriesInfo(info); err != nil {
		return nil, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	binding := resolveKLineSeriesBinding(info)
	timeExpr := quoteIdent(binding.TimeColumn)
	startArg, endArg := any(startMS), any(endMS)
	var covered []MSRange
	if IsQuestDB {
		timeExpr = fmt.Sprintf("cast(%s as long)/1000", quoteIdent(binding.TimeColumn))
		startArg = startMS * 1000
		endArg = endMS * 1000
		var err_ error
		covered, err_ = q.getCoveredRanges(ctx, target.ID, binding.Table, info.TimeFrame, startMS, endMS)
		if err_ != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err_)
		}
		if len(covered) == 0 {
			return nil, nil
		}
	}
	selectCols := []string{quoteIdent(binding.SIDColumn), timeExpr}
	for _, field := range binding.Fields {
		colExpr := quoteIdent(field.Name)
		if !IsQuestDB && field.Type == "json" {
			colExpr = fmt.Sprintf("%s::text", colExpr)
		}
		selectCols = append(selectCols, colExpr)
	}
	timeFilter := fmt.Sprintf("%s >= $2 AND %s < $3", quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
	if IsQuestDB {
		timeFilter = fmt.Sprintf("cast(%s as long) >= $2 AND cast(%s as long) < $3",
			quoteIdent(binding.TimeColumn), quoteIdent(binding.TimeColumn))
	}
	sqlText := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s ORDER BY %s",
		strings.Join(selectCols, ", "),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
		timeFilter,
		quoteIdent(binding.TimeColumn),
	)
	args := []any{target.ID, startArg, endArg}
	if limit > 0 && !IsQuestDB {
		sqlText += " LIMIT $4"
		args = append(args, limit)
	}
	dbRows, err_ := q.db.Query(ctx, sqlText, args...)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer dbRows.Close()

	tfMS := int64(utils2.TFToSecs(info.TimeFrame)) * 1000
	source := NormalizeSeriesSource(info.Name)
	out := make([]*DataSeries, 0)
	for dbRows.Next() {
		rec, err_ := scanKLineSeriesRecord(dbRows, binding.Fields)
		if err_ != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err_)
		}
		if len(rec.Values) == 0 {
			continue
		}
		if IsQuestDB && !seriesRangeCovered(rec.TimeMS, covered) {
			continue
		}
		out = append(out, &DataSeries{
			Source:    source,
			Sid:       rec.Sid,
			TimeMS:    rec.TimeMS,
			EndMS:     rec.TimeMS + tfMS,
			TimeFrame: info.TimeFrame,
			Closed:    true,
			Values:    rec.Values,
			ExSymbol:  target,
		})
		if IsQuestDB && limit > 0 && len(out) >= limit {
			break
		}
	}
	if err_ := dbRows.Err(); err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	return out, nil
}

func (s *KLineSeriesStore) info() *SeriesInfo {
	if s == nil {
		return nil
	}
	return s.Info
}

func validateKLineSeriesInfo(info *SeriesInfo) *errs.Error {
	if info == nil {
		return errs.NewMsg(core.ErrBadConfig, "kline series info is required")
	}
	if info.Name == "" {
		return errs.NewMsg(core.ErrBadConfig, "kline series name is required")
	}
	if info.TimeFrame == "" {
		return errs.NewMsg(core.ErrBadConfig, "kline series timeframe is required")
	}
	if utils2.TFToSecs(info.TimeFrame) <= 0 {
		return errs.NewMsg(core.ErrBadConfig, "kline series timeframe %q is invalid", info.TimeFrame)
	}
	binding := resolveKLineSeriesBinding(info)
	if binding.Table == "" || binding.SIDColumn == "" {
		return errs.NewMsg(core.ErrBadConfig, "kline series binding must define table/time/sid columns")
	}
	if binding.TimeColumn == "" && !isKLineSeriesBinding(info, binding) {
		return errs.NewMsg(core.ErrBadConfig, "kline series binding must define table/time/sid columns")
	}
	if binding.EndColumn != "" {
		return errs.NewMsg(core.ErrBadConfig, "kline series binding must not define an end column")
	}
	if len(binding.Fields) == 0 {
		return errs.NewMsg(core.ErrBadConfig, "kline series fields are required")
	}
	seen := map[string]bool{
		binding.SIDColumn:  true,
		binding.TimeColumn: true,
		"ts":               true,
		"time":             true,
	}
	if isKLineSeriesBinding(info, binding) {
		for _, name := range []string{"open", "high", "low", "close", "volume", "quote", "buy_volume", "trade_num"} {
			seen[name] = true
		}
	}
	for _, field := range binding.Fields {
		if field.Name == "" {
			return errs.NewMsg(core.ErrBadConfig, "kline series field name is required")
		}
		if seen[field.Name] {
			return errs.NewMsg(core.ErrBadConfig, "kline series field %q conflicts with an existing table column", field.Name)
		}
		seen[field.Name] = true
		switch field.Type {
		case "float", "int", "string", "bool", "json":
		default:
			return errs.NewMsg(core.ErrBadConfig, "unsupported kline series field type %q", field.Type)
		}
	}
	return nil
}

func normalizeKLineSeriesRows(sid int32, fields []SeriesField, rows []*DataRecord) ([]*DataRecord, *errs.Error) {
	if sid <= 0 {
		return nil, errs.NewMsg(core.ErrBadConfig, "kline series target sid is required")
	}
	items := make([]*DataRecord, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		cp := *row
		if cp.Sid == 0 {
			cp.Sid = sid
		}
		if cp.Sid != sid {
			return nil, errs.NewMsg(core.ErrBadConfig, "kline series row sid %d does not match target sid %d", cp.Sid, sid)
		}
		for _, field := range fields {
			if _, ok := cp.Values[field.Name]; !ok {
				return nil, errs.NewMsg(core.ErrBadConfig, "kline series row missing field %q", field.Name)
			}
		}
		items = append(items, &cp)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TimeMS < items[j].TimeMS
	})
	return items, nil
}

func KLineSeriesTimeColumn() string {
	if IsQuestDB {
		return "ts"
	}
	return "time"
}

func resolveKLineSeriesBinding(info *SeriesInfo) SeriesBinding {
	if info == nil {
		return SeriesBinding{}
	}
	binding := normalizedSeriesBinding(info.Binding)
	if isKLineSeriesBinding(info, binding) {
		binding.TimeColumn = KLineSeriesTimeColumn()
	}
	return binding
}

func isKLineSeriesBinding(info *SeriesInfo, binding SeriesBinding) bool {
	return info != nil && binding.Table == SeriesTableName(SeriesSourceKline, info.TimeFrame)
}

func kLineSeriesTimeArg(timeColumn string, timeMS int64) any {
	if IsQuestDB {
		return time.UnixMilli(timeMS).UTC()
	}
	return timeMS
}

func buildKLineSeriesAddColumnSQL(table string, field SeriesField) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
		quoteIdent(table),
		quoteIdent(field.Name),
		seriesSQLType(field.Type),
	)
}

func buildKLineSeriesUpdateSQL(binding SeriesBinding) string {
	assigns := make([]string, 0, len(binding.Fields))
	for idx, field := range binding.Fields {
		assigns = append(assigns, fmt.Sprintf("%s = $%d", quoteIdent(field.Name), idx+1))
	}
	sidArg := len(binding.Fields) + 1
	timeArg := len(binding.Fields) + 2
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d AND %s = $%d",
		quoteIdent(binding.Table),
		strings.Join(assigns, ", "),
		quoteIdent(binding.SIDColumn),
		sidArg,
		quoteIdent(binding.TimeColumn),
		timeArg,
	)
}

func scanKLineSeriesRecord(rows rowScanner, fields []SeriesField) (*DataRecord, error) {
	rec := &DataRecord{Values: make(map[string]any, len(fields))}
	targets := make([]any, 0, 2+len(fields))
	targets = append(targets, &rec.Sid, &rec.TimeMS)
	fieldTargets := make([]any, len(fields))
	for i, field := range fields {
		switch field.Type {
		case "float":
			var val sql.NullFloat64
			fieldTargets[i] = &val
		case "int":
			var val sql.NullInt64
			fieldTargets[i] = &val
		case "string", "json":
			var val sql.NullString
			fieldTargets[i] = &val
		case "bool":
			var val sql.NullBool
			fieldTargets[i] = &val
		default:
			return nil, fmt.Errorf("unsupported series field type %q", field.Type)
		}
		targets = append(targets, fieldTargets[i])
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	for i, field := range fields {
		switch field.Type {
		case "float":
			if val := fieldTargets[i].(*sql.NullFloat64); val.Valid {
				rec.Values[field.Name] = val.Float64
			}
		case "int":
			if val := fieldTargets[i].(*sql.NullInt64); val.Valid {
				rec.Values[field.Name] = val.Int64
			}
		case "string", "json":
			if val := fieldTargets[i].(*sql.NullString); val.Valid {
				rec.Values[field.Name] = val.String
			}
		case "bool":
			if val := fieldTargets[i].(*sql.NullBool); val.Valid {
				rec.Values[field.Name] = val.Bool
			}
		}
	}
	return rec, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func waitForQuestKLineSeriesVisible(ctx context.Context, q *Queries, info *SeriesInfo, row *DataRecord) *errs.Error {
	if info == nil || row == nil {
		return nil
	}
	binding := resolveKLineSeriesBinding(info)
	if len(binding.Fields) == 0 {
		return nil
	}
	field := binding.Fields[0]
	want, ok := row.Values[field.Name]
	if !ok {
		return nil
	}
	normWant, err := normalizeSeriesFieldValue(field.Type, want)
	if err != nil {
		return errs.NewMsg(core.ErrBadConfig, "kline series row field %q invalid: %v", field.Name, err)
	}
	visible, err := waitForQuestCondition(ctx, 5*time.Second, questReadAfterWritePollInterval, func() (bool, error) {
		var got any
		sqlText := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s = $2",
			quoteIdent(field.Name),
			quoteIdent(binding.Table),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
		)
		err_ := q.db.QueryRow(ctx, sqlText, row.Sid, time.UnixMilli(row.TimeMS).UTC()).Scan(&got)
		if err_ != nil {
			return false, nil
		}
		return fmt.Sprint(got) == fmt.Sprint(normWant), nil
	})
	if err != nil {
		return NewDbErr(core.ErrDbReadFail, err)
	}
	if visible {
		return nil
	}
	return errs.NewMsg(core.ErrDbReadFail, "questdb kline series row not visible in time: table=%s sid=%d time_ms=%d field=%s",
		binding.Table, row.Sid, row.TimeMS, field.Name)
}
