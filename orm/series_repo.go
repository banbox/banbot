package orm

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/jackc/pgx/v5"
)

type SeriesRepo interface {
	EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error
	InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error
	QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error)
	UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error
	GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error)
}

var defaultSeriesRepo SeriesRepo = &dbSeriesRepo{}

func DefaultSeriesRepo() SeriesRepo {
	return defaultSeriesRepo
}

type dbSeriesRepo struct{}

func (r *dbSeriesRepo) EnsureSeriesTable(ctx context.Context, info *SeriesInfo) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	sqlText := buildSeriesTableDDL(info)
	if _, err_ := q.db.Exec(ctx, sqlText); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (r *dbSeriesRepo) InsertSeriesBatch(ctx context.Context, info *SeriesInfo, rows []*DataRecord) *errs.Error {
	if len(rows) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	if err := r.EnsureSeriesTable(ctx, info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	binding := normalizedSeriesBinding(info.Binding)
	cols := []string{
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.EndColumn),
	}
	questVisible := make(map[int32]int64)
	for _, field := range binding.Fields {
		cols = append(cols, quoteIdent(field.Name))
	}

	const batchRows = 200
	for start := 0; start < len(rows); start += batchRows {
		stop := min(len(rows), start+batchRows)
		args := make([]any, 0, (stop-start)*(3+len(binding.Fields)))
		var sb strings.Builder
		fmt.Fprintf(&sb, "INSERT INTO %s (%s) VALUES ", quoteIdent(binding.Table), strings.Join(cols, ", "))
		for rowIdx := start; rowIdx < stop; rowIdx++ {
			row := rows[rowIdx]
			if row == nil {
				return errs.NewMsg(core.ErrBadConfig, "series row is nil")
			}
			if row.Sid <= 0 {
				return errs.NewMsg(core.ErrBadConfig, "series row sid is required")
			}
			if row.EndMS <= row.TimeMS {
				return errs.NewMsg(core.ErrBadConfig, "series row end_ms must be greater than time_ms")
			}
			if row.TimeMS > questVisible[row.Sid] {
				questVisible[row.Sid] = row.TimeMS
			}
			if rowIdx > start {
				sb.WriteByte(',')
			}
			sb.WriteByte('(')
			for colIdx := 0; colIdx < len(cols); colIdx++ {
				if colIdx > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("$%d", len(args)+colIdx+1))
			}
			sb.WriteByte(')')

			args = append(args, row.Sid)
			if IsQuestDB {
				args = append(args, time.UnixMilli(row.TimeMS).UTC())
			} else {
				args = append(args, row.TimeMS)
			}
			args = append(args, row.EndMS)
			for _, field := range binding.Fields {
				val, ok := row.Values[field.Name]
				if !ok {
					return errs.NewMsg(core.ErrBadConfig, "series row missing field %q", field.Name)
				}
				normVal, err_ := normalizeSeriesFieldValue(field.Type, val)
				if err_ != nil {
					return errs.NewMsg(core.ErrBadConfig, "series row field %q invalid: %v", field.Name, err_)
				}
				args = append(args, normVal)
			}
		}
		if !IsQuestDB {
			var assigns []string
			assigns = append(assigns, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(binding.EndColumn), quoteIdent(binding.EndColumn)))
			for _, field := range binding.Fields {
				assigns = append(assigns, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(field.Name), quoteIdent(field.Name)))
			}
			fmt.Fprintf(&sb, " ON CONFLICT (%s, %s) DO UPDATE SET %s",
				quoteIdent(binding.SIDColumn), quoteIdent(binding.TimeColumn), strings.Join(assigns, ", "))
		}
		if _, err_ := q.db.Exec(ctx, sb.String(), args...); err_ != nil {
			return NewDbErr(core.ErrDbExecFail, err_)
		}
	}
	if IsQuestDB {
		for sid, wantTimeMS := range questVisible {
			if err := waitForQuestSeriesVisible(ctx, q, info, sid, wantTimeMS); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *dbSeriesRepo) QuerySeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64, limit int) ([]*DataRecord, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return nil, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	binding := normalizedSeriesBinding(info.Binding)
	timeExpr := quoteIdent(binding.TimeColumn)
	startArg, endArg := any(startMS), any(endMS)
	if IsQuestDB {
		timeExpr = fmt.Sprintf("cast(%s as long)/1000", quoteIdent(binding.TimeColumn))
		startArg = startMS * 1000
		endArg = endMS * 1000
	}
	selectCols := []string{
		quoteIdent(binding.SIDColumn),
		timeExpr,
		quoteIdent(binding.EndColumn),
	}
	for _, field := range binding.Fields {
		colExpr := quoteIdent(field.Name)
		if !IsQuestDB && field.Type == "json" {
			colExpr = fmt.Sprintf("%s::text", colExpr)
		}
		selectCols = append(selectCols, colExpr)
	}
	sqlText := fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1 AND %s >= $2 AND %s < $3 ORDER BY %s",
		strings.Join(selectCols, ", "),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.TimeColumn),
	)
	args := []any{sid, startArg, endArg}
	if limit > 0 {
		sqlText += " LIMIT $4"
		args = append(args, limit)
	}
	rows, err_ := q.db.Query(ctx, sqlText, args...)
	if err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	defer rows.Close()

	var out []*DataRecord
	for rows.Next() {
		rec, err_ := scanSeriesRecord(rows, binding.Fields)
		if err_ != nil {
			return nil, NewDbErr(core.ErrDbReadFail, err_)
		}
		out = append(out, rec)
	}
	if err_ := rows.Err(); err_ != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err_)
	}
	return out, nil
}

func (r *dbSeriesRepo) UpdateSeriesRange(ctx context.Context, info *SeriesInfo, sid int32, startMS, endMS int64) *errs.Error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	if err_ := q.UpdateSRanges(ctx, sid, binding.Table, info.TimeFrame, startMS, endMS, true); err_ != nil {
		return NewDbErr(core.ErrDbExecFail, err_)
	}
	return nil
}

func (r *dbSeriesRepo) GetSeriesRange(ctx context.Context, info *SeriesInfo, sid int32) (int64, int64, *errs.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateSeriesInfo(info); err != nil {
		return 0, 0, err
	}
	q, conn, err := Conn(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer conn.Release()
	binding := normalizedSeriesBinding(info.Binding)
	if !IsQuestDB {
		start, stop := getKlineRangePg(ctx, sid, binding.Table, info.TimeFrame)
		return start, stop, nil
	}
	covered, err_ := q.getCoveredRanges(ctx, sid, binding.Table, info.TimeFrame, 0, math.MaxInt64)
	if err_ != nil {
		return 0, 0, NewDbErr(core.ErrDbReadFail, err_)
	}
	if len(covered) == 0 {
		return 0, 0, nil
	}
	return covered[0].Start, covered[len(covered)-1].Stop, nil
}

func waitForQuestSeriesVisible(ctx context.Context, q *Queries, info *SeriesInfo, sid int32, wantTimeMS int64) *errs.Error {
	binding := normalizedSeriesBinding(info.Binding)
	sqlText := fmt.Sprintf("SELECT max(cast(%s as long)/1000) FROM %s WHERE %s = $1",
		quoteIdent(binding.TimeColumn),
		quoteIdent(binding.Table),
		quoteIdent(binding.SIDColumn),
	)
	for range 20 {
		var maxTime *int64
		row := q.db.QueryRow(ctx, sqlText, sid)
		if err := row.Scan(&maxTime); err == nil && maxTime != nil && *maxTime >= wantTimeMS {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return errs.NewMsg(core.ErrDbReadFail, "questdb series rows not visible in time: table=%s sid=%d time_ms=%d",
		binding.Table, sid, wantTimeMS)
}

func buildSeriesTableDDL(info *SeriesInfo) string {
	binding := normalizedSeriesBinding(info.Binding)
	colDefs := []string{
		fmt.Sprintf("%s INT NOT NULL", quoteIdent(binding.SIDColumn)),
		fmt.Sprintf("%s %s NOT NULL", quoteIdent(binding.TimeColumn), seriesTimeSQLType()),
		fmt.Sprintf("%s %s NOT NULL", quoteIdent(binding.EndColumn), seriesSQLType("int")),
	}
	for _, field := range binding.Fields {
		colDefs = append(colDefs, fmt.Sprintf("%s %s NOT NULL", quoteIdent(field.Name), seriesSQLType(field.Type)))
	}
	if IsQuestDB {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) timestamp(%s) PARTITION BY MONTH WAL DEDUP UPSERT KEYS(%s, %s)",
			quoteIdent(binding.Table),
			strings.Join(colDefs, ", "),
			quoteIdent(binding.TimeColumn),
			quoteIdent(binding.SIDColumn),
			quoteIdent(binding.TimeColumn),
		)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s, %s))",
		quoteIdent(binding.Table),
		strings.Join(colDefs, ", "),
		quoteIdent(binding.SIDColumn),
		quoteIdent(binding.TimeColumn),
	)
}

func scanSeriesRecord(rows pgx.Rows, fields []SeriesField) (*DataRecord, error) {
	rec := &DataRecord{Values: make(map[string]any, len(fields))}
	targets := make([]any, 0, 3+len(fields))
	targets = append(targets, &rec.Sid, &rec.TimeMS, &rec.EndMS)
	fieldTargets := make([]any, len(fields))
	for i, field := range fields {
		switch field.Type {
		case "float":
			var val float64
			fieldTargets[i] = &val
		case "int":
			var val int64
			fieldTargets[i] = &val
		case "string", "json":
			var val string
			fieldTargets[i] = &val
		case "bool":
			var val bool
			fieldTargets[i] = &val
		default:
			return nil, fmt.Errorf("unsupported series field type %q", field.Type)
		}
		targets = append(targets, fieldTargets[i])
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	rec.Closed = true
	for i, field := range fields {
		switch field.Type {
		case "float":
			rec.Values[field.Name] = *(fieldTargets[i].(*float64))
		case "int":
			rec.Values[field.Name] = *(fieldTargets[i].(*int64))
		case "string", "json":
			rec.Values[field.Name] = *(fieldTargets[i].(*string))
		case "bool":
			rec.Values[field.Name] = *(fieldTargets[i].(*bool))
		}
	}
	return rec, nil
}

func validateSeriesInfo(info *SeriesInfo) *errs.Error {
	if info == nil {
		return errs.NewMsg(core.ErrBadConfig, "series info is required")
	}
	if info.TimeFrame == "" {
		return errs.NewMsg(core.ErrBadConfig, "series timeframe is required")
	}
	binding := normalizedSeriesBinding(info.Binding)
	if binding.Table == "" || binding.TimeColumn == "" || binding.EndColumn == "" {
		return errs.NewMsg(core.ErrBadConfig, "series binding must define table/time/end columns")
	}
	seen := map[string]bool{
		binding.TimeColumn: true,
		binding.EndColumn:  true,
		binding.SIDColumn:  true,
	}
	for _, field := range binding.Fields {
		if field.Name == "" {
			return errs.NewMsg(core.ErrBadConfig, "series field name is required")
		}
		if seen[field.Name] {
			return errs.NewMsg(core.ErrBadConfig, "duplicate series column %q", field.Name)
		}
		seen[field.Name] = true
		switch field.Type {
		case "float", "int", "string", "bool", "json":
		default:
			return errs.NewMsg(core.ErrBadConfig, "unsupported series field type %q", field.Type)
		}
	}
	return nil
}

func normalizedSeriesBinding(binding SeriesBinding) SeriesBinding {
	if binding.SIDColumn == "" {
		binding.SIDColumn = "sid"
	}
	return binding
}

func seriesTimeSQLType() string {
	if IsQuestDB {
		return "TIMESTAMP"
	}
	return "BIGINT"
}

func seriesSQLType(fieldType string) string {
	if IsQuestDB {
		switch fieldType {
		case "float":
			return "DOUBLE"
		case "int":
			return "LONG"
		case "string", "json":
			return "STRING"
		case "bool":
			return "BOOLEAN"
		}
	}
	switch fieldType {
	case "float":
		return "DOUBLE PRECISION"
	case "int":
		return "BIGINT"
	case "string":
		return "TEXT"
	case "bool":
		return "BOOLEAN"
	case "json":
		return "JSONB"
	default:
		return "TEXT"
	}
}

func normalizeSeriesFieldValue(fieldType string, value any) (any, error) {
	switch fieldType {
	case "float":
		return seriesFloatAny(value)
	case "int":
		return seriesIntAny(value)
	case "string":
		return fmt.Sprint(value), nil
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		default:
			return nil, fmt.Errorf("unsupported bool type %T", value)
		}
	case "json":
		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			data, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			return string(data), nil
		}
	default:
		return nil, fmt.Errorf("unsupported field type %q", fieldType)
	}
}

func seriesIntAny(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported int type %T", value)
	}
}

func seriesFloatAny(value any) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported float type %T", value)
	}
}

func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}
