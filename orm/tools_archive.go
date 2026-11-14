package orm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
)

const transferArchiveVersion = 1
const transferCompleteFile = "complete"

// The protobuf envelope keeps the original field numbers. Its versioned Go
// payload retains interface scalar types, integer precision and explicit NULLs.
type transferSeries struct {
	Version uint32
	QuestDB bool
	Fields  []KlineSchemaField
	Rows    []transferRow
}
type transferRow struct {
	TimeMS, EndMS int64
	Values        map[string]any
}

func init() {
	gob.Register(map[string]any{})
	gob.Register([]any{})
	gob.Register([]string{})
}

func transferColumns(ctx context.Context, q *Queries, tf string) ([]KlineSchemaField, error) {
	if _, ok := aggMap[tf]; !ok {
		return nil, fmt.Errorf("archive requires physical storage timeframe: %s", tf)
	}
	table := "kline_" + tf
	var fields []KlineSchemaField
	if q.isQuestDB() {
		columns, err := queryQuestTableColumns(ctx, q, table)
		if err != nil {
			return nil, err
		}
		for _, c := range columns {
			if c.Name != "sid" && c.Name != "ts" {
				fields = append(fields, KlineSchemaField{Name: c.Name, Type: c.Type})
			}
		}
	} else {
		rows, err := q.db.Query(ctx, `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), NOT a.attnotnull FROM pg_catalog.pg_attribute a WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var f KlineSchemaField
			var nullable bool
			if err := rows.Scan(&f.Name, &f.Type, &nullable); err != nil {
				return nil, err
			}
			if f.Name != "sid" && f.Name != "time" {
				if nullable {
					f.Nullability = "nullable"
				} else {
					f.Nullability = "not-null"
				}
				fields = append(fields, f)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	for _, f := range fields {
		if _, err := transferColumnType(f.Type, q.isQuestDB(), q.isQuestDB()); err != nil {
			return nil, err
		}
	}
	return fields, nil
}

func transferColumnType(typ string, sourceQuest, targetQuest bool) (string, error) {
	t := strings.ToLower(strings.TrimSpace(typ))
	var generic string
	switch t {
	case "double precision", "double":
		generic = "float"
	case "real", "float":
		generic = "float"
	case "smallint", "short", "integer", "int", "bigint", "long", "byte":
		generic = "int"
	case "text", "varchar", "character varying", "string", "symbol":
		generic = "string"
	case "boolean", "bool":
		generic = "bool"
	case "json", "jsonb":
		generic = "json"
	default:
		return "", fmt.Errorf("unsupported archive column type %q", typ)
	}
	if sourceQuest == targetQuest {
		return typ, nil
	}
	return seriesSQLTypeForBackend(generic, targetQuest), nil
}

func readTransferRows(ctx context.Context, q *Queries, job *ExportKlineJob, fields []KlineSchemaField, startMS int64, limit int) ([]*DataSeries, *errs.Error) {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	timeCol, timeExpr, startArg, stopArg := "time", quoteIdent("time"), startMS, job.StopMS
	if q.isQuestDB() {
		timeCol = "ts"
		timeExpr = "cast(ts as long)/1000"
		startArg *= 1000
		stopArg *= 1000
	}
	filterCol := quoteIdent(timeCol)
	if q.isQuestDB() {
		filterCol = "cast(ts as long)"
	}
	sql := fmt.Sprintf("SELECT %s,%s FROM %s WHERE sid=$1 AND %s >= $2 AND %s < $3 ORDER BY %s LIMIT %d", timeExpr, strings.Join(quoteSeriesFields(names), ","), quoteIdent("kline_"+job.TimeFrame), filterCol, filterCol, quoteIdent(timeCol), limit)
	rows, err := q.db.Query(ctx, sql, job.ID, startArg, stopArg)
	result, err := mapToSeriesFieldsWithContext(ctx, job.ExSymbol, job.TimeFrame, names, rows, err)
	if err != nil {
		return nil, NewDbErr(core.ErrDbReadFail, err)
	}
	return result, nil
}

func encodeTransferBlock(block *KlineBlock, fields []KlineSchemaField, rows []*DataSeries, quest bool) error {
	payload := transferSeries{Version: transferArchiveVersion, QuestDB: quest, Fields: fields, Rows: make([]transferRow, len(rows))}
	for i, row := range rows {
		payload.Rows[i] = transferRow{TimeMS: row.TimeMS, EndMS: row.EndMS, Values: row.Values}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
		return err
	}
	block.Series = buf.Bytes()
	if len(rows) > 0 {
		block.Start = rows[0].TimeMS
		block.End = rows[len(rows)-1].EndMS
	}
	return nil
}

func decodeTransferBlock(block *KlineBlock, sid int32) ([]*DataSeries, []KlineSchemaField, bool, error) {
	tfMS := int64(utils.TFToSecs(block.Timeframe)) * 1000
	if _, ok := aggMap[block.Timeframe]; !ok || tfMS <= 0 {
		return nil, nil, false, fmt.Errorf("invalid archive timeframe %q", block.Timeframe)
	}
	var payload transferSeries
	if len(block.Series) > 0 {
		if err := gob.NewDecoder(bytes.NewReader(block.Series)).Decode(&payload); err != nil {
			return nil, nil, false, err
		}
		if payload.Version != transferArchiveVersion {
			return nil, nil, false, fmt.Errorf("unsupported series archive version %d", payload.Version)
		}
		if len(payload.Rows) == 0 || len(payload.Fields) == 0 {
			return nil, nil, false, fmt.Errorf("empty generic archive block")
		}
	} else {
		n := len(block.Open)
		if n == 0 || len(block.High) != n || len(block.Low) != n || len(block.Close) != n || len(block.Volume) != n || (len(block.Info) != 0 && len(block.Info) != n) {
			return nil, nil, false, fmt.Errorf("invalid legacy archive arrays")
		}
		for i := 0; i < n; i++ {
			values := map[string]any{"open": block.Open[i], "high": block.High[i], "low": block.Low[i], "close": block.Close[i], "volume": block.Volume[i]}
			if len(block.Info) > 0 {
				values["buy_volume"] = block.Info[i]
			}
			ts := block.Start + int64(i)*tfMS
			payload.Rows = append(payload.Rows, transferRow{TimeMS: ts, EndMS: ts + tfMS, Values: values})
		}
	}
	rows := make([]*DataSeries, len(payload.Rows))
	var prev int64
	for i, row := range payload.Rows {
		if row.TimeMS <= 0 || (i > 0 && row.TimeMS <= prev) || row.EndMS != row.TimeMS+tfMS {
			return nil, nil, false, fmt.Errorf("invalid archive timestamps")
		}
		prev = row.TimeMS
		if len(block.Series) > 0 {
			for _, f := range payload.Fields {
				if _, ok := row.Values[f.Name]; !ok {
					return nil, nil, false, fmt.Errorf("archive row missing field %q", f.Name)
				}
			}
			if len(row.Values) != len(payload.Fields) {
				return nil, nil, false, fmt.Errorf("archive row/schema field count mismatch")
			}
		}
		rows[i] = &DataSeries{Source: SeriesSourceKline, Sid: sid, TimeFrame: block.Timeframe, TimeMS: row.TimeMS, EndMS: row.EndMS, Closed: true, Values: row.Values}
		if err := validateOHLCVSeries(rows[i]); err != nil {
			return nil, nil, false, err
		}
	}
	if rows[0].TimeMS != block.Start || rows[len(rows)-1].EndMS != block.End {
		return nil, nil, false, fmt.Errorf("archive block bounds mismatch")
	}
	return rows, payload.Fields, payload.QuestDB, nil
}

func ensureTransferColumns(ctx context.Context, q *Queries, tf string, fields []KlineSchemaField, sourceQuest bool) error {
	if len(fields) == 0 {
		return nil
	}
	existing, err := transferColumns(ctx, q, tf)
	if err != nil {
		return err
	}
	known := map[string]KlineSchemaField{}
	for _, f := range existing {
		known[f.Name] = f
	}
	for _, f := range fields {
		typ, err := transferColumnType(f.Type, sourceQuest, q.isQuestDB())
		if err != nil {
			return err
		}
		if old, ok := known[f.Name]; ok {
			if !strings.EqualFold(old.Type, typ) {
				return fmt.Errorf("archive column %q type mismatch: target=%s source=%s", f.Name, old.Type, typ)
			}
			if f.Nullability == "nullable" && old.Nullability == "not-null" {
				return fmt.Errorf("archive column %q nullability mismatch", f.Name)
			}
			continue
		}
		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", quoteIdent("kline_"+tf), quoteIdent(f.Name), typ)
		if _, err := q.db.Exec(ctx, sql); err != nil {
			return err
		}
		known[f.Name] = KlineSchemaField{Name: f.Name, Type: typ, Nullability: "nullable"}
	}
	return nil
}

// Preflight precedes all symbol/schema/data writes, including for legacy dumps.
func validateTransferArchive(dir string, info *EXInfo, files []string) error {
	if info.ArchiveVersion > transferArchiveVersion {
		return fmt.Errorf("unsupported archive version %d", info.ArchiveVersion)
	}
	if info.ArchiveVersion > 0 {
		if err := checkTransferManifest(dir, files); err != nil {
			return fmt.Errorf("incomplete export: %w", err)
		}
	}
	ids := map[int32]bool{}
	for _, s := range info.Symbols {
		if s.Id <= 0 || ids[s.Id] {
			return fmt.Errorf("invalid/duplicate archive symbol id %d", s.Id)
		}
		ids[s.Id] = true
	}
	count := 0
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		err = func() error {
			for {
				block := new(KlineBlock)
				if e := readProtoMessage(f, block); e != nil {
					return e
				}
				if block.ExsId == 0 && len(block.Open) == 0 && len(block.Series) == 0 {
					return nil
				}
				if !ids[block.ExsId] {
					return fmt.Errorf("unknown archive symbol id %d", block.ExsId)
				}
				rows, _, _, err := decodeTransferBlock(block, block.ExsId)
				if err != nil {
					return err
				}
				count += len(rows)
			}
		}()
		closeErr := f.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
	}
	if count == 0 {
		return fmt.Errorf("archive contains no K-line rows")
	}
	return nil
}

// Completion records every archive file digest, detecting missing files and
// truncation even when it occurs exactly at a protobuf block boundary.
func transferManifest(dir string, files []string) (map[string]string, error) {
	result := map[string]string{}
	for _, path := range append([]string{filepath.Join(dir, "exInfo1.dat")}, files...) {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		closeErr := f.Close()
		if err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
		result[filepath.Base(path)] = fmt.Sprintf("%x", h.Sum(nil))
	}
	return result, nil
}
func writeTransferManifest(dir string, files []string) error {
	m, err := transferManifest(dir, files)
	if err != nil {
		return err
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, transferCompleteFile), data, 0644)
}
func checkTransferManifest(dir string, files []string) error {
	data, err := os.ReadFile(filepath.Join(dir, transferCompleteFile))
	if err != nil {
		return err
	}
	var expected map[string]string
	if err := json.Unmarshal(data, &expected); err != nil {
		return err
	}
	actual, err := transferManifest(dir, files)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, actual) {
		return fmt.Errorf("archive file set or checksum differs from completion manifest")
	}
	return nil
}
