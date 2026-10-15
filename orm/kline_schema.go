package orm

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// KlineSchemaField retains physical type and nullability information. QuestDB
// uses type-specific null sentinels rather than PostgreSQL NOT NULL constraints.
type KlineSchemaField struct {
	Name, Type, Nullability string
}

type KlineProjectionSchema struct {
	Source, TimeFrame, Table string
	Fields                   []KlineSchemaField
}

// ReadKlineProjectionSchema uses the same physical table resolution as kline
// readers and requires an explicit backend owner. It performs no schema writes.
func (q *Queries) ReadKlineProjectionSchema(ctx context.Context, timeframe string, fields []string) (KlineProjectionSchema, error) {
	if ctx == nil || q == nil || q.storage == nil || q.db == nil {
		return KlineProjectionSchema{}, fmt.Errorf("kline schema requires context and explicit storage query")
	}
	if err := ctx.Err(); err != nil {
		return KlineProjectionSchema{}, err
	}
	if _, err := NormalizeSubscription(Subscription{Source: SeriesSourceKline, TimeFrame: timeframe, ExSymbol: &ExSymbol{ID: 1, Symbol: "schema"}, Fields: fields}); err != nil {
		return KlineProjectionSchema{}, err
	}
	table, _, _ := resolveTablePg(timeframe)
	if table == "" {
		return KlineProjectionSchema{}, fmt.Errorf("kline schema has no physical table for %s", timeframe)
	}
	available := map[string]KlineSchemaField{}
	timeColumn := "time"
	if q.isQuestDB() {
		timeColumn = "ts"
		columns, err := queryQuestTableColumns(ctx, q, table)
		if err != nil {
			return KlineProjectionSchema{}, err
		}
		for _, column := range columns {
			available[column.Name] = KlineSchemaField{column.Name, strings.ToUpper(strings.TrimSpace(column.Type)), "questdb-type-specific-null"}
		}
	} else {
		rows, err := q.db.Query(ctx, `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), NOT a.attnotnull
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`, table)
		if err != nil {
			return KlineProjectionSchema{}, err
		}
		defer rows.Close()
		for rows.Next() {
			var name, physicalType string
			var nullable bool
			if err := rows.Scan(&name, &physicalType, &nullable); err != nil {
				return KlineProjectionSchema{}, err
			}
			nullability := "not-null"
			if nullable {
				nullability = "nullable"
			}
			available[name] = KlineSchemaField{name, strings.ToLower(strings.TrimSpace(physicalType)), nullability}
		}
		if err := rows.Err(); err != nil {
			return KlineProjectionSchema{}, err
		}
	}
	selected := MergeSeriesFields([]string{"sid", timeColumn}, NormalizeSeriesFields(SeriesSourceKline, fields))
	sort.Strings(selected)
	schema := KlineProjectionSchema{Source: SeriesSourceKline, TimeFrame: timeframe, Table: table}
	for _, name := range selected {
		field, ok := available[name]
		if !ok || field.Type == "" {
			return KlineProjectionSchema{}, fmt.Errorf("kline schema %s has no typed field %s", table, name)
		}
		schema.Fields = append(schema.Fields, field)
	}
	return schema, nil
}
