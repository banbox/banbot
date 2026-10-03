package orm

import (
	"context"
	"fmt"
	"math"
	"reflect"
)

type seriesReadByteLimitKey struct{}

// WithSeriesReadByteLimit bounds one decoded input page, not process heap/RSS.
// Accounting includes concrete inline sizes, strings, visible map entries and
// slice backing capacity. It excludes map spare capacity, allocator overhead,
// driver buffers and borrowed ExSymbol/Adj/runtime aggregation state.
// Zero disables accounting and preserves the legacy arbitrary Values contract.
func WithSeriesReadByteLimit(ctx context.Context, bytes int64) context.Context {
	return context.WithValue(ctx, seriesReadByteLimitKey{}, bytes)
}

func SeriesReadByteLimit(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	limit, _ := ctx.Value(seriesReadByteLimitKey{}).(int64)
	return max(0, limit)
}

type SeriesByteLimitError struct{ Limit, Observed int64 }

func (e *SeriesByteLimitError) Error() string {
	return fmt.Sprintf("decoded series page byte limit exceeded: observed=%d limit=%d (not a process heap limit)", e.Observed, e.Limit)
}

type SeriesByteCounter struct{ Limit, Bytes int64 }

func (c *SeriesByteCounter) AddRecord(row *DataRecord) error {
	if c.Limit <= 0 {
		return nil
	}
	size, err := DataRecordBytes(row)
	return c.add(size, err)
}

func (c *SeriesByteCounter) AddSeries(row *DataSeries) error {
	if c.Limit <= 0 {
		return nil
	}
	size, err := DataSeriesBytes(row)
	return c.add(size, err)
}

func (c *SeriesByteCounter) add(size int64, err error) error {
	if err != nil {
		return err
	}
	c.Bytes = addSeriesBytes(c.Bytes, size)
	if c.Bytes > c.Limit {
		return &SeriesByteLimitError{c.Limit, c.Bytes}
	}
	return nil
}

func CheckDataRecordBytes(ctx context.Context, rows []*DataRecord) error {
	counter := SeriesByteCounter{Limit: SeriesReadByteLimit(ctx)}
	for _, row := range rows {
		if err := counter.AddRecord(row); err != nil {
			return err
		}
	}
	return nil
}

func CheckDataSeriesBytes(ctx context.Context, rows []*DataSeries) error {
	counter := SeriesByteCounter{Limit: SeriesReadByteLimit(ctx)}
	for _, row := range rows {
		if err := counter.AddSeries(row); err != nil {
			return err
		}
	}
	return nil
}

func DataRecordBytes(row *DataRecord) (int64, error) {
	if row == nil {
		return 0, fmt.Errorf("cannot budget a nil data record")
	}
	return seriesValueBytes(reflect.ValueOf(*row), make(map[seriesByteVisit]bool), 0)
}

func DataSeriesBytes(row *DataSeries) (int64, error) {
	if row == nil {
		return 0, fmt.Errorf("cannot budget a nil data series")
	}
	values, err := seriesValueBytes(reflect.ValueOf(row.Values), make(map[seriesByteVisit]bool), 0)
	base := int64(reflect.TypeOf(*row).Size() - reflect.TypeOf(row.Values).Size())
	return addSeriesBytes(base+int64(len(row.Source)+len(row.TimeFrame)), values), err
}

type seriesByteVisit struct {
	kind reflect.Kind
	ptr  uintptr
	typ  reflect.Type
}

func addSeriesBytes(a, b int64) int64 {
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

func seriesValueBytes(v reflect.Value, active map[seriesByteVisit]bool, depth int) (int64, error) {
	if !v.IsValid() {
		return 0, nil
	}
	if depth > 128 {
		return 0, fmt.Errorf("series byte accounting nesting exceeds 128 levels")
	}
	size := int64(v.Type().Size())
	var visit seriesByteVisit
	switch v.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer:
		if v.IsNil() {
			return size, nil
		}
		visit = seriesByteVisit{v.Kind(), uintptr(v.UnsafePointer()), v.Type()}
		if active[visit] {
			return 0, fmt.Errorf("series byte accounting found a payload cycle")
		}
		active[visit] = true
		defer delete(active, visit)
	}
	addChild := func(child reflect.Value, inline bool) error {
		childSize, err := seriesValueBytes(child, active, depth+1)
		if err != nil {
			return err
		}
		if inline {
			childSize -= int64(child.Type().Size())
		}
		size = addSeriesBytes(size, childSize)
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		size = addSeriesBytes(size, int64(v.Len()))
	case reflect.Interface:
		if !v.IsNil() {
			if err := addChild(v.Elem(), false); err != nil {
				return 0, err
			}
		}
	case reflect.Pointer:
		if err := addChild(v.Elem(), false); err != nil {
			return 0, err
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if err := addChild(iter.Key(), false); err != nil {
				return 0, err
			}
			if err := addChild(iter.Value(), false); err != nil {
				return 0, err
			}
		}
	case reflect.Slice:
		if int64(v.Cap()) > math.MaxInt64/int64(max(1, v.Type().Elem().Size())) {
			return math.MaxInt64, nil
		}
		size = addSeriesBytes(size, int64(v.Cap())*int64(v.Type().Elem().Size()))
		// Scalar slices (including byte blobs) are entirely in the backing
		// capacity already counted; visiting every byte adds no information.
		if kind := v.Type().Elem().Kind(); kind >= reflect.Bool && kind <= reflect.Complex128 {
			break
		}
		for i := 0; i < v.Len(); i++ {
			if err := addChild(v.Index(i), true); err != nil {
				return 0, err
			}
		}
	case reflect.Array:
		if kind := v.Type().Elem().Kind(); kind >= reflect.Bool && kind <= reflect.Complex128 {
			break
		}
		for i := 0; i < v.Len(); i++ {
			if err := addChild(v.Index(i), true); err != nil {
				return 0, err
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if err := addChild(v.Field(i), true); err != nil {
				return 0, err
			}
		}
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return 0, fmt.Errorf("series byte accounting cannot inspect %s", v.Type())
	}
	return size, nil
}
