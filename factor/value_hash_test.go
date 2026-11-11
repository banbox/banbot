package factor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

// legacyContentHash is the pre-optimisation reference implementation (fmt based).
func legacyContentHash(value any) (string, error) {
	var buffer bytes.Buffer
	if err := legacyCanonical(&buffer, reflect.ValueOf(value), make(map[visit]bool)); err != nil {
		return "", err
	}
	digest := sha256.Sum256(buffer.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

func legacyCanonical(buffer *bytes.Buffer, value reflect.Value, active map[visit]bool) error {
	if !value.IsValid() {
		buffer.WriteString("nil;")
		return nil
	}
	fmt.Fprintf(buffer, "%s:%s;", value.Type().PkgPath(), value.Type())
	kind := value.Kind()
	if kind == reflect.Map || kind == reflect.Slice || kind == reflect.Pointer {
		if value.IsNil() {
			buffer.WriteString("nil;")
			return nil
		}
		var address uintptr
		if kind == reflect.Map {
			address = uintptr(value.UnsafePointer())
		} else {
			address = value.Pointer()
		}
		key := visit{kind, address}
		if active[key] {
			return fmt.Errorf("factor: cyclic raw value %s", value.Type())
		}
		active[key] = true
		defer delete(active, key)
	}
	switch kind {
	case reflect.Interface:
		if value.IsNil() {
			buffer.WriteString("nil;")
			return nil
		}
		return legacyCanonical(buffer, value.Elem(), active)
	case reflect.Pointer:
		return legacyCanonical(buffer, value.Elem(), active)
	case reflect.Map:
		type entry struct {
			key   []byte
			value reflect.Value
		}
		entries := make([]entry, 0, value.Len())
		for _, key := range value.MapKeys() {
			var encoded bytes.Buffer
			if err := legacyCanonical(&encoded, key, active); err != nil {
				return err
			}
			entries = append(entries, entry{encoded.Bytes(), value.MapIndex(key)})
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		for i := 1; i < len(entries); i++ {
			if bytes.Equal(entries[i-1].key, entries[i].key) {
				return fmt.Errorf("factor: map keys have ambiguous logical identity in %s", value.Type())
			}
		}
		for _, item := range entries {
			fmt.Fprintf(buffer, "%d:", len(item.key))
			buffer.Write(item.key)
			if err := legacyCanonical(buffer, item.value, active); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		fmt.Fprintf(buffer, "%d;", value.Len())
		for i := 0; i < value.Len(); i++ {
			if err := legacyCanonical(buffer, value.Index(i), active); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			buffer.WriteString(value.Type().Field(i).Name)
			if err := legacyCanonical(buffer, value.Field(i), active); err != nil {
				return err
			}
		}
	case reflect.String:
		fmt.Fprintf(buffer, "%d:%s;", value.Len(), value.String())
	case reflect.Bool:
		fmt.Fprintf(buffer, "%t;", value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprintf(buffer, "%d;", value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		fmt.Fprintf(buffer, "%d;", value.Uint())
	case reflect.Float32, reflect.Float64:
		buffer.WriteString(strconv.FormatUint(math.Float64bits(value.Float()), 16))
		buffer.WriteByte(';')
	case reflect.Complex64, reflect.Complex128:
		fmt.Fprintf(buffer, "%v;", value.Complex())
	default:
		return fmt.Errorf("factor: unsupported raw type %s", value.Type())
	}
	return nil
}

type hashProbe struct {
	A int
	b map[string]any
	C []any
}

func randomLeaf(r *rand.Rand) any {
	switch r.Intn(14) {
	case 0:
		return nil
	case 1:
		return r.NormFloat64() * 1e6
	case 2:
		return math.NaN()
	case 3:
		return math.Inf(-1)
	case 4:
		return int64(r.Int63() - r.Int63())
	case 5:
		return r.Intn(1000) - 500
	case 6:
		return fmt.Sprintf("s%d\x00é;:", r.Intn(50))
	case 7:
		return r.Intn(2) == 0
	case 8:
		return float32(r.Float32())
	case 9:
		return uint64(r.Uint64())
	case 10:
		return time.Unix(int64(r.Intn(1e9)), 0).UTC()
	case 11:
		return []any{1.5, "x", nil}
	case 12:
		return map[string]any{"in": r.Float64(), "n": nil}
	default:
		return math.Copysign(0, -1)
	}
}

func randomValues(r *rand.Rand) map[string]any {
	if r.Intn(20) == 0 {
		return nil
	}
	m := map[string]any{}
	for i, n := 0, r.Intn(9); i < n; i++ {
		m[fmt.Sprintf("k%d%s", r.Intn(12), []string{"", "é", "close", "10"}[r.Intn(4)])] = randomLeaf(r)
	}
	return m
}

func TestContentHashMatchesLegacy(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 20000; i++ {
		v := randomValues(r)
		want, werr := legacyContentHash(v)
		got, gerr := contentHash(v)
		if (werr == nil) != (gerr == nil) || want != got {
			t.Fatalf("map %d hash mismatch %v/%v %v", i, want, got, v)
		}
		rec := VersionRecord{Revision: 1, EventTime: int64(i), SourceVersion: "v"}
		rec.Series.Values = v
		rec.Series.Source = "kline"
		w2, _ := legacyContentHash(rec)
		g2, _ := contentHash(rec)
		if w2 != g2 {
			t.Fatalf("record %d hash mismatch", i)
		}
	}
	probe := hashProbe{A: 3, b: map[string]any{"x": 1.0}, C: []any{1, "a"}}
	w, _ := legacyContentHash(probe)
	g, _ := contentHash(probe)
	if w != g {
		t.Fatal("unexported map field hash mismatch")
	}
}

func TestCloneValuesMatchesLegacy(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	for i := 0; i < 20000; i++ {
		v := randomValues(r)
		got, err := cloneValues(v)
		if err != nil {
			t.Fatal(err)
		}
		want, werr := legacyCloneValues(v)
		if werr != nil {
			t.Fatal(werr)
		}
		if (got == nil) != (want == nil) {
			t.Fatalf("nil-ness differs")
		}
		a, _ := legacyContentHash(got)
		b, _ := legacyContentHash(want)
		if a != b {
			t.Fatalf("clone %d differs", i)
		}
		if len(v) > 0 && reflect.ValueOf(got).Pointer() == reflect.ValueOf(v).Pointer() {
			t.Fatal("clone aliases the input map")
		}
	}
}

func legacyCloneValues(values map[string]any) (map[string]any, error) {
	if _, err := legacyContentHash(values); err != nil {
		return nil, err
	}
	v, err := cloneValue(reflect.ValueOf(values))
	if err != nil {
		return nil, err
	}
	return v.Interface().(map[string]any), nil
}

func BenchmarkContentHashRecord(b *testing.B) {
	rec := VersionRecord{Revision: 1, EventTime: 1, SourceVersion: "storage-latest:abc"}
	rec.Series.Source = "kline"
	rec.Series.TimeFrame = "1m"
	rec.Series.Sid = 5
	rec.Series.Values = map[string]any{"close": 100.25, "open": 100.0, "high": 101.0, "low": 99.5, "volume": 12345.5}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := contentHash(rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLegacyContentHashRecord(b *testing.B) {
	rec := VersionRecord{Revision: 1, EventTime: 1, SourceVersion: "storage-latest:abc"}
	rec.Series.Source = "kline"
	rec.Series.TimeFrame = "1m"
	rec.Series.Sid = 5
	rec.Series.Values = map[string]any{"close": 100.25, "open": 100.0, "high": 101.0, "low": 99.5, "volume": 12345.5}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := legacyContentHash(rec); err != nil {
			b.Fatal(err)
		}
	}
}
