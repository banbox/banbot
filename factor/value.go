package factor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync"
)

type Validity string

const (
	Valid      Validity = "valid"
	Missing    Validity = "missing"
	Null       Validity = "null"
	NotNumeric Validity = "not-numeric"
	NonFinite  Validity = "non-finite"
	Warmup     Validity = "warmup"
)

// Numeric is an explicit derived view. The original Values map is retained.
type Numeric struct {
	Value    float64
	Validity Validity
}

// MarshalJSON keeps invalid derived numbers expressible as JSON without
// changing raw Values or collapsing their NULL/missing distinctions.
func (n Numeric) MarshalJSON() ([]byte, error) {
	var value *float64
	if n.Validity == Valid && !math.IsNaN(n.Value) && !math.IsInf(n.Value, 0) {
		value = &n.Value
	}
	return json.Marshal(struct {
		Value    *float64
		Validity Validity
	}{value, n.Validity})
}
func (n *Numeric) UnmarshalJSON(raw []byte) error {
	var value struct {
		Value    *float64
		Validity Validity
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	n.Validity = value.Validity
	n.Value = math.NaN()
	if value.Value != nil {
		n.Value = *value.Value
	}
	return nil
}

func Number(values map[string]any, field string) Numeric {
	value, exists := values[field]
	if !exists {
		return Numeric{math.NaN(), Missing}
	}
	if value == nil {
		return Numeric{math.NaN(), Null}
	}
	v := reflect.ValueOf(value)
	var number float64
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number = float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number = float64(v.Uint())
	case reflect.Float32, reflect.Float64:
		number = v.Float()
	default:
		return Numeric{math.NaN(), NotNumeric}
	}
	return numeric(number)
}

func numeric(value float64) Numeric {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return Numeric{math.NaN(), NonFinite}
	}
	return Numeric{value, Valid}
}

func normalized(value Numeric) Numeric {
	if value.Validity != Valid {
		value.Value = math.NaN()
		return value
	}
	return numeric(value.Value)
}

// Raw values are cloned and hashed by concrete type, not converted through
// JSON. Unsupported cyclic/opaque values fail explicitly instead of losing a
// field. Persistence is optional gob export of bounded source chunks; computed
// state never retains the full raw archive.
func cloneValue(value reflect.Value) (reflect.Value, error) {
	if !value.IsValid() {
		return value, nil
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		child, err := cloneValue(value.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(child)
		return result, nil
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		for _, key := range value.MapKeys() {
			copiedKey, err := cloneValue(key)
			if err != nil {
				return reflect.Value{}, err
			}
			child, err := cloneValue(value.MapIndex(key))
			if err != nil {
				return reflect.Value{}, err
			}
			result.SetMapIndex(copiedKey, child)
		}
		return result, nil
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			child, err := cloneValue(value.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(i).Set(child)
		}
		return result, nil
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			child, err := cloneValue(value.Index(i))
			if err != nil {
				return reflect.Value{}, err
			}
			result.Index(i).Set(child)
		}
		return result, nil
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type()), nil
		}
		child, err := cloneValue(value.Elem())
		if err != nil {
			return reflect.Value{}, err
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(child)
		return result, nil
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if !value.Type().Field(i).IsExported() && mutableReference(value.Field(i)) {
				return reflect.Value{}, fmt.Errorf("factor: raw type %s contains uncloneable private mutable state", value.Type())
			}
			if result.Field(i).CanSet() && value.Type().Field(i).IsExported() {
				child, err := cloneValue(value.Field(i))
				if err != nil {
					return reflect.Value{}, err
				}
				result.Field(i).Set(child)
			}
		}
		return result, nil
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return reflect.Value{}, fmt.Errorf("factor: unsupported raw type %s", value.Type())
	default:
		return value, nil
	}
}

func mutableReference(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer, reflect.Interface:
		return !value.IsNil()
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if mutableReference(value.Field(i)) {
				return true
			}
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if mutableReference(value.Index(i)) {
				return true
			}
		}
	}
	return false
}

func cloneValues(values map[string]any) (map[string]any, error) {
	// Scalar-only rows (every K-line / extension field) need neither the
	// validation hash nor reflection: scalars are immutable values, so a map
	// copy is an exact deep clone (nil stays nil, NULL stays a nil interface).
	if copied, ok := cloneScalarValues(values); ok {
		return copied, nil
	}
	// Hash validates acyclic supported contents before recursive cloning.
	if _, err := contentHash(values); err != nil {
		return nil, err
	}
	v, err := cloneValue(reflect.ValueOf(values))
	if err != nil {
		return nil, err
	}
	return v.Interface().(map[string]any), nil
}

// isScalarLeaf reports whether v is an immutable scalar of a basic kind whose
// canonical encoding is produced by canonicalScalar.
func isScalarLeaf(v any) bool {
	switch v.(type) {
	case nil, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, string, bool:
		return true
	}
	return false
}

func cloneScalarValues(values map[string]any) (map[string]any, bool) {
	if values == nil {
		return nil, true
	}
	for _, v := range values {
		if !isScalarLeaf(v) {
			return nil, false
		}
	}
	out := make(map[string]any, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out, true
}

// ContentHash fingerprints concrete types and values, including missing versus
// NULL fields, using the same canonical encoding as immutable version stores.
func ContentHash(value any) (string, error) { return contentHash(value) }

var hashBufPool = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 0, 512)) }}

func contentHash(value any) (string, error) {
	buffer := hashBufPool.Get().(*bytes.Buffer)
	buffer.Reset()
	defer hashBufPool.Put(buffer)
	var active map[visit]bool
	if err := canonical(buffer, reflect.ValueOf(value), &active); err != nil {
		return "", err
	}
	digest := sha256.Sum256(buffer.Bytes())
	var out [sha256.Size * 2]byte
	hex.Encode(out[:], digest[:])
	return string(out[:]), nil
}

type visit struct {
	kind    reflect.Kind
	address uintptr
}

// typeHeaders caches the "<pkgpath>:<type>;" prefix written before every value.
var typeHeaders sync.Map // reflect.Type -> string

func typeHeader(t reflect.Type) string {
	if h, ok := typeHeaders.Load(t); ok {
		return h.(string)
	}
	h := t.PkgPath() + ":" + t.String() + ";"
	typeHeaders.Store(t, h)
	return h
}

func writeInt(buffer *bytes.Buffer, v int64, suffix byte) {
	var tmp [24]byte
	b := strconv.AppendInt(tmp[:0], v, 10)
	b = append(b, suffix)
	buffer.Write(b)
}

func writeUint(buffer *bytes.Buffer, v uint64, suffix byte) {
	var tmp [24]byte
	b := strconv.AppendUint(tmp[:0], v, 10)
	b = append(b, suffix)
	buffer.Write(b)
}

// writeLenString writes "%d:%s;" without fmt.
func writeLenString(buffer *bytes.Buffer, s string) {
	var tmp [24]byte
	b := strconv.AppendInt(tmp[:0], int64(len(s)), 10)
	b = append(b, ':')
	buffer.Write(b)
	buffer.WriteString(s)
	buffer.WriteByte(';')
}

func writeFloatBits(buffer *bytes.Buffer, f float64) {
	var tmp [24]byte
	b := strconv.AppendUint(tmp[:0], math.Float64bits(f), 16)
	b = append(b, ';')
	buffer.Write(b)
}

var (
	typeAnyHeader     = typeHeader(reflect.TypeOf((*any)(nil)).Elem())
	typeMapStringAny  = reflect.TypeOf(map[string]any(nil))
	typeMapStrHeader  = typeHeader(typeMapStringAny)
	typeStringHeader  = typeHeader(reflect.TypeOf(""))
	typeFloat64Header = typeHeader(reflect.TypeOf(float64(0)))
	typeInt64Header   = typeHeader(reflect.TypeOf(int64(0)))
	typeIntHeader     = typeHeader(reflect.TypeOf(int(0)))
	typeBoolHeader    = typeHeader(reflect.TypeOf(false))
)

// canonicalScalar writes the exact bytes canonical() would write for an
// interface-typed map element holding a basic scalar (nil included). It
// returns false for anything else, which then takes the reflective path.
func canonicalScalar(buffer *bytes.Buffer, v any) bool {
	switch x := v.(type) {
	case nil:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString("nil;")
	case float64:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString(typeFloat64Header)
		writeFloatBits(buffer, x)
	case int64:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString(typeInt64Header)
		writeInt(buffer, x, ';')
	case int:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString(typeIntHeader)
		writeInt(buffer, int64(x), ';')
	case string:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString(typeStringHeader)
		writeLenString(buffer, x)
	case bool:
		buffer.WriteString(typeAnyHeader)
		buffer.WriteString(typeBoolHeader)
		if x {
			buffer.WriteString("true;")
		} else {
			buffer.WriteString("false;")
		}
	default:
		return false
	}
	return true
}

func canonical(buffer *bytes.Buffer, value reflect.Value, active *map[visit]bool) error {
	if !value.IsValid() {
		buffer.WriteString("nil;")
		return nil
	}
	buffer.WriteString(typeHeader(value.Type()))
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
		if *active == nil {
			*active = make(map[visit]bool)
		}
		if (*active)[key] {
			return fmt.Errorf("factor: cyclic raw value %s", value.Type())
		}
		(*active)[key] = true
		defer delete(*active, key)
	}
	switch kind {
	case reflect.Interface:
		if value.IsNil() {
			buffer.WriteString("nil;")
			return nil
		}
		return canonical(buffer, value.Elem(), active)
	case reflect.Pointer:
		return canonical(buffer, value.Elem(), active)
	case reflect.Map:
		type entry struct {
			key   []byte
			value reflect.Value
			raw   any
		}
		fast := value.Type() == typeMapStringAny && value.CanInterface()
		entries := make([]entry, 0, value.Len())
		var scratch bytes.Buffer
		if fast {
			// Encode every key into one shared buffer; slices are taken afterwards
			// because the buffer may grow while keys are appended.
			type span struct{ from, to int }
			spans := make([]span, 0, value.Len())
			for k, v := range value.Interface().(map[string]any) {
				from := scratch.Len()
				scratch.WriteString(typeStringHeader)
				writeLenString(&scratch, k)
				spans = append(spans, span{from, scratch.Len()})
				entries = append(entries, entry{raw: v})
			}
			all := scratch.Bytes()
			for i, sp := range spans {
				entries[i].key = all[sp.from:sp.to:sp.to]
			}
		} else {
			for _, key := range value.MapKeys() {
				var encoded bytes.Buffer
				if err := canonical(&encoded, key, active); err != nil {
					return err
				}
				entries = append(entries, entry{key: encoded.Bytes(), value: value.MapIndex(key)})
			}
		}
		slices.SortFunc(entries, func(a, b entry) int { return bytes.Compare(a.key, b.key) })
		for i := 1; i < len(entries); i++ {
			if bytes.Equal(entries[i-1].key, entries[i].key) {
				return fmt.Errorf("factor: map keys have ambiguous logical identity in %s", value.Type())
			}
		}
		for _, item := range entries {
			writeInt(buffer, int64(len(item.key)), ':')
			buffer.Write(item.key)
			if fast {
				if canonicalScalar(buffer, item.raw) {
					continue
				}
				holder := item.raw
				if err := canonical(buffer, reflect.ValueOf(&holder).Elem(), active); err != nil {
					return err
				}
				continue
			}
			if err := canonical(buffer, item.value, active); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		writeInt(buffer, int64(value.Len()), ';')
		for i := 0; i < value.Len(); i++ {
			if err := canonical(buffer, value.Index(i), active); err != nil {
				return err
			}
		}
	case reflect.Struct:
		t := value.Type()
		for i := 0; i < value.NumField(); i++ {
			buffer.WriteString(t.Field(i).Name)
			if err := canonical(buffer, value.Field(i), active); err != nil {
				return err
			}
		}
	case reflect.String:
		writeLenString(buffer, value.String())
	case reflect.Bool:
		if value.Bool() {
			buffer.WriteString("true;")
		} else {
			buffer.WriteString("false;")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		writeInt(buffer, value.Int(), ';')
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		writeUint(buffer, value.Uint(), ';')
	case reflect.Float32, reflect.Float64:
		writeFloatBits(buffer, value.Float())
	case reflect.Complex64, reflect.Complex128:
		fmt.Fprintf(buffer, "%v;", value.Complex())
	default:
		return fmt.Errorf("factor: unsupported raw type %s", value.Type())
	}
	return nil
}
