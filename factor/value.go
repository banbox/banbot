package factor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
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

// ContentHash fingerprints concrete types and values, including missing versus
// NULL fields, using the same canonical encoding as immutable version stores.
func ContentHash(value any) (string, error) { return contentHash(value) }

func contentHash(value any) (string, error) {
	var buffer bytes.Buffer
	if err := canonical(&buffer, reflect.ValueOf(value), make(map[visit]bool)); err != nil {
		return "", err
	}
	digest := sha256.Sum256(buffer.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

type visit struct {
	kind    reflect.Kind
	address uintptr
}

func canonical(buffer *bytes.Buffer, value reflect.Value, active map[visit]bool) error {
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
		return canonical(buffer, value.Elem(), active)
	case reflect.Pointer:
		return canonical(buffer, value.Elem(), active)
	case reflect.Map:
		type entry struct {
			key   []byte
			value reflect.Value
		}
		entries := make([]entry, 0, value.Len())
		for _, key := range value.MapKeys() {
			var encoded bytes.Buffer
			if err := canonical(&encoded, key, active); err != nil {
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
			if err := canonical(buffer, item.value, active); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		fmt.Fprintf(buffer, "%d;", value.Len())
		for i := 0; i < value.Len(); i++ {
			if err := canonical(buffer, value.Index(i), active); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			buffer.WriteString(value.Type().Field(i).Name)
			if err := canonical(buffer, value.Field(i), active); err != nil {
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
