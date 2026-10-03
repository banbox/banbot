package entry

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"

	"github.com/banbox/banbot/factor"
	"gopkg.in/yaml.v3"
)

// Archive JSON has no concrete number types. An optional per-source schema
// restores them before hashing/archiving; the legacy float default rejects
// integers whose exact value would be lost.
type archiveFieldTypes map[string]map[string]string

func readArchiveFieldTypes(path string) (archiveFieldTypes, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var schema archiveFieldTypes
	if err := yaml.Unmarshal(raw, &schema); err != nil {
		return nil, err
	}
	for source, fields := range schema {
		if source == "" || len(fields) == 0 {
			return nil, fmt.Errorf("archive schema requires source fields")
		}
		for name, kind := range fields {
			if name == "" || !validArchiveType(kind) {
				return nil, fmt.Errorf("archive schema %s.%s: unsupported type %q", source, name, kind)
			}
		}
	}
	return schema, nil
}

func validArchiveType(kind string) bool {
	switch kind {
	case "int", "int8", "int16", "int32", "int64", "uint", "uint8", "uint16", "uint32", "uint64", "float", "float32", "float64", "string", "bool", "json":
		return true
	}
	return false
}

func restoreArchiveRecordTypes(row *factor.VersionRecord, schema archiveFieldTypes) error {
	fields, declared := schema[row.Series.Source]
	if schema != nil && !declared {
		return fmt.Errorf("archive schema missing source %s", row.Series.Source)
	}
	for field, value := range row.Series.Values {
		kind, known := fields[field]
		if declared && !known {
			return fmt.Errorf("archive schema missing field %s.%s", row.Series.Source, field)
		}
		if value == nil {
			continue
		}
		restored, err := restoreArchiveValue(value, kind)
		if err != nil {
			return fmt.Errorf("archive %s.%s: %w", row.Series.Source, field, err)
		}
		row.Series.Values[field] = restored
	}
	return nil
}

func restoreArchiveValue(value any, kind string) (any, error) {
	if value == nil {
		return nil, nil
	}
	if kind == "string" || kind == "bool" {
		if reflect.TypeOf(value).Kind().String() != kind {
			return nil, fmt.Errorf("expected %s, got %T", kind, value)
		}
		return value, nil
	}
	if number, ok := value.(json.Number); ok {
		if strings.HasPrefix(kind, "int") || strings.HasPrefix(kind, "uint") {
			unsigned := strings.HasPrefix(kind, "uint")
			bits := strconv.IntSize
			prefix := "int"
			if unsigned {
				prefix = "uint"
			}
			if suffix := strings.TrimPrefix(kind, prefix); suffix != "" {
				bits, _ = strconv.Atoi(suffix)
			}
			var result reflect.Value
			if unsigned {
				n, err := strconv.ParseUint(string(number), 10, bits)
				if err != nil {
					return nil, err
				}
				switch kind {
				case "uint":
					result = reflect.ValueOf(uint(n))
				case "uint8":
					result = reflect.ValueOf(uint8(n))
				case "uint16":
					result = reflect.ValueOf(uint16(n))
				case "uint32":
					result = reflect.ValueOf(uint32(n))
				default:
					result = reflect.ValueOf(n)
				}
			} else {
				n, err := strconv.ParseInt(string(number), 10, bits)
				if err != nil {
					return nil, err
				}
				switch kind {
				case "int":
					result = reflect.ValueOf(int(n))
				case "int8":
					result = reflect.ValueOf(int8(n))
				case "int16":
					result = reflect.ValueOf(int16(n))
				case "int32":
					result = reflect.ValueOf(int32(n))
				default:
					result = reflect.ValueOf(n)
				}
			}
			return result.Interface(), nil
		}
		if kind != "" && kind != "json" && kind != "float" && kind != "float32" && kind != "float64" {
			return nil, fmt.Errorf("expected %s, got number", kind)
		}
		if kind == "json" && !strings.ContainsAny(string(number), ".eE") {
			n, err := number.Int64()
			if err != nil {
				return nil, err
			}
			return n, nil
		}
		bits := 64
		if kind == "float32" {
			bits = 32
		}
		n, err := strconv.ParseFloat(string(number), bits)
		if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
			return nil, fmt.Errorf("invalid finite %s number %s", kind, number)
		}
		if kind == "" && !strings.ContainsAny(string(number), ".eE") {
			i, err := number.Int64()
			if err != nil || i > 9007199254740992 || i < -9007199254740992 {
				return nil, fmt.Errorf("integer %s requires an explicit archive schema to preserve its type and value", number)
			}
		}
		if bits == 32 {
			return float32(n), nil
		}
		return n, nil
	}
	if kind != "" && kind != "json" {
		return nil, fmt.Errorf("expected %s, got %T", kind, value)
	}
	switch item := value.(type) {
	case map[string]any:
		for key, child := range item {
			got, err := restoreArchiveValue(child, "json")
			if err != nil {
				return nil, err
			}
			item[key] = got
		}
	case []any:
		for index, child := range item {
			got, err := restoreArchiveValue(child, "json")
			if err != nil {
				return nil, err
			}
			item[index] = got
		}
	}
	return value, nil
}
