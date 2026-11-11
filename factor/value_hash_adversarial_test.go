package factor

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

type myF float64
type myS string
type inner struct {
	A int
	B string
	c any
}

func TestHashAdversarialTypes(t *testing.T) {
	f := 1.5
	var np *float64
	var nm map[string]any
	var ns []any
	m := map[string]any{"a": 1}
	cases := map[string]any{
		"nilmap": nm, "nilslice": ns, "emptyslice": []any{}, "emptymap": map[string]any{},
		"nilmapinmap":   map[string]any{"x": nm},
		"ptr":           map[string]any{"x": &f, "y": np},
		"named":         map[string]any{"x": myF(1), "y": myS("s")},
		"int8":          map[string]any{"x": int8(-3), "y": uint8(3), "z": uint16(9), "w": int32(5), "v": uint32(1), "u": float32(math.NaN())},
		"int_vs_i64":    map[string]any{"x": int(1)},
		"i64":           map[string]any{"x": int64(1)},
		"f_vs_int":      map[string]any{"x": float64(1)},
		"negzero":       map[string]any{"x": math.Copysign(0, -1)},
		"zero":          map[string]any{"x": 0.0},
		"inf":           map[string]any{"x": math.Inf(1), "y": math.Inf(-1), "n": math.NaN()},
		"nan2":          map[string]any{"x": math.Float64frombits(0x7ff8000000000123)},
		"time":          map[string]any{"x": time.Unix(5, 7).UTC(), "d": time.Second},
		"jsonnum":       map[string]any{"x": json.Number("12")},
		"bytes":         map[string]any{"x": []byte("abc")},
		"arr":           map[string]any{"x": [2]int{1, 2}},
		"struct":        map[string]any{"x": inner{1, "b", 2}},
		"structptr":     map[string]any{"x": &inner{1, "b", nil}},
		"intkeymap":     map[string]any{"x": map[int]any{2: 1, 1: "a"}},
		"strstrmap":     map[string]any{"x": map[string]string{"a": "b"}},
		"nestedptrmap":  map[string]any{"x": &m},
		"nestedany":     map[string]any{"x": any(any(3))},
		"complex":       map[string]any{"x": complex(1, 2)},
		"emptykey":      map[string]any{"": 1, ";": 2, "1:a;": 3, "a": "1:b;"},
		"strdigits":     map[string]any{"a": "3:abc;"},
		"uintptr":       map[string]any{"x": uintptr(5)},
		"bool":          map[string]any{"t": true, "f": false},
		"deep":          map[string]any{"x": []any{map[string]any{"y": []any{nil, 1.0, "q"}}, []any{}, ns}},
		"chan":          map[string]any{"x": make(chan int)},
		"func":          map[string]any{"x": func() {}},
		"unexportedrec": VersionRecord{Revision: 2, SourceVersion: "v"},
		"sliceofrec":    []VersionRecord{{Revision: 1}, {Revision: 2}},
		"ptrrec":        &VersionRecord{Revision: 1},
		"nilany":        nil,
		"string":        "abc",
		"mapnonstr":     map[any]any{"a": 1, 2: "b"},
	}
	cyc := map[string]any{}
	cyc["self"] = cyc
	cases["cycle"] = cyc
	cs := []any{nil}
	cs[0] = cs
	cases["slicecycle"] = map[string]any{"s": cs}
	// shared (non-cyclic) substructure referenced twice must not be flagged
	shared := map[string]any{"k": 1.0}
	cases["shared"] = map[string]any{"a": shared, "b": shared}
	for name, v := range cases {
		w, we := legacyContentHash(v)
		g, ge := contentHash(v)
		if (we == nil) != (ge == nil) || w != g {
			t.Errorf("%s: legacy=%q/%v new=%q/%v", name, w, we, g, ge)
		}
		if mv, ok := v.(map[string]any); ok || v == nil {
			if v == nil {
				continue
			}
			wc, we2 := legacyCloneValues(mv)
			gc, ge2 := cloneValues(mv)
			if (we2 == nil) != (ge2 == nil) {
				t.Errorf("%s clone err parity legacy=%v new=%v", name, we2, ge2)
				continue
			}
			if we2 != nil {
				continue
			}
			a, _ := legacyContentHash(wc)
			b, _ := legacyContentHash(gc)
			if a != b || (wc == nil) != (gc == nil) || !reflect.DeepEqual(wc, gc) && name != "nan2" && name != "inf" && name != "int8" {
				t.Errorf("%s clone differs", name)
			}
		}
	}
}

func TestCloneAliasingIsolation(t *testing.T) {
	orig := map[string]any{"a": 1.0, "b": "x", "c": nil}
	cl, _ := cloneValues(orig)
	cl["a"] = 99.0
	cl["new"] = 1
	delete(cl, "b")
	if orig["a"] != 1.0 || orig["b"] != "x" || len(orig) != 3 {
		t.Fatal("scalar alias")
	}
	orig2 := map[string]any{"a": []any{1.0}, "m": map[string]any{"q": 1.0}}
	c2, _ := cloneValues(orig2)
	c2["a"].([]any)[0] = 7.0
	c2["m"].(map[string]any)["q"] = 7.0
	if orig2["a"].([]any)[0] != 1.0 || orig2["m"].(map[string]any)["q"] != 1.0 {
		t.Fatal("nested alias")
	}
	// record-level: CloneVersionRecord must not share Values map
	rec := VersionRecord{Revision: 1, SourceVersion: "v"}
	rec.Series.Sid, rec.Series.Source, rec.Series.TimeFrame = 1, "kline", "1m"
	rec.Series.Values = map[string]any{"close": 1.0}
	c3, err := CloneVersionRecord(rec)
	if err != nil {
		t.Fatal(err)
	}
	c3.Series.Values["close"] = 2.0
	if rec.Series.Values["close"] != 1.0 {
		t.Fatal("record alias")
	}
	// clone of clone is independent
	c4, _ := CloneVersionRecord(c3)
	c4.Series.Values["close"] = 3.0
	if c3.Series.Values["close"] != 2.0 {
		t.Fatal("clone2 alias")
	}
}

func TestHashConcurrentCalls(t *testing.T) {
	rec := VersionRecord{Revision: 1, SourceVersion: "v"}
	rec.Series.Values = map[string]any{"close": 1.0, "x": []any{1, "a"}, "t": time.Unix(1, 0)}
	want, _ := legacyContentHash(rec)
	done := make(chan bool, 16)
	for i := 0; i < 16; i++ {
		go func() {
			for j := 0; j < 2000; j++ {
				g, _ := contentHash(rec)
				if g != want {
					t.Error("concurrent mismatch")
					break
				}
			}
			done <- true
		}()
	}
	for i := 0; i < 16; i++ {
		<-done
	}
}
