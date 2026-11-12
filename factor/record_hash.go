package factor

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync"

	"github.com/banbox/banbot/orm"
)

// RecordHasher computes ContentHash(record) for VersionRecords whose Values hold
// only basic scalars (every K-line / extension row) without reflection, map
// iteration or per-call allocation. The digest is byte-identical to
// ContentHash(record): the canonical encoding is produced by the same layout
// (type headers, field names, key order) and then hashed with sha256, so
// consumers that fold per-record digests (the replay InputDigest) see the same
// value as before. Records outside the fast contract (non-scalar Values, symbol
// or adjustment pointers, private aggregation state) take the reflective path.
//
// A RecordHasher is not safe for concurrent use.
type RecordHasher struct {
	buf []byte
	// keys remembers the sorted canonical key encodings of the last Values map.
	// Another map with the same length whose every cached key is present has
	// exactly the same key set, so the sorted order is reused without iterating.
	keys []hashedKey
}

type hashedKey struct {
	name    string
	encoded []byte // "<len>:" + typeStringHeader + "<n>:<name>;"
}

// recordLayout caches the constant byte runs between the variable parts of the
// canonical encoding of VersionRecord (derived by reflection once, so a future
// struct change makes the fast path switch itself off instead of diverging).
var recordLayout = buildRecordLayout()

type recordLayoutBytes struct {
	ok bool
	// each piece ends right before the next variable value
	head       []byte // VersionRecord header + "Series" + DataSeries header + "Source" + string header
	sid        []byte // "Sid" + int32 header
	timeMS     []byte
	endMS      []byte
	timeFrame  []byte
	closed     []byte
	isWarmUp   []byte
	values     []byte // "Values" + map header
	tail       []byte // "ExSymbol" ptr nil, "Adj" ptr nil, "aggRows" nil, then EventTime...
	eventTime  []byte
	revision   []byte
	availableA []byte
	ingestedAt []byte
	sourceVer  []byte
	nilValues  []byte
}

func buildRecordLayout() recordLayoutBytes {
	vt := reflect.TypeOf(VersionRecord{})
	st := reflect.TypeOf(orm.DataSeries{})
	wantV := []struct {
		name string
		kind reflect.Kind
	}{{"Series", reflect.Struct}, {"EventTime", reflect.Int64}, {"Revision", reflect.Uint64}, {"AvailableAt", reflect.Int64}, {"IngestedAt", reflect.Int64}, {"SourceVersion", reflect.String}}
	if vt.NumField() != len(wantV) {
		return recordLayoutBytes{}
	}
	for i, w := range wantV {
		if f := vt.Field(i); f.Name != w.name || f.Type.Kind() != w.kind {
			return recordLayoutBytes{}
		}
	}
	wantS := []struct {
		name string
		typ  reflect.Type
	}{
		{"Source", reflect.TypeOf("")}, {"Sid", reflect.TypeOf(int32(0))}, {"TimeMS", reflect.TypeOf(int64(0))},
		{"EndMS", reflect.TypeOf(int64(0))}, {"TimeFrame", reflect.TypeOf("")}, {"Closed", reflect.TypeOf(false)},
		{"IsWarmUp", reflect.TypeOf(false)}, {"Values", reflect.TypeOf(map[string]any(nil))},
		{"ExSymbol", reflect.TypeOf((*orm.ExSymbol)(nil))}, {"Adj", reflect.TypeOf((*orm.AdjInfo)(nil))},
		{"aggRows", reflect.TypeOf([]*orm.DataSeries(nil))},
	}
	if st.NumField() != len(wantS) {
		return recordLayoutBytes{}
	}
	for i, w := range wantS {
		if f := st.Field(i); f.Name != w.name || f.Type != w.typ {
			return recordLayoutBytes{}
		}
	}
	h := typeHeader
	l := recordLayoutBytes{ok: true}
	l.head = []byte(h(vt) + "Series" + h(st) + "Source" + h(wantS[0].typ))
	l.sid = []byte("Sid" + h(wantS[1].typ))
	l.timeMS = []byte("TimeMS" + h(wantS[2].typ))
	l.endMS = []byte("EndMS" + h(wantS[3].typ))
	l.timeFrame = []byte("TimeFrame" + h(wantS[4].typ))
	l.closed = []byte("Closed" + h(wantS[5].typ))
	l.isWarmUp = []byte("IsWarmUp" + h(wantS[6].typ))
	l.values = []byte("Values" + h(wantS[7].typ))
	l.nilValues = []byte("nil;")
	l.tail = []byte("ExSymbol" + h(wantS[8].typ) + "nil;" + "Adj" + h(wantS[9].typ) + "nil;" + "aggRows" + h(wantS[10].typ) + "nil;" + "EventTime" + h(reflect.TypeOf(int64(0))))
	l.revision = []byte("Revision" + h(reflect.TypeOf(uint64(0))))
	l.availableA = []byte("AvailableAt" + h(reflect.TypeOf(int64(0))))
	l.ingestedAt = []byte("IngestedAt" + h(reflect.TypeOf(int64(0))))
	l.sourceVer = []byte("SourceVersion" + h(reflect.TypeOf("")))
	return l
}

func appendLenString(b []byte, s string) []byte {
	b = strconv.AppendInt(b, int64(len(s)), 10)
	b = append(b, ':')
	b = append(b, s...)
	return append(b, ';')
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, "true;"...)
	}
	return append(b, "false;"...)
}

// appendScalarAny appends exactly what canonicalScalar writes for an
// interface-typed map element; false means the value needs the reflective path.
func appendScalarAny(b []byte, v any) ([]byte, bool) {
	switch x := v.(type) {
	case nil:
		b = append(b, typeAnyHeader...)
		return append(b, "nil;"...), true
	case float64:
		b = append(b, typeAnyHeader...)
		b = append(b, typeFloat64Header...)
		b = strconv.AppendUint(b, math.Float64bits(x), 16)
		return append(b, ';'), true
	case int64:
		b = append(b, typeAnyHeader...)
		b = append(b, typeInt64Header...)
		b = strconv.AppendInt(b, x, 10)
		return append(b, ';'), true
	case int:
		b = append(b, typeAnyHeader...)
		b = append(b, typeIntHeader...)
		b = strconv.AppendInt(b, int64(x), 10)
		return append(b, ';'), true
	case string:
		b = append(b, typeAnyHeader...)
		b = append(b, typeStringHeader...)
		return appendLenString(b, x), true
	case bool:
		b = append(b, typeAnyHeader...)
		b = append(b, typeBoolHeader...)
		return appendBool(b, x), true
	}
	return b, false
}

// fastEncode appends the canonical encoding of r, or reports false when r is
// outside the fast contract.
func (h *RecordHasher) fastEncode(r *VersionRecord) bool {
	l := &recordLayout
	if !l.ok || r.Series.ExSymbol != nil || r.Series.Adj != nil || !r.Series.HasNoAggRows() {
		return false
	}
	b := h.buf[:0]
	b = append(b, l.head...)
	b = appendLenString(b, r.Series.Source)
	b = append(b, l.sid...)
	b = strconv.AppendInt(b, int64(r.Series.Sid), 10)
	b = append(b, ';')
	b = append(b, l.timeMS...)
	b = strconv.AppendInt(b, r.Series.TimeMS, 10)
	b = append(b, ';')
	b = append(b, l.endMS...)
	b = strconv.AppendInt(b, r.Series.EndMS, 10)
	b = append(b, ';')
	b = append(b, l.timeFrame...)
	b = appendLenString(b, r.Series.TimeFrame)
	b = append(b, l.closed...)
	b = appendBool(b, r.Series.Closed)
	b = append(b, l.isWarmUp...)
	b = appendBool(b, r.Series.IsWarmUp)
	b = append(b, l.values...)
	values := r.Series.Values
	if values == nil {
		b = append(b, l.nilValues...)
	} else {
		var ok bool
		if b, ok = h.appendValues(b, values); !ok {
			return false
		}
	}
	b = append(b, l.tail...)
	b = strconv.AppendInt(b, r.EventTime, 10)
	b = append(b, ';')
	b = append(b, l.revision...)
	b = strconv.AppendUint(b, r.Revision, 10)
	b = append(b, ';')
	b = append(b, l.availableA...)
	b = strconv.AppendInt(b, r.AvailableAt, 10)
	b = append(b, ';')
	b = append(b, l.ingestedAt...)
	b = strconv.AppendInt(b, r.IngestedAt, 10)
	b = append(b, ';')
	b = append(b, l.sourceVer...)
	b = appendLenString(b, r.SourceVersion)
	h.buf = b
	return true
}

func (h *RecordHasher) appendValues(b []byte, values map[string]any) ([]byte, bool) {
	if !h.sameKeys(values) {
		if !h.rebuildKeys(values) {
			return b, false
		}
	}
	for _, k := range h.keys {
		// the key length prefix of the map entry, then the encoded key, then the value
		b = strconv.AppendInt(b, int64(len(k.encoded)), 10)
		b = append(b, ':')
		b = append(b, k.encoded...)
		var ok bool
		if b, ok = appendScalarAny(b, values[k.name]); !ok {
			return b, false
		}
	}
	return b, true
}

func (h *RecordHasher) sameKeys(values map[string]any) bool {
	if len(values) != len(h.keys) {
		return false
	}
	for _, k := range h.keys {
		if _, ok := values[k.name]; !ok {
			return false
		}
	}
	return true
}

func (h *RecordHasher) rebuildKeys(values map[string]any) bool {
	keys := make([]hashedKey, 0, len(values))
	for name := range values {
		enc := append([]byte(typeStringHeader), strconv.Itoa(len(name))...)
		enc = append(enc, ':')
		enc = append(enc, name...)
		enc = append(enc, ';')
		keys = append(keys, hashedKey{name: name, encoded: enc})
	}
	slices.SortFunc(keys, func(a, b hashedKey) int { return compareBytes(a.encoded, b.encoded) })
	h.keys = keys
	return true
}

func compareBytes(a, b []byte) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

// Digest returns hex(sha256(canonical(record))), identical to ContentHash(record).
func (h *RecordHasher) Digest(r *VersionRecord) ([sha256.Size * 2]byte, error) {
	var out [sha256.Size * 2]byte
	if h.fastEncode(r) {
		sum := sha256.Sum256(h.buf)
		hex.Encode(out[:], sum[:])
		return out, nil
	}
	s, err := contentHash(*r)
	if err != nil {
		return out, err
	}
	copy(out[:], s)
	return out, nil
}

var recordHasherPool = sync.Pool{New: func() any { return &RecordHasher{} }}

// contentHashRecord is contentHash for a VersionRecord value.
func contentHashRecord(r VersionRecord) (string, bool) {
	h := recordHasherPool.Get().(*RecordHasher)
	defer recordHasherPool.Put(h)
	if !h.fastEncode(&r) {
		return "", false
	}
	sum := sha256.Sum256(h.buf)
	return hex.EncodeToString(sum[:]), true
}
