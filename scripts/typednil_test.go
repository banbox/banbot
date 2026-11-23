// Typed-nil gate. Run from the repository root:
//
//	go test ./scripts/ -run TestNoUnguardedErrsErrorToError -count=1
//
// It type-checks the whole module once and fails when a value of static type
// *errs.Error (github.com/banbox/banexg/errs) flows into a destination of
// static type error without a nil guard. A nil *errs.Error converted to error
// is a NON-nil error, which silently turns "success" into "failure"
// (e.g. `return session.Start()` in a func() error).
//
// Reported kinds: assign, assign-tuple, return, return-tuple, var, var-tuple,
// field and send. Call arguments ("arg") are not checked, and a return of an
// identifier inside an `if x != nil` / `else` of `x == nil` is treated as
// guarded, as is a direct errs.New*(...) call.
//
// When a new site is flagged, prefer fixing it:
//
//	if e := f(); e != nil { return e }; return nil
//
// Only if the site is provably nil-guarded in a way the checker cannot see,
// add it to typedNilAllowlist with a reason. Entries are keyed by
// file|enclosing function|kind: expression text (never by line number).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var typedNilAllowlist = map[string]string{
	"config/biz.go|(*BadParamError).Unwrap|return: e.Err":                                      "preceded by `if e == nil || e.Err == nil { return nil }` (guard on a selector, which the checker does not track)",
	"data/live_generation.go|(*LiveProvider).PrepareSubscriptionPlan|assign: err = warmErr":    "only executed inside `if warmErr := ...; warmErr != nil`",
	"entry/main.go|ExecuteContext|assign: execErr = badParam.BanError()":                       "BanError returns nil only for a nil receiver; badParam is non-nil when ok is true",
	"entry/unified_backtest.go|finishUnifiedFactorBacktestSkipped|assign: err = writeErr":      "only executed inside `if writeErr != nil`",
	"entry/unified_backtest.go|finishUnifiedFactorBacktestSkipped|assign: primary = resultErr": "only executed inside `if resultErr != nil`",
	"entry/unified_backtest.go|unifiedFactorBacktestOutputContext|assign: primary = resultErr": "only executed inside `if resultErr != nil`",
	"execution/banexg_adapter.go|(*BanexgAdapter).Submit|assign: sdkErr = e":                   "only executed inside `if e != nil`",
	"execution/banexg_adapter.go|(*BanexgAdapter).fetchAccount|assign: e = oe":                 "only executed inside `if oe != nil`",
	"rpc/telegram.go|(*Telegram).send|assign: err = err2":                                      "only executed inside `if err2 != nil`",
}

func TestNoUnguardedErrsErrorToError(t *testing.T) {
	if testing.Short() {
		t.Skip("typed-nil gate loads the whole module; skipped in -short")
	}
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "go.mod")); statErr != nil {
		t.Fatalf("repository root not found at %s: %v", root, statErr)
	}
	pkgs := loadModule(t, root)
	found := map[string]bool{}
	checked := 0
	for _, p := range pkgs {
		checked++
		for _, f := range p.files {
			rel, relErr := filepath.Rel(root, p.fset.Position(f.Pos()).Filename)
			if relErr != nil || strings.HasPrefix(rel, "..") || strings.HasSuffix(rel, "_test.go") {
				continue
			}
			for _, key := range typedNilSites(p.info, f, filepath.ToSlash(rel)) {
				found[key] = true
			}
		}
	}
	if checked < 20 {
		t.Fatalf("only %d packages were type-checked; the gate is not effective", checked)
	}
	var fresh, stale []string
	for key := range found {
		if _, ok := typedNilAllowlist[key]; !ok {
			fresh = append(fresh, key)
		}
	}
	for key := range typedNilAllowlist {
		if !found[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(fresh)
	sort.Strings(stale)
	for _, key := range fresh {
		t.Errorf("unguarded *errs.Error -> error conversion (a nil pointer becomes a non-nil error): %s", key)
	}
	for _, key := range stale {
		t.Errorf("stale allowlist entry (site fixed or renamed); remove or update it: %s", key)
	}
}

func isErrsErrorPtr(typ types.Type) bool {
	ptr, ok := typ.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	return ok && named.Obj().Name() == "Error" && named.Obj().Pkg() != nil &&
		strings.HasSuffix(named.Obj().Pkg().Path(), "banexg/errs")
}

func isErrorIface(typ types.Type) bool {
	return typ != nil && types.Identical(typ, types.Universe.Lookup("error").Type())
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	switch rt := fd.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		return "(*" + types.ExprString(rt.X) + ")." + fd.Name.Name
	default:
		return types.ExprString(rt) + "." + fd.Name.Name
	}
}

// typedNilSites returns the allowlist keys of every flagged site in one file.
func typedNilSites(info *types.Info, file *ast.File, rel string) []string {
	var out []string
	var stack []ast.Node
	add := func(kind string, expr string) {
		fn := "<package-level>"
		for i := len(stack) - 1; i >= 0; i-- {
			if fd, ok := stack[i].(*ast.FuncDecl); ok {
				fn = funcName(fd)
				break
			}
		}
		out = append(out, fmt.Sprintf("%s|%s|%s: %s", rel, fn, kind, expr))
	}
	sigs := func() *types.Signature {
		for i := len(stack) - 1; i >= 0; i-- {
			switch x := stack[i].(type) {
			case *ast.FuncLit:
				s, _ := info.TypeOf(x).(*types.Signature)
				return s
			case *ast.FuncDecl:
				if obj := info.Defs[x.Name]; obj != nil {
					s, _ := obj.Type().(*types.Signature)
					return s
				}
				return nil
			}
		}
		return nil
	}
	guarded := func(id *ast.Ident) bool {
		obj := info.Uses[id]
		if obj == nil {
			return false
		}
		for i := len(stack) - 2; i >= 0; i-- {
			ifs, ok := stack[i].(*ast.IfStmt)
			if !ok {
				continue
			}
			child := stack[i+1]
			switch child {
			case ifs.Body:
				if nilCheck(ifs.Cond, token.NEQ, true, obj, info) {
					return true
				}
			case ifs.Else:
				if nilCheck(ifs.Cond, token.EQL, false, obj, info) {
					return true
				}
			}
		}
		return false
	}
	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		stack = append(stack, n)
		switch x := n.(type) {
		case *ast.AssignStmt:
			if len(x.Rhs) == 1 && len(x.Lhs) > 1 {
				if tup, ok := info.TypeOf(x.Rhs[0]).(*types.Tuple); ok {
					for i, l := range x.Lhs {
						if id, ok := l.(*ast.Ident); ok && id.Name == "_" || i >= tup.Len() {
							continue
						}
						if isErrorIface(info.TypeOf(l)) && isErrsErrorPtr(tup.At(i).Type()) {
							add("assign-tuple", types.ExprString(l)+" = "+types.ExprString(x.Rhs[0]))
						}
					}
				}
			} else {
				for i, l := range x.Lhs {
					if id, ok := l.(*ast.Ident); ok && id.Name == "_" || i >= len(x.Rhs) {
						continue
					}
					if isErrorIface(info.TypeOf(l)) && isErrsErrorPtr(info.TypeOf(x.Rhs[i])) {
						add("assign", types.ExprString(l)+" = "+types.ExprString(x.Rhs[i]))
					}
				}
			}
		case *ast.ReturnStmt:
			sig := sigs()
			if sig == nil {
				break
			}
			res := sig.Results()
			if len(x.Results) == 1 && res.Len() > 1 {
				if tup, ok := info.TypeOf(x.Results[0]).(*types.Tuple); ok {
					for i := 0; i < tup.Len() && i < res.Len(); i++ {
						if isErrorIface(res.At(i).Type()) && isErrsErrorPtr(tup.At(i).Type()) {
							add("return-tuple", types.ExprString(x.Results[0]))
						}
					}
				}
				break
			}
			for i, r := range x.Results {
				if i >= res.Len() || !isErrorIface(res.At(i).Type()) || !isErrsErrorPtr(info.TypeOf(r)) {
					continue
				}
				switch v := r.(type) {
				case *ast.Ident:
					if guarded(v) {
						continue
					}
				case *ast.CallExpr:
					if se, ok := v.Fun.(*ast.SelectorExpr); ok && strings.HasPrefix(se.Sel.Name, "New") {
						if pk, ok := se.X.(*ast.Ident); ok && pk.Name == "errs" {
							continue
						}
					}
				}
				add("return", types.ExprString(r))
			}
		case *ast.ValueSpec:
			if x.Type != nil && isErrorIface(info.TypeOf(x.Type)) {
				for _, v := range x.Values {
					if isErrsErrorPtr(info.TypeOf(v)) {
						add("var", types.ExprString(v))
					}
				}
				if len(x.Values) == 1 && len(x.Names) > 1 {
					if tup, ok := info.TypeOf(x.Values[0]).(*types.Tuple); ok {
						for i := 0; i < tup.Len(); i++ {
							if isErrsErrorPtr(tup.At(i).Type()) {
								add("var-tuple", types.ExprString(x.Values[0]))
							}
						}
					}
				}
			}
		case *ast.CompositeLit:
			if st, ok := info.TypeOf(x).Underlying().(*types.Struct); ok {
				for i, el := range x.Elts {
					if kv, ok := el.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok {
							for j := 0; j < st.NumFields(); j++ {
								if st.Field(j).Name() == id.Name && isErrorIface(st.Field(j).Type()) && isErrsErrorPtr(info.TypeOf(kv.Value)) {
									add("field", id.Name+": "+types.ExprString(kv.Value))
								}
							}
						}
					} else if i < st.NumFields() && isErrorIface(st.Field(i).Type()) && isErrsErrorPtr(info.TypeOf(el)) {
						add("field", types.ExprString(el))
					}
				}
			}
		case *ast.SendStmt:
			if ch, ok := info.TypeOf(x.Chan).Underlying().(*types.Chan); ok && isErrorIface(ch.Elem()) && isErrsErrorPtr(info.TypeOf(x.Value)) {
				add("send", types.ExprString(x.Chan)+" <- "+types.ExprString(x.Value))
			}
		}
		return true
	})
	return out
}

// nilCheck reports whether cond proves obj != nil (want body: op NEQ joined by
// &&) or, for an else branch, that obj == nil was the condition (op EQL joined by ||).
func nilCheck(cond ast.Expr, op token.Token, conj bool, obj types.Object, info *types.Info) bool {
	switch b := cond.(type) {
	case *ast.ParenExpr:
		return nilCheck(b.X, op, conj, obj, info)
	case *ast.BinaryExpr:
		join := token.LOR
		if conj {
			join = token.LAND
		}
		if b.Op == join {
			return nilCheck(b.X, op, conj, obj, info) || nilCheck(b.Y, op, conj, obj, info)
		}
		if b.Op == op {
			for _, p := range [][2]ast.Expr{{b.X, b.Y}, {b.Y, b.X}} {
				id, ok := p[0].(*ast.Ident)
				nilID, ok2 := p[1].(*ast.Ident)
				if ok && ok2 && info.Uses[id] == obj && nilID.Name == "nil" {
					return true
				}
			}
		}
	}
	return false
}

type loadedPkg struct {
	fset  *token.FileSet
	files []*ast.File
	info  *types.Info
}

type listedPkg struct {
	ImportPath      string
	Dir             string
	Export          string
	CompiledGoFiles []string
	DepOnly         bool
	Standard        bool
	ImportMap       map[string]string
	Error           *struct{ Err string }
}

// loadModule type-checks every non-test package of the module using only the
// standard library: `go list -export -compiled` supplies compiler export data
// for all dependencies (from the build cache) and go/types checks the module's
// own sources, so no golang.org/x/tools dependency is needed.
func loadModule(t *testing.T, root string) []*loadedPkg {
	t.Helper()
	cmd := exec.Command("go", "list", "-e", "-json=ImportPath,Dir,Export,CompiledGoFiles,DepOnly,Standard,ImportMap,Error", "-export", "-compiled", "-deps", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list failed: %v\n%s", err, stderr.String())
	}
	exports := map[string]string{}
	var roots []listedPkg
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var p listedPkg
		if decErr := dec.Decode(&p); decErr == io.EOF {
			break
		} else if decErr != nil {
			t.Fatal(decErr)
		}
		if p.Export != "" {
			exports[p.ImportPath] = p.Export
		}
		if !p.DepOnly {
			roots = append(roots, p)
		}
	}
	fset := token.NewFileSet()
	imp := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(file)
	})
	var res []*loadedPkg
	for _, p := range roots {
		if p.Error != nil {
			t.Logf("skipping %s (not buildable: %s)", p.ImportPath, strings.TrimSpace(p.Error.Err))
			continue
		}
		var files []*ast.File
		for _, name := range p.CompiledGoFiles {
			if !filepath.IsAbs(name) {
				name = filepath.Join(p.Dir, name)
			}
			f, parseErr := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
			if parseErr != nil {
				t.Logf("skipping file %s: %v", name, parseErr)
				continue
			}
			files = append(files, f)
		}
		if len(files) == 0 {
			continue
		}
		info := &types.Info{
			Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{},
		}
		conf := types.Config{Importer: imp, Error: func(e error) { t.Logf("type error (the gate may miss sites): %v", e) }}
		_, _ = conf.Check(p.ImportPath, fset, files, info)
		res = append(res, &loadedPkg{fset: fset, files: files, info: info})
	}
	return res
}
