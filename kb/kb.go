// Package kb embeds the banbot strategy-engine knowledge base documents
// (time-series, factor, statarb) in Chinese and English, so they can be
// loaded from code (e.g. to feed AI agents or serve over an API).
package kb

import (
	"embed"
	"fmt"
	"sort"
	"strings"
)

//go:embed kb_*.md
var files embed.FS

// Separator is inserted between the contents of multiple documents returned by GetKB.
const Separator = "\n\n---\n\n"

// Supported languages.
const (
	LangCN = "cn"
	LangEN = "en"
)

// Names lists the available knowledge base topics.
func Names() []string {
	entries, _ := files.ReadDir(".")
	seen := map[string]bool{}
	for _, e := range entries {
		n := strings.TrimSuffix(strings.TrimPrefix(e.Name(), "kb_"), ".md")
		if i := strings.LastIndex(n, "_"); i > 0 {
			seen[n[:i]] = true
		}
	}
	res := make([]string, 0, len(seen))
	for k := range seen {
		res = append(res, k)
	}
	sort.Strings(res)
	return res
}

// GetKB returns the content of the given knowledge base topics (e.g. "ts", "factor", "statarb")
// in the given language ("cn" or "en"). Multiple topics are joined by Separator.
//
//	kb.GetKB("cn", "ts", "factor")
func GetKB(lang string, names ...string) (string, error) {
	if lang != LangCN && lang != LangEN {
		return "", fmt.Errorf("kb: unsupported lang %q, want cn or en", lang)
	}
	if len(names) == 0 {
		return "", fmt.Errorf("kb: at least one name required, available: %s", strings.Join(Names(), ","))
	}
	parts := make([]string, 0, len(names))
	for _, name := range names {
		data, err := files.ReadFile("kb_" + strings.ToLower(strings.TrimSpace(name)) + "_" + lang + ".md")
		if err != nil {
			return "", fmt.Errorf("kb: unknown name %q, available: %s", name, strings.Join(Names(), ","))
		}
		parts = append(parts, strings.TrimSpace(string(data)))
	}
	return strings.Join(parts, Separator) + "\n", nil
}
