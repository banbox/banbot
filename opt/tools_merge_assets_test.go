package opt

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMergeAssetsHtmlRejectsSingleSampleData(t *testing.T) {
	dir := t.TempDir()
	content := `<script>chartData = {"labels":["2024-01-01"],"datasets":[{"label":"Real","data":[1]}]};</script>`
	paths := []string{filepath.Join(dir, "first.html"), filepath.Join(dir, "second.html")}
	for _, path := range paths {
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := MergeAssetsHtml(filepath.Join(dir, "merged.html"), map[string]string{paths[0]: "first", paths[1]: "second"}, nil, false); err == nil {
		t.Fatal("single-sample assets data was accepted")
	}
}
