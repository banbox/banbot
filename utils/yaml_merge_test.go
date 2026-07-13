package utils

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMergeYamlStr(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "config.yml"),
		filepath.Join(dir, "config.local.yml"),
	}
	fixtures := []string{
		"app:\n  host: localhost\n  port: 8080\nitems:\n  - base\nenabled: true\n",
		"app:\n  port: 9090\n  debug: true\nitems:\n  - local\nnew_key: value\n",
	}
	for i, path := range paths {
		if err := os.WriteFile(path, []byte(fixtures[i]), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	data, err := MergeYamlStr(paths)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err = yaml.Unmarshal([]byte(data), &got); err != nil {
		t.Fatalf("merged output is invalid YAML: %v\n%s", err, data)
	}
	want := map[string]interface{}{
		"app":     map[string]interface{}{"host": "localhost", "port": 9090, "debug": true},
		"items":   []interface{}{"local"},
		"enabled": true,
		"new_key": "value",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected merged YAML:\ngot:  %#v\nwant: %#v\noutput:\n%s", got, want, data)
	}
}
