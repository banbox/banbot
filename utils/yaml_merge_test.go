package utils

import (
	"fmt"
	"testing"
)

func TestMergeYamlStr(t *testing.T) {
	var paths = []string{
		"E:\\trade\\go\\bandata\\config.yml",
		"E:\\trade\\go\\bandata\\config.local.yml",
		"E:\\temp\\bbb\\config.yml",
	}
	data, err := MergeYamlStr(paths)
	if err != nil {
		panic(err)
	}
	fmt.Print(data)
}
