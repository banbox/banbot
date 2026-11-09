package utils

import (
	"github.com/banbox/banbot/core"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	emptyLineRe = regexp.MustCompile(`\n(\s*\n)+`)
	keyStartRe  = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]`)
	commaRe     = regexp.MustCompile(`:($|\s)`)
)

// stripYamlComments removes real inline comments only: '#' at line start or preceded
// by whitespace, and not inside quotes. Values like abc#1 are kept intact.
func stripYamlComments(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		var quote byte
		for j := 0; j < len(line); j++ {
			c := line[j]
			if quote != 0 {
				if quote == '"' && c == '\\' {
					j++
				} else if c == quote {
					quote = 0
				}
				continue
			}
			if (c == '"' || c == '\'') && (j == 0 || strings.IndexByte(" \t[{,", line[j-1]) >= 0) {
				quote = c
			} else if c == '#' && (j == 0 || line[j-1] == ' ' || line[j-1] == '\t') {
				lines[i] = strings.TrimRight(line[:j], " \t")
				break
			}
		}
	}
	return strings.Join(lines, "\n")
}

// MergeYamlStr 合并多个yaml文件内容，保持键的顺序
func MergeYamlStr(paths []string, skips ...string) (string, error) {
	keys := make([]string, 0)
	resMap := make(map[string]string)
	lastIdx := -1

	for _, path := range paths {
		// 读取文件内容
		content, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}

		// 删除注释和多余空行
		text := strings.ReplaceAll(string(content), "\r\n", "\n")
		text = strings.ReplaceAll(text, "\r", "\n")
		text = stripYamlComments(text)
		text = emptyLineRe.ReplaceAllString(text, "\n")

		lines := strings.Split(text, "\n")
		lines = append(lines, "tmp: tmp")
		var currentKey string
		var valueBuilder strings.Builder

		for i := 0; i < len(lines); i++ {
			line := lines[i]
			if line == "" {
				continue
			}

			// 检查是否是新的键开始
			if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "-") && keyStartRe.MatchString(line) {
				// 保存之前的键值对
				if currentKey != "" {
					newVal := valueBuilder.String()
					valueBuilder.Reset()
					if _, exists := resMap[currentKey]; !exists {
						if lastIdx >= len(keys)-1 {
							keys = append(keys, currentKey)
							lastIdx = len(keys) - 1
						} else {
							keys = append(keys[:lastIdx+1], append([]string{currentKey}, keys[lastIdx+1:]...)...)
							lastIdx += 1
						}
					} else if strings.Contains(newVal, ": ") && strings.Contains(newVal, "\n") &&
						!strings.HasPrefix(strings.TrimSpace(newVal), "-") {
						// 当key已存在时，进行深度合并
						oldValue := resMap[currentKey]
						oldMap := make(map[string]interface{})
						newMap := make(map[string]interface{})

						if err := yaml.Unmarshal([]byte(oldValue), &oldMap); err == nil {
							if err := yaml.Unmarshal([]byte(newVal), &newMap); err == nil {
								DeepCopyMap(oldMap, newMap)
								if mergedData, err := core.MarshalYaml(oldMap); err == nil {
									mergedStr := strings.TrimRight(string(mergedData), "\n\r")
									newVal = "\n  " + strings.ReplaceAll(mergedStr, "\n", "\n  ")
								}
							}
						}
					}
					resMap[currentKey] = newVal
				}

				parts := commaRe.Split(line, 2)
				currentKey = parts[0]
				if len(parts) > 1 {
					valueBuilder.WriteString(parts[1])
				}
			} else {
				valueBuilder.WriteString("\n")
				valueBuilder.WriteString(line)
			}
		}
		// 无需保存最后一个键值对，因为是附加的
	}

	// 生成最终的yaml字符串
	skipMap := make(map[string]bool)
	for _, k := range skips {
		skipMap[k] = true
	}
	var result strings.Builder
	for _, key := range keys {
		if _, ok := skipMap[key]; ok {
			continue
		}
		value := strings.TrimRight(resMap[key], "\n\r")
		result.WriteString(key + ": " + value + "\n")
	}

	return result.String(), nil
}
