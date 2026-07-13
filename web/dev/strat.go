package dev

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/utils"
)

var (
	reStratName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]+$`)
	errNoInit   = errors.New("no init found")
	errBadRoot  = errors.New("invalid strategy root dir, go.mod & main.go must be included")
)

func getRootDir() (string, error) {
	stratDir := config.GetStratDir()
	if stratDir != "" {
		return stratDir, nil
	}

	// 获取工作目录
	workDir, err := os.Getwd()
	if workDir == "" || err != nil {
		// 获取可执行文件路径
		execPath, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("get strategy root dir fail: %v", err)
		}
		workDir = filepath.Dir(execPath)
	}

	// 检查 go.mod main.go 文件是否存在
	_, err = os.Stat(filepath.Join(workDir, "go.mod"))
	if err != nil && os.IsNotExist(err) {
		return "", errBadRoot
	}
	_, err = os.Stat(filepath.Join(workDir, "main.go"))
	if err != nil && os.IsNotExist(err) {
		return "", errBadRoot
	}
	return workDir, nil
}

func makeNewStrat(folder, name string) error {
	// 验证策略名称格式
	if ok := reStratName.MatchString(name); !ok {
		return fmt.Errorf("invalid strategy name format")
	}

	baseDir, err := getRootDir()
	if err != nil {
		return err
	}

	// 检查并构建完整路径
	stratDir := filepath.Join(baseDir, folder)
	if _, err := os.Stat(stratDir); os.IsNotExist(err) {
		return fmt.Errorf("strategy folder does not exist: %s", folder)
	}

	fullPath := filepath.Join(stratDir, name+".go")

	// 检查文件是否已存在
	if _, err := os.Stat(fullPath); err == nil {
		return fmt.Errorf("strategy file already exists: %s", name)
	}

	content := fmt.Sprintf(`package %s

import (
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/strat"
)

func %s(pol *config.RunPolicyConfig) *strat.TradeStrat {
	return &strat.TradeStrat{
		WarmupNum:   100,
		OnBar: func(s *strat.StratJob) {
		},
	}
}
`, filepath.Base(stratDir), name)

	if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
		return err
	}

	if err := registerStrategyInInit(stratDir, name); err != nil {
		return err
	}

	return ensurePkgPack(baseDir, folder)
}

func registerStrategyInInit(stratDir, stratName string) error {
	files, err := os.ReadDir(stratDir)
	if err != nil {
		return err
	}

	// 优先处理 main.go
	mainGoPath := filepath.Join(stratDir, "main.go")
	mainExists := utils.Exists(mainGoPath)
	err = regGoInit(mainGoPath, stratDir, stratName)
	if err == nil {
		return nil
	} else if !errors.Is(err, errNoInit) && !os.IsNotExist(err) {
		return err
	}

	// 如果 main.go 不存在或处理失败，遍历其他 .go 文件
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".go") && file.Name() != "main.go" {
			filePath := filepath.Join(stratDir, file.Name())
			err = regGoInit(filePath, stratDir, stratName)
			if err == nil {
				return nil
			} else if !errors.Is(err, errNoInit) && !os.IsNotExist(err) {
				return err
			}
		}
	}

	// 如果没有找到合适的文件，创建或更新 main.go
	pkgName := filepath.Base(stratDir)
	initBody := fmt.Sprintf(`
func init() {
	strat.AddStratGroup("%s", map[string]strat.FuncMakeStrat{
		"%s": %s,
	})
}

`, pkgName, stratName, stratName)
	if !mainExists {
		content := fmt.Sprintf(`package %s

import "github.com/banbox/banbot/strat"
%v`, pkgName, initBody)
		return os.WriteFile(mainGoPath, []byte(content), 0644)
	}

	// 在 main.go 中添加 init 函数
	file, err := os.OpenFile(mainGoPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	text := strings.TrimSpace(string(content))
	if !strings.HasPrefix(text, "package") {
		text = fmt.Sprintf("package %s\n\n%s", pkgName, text)
	}

	// 检查是否需要添加 strat 包导入
	if !strings.Contains(text, "github.com/banbox/banbot/strat") {
		lines := strings.Split(text, "\n")
		var newLines []string
		importAdded := false
		for i, line := range lines {
			newLines = append(newLines, line)
			if i == 1 {
				newLines = append(newLines, "import \"github.com/banbox/banbot/strat\"")
				importAdded = true
			}
		}
		if !importAdded {
			newLines = append([]string{lines[0], "", "import \"github.com/banbox/banbot/strat\""}, lines[1:]...)
		}
		text = strings.Join(newLines, "\n")
	}

	// 在第一个 func 前添加 init 函数
	if idx := strings.Index(text, "func "); idx != -1 {
		text = text[:idx] + initBody + text[idx:]
	} else {
		text += initBody
	}

	return os.WriteFile(mainGoPath, []byte(text), 0644)
}

func regGoInit(filePath, stratDir, stratName string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	var initFound bool
	var inInit bool
	var mapFound bool
	var baseName = filepath.Base(stratDir)

	for scanner.Scan() {
		line := scanner.Text()
		if !initFound && strings.Contains(line, "func init() {") {
			initFound = true
			inInit = true
		} else if inInit {
			if strings.HasPrefix(line, "}") {
				if !mapFound {
					lines = append(lines, fmt.Sprintf(`
	strat.AddStratGroup("%s", map[string]strat.FuncMakeStrat{
		"%s": %s,
	})
`, baseName, stratName, stratName))
				}
				inInit = false
			} else {
				if strings.Contains(line, "map[string]strat.FuncMakeStrat{") {
					mapFound = true
				} else if mapFound {
					indentation := regexp.MustCompile(`^\s*`).FindString(line)
					lines = append(lines, fmt.Sprintf("%s\"%s\": %s,", indentation, stratName, stratName))
					inInit = false
				}
			}
		}
		lines = append(lines, line)
	}
	if !initFound {
		return errNoInit
	}

	return os.WriteFile(filePath, []byte(strings.Join(lines, "\n")), 0644)
}

func ensurePkgPack(rootPath, stratDir string) error {
	// 读取 go.mod 文件
	modPath := filepath.Join(rootPath, "go.mod")
	modContent, err := os.ReadFile(modPath)
	if err != nil {
		return fmt.Errorf("failed to read go.mod: %v", err)
	}

	// 查找模块名
	scanner := bufio.NewScanner(bytes.NewReader(modContent))
	var appPkg string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "module ") {
			appPkg = strings.TrimSpace(strings.TrimPrefix(line, "module "))
			break
		}
	}
	if appPkg == "" {
		return fmt.Errorf("module declaration not found in go.mod")
	}

	// 构建完整的包路径
	pkgPath := appPkg + "/" + strings.TrimSuffix(stratDir, "/")

	// 读取 main.go
	mainPath := filepath.Join(rootPath, "main.go")
	if !utils.Exists(mainPath) {
		return fmt.Errorf("main.go not found in root path")
	}

	mainContent, err := os.ReadFile(mainPath)
	if err != nil {
		return fmt.Errorf("failed to read main.go: %v", err)
	}

	lines := strings.Split(string(mainContent), "\n")
	var newLines []string
	inImport := false
	found := false
	lastImportLine := -1
	quotePkg := fmt.Sprintf(`"%s"`, pkgPath)

	for i, line := range lines {
		if strings.HasPrefix(line, "import (") {
			inImport = true
		} else if inImport {
			if strings.HasPrefix(line, ")") {
				inImport = false
				lastImportLine = i
			} else if strings.Contains(line, quotePkg) {
				found = true
			}
		}
		newLines = append(newLines, line)
	}

	if !found && lastImportLine > 0 {
		// 在 import 块的末尾添加新的导入
		importLine := fmt.Sprintf("\t_ %s", quotePkg)
		newLines = append(newLines[:lastImportLine], append([]string{importLine}, newLines[lastImportLine:]...)...)
	}

	return os.WriteFile(mainPath, []byte(strings.Join(newLines, "\n")), 0644)
}
