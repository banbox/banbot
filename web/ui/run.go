package ui

import (
	"archive/zip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/log"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
)

const downUrlTpl = "https://github.com/banbox/banbot/releases/download/{tag}/dist.zip"

func ServeStatic(app *fiber.App) error {
	return ServeStaticAt(app, config.GetDataDir(), core.SysLang)
}

// ServeStaticAt serves UI assets from a runtime-owned data directory. Callers
// that run multiple runtimes must pass their own directory instead of using
// the legacy configuration facade.
func ServeStaticAt(app *fiber.App, dataDir, sysLang string) error {
	uiDistDir := filepath.Join(dataDir, "uidist")
	indexPath := filepath.Join(uiDistDir, "index.html")
	verPath := filepath.Join(uiDistDir, "version.txt")
	oldVer, err2 := utils.ReadTextFile(verPath)
	reDown := 0
	errMsg := ""
	if !utils.Exists(indexPath) {
		reDown = 1
		errMsg = "$uidist/index.html not exists"
	} else if err2 != nil || oldVer != core.UIVersion {
		reDown = 2
		errMsg = "uidist is too old"
	}
	if reDown > 0 {
		err := downNewUI(errMsg, uiDistDir, verPath, dataDir, sysLang)
		if err != nil {
			if utils.Exists(indexPath) {
				// 有旧的UI，继续使用不中断
				log.Warn("update to new uidist fail", zap.Error(err))
			} else {
				return err
			}
		}
	}
	app.Static("/", uiDistDir)
	return nil
}

func downNewUI(errMsg, uiDistDir, verPath, dataDir, sysLang string) error {
	downUrl := strings.Replace(downUrlTpl, "{tag}", core.UIVersion, 1)
	if sysLang == "zh-CN" {
		// 对简体中文的环境，使用gitee下载，避免访问外网可能失败
		downUrl = strings.Replace(downUrl, "github", "gitee", 1)
	}
	log.Info(errMsg+", downloading", zap.String("url", downUrl))

	// 创建临时目录
	tmpDir := filepath.Join(dataDir, "tmp")
	if err := utils.EnsureDir(tmpDir, 0755); err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// 下载zip文件
	zipPath := filepath.Join(tmpDir, "dist.zip")
	resp, err := http.Get(downUrl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 404 && !strings.Contains(downUrl, "github") {
			// 非github下载404，尝试从github下载
			downUrl = strings.Replace(downUrl, "gitee", "github", 1)
			log.Warn("download 404, retry from github")
			resp, err = http.Get(downUrl)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("download failed with status: %s", resp.Status)
			}
		} else {
			return fmt.Errorf("download failed with status: %s", resp.Status)
		}
	}

	out, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer out.Close()

	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	log.Debug("downloaded ui dist", zap.Int64("size", written))

	if err = out.Sync(); err != nil {
		return err
	}
	out.Close()

	// 解压zip文件
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	if len(r.File) == 0 {
		return fmt.Errorf("zip file is empty")
	}

	err = os.RemoveAll(uiDistDir)
	if err != nil {
		log.Warn("del old uidist fail", zap.Error(err))
	}
	if err = utils.EnsureDir(uiDistDir, 0755); err != nil {
		return err
	}

	for _, f := range r.File {
		if f.FileInfo().IsDir() {
			continue
		}

		// 忽略根目录dist
		name := f.Name
		name = strings.TrimPrefix(name, "dist/")
		if name == "" || strings.Contains(name, "..") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}

		path := filepath.Join(uiDistDir, name)
		if err := utils.EnsureDir(filepath.Dir(path), 0755); err != nil {
			rc.Close()
			return err
		}

		dst, err := os.Create(path)
		if err != nil {
			rc.Close()
			return err
		}

		_, err = io.Copy(dst, rc)
		dst.Close()
		rc.Close()
		if err != nil {
			return err
		}
	}
	verFile, err := os.Create(verPath)
	if err != nil {
		return err
	}
	verFile.WriteString(core.UIVersion)
	verFile.Close()
	log.Info("uidist init successfully")
	return nil
}
