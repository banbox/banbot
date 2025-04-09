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
	uiDistDir := filepath.Join(config.GetDataDir(), "uidist")
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
		err := os.RemoveAll(uiDistDir)
		if err != nil {
			log.Warn("del old uidist fail", zap.Error(err))
		}
	}
	if reDown > 0 {
		downUrl := strings.Replace(downUrlTpl, "{tag}", core.UIVersion, 1)
		log.Info(errMsg+", downloading", zap.String("url", downUrl))

		// 创建临时目录
		tmpDir := filepath.Join(config.GetDataDir(), "tmp")
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
			return fmt.Errorf("download failed with status: %s", resp.Status)
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

		if err := utils.EnsureDir(uiDistDir, 0755); err != nil {
			return err
		}

		for _, f := range r.File {
			if f.FileInfo().IsDir() {
				continue
			}

			// 忽略根目录dist
			name := f.Name
			name = strings.TrimPrefix(name, "dist/")
			if name == "" {
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
	}
	app.Static("/", uiDistDir)
	return nil
}
