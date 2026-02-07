package orm

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
)

const (
	qdbVersion    = "9.3.2"
	qdbInstallDir = "/opt/questdb" // linux default
)

// qdbUserDir returns a user-writable install directory as fallback.
func qdbUserDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "questdb")
}

// qdbInstallPath returns a writable install directory for QuestDB.
// Prefers /opt/questdb; falls back to ~/.local/share/questdb if not writable.
func qdbInstallPath() string {
	if runtime.GOOS == "windows" {
		home, _ := os.UserHomeDir()
		return filepath.Join(home, "questdb")
	}
	// Try creating /opt/questdb to test write permission.
	if err := os.MkdirAll(qdbInstallDir, 0755); err == nil {
		return qdbInstallDir
	}
	return qdbUserDir()
}

// ensureQuestDB tries to start QuestDB if it is installed, or download and
// install it natively (no Docker) when it is missing.  Supports Linux, macOS
// and Windows.
func ensureQuestDB(port uint16) *errs.Error {
	// 1. Try to start if already installed.
	bin, dataDir := qdbPaths()
	if bin != "" {
		return startAndWait(bin, dataDir, port)
	}

	// 2. Not installed – install per platform.
	log.Info("QuestDB not found, installing ...")
	var err *errs.Error
	switch runtime.GOOS {
	case "darwin":
		err = installQdbDarwin()
	case "linux":
		err = installQdbBinary("linux")
	case "windows":
		err = installQdbBinary("windows")
	default:
		return errs.NewMsg(core.ErrDbConnFail, "unsupported OS for auto-install QuestDB: %s", runtime.GOOS)
	}
	if err != nil {
		return err
	}

	bin, dataDir = qdbPaths()
	if bin == "" {
		return errs.NewMsg(core.ErrDbConnFail, "QuestDB installed but binary not found")
	}
	return startAndWait(bin, dataDir, port)
}

// qdbPaths returns the QuestDB binary path and data directory if QuestDB is
// installed, or ("", "") otherwise.
func qdbPaths() (bin string, dataDir string) {
	dataDir = "/opt/qdb"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		home, _ := os.UserHomeDir()
		dataDir = filepath.Join(home, ".local", "share", "qdb")
	}
	switch runtime.GOOS {
	case "darwin":
		// Homebrew
		if p, err := exec.LookPath("questdb"); err == nil {
			return p, dataDir
		}
		// Check user dir fallback.
		sh := filepath.Join(qdbUserDir(), "bin", "questdb.sh")
		if _, err := os.Stat(sh); err == nil {
			return sh, dataDir
		}
	case "linux":
		// Check standard install location.
		for _, dir := range []string{qdbInstallDir, qdbUserDir()} {
			sh := filepath.Join(dir, "bin", "questdb.sh")
			if _, err := os.Stat(sh); err == nil {
				return sh, dataDir
			}
		}
	case "windows":
		home, _ := os.UserHomeDir()
		exe := filepath.Join(home, "questdb", "bin", "questdb.exe")
		if _, err := os.Stat(exe); err == nil {
			return exe, filepath.Join(home, ".questdb")
		}
	}
	return "", ""
}

// startAndWait starts QuestDB and waits for the PGWire port to become reachable.
func startAndWait(bin, dataDir string, port uint16) *errs.Error {
	args := []string{"start", "-d", dataDir}
	log.Info("starting QuestDB ...", zap.String("bin", bin), zap.String("dataDir", dataDir))
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "failed to start QuestDB: %v", err)
	}
	return waitForPort(port)
}

// waitForPort polls the PGWire port until it is reachable or timeout.
func waitForPort(port uint16) *errs.Error {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	log.Info("waiting for QuestDB to be ready ...", zap.String("addr", addr))
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = conn.Close()
			log.Info("QuestDB is ready")
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return errs.NewMsg(core.ErrDbConnFail, "QuestDB did not become ready within 30s on %s", addr)
}

// installQdbDarwin installs QuestDB on macOS via Homebrew.
func installQdbDarwin() *errs.Error {
	if _, err := exec.LookPath("brew"); err != nil {
		// Homebrew not available, fall back to binary download.
		return installQdbBinary("darwin")
	}
	log.Info("installing QuestDB via Homebrew ...")
	cmd := exec.Command("brew", "install", "questdb")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "brew install questdb failed: %v", err)
	}
	return nil
}

// installQdbBinary downloads and extracts the QuestDB binary tarball for the
// given platform (linux/windows/darwin).
func installQdbBinary(platform string) *errs.Error {
	arch := runtime.GOARCH
	var assetName string
	switch {
	case platform == "linux" && arch == "amd64":
		assetName = fmt.Sprintf("questdb-%s-rt-linux-x86-64.tar.gz", qdbVersion)
	case platform == "windows" && arch == "amd64":
		assetName = fmt.Sprintf("questdb-%s-rt-windows-x86-64.tar.gz", qdbVersion)
	case platform == "darwin":
		// No official macOS binary with embedded JRE; use no-jre variant.
		assetName = fmt.Sprintf("questdb-%s-no-jre-bin.tar.gz", qdbVersion)
	default:
		// ARM Linux or other – use no-jre variant (requires local Java 17).
		assetName = fmt.Sprintf("questdb-%s-no-jre-bin.tar.gz", qdbVersion)
	}

	url := fmt.Sprintf("https://github.com/questdb/questdb/releases/download/%s/%s", qdbVersion, assetName)
	log.Info("downloading QuestDB ...", zap.String("url", url))

	resp, err := http.Get(url)
	if err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "failed to download QuestDB: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errs.NewMsg(core.ErrDbConnFail, "failed to download QuestDB: HTTP %d", resp.StatusCode)
	}

	installDir := qdbInstallPath()

	if err2 := extractTarGz(resp.Body, installDir); err2 != nil {
		return err2
	}
	log.Info("QuestDB installed", zap.String("dir", installDir))
	return nil
}

// extractTarGz extracts a .tar.gz stream into destDir, stripping the
// top-level directory from the archive (e.g. questdb-9.3.2-rt-linux-x86-64/).
func extractTarGz(r io.Reader, destDir string) *errs.Error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "gzip open: %v", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	prefix := "" // will be set from the first entry
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errs.NewMsg(core.ErrDbConnFail, "tar read: %v", err)
		}

		// Determine and strip the top-level directory prefix.
		if prefix == "" {
			parts := strings.SplitN(hdr.Name, "/", 2)
			if len(parts) > 1 {
				prefix = parts[0] + "/"
			}
		}
		relPath := strings.TrimPrefix(hdr.Name, prefix)
		if relPath == "" || relPath == "." {
			continue
		}
		target := filepath.Join(destDir, filepath.FromSlash(relPath))

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return errs.NewMsg(core.ErrDbConnFail, "mkdir %s: %v", target, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return errs.NewMsg(core.ErrDbConnFail, "mkdir %s: %v", filepath.Dir(target), err)
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return errs.NewMsg(core.ErrDbConnFail, "create %s: %v", target, err)
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return errs.NewMsg(core.ErrDbConnFail, "write %s: %v", target, err)
			}
			f.Close()
		}
	}
	return nil
}
