package orm

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/schollz/progressbar/v3"
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
	dataDir = filepath.Join(config.GetDataDir(), "qdbdata")
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
			return exe, dataDir
		}
	}
	return "", ""
}

// startAndWait starts QuestDB and waits for the PGWire port to become reachable.
func startAndWait(bin, dataDir string, port uint16) *errs.Error {
	log.Info("starting QuestDB ...", zap.String("bin", bin), zap.String("dataDir", dataDir))
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "mkdir dataDir %s: %v", dataDir, err)
	}
	if err := applyQdbMemConfig(dataDir); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		// On Windows, "questdb.exe start" installs a service requiring Administrator.
		// Run interactively as a background process instead; it uses cwd as root dir.
		cmd := exec.Command(bin)
		cmd.Dir = dataDir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			return errs.NewMsg(core.ErrDbConnFail, "failed to start QuestDB: %v", err)
		}
	} else {
		cmd := exec.Command(bin, "start", "-d", dataDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return errs.NewMsg(core.ErrDbConnFail, "failed to start QuestDB: %v", err)
		}
	}
	return waitForPort(port)
}

const (
	qdbMemPctDefault   = 0.3
	qdbMaxMemMBDefault = 16384
	qdbMinMemMB        = 500
)

// applyQdbMemConfig detects system memory and writes QuestDB server.conf with
// appropriate JVM heap and O3 column memory settings.
func applyQdbMemConfig(dataDir string) *errs.Error {
	totalMB := sysTotalMemMB()
	if totalMB <= 0 {
		log.Warn("cannot detect system memory, skip QuestDB memory tuning")
		return nil
	}
	dbCfg := config.Database
	memPct := qdbMemPctDefault
	maxMemMB := qdbMaxMemMBDefault
	if dbCfg != nil {
		if dbCfg.QdbMemPct > 0 && dbCfg.QdbMemPct <= 1 {
			memPct = dbCfg.QdbMemPct
		}
		if dbCfg.QdbMaxMemMB > 0 {
			maxMemMB = dbCfg.QdbMaxMemMB
		}
	}
	allocMB := int(float64(totalMB) * memPct)
	if allocMB < qdbMinMemMB {
		allocMB = qdbMinMemMB
	}
	if allocMB > maxMemMB {
		allocMB = maxMemMB
	}
	// JVM heap = 1/3 of allocated memory (rest for off-heap / OS page cache)
	heapMB := allocMB / 3
	if heapMB < 128 {
		heapMB = 128
	}
	// O3 column memory size: scale down on small machines
	o3ColMem := "8M"
	if totalMB <= 4096 {
		o3ColMem = "1M"
	} else if totalMB <= 8192 {
		o3ColMem = "4M"
	}
	// Worker count: limit on small machines
	workers := runtime.NumCPU()
	if workers > 4 && totalMB <= 4096 {
		workers = 2
	}

	confDir := filepath.Join(dataDir, "conf")
	if err := os.MkdirAll(confDir, 0755); err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "mkdir conf %s: %v", confDir, err)
	}
	confPath := filepath.Join(confDir, "server.conf")
	// Read existing config, update only managed keys
	managed := map[string]string{
		"cairo.o3.column.memory.size": o3ColMem,
		"shared.worker.count":         strconv.Itoa(workers),
	}
	existing := readConfFile(confPath)
	for k, v := range managed {
		existing[k] = v
	}
	if err := writeConfFile(confPath, existing); err != nil {
		return errs.NewMsg(core.ErrDbConnFail, "write server.conf: %v", err)
	}
	// Set JVM heap via environment variable
	javaOpts := fmt.Sprintf("-Xms%dm -Xmx%dm", heapMB, heapMB)
	os.Setenv("QDB_JAVA_OPTS", javaOpts)
	log.Info("QuestDB memory config applied",
		zap.Int("totalMB", totalMB), zap.Int("allocMB", allocMB),
		zap.Int("heapMB", heapMB), zap.String("o3ColMem", o3ColMem),
		zap.Int("workers", workers))
	return nil
}

// sysTotalMemMB returns total physical memory in MB. Returns 0 on failure.
func sysTotalMemMB() int {
	switch runtime.GOOS {
	case "linux":
		data, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return 0
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					kb, _ := strconv.Atoi(fields[1])
					return kb / 1024
				}
			}
		}
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err != nil {
			return 0
		}
		b, _ := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
		return int(b / 1024 / 1024)
	case "windows":
		out, err := exec.Command("wmic", "ComputerSystem", "get", "TotalPhysicalMemory").Output()
		if err != nil {
			return 0
		}
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "Total") {
				continue
			}
			b, _ := strconv.ParseInt(line, 10, 64)
			if b > 0 {
				return int(b / 1024 / 1024)
			}
		}
	}
	return 0
}

// readConfFile reads a QuestDB server.conf into a key=value map.
func readConfFile(path string) map[string]string {
	result := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return result
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if idx := strings.IndexByte(line, '='); idx > 0 {
			result[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
		}
	}
	return result
}

// writeConfFile writes a key=value map to a QuestDB server.conf file.
func writeConfFile(path string, kv map[string]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range kv {
		fmt.Fprintf(w, "%s=%s\n", k, v)
	}
	return w.Flush()
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

	downUrl := fmt.Sprintf("https://github.com/questdb/questdb/releases/download/%s/%s", qdbVersion, assetName)

	// Use banexg global proxy (set during config init via banexg.SetProxy).
	proxyUrl := banexg.GetProxy()
	client := banexg.NewHttpClient()
	client.Timeout = 300 * time.Second
	if proxyUrl != "" {
		log.Info("downloading QuestDB via proxy ...", zap.String("url", downUrl), zap.String("proxy", proxyUrl))
	} else {
		log.Info("downloading QuestDB ...", zap.String("url", downUrl))
	}

	resp, err_ := client.Get(downUrl)
	if err_ != nil {
		if proxyUrl == "" {
			exgName := ""
			if config.Exchange != nil {
				exgName = config.Exchange.Name
			}
			return errs.NewMsg(core.ErrDbConnFail,
				"failed to download QuestDB: %v\nPlease configure `proxy` in your exchange config (e.g. exchange.%s.proxy) or set HTTPS_PROXY env",
				err_, exgName)
		}
		return errs.NewMsg(core.ErrDbConnFail, "failed to download QuestDB (proxy=%s): %v", proxyUrl, err_)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errs.NewMsg(core.ErrDbConnFail, "failed to download QuestDB: HTTP %d", resp.StatusCode)
	}

	installDir := qdbInstallPath()

	bar := progressbar.DefaultBytes(resp.ContentLength, "downloading QuestDB")
	reader := io.TeeReader(resp.Body, bar)

	if err2 := extractTarGz(reader, installDir); err2 != nil {
		return err2
	}
	_ = bar.Close()
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
