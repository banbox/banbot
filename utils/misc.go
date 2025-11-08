package utils

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/cron/v3"
	"github.com/felixge/fgprof"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

var (
	regHolds, _  = regexp.Compile("[{]([^}]+)[}]")
	dockerStatus = 0
	langCache    = ""
)

/*
SplitSolid
String segmentation, ignore empty strings in the return result
字符串分割，忽略返回结果中的空字符串
*/
func SplitSolid(text string, sep string, unique bool) []string {
	arr := strings.Split(text, sep)
	var result []string
	var hit = make(map[string]bool)
	for _, str := range arr {
		if str != "" {
			if unique {
				if _, ok := hit[str]; !ok {
					hit[str] = true
					result = append(result, str)
				}
			} else {
				result = append(result, str)
			}
		}
	}
	return result
}

func SplitToMap(text string, sep string) map[string]bool {
	arr := strings.Split(text, sep)
	var result = make(map[string]bool)
	for _, str := range arr {
		str = strings.TrimSpace(str)
		if str != "" {
			result[str] = true
		}
	}
	return result
}

func KeysOfMap[M ~map[K]V, K comparable, V any](m M) []K {
	r := make([]K, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

func ValsOfMap[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m))
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func CutMap[M ~map[K]V, K comparable, V any](m M, keys ...K) M {
	r := make(M)
	for _, k := range keys {
		if v, ok := m[k]; ok {
			r[k] = v
		}
	}
	return r
}

func Check(err error) {
	if err != nil {
		panic(err)
	}
}

func UnionArr[T comparable](arrs ...[]T) []T {
	hit := make(map[T]struct{})
	result := make([]T, 0, len(hit))
	for _, arr := range arrs {
		for _, text := range arr {
			if _, ok := hit[text]; !ok {
				result = append(result, text)
				hit[text] = struct{}{}
			}
		}
	}
	return result
}

func ReverseArr[T any](s []T) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func GetAddsRemoves[T comparable](news, olds []T) ([]T, []T) {
	oldMap := make(map[T]bool)
	for _, v := range olds {
		oldMap[v] = true
	}
	var adds []T
	for _, v := range news {
		if _, ok := oldMap[v]; ok {
			delete(oldMap, v)
		} else {
			adds = append(adds, v)
		}
	}
	removes := make([]T, 0, len(oldMap))
	for v := range oldMap {
		removes = append(removes, v)
	}
	return adds, removes
}

func ConvertArr[T1, T2 any](arr []T1, doMap func(T1) T2) []T2 {
	var res = make([]T2, len(arr))
	for i, item := range arr {
		res[i] = doMap(item)
	}
	return res
}

func ArrToMap[T1 comparable, T2 any](arr []T2, doMap func(T2) T1) map[T1][]T2 {
	res := make(map[T1][]T2)
	for _, v := range arr {
		key := doMap(v)
		if old, ok := res[key]; ok {
			res[key] = append(old, v)
		} else {
			res[key] = []T2{v}
		}
	}
	return res
}

func RemoveFromArr[T comparable](arr []T, it T, num int) []T {
	res := make([]T, 0, len(arr))
	for _, v := range arr {
		if v == it && (num < 0 || num > 0) {
			num -= 1
			continue
		}
		res = append(res, v)
	}
	return res
}

func FormatWithMap(text string, args map[string]interface{}) string {
	var b strings.Builder
	matches := regHolds.FindAllStringSubmatchIndex(text, -1)
	var lastEnd int
	for _, mat := range matches {
		start, end := mat[2], mat[3]
		b.WriteString(text[lastEnd : start-1])
		holdText := text[start:end]
		parts := strings.Split(holdText, ":")
		valFmt := "%v"
		if len(parts) > 1 && len(parts[1]) > 0 {
			valFmt = "%" + parts[1]
		}
		if val, ok := args[parts[0]]; ok {
			b.WriteString(fmt.Sprintf(valFmt, val))
		}
		lastEnd = end + 1
	}
	b.WriteString(text[lastEnd:])
	return b.String()
}

func PrintErr(e error) string {
	if e == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if errors.As(e, &pgErr) {
		return fmt.Sprintf("(SqlState %v) %s: %s, where=%s from %s.%s", pgErr.Code, pgErr.Message,
			pgErr.Detail, pgErr.Where, pgErr.SchemaName, pgErr.TableName)
	}
	return e.Error()
}

func DeepCopyMap(dst, src map[string]interface{}) {
	if src == nil {
		return
	}
	for k, v := range src {
		if vSrcMap, ok := v.(map[string]interface{}); ok {
			if vDstMap, ok := dst[k].(map[string]interface{}); ok {
				DeepCopyMap(vDstMap, vSrcMap)
				continue
			}
		}
		dst[k] = v
	}
}

func ParallelRun[T any](items []T, concurNum int, handle func(int, T) *errs.Error) *errs.Error {
	var retErr *errs.Error
	guard := make(chan struct{}, concurNum)
	var wg sync.WaitGroup
	for i_, item_ := range items {
		// If the concurrency limit is reached, it will block and wait here
		// 如果达到并发限制，这里会阻塞等待
		guard <- struct{}{}
		if retErr != nil {
			// 出错，终止返回
			break
		}
		wg.Add(1)
		go func(i int, item T) {
			defer func() {
				// Complete a task and pop up a pop-up from chan
				// 完成一个任务，从chan弹出一个
				<-guard
				wg.Done()
			}()
			err := handle(i, item)
			if err != nil {
				retErr = err
			}
		}(i_, item_)
	}
	wg.Wait()
	return retErr
}

func ReadInput(tips []string) (string, error) {
	for _, l := range tips {
		fmt.Println(l)
	}
	var input string
	_, err_ := fmt.Scanln(&input)
	if err_ != nil {
		return "", err_
	}
	return input, nil
}

func ReadConfirm(tips []string, ok, fail string, exitAny bool) bool {
	input, err_ := ReadInput(tips)
	if err_ != nil {
		log.Warn("read confirm fail", zap.Error(err_))
		return false
	}
	if input == ok {
		return true
	} else if input == fail {
		return false
	} else if exitAny {
		return false
	}
	tip := fmt.Sprintf("unknown, input %s/%s", ok, fail)
	for {
		input, err_ = ReadInput([]string{tip})
		if err_ != nil {
			log.Warn("read confirm fail", zap.Error(err_))
			return false
		}
		if input == ok {
			return true
		} else if input == fail {
			return false
		}
	}
}

func MD5(data []byte) string {
	hash := md5.New()
	hash.Write(data)
	hashInBytes := hash.Sum(nil)

	return hex.EncodeToString(hashInBytes)
}

func IsDocker() bool {
	if dockerStatus != 0 {
		return dockerStatus == 1
	}
	dockerStatus = -1
	if _, err := os.Stat("/.dockerenv"); err == nil {
		dockerStatus = 1
		return true
	}
	file, err := os.Open("/proc/1/cgroup")
	if err != nil {
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "docker") {
			dockerStatus = 1
			return true
		}
	}

	return false
}

func OpenBrowser(url string) error {
	var cmd string
	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		return exec.Command(cmd, "/c", "start", url).Start()
	case "darwin":
		cmd = "open"
		return exec.Command(cmd, url).Start()
	case "linux":
		cmd = "xdg-open"
		return exec.Command(cmd, url).Start()
	default:
		return fmt.Errorf("unsupported platform: " + runtime.GOOS)
	}
}

func OpenBrowserDelay(url string, delayMS int) {
	timer := time.NewTimer(time.Duration(delayMS) * time.Millisecond)
	go func() {
		<-timer.C
		err_ := OpenBrowser(url)
		if err_ != nil {
			log.Warn("open browser fail", zap.Error(err_))
		}
	}()
}

// IntToBytes convert uint32 to [4]byte
func IntToBytes(n uint32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, n)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

/*
GetSystemLanguage returns the current system language code.
Possible return values (ISO 639-1 with optional ISO 3166-1 country code):
- en-US: English
- zh-CN: Chinese (Simplified)
...
*/
func GetSystemLanguage() string {
	if langCache != "" {
		return langCache
	}
	langCache = normalizeLanguageCode(detectLangCode())
	return langCache
}

func detectLangCode() string {
	if lang := os.Getenv("LANG"); lang != "" {
		return lang
	}
	switch runtime.GOOS {
	case "windows":
		cmd := exec.Command("powershell", "-Command", "Get-Culture | select -exp Name")
		output, err := cmd.Output()
		if err == nil {
			lang := strings.TrimSpace(string(output))
			return strings.Replace(strings.ToLower(lang), "_", "-", -1)
		}
	case "darwin":
		cmd := exec.Command("defaults", "read", ".GlobalPreferences", "AppleLanguages")
		output, err := cmd.Output()
		if err == nil {
			// 解析输出的第一个语言
			lines := strings.Split(string(output), "\n")
			if len(lines) > 1 {
				// 提取第一个语言代码
				lang := strings.Trim(lines[1], " \t(\")")
				return strings.Replace(strings.ToLower(lang), "_", "-", -1)
			}
		}
	case "linux":
		// 尝试从环境变量获取
		for _, envVar := range []string{"LANGUAGE", "LC_ALL", "LC_MESSAGES"} {
			if lang := os.Getenv(envVar); lang != "" {
				// 提取语言代码部分
				parts := strings.Split(lang, ".")
				if len(parts) > 0 {
					return strings.Replace(strings.ToLower(parts[0]), "_", "-", -1)
				}
			}
		}
	}

	return "en-US"
}

func normalizeLanguageCode(code string) string {
	// 处理形如 "zh_CN.UTF-8" 的格式
	code = strings.Split(code, ".")[0]
	code = strings.Replace(code, "_", "-", -1)

	// 标准化语言代码
	switch strings.ToLower(code) {
	case "zh-cn", "zh-hans", "zh-hans-cn":
		return "zh-CN"
	case "zh-tw", "zh-hant", "zh-hant-tw":
		return "zh-TW"
	case "zh-hk", "zh-hant-hk":
		return "zh-HK"
	case "en", "en-us":
		return "en-US"
	case "en-gb":
		return "en-GB"
	case "ja", "ja-jp":
		return "ja-JP"
	case "ko", "ko-kr":
		return "ko-KR"
	case "fr", "fr-fr":
		return "fr-FR"
	case "de", "de-de":
		return "de-DE"
	case "es", "es-es":
		return "es-ES"
	case "it", "it-it":
		return "it-IT"
	case "ru", "ru-ru":
		return "ru-RU"
	case "pt-br":
		return "pt-BR"
	case "pt", "pt-pt":
		return "pt-PT"
	case "nl", "nl-nl":
		return "nl-NL"
	case "pl", "pl-pl":
		return "pl-PL"
	case "tr", "tr-tr":
		return "tr-TR"
	case "ar", "ar-sa":
		return "ar-SA"
	case "th", "th-th":
		return "th-TH"
	case "vi", "vi-vn":
		return "vi-VN"
	default:
		return "en-US" // 未知语言代码返回美式英语
	}
}

func StartCpuProfile(path string, port int) *errs.Error {
	if _, err_ := os.Stat(path); err_ == nil {
		err_ = os.Remove(path)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
	}
	f, err_ := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	} else {
		err_ = pprof.StartCPUProfile(f)
		if err_ != nil {
			return errs.New(errs.CodeRunTime, err_)
		}
		http.DefaultServeMux.Handle("/debug/fgprof", fgprof.Handler())
		go func() {
			log.Info("cpu profile http started", zap.Int("port", port))
			err_ = http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
			if err_ != nil {
				log.Warn("serve fgprof fail", zap.Int("port", port), zap.Error(err_))
			}
		}()
		core.ExitCalls = append(core.ExitCalls, func() {
			pprof.StopCPUProfile()
			err_ = f.Close()
			if err_ != nil {
				log.Error("save cpu.profile fail", zap.Error(err_))
			}
		})
	}
	return nil
}

func NewCronScheduler(exp string) (cron.Schedule, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	return parser.Parse(exp, btime.LocShow)
}

func CronAlign(scd cron.Schedule, stamp time.Time) time.Time {
	var endTime = stamp
	for i := 0; i < 5; i++ {
		endTime = scd.Next(endTime)
	}
	curTime := stamp.Add(-endTime.Sub(stamp))
	var prev = curTime
	for curTime.Before(stamp) || curTime.Equal(stamp) {
		prev = curTime
		curTime = scd.Next(curTime)
	}
	return prev
}

func ReadChanBatch[T comparable](c chan T, withNil bool) []T {
	var result []T
	var zero T
readCache:
	for {
		select {
		case val := <-c:
			if withNil || val != zero {
				result = append(result, val)
			}
		default:
			break readCache
		}
	}
	return result
}

func ReadScanner(out io.ReadCloser) *bufio.Scanner {
	scanner := bufio.NewScanner(out)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 1024*1024)
	return scanner
}

// EncryptData encrypts data using AES-GCM with the provided key
// Key should be 16, 24 or 32 bytes for AES-128, AES-192 or AES-256
func EncryptData(data []byte, keyStr string) ([]byte, error) {
	key, _ := hex.DecodeString(keyStr)
	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt data
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext, nil
}

// DecryptData decrypts data using AES-GCM with the provided key
// Key should be 16, 24 or 32 bytes for AES-128, AES-192 or AES-256
func DecryptData(data []byte, keyStr string) ([]byte, error) {
	key, _ := hex.DecodeString(keyStr)
	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Extract nonce and ciphertext
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt data
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}
