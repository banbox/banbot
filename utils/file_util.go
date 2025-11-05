package utils

import (
	"archive/zip"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banexg"
	"github.com/banbox/banexg/errs"
	"github.com/flopp/go-findfont"
	"github.com/xuri/excelize/v2"
	"golang.org/x/image/font/opentype"
)

func CopyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err = EnsureDir(dst, 0755); err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(src, entry.Name())
		destPath := filepath.Join(dst, entry.Name())

		fileInfo, err := os.Lstat(sourcePath)
		if err != nil {
			return err
		}

		switch fileInfo.Mode() & os.ModeType {
		case os.ModeDir:
			if err = EnsureDir(destPath, 0755); err != nil {
				return err
			}
			if err = CopyDir(sourcePath, destPath); err != nil {
				return err
			}
		case os.ModeSymlink:
			if err = CopySymLink(sourcePath, destPath); err != nil {
				return err
			}
		default:
			if err = Copy(sourcePath, destPath); err != nil {
				return err
			}
		}

		fInfo, err := entry.Info()
		if err != nil {
			return err
		}

		isSymlink := fInfo.Mode()&os.ModeSymlink != 0
		if !isSymlink {
			if err = os.Chmod(destPath, fInfo.Mode()); err != nil {
				return err
			}
		}
	}
	return nil
}

func Copy(srcFile, dstFile string) error {
	out, err := os.Create(dstFile)
	if err != nil {
		return err
	}

	defer out.Close()

	in, err := os.Open(srcFile)
	if err != nil {
		return err
	}

	defer in.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return nil
}

func Exists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func EnsureDir(dir string, perm os.FileMode) error {
	if Exists(dir) {
		return nil
	}

	if err := os.MkdirAll(dir, perm); err != nil {
		return fmt.Errorf("failed to create directory: '%s', error: '%s'", dir, err.Error())
	}

	return nil
}

func RemovePath(path string, recursive bool) error {
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("check path fail: %s %w", path, err)
	}

	// check is dir
	if fileInfo.IsDir() {
		// not allow to delete by recursive
		if !recursive {
			// check is empty
			entries, err := os.ReadDir(path)
			if err != nil {
				return fmt.Errorf("read dir fail: %s %w", path, err)
			}

			if len(entries) > 0 {
				return fmt.Errorf("dir is not empty, enable recursive to delete: %s", path)
			}

			// delete empty dir
			return os.Remove(path)
		}

		// delete all
		return os.RemoveAll(path)
	}

	// delete file
	return os.Remove(path)
}

// FindSubPath searches for the first occurrence of a directory named tgtName
// within the specified workDir and its subdirectories up to two levels deep.
func FindSubPath(parDir, tgtName string, maxDepth int) (string, error) {
	if maxDepth < 1 {
		return "", fmt.Errorf("maxDepth must >= 1")
	}
	if _, err := os.Stat(parDir); os.IsNotExist(err) {
		return "", fmt.Errorf("directory %s does not exist", parDir)
	}
	var tgtPath string
	err := filepath.Walk(parDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relativePath, err := filepath.Rel(parDir, path)
		if err != nil {
			return err
		}
		// calculate depth of current path
		depth := len(strings.Split(filepath.ToSlash(relativePath), "/"))
		if depth > maxDepth {
			return filepath.SkipDir
		}

		if info.Name() == tgtName {
			tgtPath = path
			return filepath.SkipDir
		}
		return nil
	})

	if err != nil {
		return "", err
	}

	if tgtPath == "" {
		return "", fmt.Errorf("target directory %s not found within two levels of %s", tgtName, parDir)
	}

	return tgtPath, nil
}

func CopySymLink(source, dest string) error {
	link, err := os.Readlink(source)
	if err != nil {
		return err
	}
	return os.Symlink(link, dest)
}

func MovePath(src, tgt string) error {
	srcAbs, err := filepath.Abs(src)
	if err != nil {
		return err
	}
	tgtAbs, err := filepath.Abs(tgt)
	if err != nil {
		return err
	}

	_, err = os.Stat(srcAbs)
	if os.IsNotExist(err) {
		return fmt.Errorf("source not exist: %v", src)
	}

	tgtDir := filepath.Dir(tgtAbs)
	if err = os.MkdirAll(tgtDir, 0755); err != nil {
		return err
	}

	// try rename/move directly
	err = os.Rename(srcAbs, tgtAbs)

	// copy and delete if move fail
	if err != nil {
		if err = copyPath(srcAbs, tgtAbs); err != nil {
			return err
		}

		if err = os.RemoveAll(srcAbs); err != nil {
			return err
		}
	}
	return nil
}

// copyPath 递归复制文件或文件夹
func copyPath(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	// 如果是文件夹，递归复制
	if srcInfo.IsDir() {
		if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil {
			return err
		}

		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			srcPath := filepath.Join(src, entry.Name())
			dstPath := filepath.Join(dst, entry.Name())

			if entry.IsDir() {
				if err := copyPath(srcPath, dstPath); err != nil {
					return err
				}
			} else {
				if err := copyFile(srcPath, dstPath); err != nil {
					return err
				}
			}
		}
	} else {
		// 如果是文件，直接复制
		if err := copyFile(src, dst); err != nil {
			return err
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcContent, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	return os.WriteFile(dst, srcContent, 0644)
}

func WriteCsvFile(path string, rows [][]string, compress bool) *errs.Error {
	var fileWriter io.Writer
	var err_ error
	folder := filepath.Dir(path)
	err_ = EnsureDir(folder, 0755)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	cleanPath := strings.TrimSuffix(path, filepath.Ext(path))
	path = cleanPath + ".csv"
	if compress {
		zipPath := cleanPath + ".zip"
		zipFile, err_ := os.Create(zipPath)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
		zipWriter := zip.NewWriter(zipFile)
		defer func() {
			zipWriter.Close()
			zipFile.Close()
		}()
		header := &zip.FileHeader{
			Name:     filepath.Base(path),
			Method:   zip.Deflate,
			Modified: time.Now(),
		}
		fileWriter, err_ = zipWriter.CreateHeader(header)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
	} else {
		file, err_ := os.Create(path)
		if err_ != nil {
			return errs.New(errs.CodeIOWriteFail, err_)
		}
		defer file.Close()
		fileWriter = file
	}
	writer := csv.NewWriter(fileWriter)
	defer writer.Flush()
	err_ = writer.WriteAll(rows)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	return nil
}

func WriteFile(path string, data []byte) *errs.Error {
	file, err_ := os.Create(path)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	defer file.Close()
	_, err_ = file.Write(data)
	if err_ != nil {
		return errs.New(errs.CodeIOWriteFail, err_)
	}
	return nil
}

func KlineToStr(klines []*banexg.Kline, loc *time.Location) [][]string {
	rows := make([][]string, 0, len(klines))
	for _, k := range klines {
		var dateStr string
		if loc != nil {
			dateStr = btime.ToTime(k.Time).In(loc).Format(core.DefaultDateFmt)
		} else {
			dateStr = strconv.FormatInt(k.Time/1000, 10)
		}
		row := []string{
			dateStr,
			strconv.FormatFloat(k.Open, 'f', -1, 64),
			strconv.FormatFloat(k.High, 'f', -1, 64),
			strconv.FormatFloat(k.Low, 'f', -1, 64),
			strconv.FormatFloat(k.Close, 'f', -1, 64),
			strconv.FormatFloat(k.Volume, 'f', -1, 64),
			strconv.FormatFloat(k.Info, 'f', -1, 64),
		}
		rows = append(rows, row)
	}
	return rows
}

/*
ReadFileTail 从文件尾部或指定位置从后向前读取
*/
func ReadFileTail(filePath string, size int64, end int64) ([]byte, int64, error) {
	if size <= 0 {
		size = 10240
	}
	offset := end
	if offset == 0 {
		offset = -1
	}
	return ReadFileRange(filePath, -size, offset)
}

/*
ReadFileRange
Read lineNum lines forward or backward from the file at the specified offset position;
Offset>=0 indicates the position from head to back. Offset<0 indicates that the position is determined from the tail. (-1 means end)
LineNum>0 indicates reading backwards from the initial position, while lineNum<0 indicates reading forwards.

对文件按offset指定位置向前或向后读取lineNum行；
offset>=0表示从头部往后位置。offset<0表示从尾部确定位置。（-1表示末尾）
lineNum>0表示从初始位置向后读取，lineNum<0表示向前读取。
*/

func ReadFileRange(filePath string, size int64, offset int64) ([]byte, int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()
	return ReadFileObjRange(file, size, offset)
}

func ReadFileObjRange(file *os.File, size int64, offset int64) ([]byte, int64, error) {
	var data []byte

	// 获取文件大小
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	fileSize := fileInfo.Size()

	var pos = offset
	if pos < 0 {
		pos = fileSize + pos + 1
	}
	// 设定缓冲区大小和读取偏移量
	bufferSize := 4096
	var buffer = make([]byte, bufferSize)
	dirt := int64(1)
	if size < 0 {
		// 负数表示从后往前读取
		size *= -1
		dirt = -1
	}

	for int64(len(data)) < size {
		if dirt < 0 && pos <= 0 || dirt > 0 && pos >= fileSize {
			// end to front, pos till 0; front to end, pos to end
			break
		}
		if dirt < 0 {
			if int64(bufferSize) > pos {
				bufferSize = int(pos)
				pos = 0
			} else {
				pos -= int64(bufferSize)
			}
		} else {
			if pos+int64(bufferSize) > fileSize {
				bufferSize = int(fileSize - pos)
			}
		}

		_, err = file.Seek(pos, 0)
		if err != nil {
			return nil, 0, fmt.Errorf("seek file failed: %v", err)
		}
		n, err := file.Read(buffer[:bufferSize])
		if err != nil && err != io.EOF {
			return nil, 0, fmt.Errorf("read file failed: %v", err)
		}

		newData := buffer[:n]

		if dirt < 0 {
			// end to front
			merge := make([]byte, 0, len(data)+len(newData))
			merge = append(merge, newData...)
			merge = append(merge, data...)
			data = merge
		} else {
			// front to end, move pos
			pos += int64(n)
			data = append(data, newData...)
		}
	}
	if int64(len(data)) > size {
		sepId := int64(len(data)) - size
		if dirt < 0 {
			pos += sepId
			data = data[sepId:]
		} else {
			pos -= sepId
			data = data[:size]
		}
	}

	return data, pos, nil
}

func ReadCSV(path string) ([][]string, *errs.Error) {
	file, err_ := os.Open(path)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	defer file.Close()
	rows, err_ := csv.NewReader(file).ReadAll()
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	return rows, nil
}

func ReadTextFile(path string) (string, *errs.Error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", errs.New(errs.CodeIOReadFail, err)
	}

	if info.IsDir() {
		return "", errs.NewMsg(errs.CodeIOReadFail, "File is a directory")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return "", errs.New(errs.CodeIOReadFail, err)
	}

	if !IsTextContent(content) {
		return "", errs.NewMsg(errs.CodeIOReadFail, "File is not a text file")
	}

	return string(content), nil
}

/*
ReadXlsx use first if `sheet` is empty
*/
func ReadXlsx(path, sheet string) ([][]string, *errs.Error) {
	f, err_ := excelize.OpenFile(path)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	defer f.Close()
	if sheet == "" {
		sheet = f.GetSheetName(0)
	}
	rows, err_ := f.Rows(sheet)
	if err_ != nil {
		return nil, errs.New(errs.CodeIOReadFail, err_)
	}
	var res [][]string
	for rows.Next() {
		opts := rows.GetRowOpts()
		if opts.Hidden {
			continue
		}
		cells, err_ := rows.Columns()
		if err_ != nil {
			return nil, errs.New(errs.CodeIOReadFail, err_)
		}
		if len(cells) > 1 {
			// skip row index
			res = append(res, cells[1:])
		}
	}
	return res, nil
}

func GetFontData(name string) ([]byte, error) {
	if name == "" {
		name = "arial.ttf"
	}
	path, err := findfont.Find(name)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetOpenFont(name string) (*opentype.Font, error) {
	fontData, err := GetFontData(name)
	if err != nil {
		return nil, err
	}
	fontFace, err := opentype.Parse(fontData)
	return fontFace, err
}

func GetFilesWithPrefix(filePath string) ([]string, error) {
	dir := filepath.Dir(filePath)
	fileName := filepath.Base(filePath)
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 判断文件路径是否以指定的前缀开头
		if strings.HasPrefix(filepath.Base(path), fileName) {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

func CreateNumFile(outDir string, prefix string, ext string) (*os.File, error) {
	files, err := filepath.Glob(filepath.Join(outDir, prefix+"*."+ext))
	if err != nil {
		return nil, err
	}
	fileCount := len(files)

	var newFile *os.File
	for {
		fileCount++
		newFileName := fmt.Sprintf("%s%d.%s", prefix, fileCount, ext)
		newFilePath := filepath.Join(outDir, newFileName)

		newFile, err = os.Create(newFilePath)
		if err == nil {
			break // 成功创建文件，退出循环
		}
		if !os.IsExist(err) {
			return nil, err // 返回其他类型的错误
		}
	}

	return newFile, nil
}

func EncodeGob(path string, data any) *errs.Error {
	folder := filepath.Dir(path)
	err := EnsureDir(folder, 0755)
	if err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	file, err := os.Create(path)
	if err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(data)
	if err != nil {
		return errs.New(errs.CodeIOWriteFail, err)
	}
	return nil
}

func DecodeGobFile(path string, data any) *errs.Error {
	file, err := os.Open(path)
	if err != nil {
		return errs.New(errs.CodeIOReadFail, err)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(data)
	if err != nil {
		return errs.New(errs.CodeIOReadFail, err)
	}
	return nil
}
