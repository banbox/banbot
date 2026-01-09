package base

import (
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/utils"
	"github.com/gofiber/fiber/v2"
)

type csvRow struct {
	timeMS int64
	values map[string]float64
}

func RegApiCsv(api fiber.Router) {
	api.Post("/csv/upload", postCsvUpload)
	api.Get("/csv/list", getCsvList)
	api.Post("/csv/data", postCsvData)
}

func getCsvDir() string {
	return filepath.Join(config.GetDataDir(), "csv")
}

/*
postCsvUpload 上传CSV文件到BanDataDir/csv/
*/
func postCsvUpload(c *fiber.Ctx) error {
	file, err := c.FormFile("file")
	if err != nil {
		return &fiber.Error{Code: fiber.StatusBadRequest, Message: "file is required"}
	}
	if !strings.HasSuffix(strings.ToLower(file.Filename), ".csv") {
		return &fiber.Error{Code: fiber.StatusBadRequest, Message: "only .csv files are allowed"}
	}

	csvDir := getCsvDir()
	if err := utils.EnsureDir(csvDir, 0755); err != nil {
		return err
	}

	dstPath := filepath.Join(csvDir, file.Filename)
	if err := c.SaveFile(file, dstPath); err != nil {
		return err
	}

	return c.JSON(fiber.Map{
		"code": 200,
		"msg":  "upload success",
		"name": file.Filename,
	})
}

/*
getCsvList 获取CSV云端指标列表
扫描BanDataDir/csv/目录下的所有csv文件
*/
func getCsvList(c *fiber.Ctx) error {
	csvDir := getCsvDir()
	if err := utils.EnsureDir(csvDir, 0755); err != nil {
		return err
	}

	entries, err := os.ReadDir(csvDir)
	if err != nil {
		return err
	}

	result := make([]map[string]interface{}, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".csv") {
			continue
		}
		result = append(result, map[string]interface{}{
			"name":    name,
			"title":   name,
			"is_main": false,
			"cloud":   true,
		})
	}

	return c.JSON(fiber.Map{
		"code": 200,
		"data": result,
	})
}

type CsvDataArgs struct {
	Name      string   `json:"name" validate:"required"`
	StartMS   int64    `json:"start_ms" validate:"required"`
	EndMS     int64    `json:"end_ms" validate:"required"`
	TFSecs    int      `json:"tf_secs" validate:"required"`
	TimeCol   string   `json:"time_col"`
	ValueCols []string `json:"value_cols"`
}

/*
postCsvData 获取CSV数据，按kline时间范围和周期进行标准化
*/
func postCsvData(c *fiber.Ctx) error {
	var args CsvDataArgs
	if err := VerifyArg(c, &args, ArgBody); err != nil {
		return err
	}

	csvPath := filepath.Join(getCsvDir(), args.Name)
	if _, err := os.Stat(csvPath); os.IsNotExist(err) {
		return &fiber.Error{Code: fiber.StatusNotFound, Message: "csv file not found: " + args.Name}
	}

	file, err := os.Open(csvPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// 读取表头
	headers, err := reader.Read()
	if err != nil {
		return &fiber.Error{Code: fiber.StatusBadRequest, Message: "failed to read csv headers"}
	}

	// 找到time列索引
	timeCol := args.TimeCol
	if timeCol == "" {
		timeCol = "time"
	}
	timeIdx := -1
	colIdxMap := make(map[string]int)
	for i, h := range headers {
		h = strings.TrimSpace(h)
		headers[i] = h
		colIdxMap[h] = i
		if strings.ToLower(h) == strings.ToLower(timeCol) {
			timeIdx = i
		}
	}
	if timeIdx < 0 {
		return &fiber.Error{Code: fiber.StatusBadRequest, Message: "time column not found in csv"}
	}

	// 确定要读取的值列
	valueCols := args.ValueCols
	if len(valueCols) == 0 {
		// 默认读取除time外的所有列
		for _, h := range headers {
			if h != timeCol {
				valueCols = append(valueCols, h)
			}
		}
	}

	// 读取所有数据，先不过滤时间范围以获取文件概况
	var allRows []csvRow

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		// 解析时间
		timeVal, err := btime.ParseTimeMS(record[timeIdx])
		if err != nil {
			continue
		}

		// 读取值列
		values := make(map[string]float64)
		for _, col := range valueCols {
			if idx, ok := colIdxMap[col]; ok && idx < len(record) {
				if v, err := strconv.ParseFloat(strings.TrimSpace(record[idx]), 64); err == nil {
					values[col] = v
				}
			}
		}

		allRows = append(allRows, csvRow{timeMS: timeVal, values: values})
	}

	// 构建文件概况
	fileSummary := map[string]interface{}{
		"total_rows": len(allRows),
	}

	if len(allRows) > 0 {
		// 按时间排序以获取准确的起止时间
		sort.Slice(allRows, func(i, j int) bool {
			return allRows[i].timeMS < allRows[j].timeMS
		})
		fileSummary["start_ms"] = allRows[0].timeMS
		fileSummary["end_ms"] = allRows[len(allRows)-1].timeMS
		if len(allRows) > 1 {
			fileSummary["detected_tf_ms"] = detectTimeFrame(allRows)
		}
	}

	// 过滤时间范围
	var rows []csvRow
	for _, row := range allRows {
		if row.timeMS >= args.StartMS && row.timeMS <= args.EndMS {
			rows = append(rows, row)
		}
	}

	fileSummary["filtered_rows"] = len(rows)
	fileSummary["request_start_ms"] = args.StartMS
	fileSummary["request_end_ms"] = args.EndMS

	if len(rows) == 0 {
		return c.JSON(fiber.Map{
			"code":    200,
			"data":    []interface{}{},
			"columns": valueCols,
			"summary": fileSummary,
		})
	}

	// 按时间排序
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].timeMS < rows[j].timeMS
	})

	// 计算CSV数据的时间周期
	csvTFMS := detectTimeFrame(rows)
	klineTFMS := int64(args.TFSecs) * 1000

	// 标准化数据到kline周期
	result := normalizeToKlineTF(rows, klineTFMS, csvTFMS, args.StartMS, args.EndMS, valueCols)

	return c.JSON(fiber.Map{
		"code":    200,
		"data":    result,
		"columns": valueCols,
		"summary": fileSummary,
	})
}

// detectTimeFrame 检测CSV数据的时间周期（毫秒）
func detectTimeFrame(rows []csvRow) int64 {
	if len(rows) < 2 {
		return 60000 // 默认1分钟
	}

	// 统计相邻时间差
	diffs := make(map[int64]int)
	for i := 1; i < len(rows) && i < 100; i++ {
		diff := rows[i].timeMS - rows[i-1].timeMS
		if diff > 0 {
			diffs[diff]++
		}
	}

	// 找到最常见的时间差
	maxCount := 0
	var mostCommon int64 = 60000
	for diff, count := range diffs {
		if count > maxCount {
			maxCount = count
			mostCommon = diff
		}
	}

	return mostCommon
}

// normalizeToKlineTF 将CSV数据标准化到kline的时间周期
// 返回数组长度严格匹配: (endMS - startMS) / 1000 / tfSecs
func normalizeToKlineTF(rows []csvRow, klineTFMS, csvTFMS, startMS, endMS int64, valueCols []string) []map[string]float64 {
	// 计算期望的输出数量
	count := int((endMS - startMS) / klineTFMS)
	result := make([]map[string]float64, count)

	// 初始化所有位置为空map
	for i := range result {
		result[i] = make(map[string]float64)
	}

	if len(rows) == 0 {
		return result
	}

	// 构建时间到数据的映射，按kline周期对齐
	// 对于CSV周期<=kline周期：采样，取每个kline周期内最后一个值
	barData := make(map[int64]map[string]float64)
	for _, row := range rows {
		barTime := alignTime(row.timeMS, klineTFMS)
		// 每个barTime保留最后一个值（采样）
		barData[barTime] = row.values
	}

	// 填充结果数组
	var prevValues map[string]float64
	for i := 0; i < count; i++ {
		t := startMS + int64(i)*klineTFMS
		if data, ok := barData[t]; ok {
			// 有数据，复制到结果
			for k, v := range data {
				result[i][k] = v
			}
			prevValues = data
		} else if csvTFMS > klineTFMS && prevValues != nil {
			// CSV周期更大（数据更稀疏），用前一个值填充
			for k, v := range prevValues {
				result[i][k] = v
			}
		}
		// 其他情况保持空map（前面缺失、CSV周期更小时的中间缺失）
	}

	return result
}

// alignTime 将时间对齐到指定周期
func alignTime(timeMS, tfMS int64) int64 {
	return (timeMS / tfMS) * tfMS
}
