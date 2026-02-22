# Utils 包

本包提供了一系列通用工具函数，包括文件操作、数学计算、字符串处理等功能。

## 文件操作

### CopyDir
复制整个目录及其内容到目标位置。

参数:
- `src string` - 源目录路径
- `dst string` - 目标目录路径

返回:
- `error` - 操作过程中的错误信息

### Copy
复制单个文件到目标位置。

参数:
- `srcFile string` - 源文件路径
- `dstFile string` - 目标文件路径

返回:
- `error` - 操作过程中的错误信息

### Exists
检查文件或目录是否存在。

参数:
- `filePath string` - 要检查的文件或目录路径

返回:
- `bool` - true表示存在，false表示不存在

### EnsureDir
确保目录存在，如果不存在则创建。

参数:
- `dir string` - 目录路径
- `perm os.FileMode` - 目录权限

返回:
- `error` - 操作过程中的错误信息

### FindSubPath
在指定目录下递归查找目标文件或目录。

参数:
- `parDir string` - 父目录路径
- `tgtName string` - 要查找的目标名称
- `maxDepth int` - 最大递归深度

返回:
- `string` - 找到的路径
- `error` - 查找过程中的错误信息

## 数学计算

### DecPow
计算十进制数的幂。

参数:
- `x decimal.Decimal` - 底数
- `y decimal.Decimal` - 指数

返回:
- `decimal.Decimal` - 计算结果

### DecArithMean
计算一组十进制数的算术平均值。

参数:
- `values []decimal.Decimal` - 数值数组

返回:
- `decimal.Decimal` - 平均值
- `error` - 计算过程中的错误信息

### DecStdDev
计算一组十进制数的总体标准差。

参数:
- `values []decimal.Decimal` - 数值数组

返回:
- `decimal.Decimal` - 标准差
- `error` - 计算过程中的错误信息

### SharpeRatio
计算夏普比率，使用默认252个交易日和年化处理。

参数:
- `moReturns []float64` - 收益率数组
- `riskFree float64` - 无风险利率

返回:
- `float64` - 夏普比率
- `error` - 计算过程中的错误信息

### SortinoRatio
计算索提诺比率，使用默认252个交易日和年化处理。

参数:
- `moReturns []float64` - 收益率数组
- `riskFree float64` - 无风险利率

返回:
- `float64` - 索提诺比率
- `error` - 计算过程中的错误信息

## 字符串处理

### SnakeToCamel
将下划线命名转换为驼峰命名。

参数:
- `input string` - 输入的下划线命名字符串

返回:
- `string` - 转换后的驼峰命名字符串

### PadCenter
将字符串居中填充到指定宽度。

参数:
- `s string` - 原始字符串
- `width int` - 目标宽度
- `padText string` - 填充字符

返回:
- `string` - 填充后的字符串

### RandomStr
生成指定长度的随机字符串。

参数:
- `length int` - 字符串长度

返回:
- `string` - 生成的随机字符串

### FormatWithMap
使用map中的值格式化字符串。

参数:
- `text string` - 包含占位符的字符串
- `args map[string]interface{}` - 替换值的map

返回:
- `string` - 格式化后的字符串

### PrintErr
格式化错误信息。

参数:
- `e error` - 错误对象

返回:
- `string` - 格式化后的错误信息

### GroupByPairQuotes
将交易对按报价货币分组。

格式: `[key]:pairs...` 转换为:
```
【key】
Quote: Base1 Base2 ...
```

参数:
- `items map[string][]string` - 交易对映射

返回:
- `string` - 格式化后的分组字符串

### CountDigit
计算字符串中数字字符的数量。

参数:
- `text string` - 输入字符串

返回:
- `int` - 数字字符的数量

### SplitSolid
分割字符串并忽略空字符串。

参数:
- `text string` - 要分割的字符串
- `sep string` - 分隔符

返回:
- `[]string` - 分割后的非空字符串数组

## 切片和映射操作

### SplitSolid
字符串分割，忽略返回结果中的空字符串。

参数:
- `text string` - 要分割的字符串
- `sep string` - 分隔符

返回:
- `[]string` - 分割后的字符串数组

### KeysOfMap
获取map的所有键。

参数:
- `m M` - 输入的map，支持泛型

返回:
- `[]K` - 键数组

### ValsOfMap
获取map的所有值。

参数:
- `m M` - 输入的map，支持泛型

返回:
- `[]V` - 值数组

### CutMap
从map中提取指定键的子集。

参数:
- `m M` - 输入的map，支持泛型
- `keys ...K` - 要提取的键列表

返回:
- `M` - 包含指定键的新map

### UnionArr
合并多个数组并去重。

参数:
- `arrs ...[]T` - 要合并的数组列表，支持泛型

返回:
- `[]T` - 合并后的去重数组

### ReverseArr
原地反转数组元素顺序。

参数:
- `s []T` - 要反转的数组，支持泛型

### ConvertArr
将一个类型的数组转换为另一个类型。

参数:
- `arr []T1` - 源数组
- `doMap func(T1) T2` - 转换函数

返回:
- `[]T2` - 转换后的数组

### ArrToMap
将数组转换为map，可能存在多个元素映射到同一个键。

参数:
- `arr []T2` - 源数组
- `doMap func(T2) T1` - 键映射函数

返回:
- `map[T1][]T2` - 转换后的map，值为数组

### RemoveFromArr
从数组中移除指定元素。

参数:
- `arr []T` - 源数组
- `it T` - 要移除的元素
- `num int` - 要移除的数量，负数表示移除所有

返回:
- `[]T` - 移除元素后的新数组

### UniqueItems
获取数组中的唯一元素和重复元素。

参数:
- `arr []T` - 输入数组，支持泛型

返回:
- `[]T` - 唯一元素数组
- `[]T` - 重复元素数组

### DeepCopyMap
深度复制map。

参数:
- `dst map[string]interface{}` - 目标map
- `src map[string]interface{}` - 源map

### MapToStr
将map转换为字符串表示。

参数:
- `m map[string]float64` - 要转换的map

返回:
- `string` - 转换后的字符串
- `int` - 数值部分的总长度

## 网络操作

### DoHttp
执行HTTP请求并返回结果。

参数:
- `client *http.Client` - HTTP客户端
- `req *http.Request` - HTTP请求

返回:
- `*banexg.HttpRes` - HTTP响应结果

## 网络通信

### NewBanServer
创建一个新的BanServer实例，用于处理TCP网络通信。

参数:
- `addr string` - 服务器监听地址(如:"127.0.0.1:6789")
- `name string` - 服务器名称

返回:
- `*ServerIO` - 服务器实例

### NewClientIO
创建一个新的BanClient实例，用于连接BanServer。

参数:
- `addr string` - 服务器地址(如:"127.0.0.1:6789")

返回:
- `*ClientIO` - 客户端实例
- `*errs.Error` - 错误信息

### GetServerData
从BanServer或BanClient获取数据。

参数:
- `key string` - 数据键名

返回:
- `string` - 获取的数据值
- `*errs.Error` - 错误信息

### SetServerData
向BanServer或BanClient设置数据。

参数:
- `args *KeyValExpire` - 包含键值和过期时间的数据结构
  - `Key string` - 键名
  - `Val string` - 值
  - `ExpireSecs int` - 过期时间(秒)

返回:
- `*errs.Error` - 错误信息

### GetNetLock
获取分布式锁。

参数:
- `key string` - 锁的键名
- `timeout int` - 获取锁的超时时间(秒)

返回:
- `int32` - 锁的值(用于解锁)
- `*errs.Error` - 错误信息

### DelNetLock
删除分布式锁。

参数:
- `key string` - 锁的键名
- `lockVal int32` - 锁的值(从GetNetLock获得)

返回:
- `*errs.Error` - 错误信息

## 其他工具

### MD5
计算数据的MD5哈希值。

参数:
- `data []byte` - 输入数据

返回:
- `string` - MD5哈希值的十六进制字符串

### GetSystemLanguage
获取系统语言设置。

返回:
- `string` - 系统语言代码

## 相关性计算

### CalcCorrMat
计算多个数据序列之间的相关性矩阵。

参数:
- `arrLen int` - 数据长度
- `dataArr [][]float64` - 二维数据数组
- `useChgRate bool` - 是否使用变化率计算

返回:
- `*mat.SymDense` - 相关性矩阵
- `[]float64` - 每个序列的平均相关性
- `error` - 错误信息

### GenCorrImg
生成相关性矩阵的热力图。

参数:
- `m *mat.SymDense` - 相关性矩阵
- `title string` - 图表标题
- `names []string` - 序列名称列表
- `fontName string` - 字体名称
- `fontSize float64` - 字体大小

返回:
- `[]byte` - PNG图片数据
- `error` - 错误信息

### CalcEnvsCorr
计算多个K线环境的相关性。

参数:
- `envs []*ta.BarEnv` - K线环境列表
- `hisNum int` - 历史数据数量

返回:
- `*mat.SymDense` - 相关性矩阵
- `[]float64` - 每个环境的平均相关性
- `error` - 错误信息

## 性能指标计算

### CalcExpectancy
计算收益期望和风险回报比。

参数:
- `profits []float64` - 收益数组

返回:
- `float64` - 期望收益
- `float64` - 风险回报比

### CalcMaxDrawDown
计算最大回撤。

参数:
- `profits []float64` - 收益数组
- `initBalance float64` - 初始余额

返回:
- `float64` - 最大回撤金额
- `float64` - 最大回撤比例
- `int` - 回撤开始位置
- `int` - 回撤结束位置
- `float64` - 回撤开始时的余额
- `float64` - 回撤结束时的余额

### AutoCorrPenalty
计算自相关惩罚因子。

参数:
- `returns []float64` - 收益率数组

返回:
- `float64` - 惩罚因子

### KMeansVals
对数值序列进行K均值聚类。

参数:
- `vals []float64` - 数值数组
- `num int` - 聚类数量

返回:
- `*ClusterRes` - 聚类结果

### StdDevVolatility
计算标准差波动率。

参数:
- `data []float64` - 数据数组
- `rate float64` - 衰减率

返回:
- `float64` - 波动率

### NearScore
计算接近中心点的得分。

参数:
- `x float64` - 当前值
- `mid float64` - 中心值
- `rate float64` - 衰减率

返回:
- `float64` - 得分(0-1之间)

## 文件读写

### ReadCSV
读取CSV文件内容。

参数:
- `path string` - CSV文件路径

返回:
- `[][]string` - CSV内容的二维数组
- `*errs.Error` - 错误信息

### ReadXlsx
读取Excel文件内容。

参数:
- `path string` - Excel文件路径
- `sheet string` - 工作表名称(为空时使用第一个工作表)

返回:
- `[][]string` - Excel内容的二维数组
- `*errs.Error` - 错误信息

### ReadTextFile
读取文本文件内容。

参数:
- `path string` - 文件路径

返回:
- `string` - 文件内容
- `*errs.Error` - 错误信息

### ReadLastNLines
读取文件最后N行内容。

参数:
- `filePath string` - 文件路径
- `lineCount int` - 要读取的行数

返回:
- `[]string` - 最后N行内容
- `error` - 错误信息

### WriteCsvFile
写入CSV文件。

参数:
- `path string` - 文件路径
- `rows [][]string` - 要写入的数据
- `compress bool` - 是否压缩

返回:
- `*errs.Error` - 错误信息

### WriteFile
写入普通文件。

参数:
- `path string` - 文件路径
- `data []byte` - 要写入的数据

返回:
- `*errs.Error` - 错误信息

### KlineToStr
将K线数据转换为字符串数组。

参数:
- `klines []*banexg.Kline` - K线数据数组
- `loc *time.Location` - 时区信息

返回:
- `[][]string` - 转换后的字符串数组

### GetFontData
获取字体文件数据。

参数:
- `name string` - 字体名称(为空时使用arial.ttf)

返回:
- `[]byte` - 字体文件数据
- `error` - 错误信息

### GetOpenFont
获取OpenType字体。

参数:
- `name string` - 字体名称

返回:
- `*opentype.Font` - OpenType字体对象
- `error` - 错误信息

### IsTextContent
检查数据是否为文本内容。

参数:
- `data []byte` - 要检查的数据

返回:
- `bool` - true表示是文本内容，false表示可能是二进制内容

## 工具函数

### ParallelRun
并行执行任务。

参数:
- `items []T` - 要处理的项目列表，支持泛型
- `concurNum int` - 最大并发数
- `handle func(int, T) *errs.Error` - 处理函数，接收索引和项目

返回:
- `*errs.Error` - 执行过程中的错误

### IsDocker
检查当前是否在Docker容器中运行。

返回:
- `bool` - 是否在Docker容器中

### OpenBrowser
打开系统默认浏览器访问指定URL。

参数:
- `url string` - 要访问的URL

返回:
- `error` - 执行过程中的错误

### OpenBrowserDelay
延迟一定时间后打开浏览器访问URL。

参数:
- `url string` - 要访问的URL
- `delayMS int` - 延迟的毫秒数

### IntToBytes
将uint32转换为字节数组。

参数:
- `n uint32` - 要转换的数字

返回:
- `[]byte` - 转换后的字节数组
- `error` - 转换过程中的错误

### MD5
计算数据的MD5哈希值。

参数:
- `data []byte` - 要计算哈希的数据

返回:
- `string` - MD5哈希值的十六进制字符串

### ReadInput
从标准输入读取用户输入。

参数:
- `tips []string` - 提示信息列表

返回:
- `string` - 用户输入的字符串
- `error` - 读取过程中的错误

### ReadConfirm
从标准输入读取用户确认。

参数:
- `tips []string` - 提示信息列表
- `ok string` - 确认字符串
- `fail string` - 取消字符串
- `exitAny bool` - 是否允许任意输入退出

返回:
- `bool` - 用户是否确认

### NewPrgBar
创建新的进度条。

参数:
- `totalNum int` - 总进度数
- `title string` - 进度条标题

返回:
- `*PrgBar` - 进度条对象


### NewStagedPrg
创建多阶段进度条。

参数:
- `tasks []string` - 任务名称列表，按执行顺序，不可重复
- `weights []float64` - 各个任务权重，>0，内部会自动归一化

返回:
- `*StagedPrg` - 多阶段进度条对象

## 系统相关函数

### GetSystemLanguage
获取当前系统语言代码。

返回:
- `string` - ISO 639-1语言代码(可选带ISO 3166-1国家代码)，如:
  - en-US: 英语(美国)
  - zh-CN: 中文(简体)
  - zh-TW: 中文(繁体)
  - ja-JP: 日语
  - ko-KR: 韩语
  - fr-FR: 法语
  - de-DE: 德语
  - es-ES: 西班牙语
  等

