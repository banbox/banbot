module github.com/banbox/banbot

go 1.23.0

//replace (
//	github.com/banbox/banexg => ../banexg
//github.com/banbox/banta => ../banta
//)

require (
	github.com/anyongjin/go-bayesopt v1.0.2
	github.com/banbox/banexg v0.2.34-beta.11
	github.com/banbox/banta v0.3.4
	github.com/c-bata/goptuna v0.9.0
	github.com/dgraph-io/ristretto v0.2.0
	github.com/jackc/pgx/v5 v5.7.5
	github.com/muesli/clusters v0.0.0-20200529215643-2700303c1762
	github.com/muesli/kmeans v0.3.1
	github.com/olekukonko/tablewriter v1.0.9
	// github.com/pkujhd/goloader v0.0.0-20240113094056-ff3a1e01ffcb
	github.com/schollz/progressbar/v3 v3.18.0
	github.com/shopspring/decimal v1.4.0
	github.com/xuri/excelize/v2 v2.9.1
	go.uber.org/zap v1.27.0
	golang.org/x/text v0.28.0
	gonum.org/v1/gonum v0.16.0
	google.golang.org/grpc v1.75.0
	google.golang.org/protobuf v1.36.8
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/anyongjin/cron v0.4.1
	github.com/felixge/fgprof v0.9.5
	github.com/flopp/go-findfont v0.1.0
	github.com/fogleman/gg v1.3.0
	github.com/go-playground/validator/v10 v10.27.0
	github.com/go-telegram/bot v1.17.0
	github.com/go-viper/mapstructure/v2 v2.4.0
	github.com/gofiber/contrib/websocket v1.3.4
	github.com/gofiber/fiber/v2 v2.52.9
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/google/uuid v1.6.0
	github.com/sasha-s/go-deadlock v0.3.6
	github.com/shirou/gopsutil/v4 v4.25.8
	golang.org/x/image v0.30.0
	modernc.org/sqlite v1.38.2
)

require (
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/beevik/ntp v1.4.3 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.14.1 // indirect
	github.com/bytedance/sonic/loader v0.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/fasthttp/websocket v1.5.12 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.9 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/google/pprof v0.0.0-20250317173921-a4b03ec1a45e // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20240909124753-873cd0166683 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/mitchellh/colorstring v0.0.0-20190213212951-d06e56a500db // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/olekukonko/errors v1.1.0 // indirect
	github.com/olekukonko/ll v0.0.9 // indirect
	github.com/petermattis/goid v0.0.0-20250813065127-a731cc31b4fe // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.4 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/savsgio/gotils v0.0.0-20240704082632-aef3928b8a38 // indirect
	github.com/tiendc/go-deepcopy v1.6.1 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.64.0 // indirect
	github.com/xuri/efp v0.0.1 // indirect
	github.com/xuri/nfp v0.0.1 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/arch v0.19.0 // indirect
	golang.org/x/crypto v0.40.0 // indirect
	golang.org/x/exp v0.0.0-20250718183923-645b1fa84792 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/term v0.33.0 // indirect
	golang.org/x/tools v0.35.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250721164621-a45f3dfb1074 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	modernc.org/libc v1.66.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
