package entry

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/opt"
	runtimectx "github.com/banbox/banbot/runtime"
	"github.com/banbox/banbot/utils"
	"github.com/banbox/banbot/web"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/sasha-s/go-deadlock"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

func RunCmd() {
	defer func() {
		if recovered := recover(); recovered != nil {
			_, _ = fmt.Fprintf(os.Stderr, "banbot panic raw stack:\n%s", panicStack())
			if err, ok := recovered.(*errs.Error); ok {
				log.Error("banbot panic", zap.Any("error", err))
			} else {
				log.Error("banbot panic", zap.Any("error", recovered), zap.Stack("stack"))
			}
			core.RunExitCalls()
			os.Exit(1)
		}
		core.RunExitCalls()
	}()

	installSignalHandler()
	deadlock.Opts.Disable = true
	if err := Execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		core.RunExitCalls()
		os.Exit(1)
	}
}

func panicStack() []byte {
	return debug.Stack()
}

// Execute runs banbot with an explicit argument list. It is separated from
// RunCmd so callers and tests can execute the Cobra command tree without exits.
func Execute(args []string) error {
	command := NewRootCommand()
	if isImplicitWebInvocation(args) {
		command = web.NewDevCommandWithFactory(newDevWebServer)
		command.SilenceErrors = true
		command.SilenceUsage = true
	}
	command.SetArgs(normalizeLegacyFlags(command, args))
	return command.Execute()
}

func isImplicitWebInvocation(args []string) bool {
	if len(args) == 0 || !strings.HasPrefix(args[0], "-") {
		return false
	}
	switch args[0] {
	case "-h", "-help", "--help", "-v", "-version", "--version":
		return false
	default:
		return true
	}
}

// NewRootCommand builds the complete Cobra command tree.
func NewRootCommand() *cobra.Command {
	rootCommandMu.Lock()
	defer rootCommandMu.Unlock()
	root := &cobra.Command{
		Use:           "banbot",
		Short:         "Banbot quantitative trading and data tools",
		Version:       core.Version,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE:          func(_ *cobra.Command, _ []string) error { return runDefaultWeb() },
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetVersionTemplate("banbot {{.Version}}\n")

	extraGroups, extraCommands := commandRegistrySnapshot()
	groups := make(map[string]*cobra.Command, len(commandGroups)+len(extraGroups))
	allGroups := append(append([]commandGroup{}, commandGroups...), extraGroups...)
	for _, group := range allGroups {
		command := &cobra.Command{
			Use:   group.name,
			Short: group.help,
			Args:  cobra.NoArgs,
			RunE: func(command *cobra.Command, _ []string) error {
				return command.Help()
			},
		}
		groups[group.name] = command
		root.AddCommand(command)
	}

	registerBuiltInCommands(root, groups)
	registerExtraCommands(root, groups, extraCommands)
	return root
}

func runDefaultWeb() error {
	command := web.NewDevCommandWithFactory(newDevWebServer)
	command.SetArgs([]string{})
	return command.Execute()
}

func installSignalHandler() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		// Stop is only cancellation. Wait for each explicit Runtime owner to
		// join callbacks and flush state before the command returns; os.Exit
		// would bypass the entry/session defers that close storage and exchange
		// resources.
		runtimectx.StopAndWaitProcesses()
		if core.StopAll != nil {
			core.StopAll()
		}
		core.RunExitCalls()
	}()
}

func newRuntimeConfigCommand(name, help string, run FuncEntry, allowDeadlock bool, binders ...flagBinder) *cobra.Command {
	args := &config.CmdArgs{}
	options := &runtimeCommandFlags{}
	command := &cobra.Command{
		Use:   name,
		Short: help,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			args.BTStrictSet = command.Flags().Changed("bt-strict")
			args.NetDisable = options.netDisable
			args.CPUProfile = options.cpuProfile
			args.MemProfile = options.memProfile
			if err := run(args); err != nil {
				return err
			}
			return nil
		},
	}
	bindCommonFlags(args, options, command.Flags(), allowDeadlock)
	for _, bind := range binders {
		bind(args, command.Flags())
	}
	return command
}

type runtimeCommandFlags struct {
	cpuProfile bool
	memProfile bool
	netDisable bool
}

func bindCommonFlags(args *config.CmdArgs, options *runtimeCommandFlags, flags *pflag.FlagSet, allowDeadlock bool) {
	flags.StringVar(&args.DataDir, "datadir", "", "path to the data directory")
	flags.StringArrayVar((*[]string)(&args.Configs), "config", nil, "config path; may be repeated")
	flags.BoolVar(&args.NoDefault, "no-default", false, "ignore config.yml and config.local.yml")
	flags.StringVar(&args.ConfigData, "config-data", "", "inline YAML config")
	flags.StringVar(&args.Logfile, "logfile", "", "log file path")
	flags.StringVar(&args.LogLevel, "level", "info", "logging level")
	flags.IntVar(&args.MaxPoolSize, "max-pool-size", 0, "maximum database pool size")
	if allowDeadlock {
		flags.BoolVar(&args.DeadLock, "dlock", false, "enable deadlock detection")
	}
	flags.BoolVar(&options.cpuProfile, "cpu-profile", false, "enable CPU profiling")
	flags.BoolVar(&options.memProfile, "mem-profile", false, "enable memory profiling")
	flags.BoolVar(&options.netDisable, "net-off", false, "disable network requests")
}

func startProfilesFor(cpuProfile, memProfile bool) (func(), *errs.Error) {
	var cleanups []func()
	if memProfile && !cpuProfile {
		server := &http.Server{Addr: ":6060"}
		go func() {
			log.Info("memory profile server listening", zap.String("address", ":6060"))
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("memory profile server failed", zap.Error(err))
			}
		}()
		cleanups = append(cleanups, func() { _ = server.Close() })
	}
	if cpuProfile {
		wd, err := os.Getwd()
		if err != nil {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
			return nil, errs.New(errs.CodeRunTime, err)
		}
		outPath := filepath.Join(wd, "cpu.profile")
		cleanup, profileErr := utils.StartCpuProfileScoped(outPath, 6060)
		if profileErr != nil {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
			return nil, profileErr
		}
		cleanups = append(cleanups, cleanup)
		log.Info("CPU profile started", zap.String("path", outPath))
	}
	if len(cleanups) == 0 {
		return nil, nil
	}
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			for i := len(cleanups) - 1; i >= 0; i-- {
				cleanups[i]()
			}
		})
	}
	return cleanup, nil
}

func newMergeAssetsCommand() *cobra.Command {
	var outPath string
	var lines string
	var dataDir string
	command := &cobra.Command{
		Use:     "merge-assets FILE FILE [FILE...]",
		Aliases: []string{"merge_assets"},
		Short:   "merge multiple assets.html files",
		Args:    cobra.MinimumNArgs(2),
		RunE: func(_ *cobra.Command, files []string) error {
			if outPath == "" {
				return errs.NewMsg(errs.CodeParamRequired, "--out is required")
			}
			filesMap := make(map[string]string, len(files))
			if dataDir == "" {
				dataDir = os.Getenv("BanDataDir")
			}
			if dataDir == "" {
				dataDir = "."
			}
			snapshot := config.NewSnapshotWithDirs(nil, dataDir, "")
			for _, file := range files {
				filesMap[snapshot.ParsePath(file)] = ""
			}
			outPath = snapshot.ParsePath(outPath)
			if err := opt.MergeAssetsHtml(outPath, filesMap, utils.SplitSolid(lines, ",", true), false); err != nil {
				return err
			}
			log.Info("assets merged", zap.String("path", outPath))
			return nil
		},
	}
	command.Flags().StringVar(&outPath, "out", "merged_assets.html", "output HTML file")
	command.Flags().StringVar(&dataDir, "datadir", "", "path used to resolve @/$ file paths")
	command.Flags().StringVar(&lines, "lines", "Real,Available", "comma-separated line names to extract")
	return command
}

func normalizeLegacyFlags(root *cobra.Command, args []string) []string {
	longFlags := map[string]bool{"help": true, "version": true}
	var collectFlags func(command *cobra.Command)
	collectFlags = func(command *cobra.Command) {
		command.Flags().VisitAll(func(flag *pflag.Flag) {
			longFlags[flag.Name] = true
		})
		for _, child := range command.Commands() {
			collectFlags(child)
		}
	}
	collectFlags(root)

	normalized := append([]string(nil), args...)
	flagsEnded := false
	for i, arg := range normalized {
		if arg == "--" {
			flagsEnded = true
			continue
		}
		if flagsEnded {
			continue
		}
		if len(arg) < 3 || !strings.HasPrefix(arg, "-") || strings.HasPrefix(arg, "--") {
			continue
		}
		first := arg[1]
		if first >= '0' && first <= '9' || first == '.' {
			continue
		}
		name := strings.SplitN(arg[1:], "=", 2)[0]
		if !longFlags[name] {
			continue
		}
		normalized[i] = "-" + arg
	}
	return normalized
}
