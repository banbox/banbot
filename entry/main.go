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
	"github.com/banbox/banbot/legacygate"
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
		command = withLegacyCommand(web.NewCommand())
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
	root := &cobra.Command{
		Use:           "banbot",
		Short:         "Banbot quantitative trading and data tools",
		Version:       core.Version,
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		Annotations:   map[string]string{legacyGateAnnotation: "1"},
		RunE: func(_ *cobra.Command, _ []string) error {
			return web.RunDev([]string{})
		},
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetVersionTemplate("banbot {{.Version}}\n")

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
	registerExtraCommands(root, groups)
	return root
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

func newConfigCommand(name, help string, run FuncEntry, allowDeadlock bool, binders ...flagBinder) *cobra.Command {
	return newConfigCommandWithGate(name, help, run, allowDeadlock, true, binders...)
}

func newRuntimeConfigCommand(name, help string, run FuncEntry, allowDeadlock bool, binders ...flagBinder) *cobra.Command {
	command := newConfigCommandWithGate(name, help, run, allowDeadlock, false, binders...)
	// Keep the historical marker for command-tree compatibility. Unlike a
	// legacy command, this callback does not acquire the gate.
	command.Annotations = map[string]string{legacyGateAnnotation: "1"}
	return command
}

func newConfigCommandWithGate(name, help string, run FuncEntry, allowDeadlock, legacyGate bool, binders ...flagBinder) *cobra.Command {
	args := &config.CmdArgs{}
	legacy := &legacyCommandFlags{}
	command := &cobra.Command{
		Use:   name,
		Short: help,
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			args.BTStrictSet = command.Flags().Changed("bt-strict")
			if !legacyGate {
				args.NetDisable = legacy.netDisable
				args.CPUProfile = legacy.cpuProfile
				args.MemProfile = legacy.memProfile
				if err := run(args); err != nil {
					return err
				}
				return nil
			}
			return runConfigCommand(args, legacy, run)
		},
	}
	if legacyGate {
		command.Annotations = map[string]string{legacyGateAnnotation: "1"}
	}
	bindCommonFlags(args, legacy, command.Flags(), allowDeadlock)
	for _, bind := range binders {
		bind(args, command.Flags())
	}
	return command
}

func newLegacySessionConfigCommand(name, help string, run func(*config.CmdArgs, opt.LegacySession) *errs.Error, allowDeadlock bool, binders ...flagBinder) *cobra.Command {
	args := &config.CmdArgs{}
	legacy := &legacyCommandFlags{}
	command := &cobra.Command{
		Use:         name,
		Short:       help,
		Args:        cobra.NoArgs,
		Annotations: map[string]string{legacyGateAnnotation: "1"},
		RunE: func(command *cobra.Command, _ []string) error {
			args.BTStrictSet = command.Flags().Changed("bt-strict")
			return runConfigCommandWithLegacySession(args, legacy, run)
		},
	}
	bindCommonFlags(args, legacy, command.Flags(), allowDeadlock)
	for _, bind := range binders {
		bind(args, command.Flags())
	}
	return command
}

const legacyGateAnnotation = legacygate.Annotation

func hasLegacyGate(command *cobra.Command) bool {
	return command != nil && command.Annotations != nil && command.Annotations[legacyGateAnnotation] == "1"
}

func markLegacyGate(command *cobra.Command) {
	if command == nil {
		return
	}
	if command.Annotations == nil {
		command.Annotations = make(map[string]string)
	}
	command.Annotations[legacyGateAnnotation] = "1"
}

// withLegacyCommand serializes every runnable command in a Cobra tree that
// still reaches package-level compatibility state.
func withLegacyCommand(command *cobra.Command) *cobra.Command {
	if command == nil {
		return nil
	}
	if !hasLegacyGate(command) {
		if run := command.RunE; run != nil {
			command.RunE = func(cmd *cobra.Command, args []string) error {
				return opt.WithCommandLegacySession(func(opt.LegacySession) error { return run(cmd, args) })
			}
		} else if run := command.Run; run != nil {
			command.Run = func(cmd *cobra.Command, args []string) {
				opt.WithCommandLegacySession(func(opt.LegacySession) struct{} {
					run(cmd, args)
					return struct{}{}
				})
			}
		}
		markLegacyGate(command)
	}
	for _, child := range command.Commands() {
		withLegacyCommand(child)
	}
	return command
}

type legacyCommandFlags struct {
	cpuProfile bool
	memProfile bool
	netDisable bool
}

func bindCommonFlags(args *config.CmdArgs, legacy *legacyCommandFlags, flags *pflag.FlagSet, allowDeadlock bool) {
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
	flags.BoolVar(&legacy.cpuProfile, "cpu-profile", false, "enable CPU profiling")
	flags.BoolVar(&legacy.memProfile, "mem-profile", false, "enable memory profiling")
	flags.BoolVar(&legacy.netDisable, "net-off", false, "disable network requests")
}

func runConfigCommand(args *config.CmdArgs, legacy *legacyCommandFlags, run FuncEntry) error {
	return opt.WithLegacySession(func(opt.LegacySession) error {
		core.CPUProfile, core.MemProfile, core.NetDisable = legacy.cpuProfile, legacy.memProfile, legacy.netDisable
		core.SetRunMode(core.RunModeOther)
		args.Init()
		startProfiles()
		if err := run(args); err != nil {
			return err
		}
		return nil
	})
}

func runConfigCommandWithLegacySession(args *config.CmdArgs, legacy *legacyCommandFlags, run func(*config.CmdArgs, opt.LegacySession) *errs.Error) error {
	return opt.WithLegacySession(func(session opt.LegacySession) error {
		core.CPUProfile, core.MemProfile, core.NetDisable = legacy.cpuProfile, legacy.memProfile, legacy.netDisable
		core.SetRunMode(core.RunModeOther)
		args.Init()
		startProfiles()
		if err := run(args, session); err != nil {
			return err
		}
		return nil
	})
}

func startProfiles() func() {
	cleanup, err := startProfilesFor(core.CPUProfile, core.MemProfile)
	if err != nil {
		panic(err)
	}
	if cleanup != nil {
		core.AddExitCall(cleanup)
	}
	return cleanup
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

func newPositionalCommand(name, help, argName string, run func(args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:         fmt.Sprintf("%s %s", name, argName),
		Short:       help,
		Args:        cobra.ExactArgs(1),
		Annotations: map[string]string{legacyGateAnnotation: "1"},
		RunE: func(_ *cobra.Command, args []string) error {
			return opt.WithLegacySession(func(opt.LegacySession) error {
				core.SetRunMode(core.RunModeOther)
				return run(args)
			})
		},
	}
}

func newMergeAssetsCommand() *cobra.Command {
	var outPath string
	var lines string
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
			for _, file := range files {
				filesMap[config.ParsePath(file)] = ""
			}
			outPath = config.ParsePath(outPath)
			if err := opt.MergeAssetsHtml(outPath, filesMap, utils.SplitSolid(lines, ",", true), false); err != nil {
				return err
			}
			log.Info("assets merged", zap.String("path", outPath))
			return nil
		},
	}
	command.Flags().StringVar(&outPath, "out", "merged_assets.html", "output HTML file")
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
