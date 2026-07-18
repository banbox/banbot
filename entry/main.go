package entry

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/opt"
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

// Execute runs banbot with an explicit argument list. It is separated from
// RunCmd so callers and tests can execute the Cobra command tree without exits.
func Execute(args []string) error {
	command := NewRootCommand()
	if isImplicitWebInvocation(args) {
		command = web.NewCommand()
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
		if core.StopAll != nil {
			core.StopAll()
		}
		core.RunExitCalls()
		os.Exit(0)
	}()
}

func newConfigCommand(name, help string, run FuncEntry, allowDeadlock bool, binders ...flagBinder) *cobra.Command {
	args := &config.CmdArgs{}
	command := &cobra.Command{
		Use:   name,
		Short: help,
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runConfigCommand(args, run)
		},
	}
	bindCommonFlags(args, command.Flags(), allowDeadlock)
	for _, bind := range binders {
		bind(args, command.Flags())
	}
	return command
}

func bindCommonFlags(args *config.CmdArgs, flags *pflag.FlagSet, allowDeadlock bool) {
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
	flags.BoolVar(&core.CPUProfile, "cpu-profile", false, "enable CPU profiling")
	flags.BoolVar(&core.MemProfile, "mem-profile", false, "enable memory profiling")
	flags.BoolVar(&core.NetDisable, "net-off", false, "disable network requests")
}

func runConfigCommand(args *config.CmdArgs, run FuncEntry) error {
	core.SetRunMode(core.RunModeOther)
	args.Init()
	startProfiles()
	if err := run(args); err != nil {
		return err
	}
	return nil
}

func startProfiles() {
	if core.MemProfile {
		go func() {
			log.Info("memory profile server listening", zap.String("address", ":6060"))
			if err := http.ListenAndServe(":6060", nil); err != nil {
				log.Error("memory profile server failed", zap.Error(err))
			}
		}()
	}
	if !core.CPUProfile {
		return
	}
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	outPath := filepath.Join(wd, "cpu.profile")
	if err = utils.StartCpuProfile(outPath, 6060); err != nil {
		panic(err)
	}
	log.Info("CPU profile started", zap.String("path", outPath))
}

func newPositionalCommand(name, help, argName string, run func(args []string) error) *cobra.Command {
	return &cobra.Command{
		Use:   fmt.Sprintf("%s %s", name, argName),
		Short: help,
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			core.SetRunMode(core.RunModeOther)
			return run(args)
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
