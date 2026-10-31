package entry

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	"github.com/banbox/banbot/data"
	"github.com/banbox/banbot/execution"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newHubCommand() *cobra.Command {
	var socket string
	command := newRuntimeConfigCommand("hub", "host public market data and request-owned execution accounts", func(args *config.CmdArgs) *errs.Error {
		return runHubWith(args, socket, nil)
	}, false)
	command.Aliases = []string{"spider"}
	command.Flags().StringVar(&socket, "execution-socket", "", "private local execution socket (default: <datadir>/execution/hub.sock)")
	return command
}

func RunHub(args *config.CmdArgs) *errs.Error { return runHubWith(args, "", nil) }

// publicHubSnapshot deliberately does not inherit account credentials or
// arbitrary SDK options from hub YAML. Private clients are request-owned.
func publicHubSnapshot(snapshot *config.Snapshot) (*config.Snapshot, error) {
	if snapshot == nil || snapshot.View() == nil {
		return nil, errors.New("hub: configuration is required")
	}
	public := snapshot.Clone()
	cfg := public.View()
	if cfg.Env != core.RunEnvProd && cfg.Env != core.RunEnvDryRun {
		return nil, errors.New("hub: only prod and dry_run environments are supported; backtests execute in process")
	}
	if cfg.Exchange == nil || cfg.Exchange.Name == "" {
		return nil, errors.New("hub: public exchange is required")
	}
	cfg.Accounts = nil
	cfg.Exchange.Items = nil
	cfg.RunPolicy = nil
	return public, nil
}

func runHubWith(args *config.CmdArgs, socket string, startup data.SpiderStartupFunc) (result *errs.Error) {
	session, original, err := openExplicitEntrySession(args, "hub")
	if err != nil {
		return err
	}
	defer session.close()
	snapshot, publicErr := publicHubSnapshot(original)
	if publicErr != nil {
		return errs.New(core.ErrBadConfig, publicErr)
	}
	// Data mode avoids constructing the retired live sender for a public miner.
	rt, err := session.newRuntime(snapshot, core.RunModeData, btime.UTCStamp())
	if err != nil {
		return err
	}
	defer func() { rt.Close(); rt.Join() }()
	factory, closeFactory := session.spiderExchangeFactory(snapshot, rt.Exchange)
	defer closeFactory()
	runtimeFactory := data.SpiderRuntimeFactory(func(ctx context.Context, name, market string) (*data.RuntimeDeps, func(), *errs.Error) {
		if name == rt.Core.ExgName && market == rt.Core.Market {
			return rt.DataDeps(), func() {}, nil
		}
		return newDevChildRuntime(session, snapshot, ctx, name, market, factory)
	})
	root := filepath.Join(snapshot.DataDir, "execution")
	if err := os.MkdirAll(root, 0700); err != nil {
		return errs.New(core.ErrRunTime, err)
	}
	if socket == "" {
		socket = filepath.Join(root, "hub.sock")
	}
	var leaseDir string
	if configured, _ := session.runSpec.Config().Execution["sender_lease_dir"].(string); configured != "" {
		var pathErr error
		leaseDir, pathErr = session.runSpec.ResolvePath("execution.sender_lease_dir")
		if pathErr != nil {
			return errs.New(core.ErrBadConfig, pathErr)
		}
	}
	feeds := newHubFeedHost()
	private, hubErr := execution.NewHubWithRecovery(feeds.factory(newHubAccountFactory(root, leaseDir, session.netDisable), root), filepath.Join(root, "accounts.json"))
	if hubErr != nil {
		return errs.New(core.ErrRunTime, hubErr)
	}
	private.SetPendingFeedAdmission(func(ctx context.Context, binding execution.HubPendingFeed) error {
		return feeds.admit(ctx, private, binding)
	})
	defer func() {
		feeds.close()
		if closeErr := private.Close(); closeErr != nil {
			if result != nil {
				closeErr = errors.Join(result, closeErr)
			}
			result = errs.New(core.ErrRunTime, closeErr)
		}
	}()
	statuses, statusErr := private.RecoveryStatuses(rt.Context())
	if statusErr != nil {
		return errs.New(core.ErrRunTime, statusErr)
	}
	for _, status := range statuses {
		log.Warn("hub account requires reauthentication; local protection is NOT active", zap.Any("account", status.Account), zap.String("environment", status.Env), zap.String("state", status.State))
	}
	server, listenErr := execution.ListenHubUnix(socket, private)
	if listenErr != nil {
		return errs.New(core.ErrRunTime, listenErr)
	}
	defer func() {
		if closeErr := server.Close(); closeErr != nil {
			if result != nil {
				closeErr = errors.Join(result, closeErr)
			}
			result = errs.New(core.ErrRunTime, closeErr)
		}
	}()
	addr := snapshot.View().SpiderAddr
	if addr == "" {
		addr = "127.0.0.1:6789"
	}
	hubStartup := func(ctx context.Context, spider *data.LiveSpider) error {
		if startup != nil {
			if err := startup(ctx, spider); err != nil {
				return err
			}
		}
		return feeds.start(ctx, spider, private)
	}
	return data.RunLiveSpiderWithRuntimeDeps(rt.Context(), addr, rt.DataDeps(), runtimeFactory, hubStartup)
}
