package entry

import (
	"slices"
	"testing"

	"github.com/banbox/banbot/config"
)

func dashboardTestRoot(enable bool, accounts ...string) *config.Config {
	root := &config.Config{
		Accounts: map[string]*config.AccountConfig{},
		APIServer: &config.APIServerConfig{
			Enable:       enable,
			BindIPAddr:   "127.0.0.1",
			Port:         8001,
			Verbosity:    "info",
			JWTSecretKey: "secret",
			CORSOrigins:  []string{"http://a", "http://b"},
			Users:        []*config.UserConfig{{Username: "u1"}, {Username: "u2"}},
		},
	}
	for _, a := range accounts {
		root.Accounts[a] = &config.AccountConfig{}
	}
	return root
}

func TestAccountDashboardConfigNilCases(t *testing.T) {
	if accountDashboardConfig(nil, "a") != nil {
		t.Fatal("nil root must return nil")
	}
	if accountDashboardConfig(&config.Config{}, "a") != nil {
		t.Fatal("nil APIServer must return nil")
	}
	if accountDashboardConfig(dashboardTestRoot(false, "a"), "a") != nil {
		t.Fatal("disabled APIServer must return nil")
	}
}

func TestAccountDashboardConfigSingleAccountReturnsIndependentCopy(t *testing.T) {
	root := dashboardTestRoot(true, "a")
	got := accountDashboardConfig(root, "a")
	if got == nil {
		t.Fatal("expected config for single account")
	}
	if got == root.APIServer {
		t.Fatal("must return a distinct copy")
	}
	if got.Enable != true || got.BindIPAddr != "127.0.0.1" || got.Port != 8001 ||
		got.Verbosity != "info" || got.JWTSecretKey != "secret" ||
		!slices.Equal(got.CORSOrigins, root.APIServer.CORSOrigins) ||
		len(got.Users) != 2 || got.Users[0].Username != "u1" || got.Users[1].Username != "u2" {
		t.Fatalf("copy fields differ: %+v", got)
	}
	got.Port = 9
	got.CORSOrigins[0] = "changed"
	got.CORSOrigins = append(got.CORSOrigins, "x")
	got.Users[0] = &config.UserConfig{Username: "changed"}
	got.Users = got.Users[:1]
	orig := root.APIServer
	if orig.Port != 8001 || len(orig.CORSOrigins) != 2 || orig.CORSOrigins[0] != "http://a" ||
		len(orig.Users) != 2 || orig.Users[0].Username != "u1" {
		t.Fatalf("mutating the copy changed the original: %+v", orig)
	}
}

func TestAccountDashboardConfigMultipleAccountsReturnsNil(t *testing.T) {
	if accountDashboardConfig(dashboardTestRoot(true, "a", "b"), "a") != nil {
		t.Fatal("multiple accounts must return nil")
	}
	if accountDashboardConfig(dashboardTestRoot(true), "a") != nil {
		t.Fatal("zero accounts must return nil")
	}
}

// Pins current behaviour: Config.Clone does not carry APIServer. This is why
// accountDashboardConfig must read APIServer from the root config instead of
// from a cloned per-account config. If Clone starts copying APIServer, this
// test may be updated and the helper reconsidered.
func TestConfigCloneDoesNotCarryAPIServerPinsCurrentBehavior(t *testing.T) {
	cfg := dashboardTestRoot(true, "a")
	if cfg.Clone().APIServer != nil {
		t.Fatal("Config.Clone now carries APIServer; revisit accountDashboardConfig")
	}
}
