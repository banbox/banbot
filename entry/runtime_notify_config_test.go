package entry

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func notifyRoot(accounts ...string) *config.Config {
	root := &config.Config{
		RPCChannels: map[string]map[string]interface{}{
			"tg": {"type": "telegram", "token": "t", "chat_id": 1},
			"we": {"type": "wework"},
		},
		Webhook:  map[string]map[string]string{"entry": {"content": "x"}},
		Accounts: map[string]*config.AccountConfig{},
	}
	for _, name := range accounts {
		root.Accounts[name] = &config.AccountConfig{}
	}
	return root
}

func TestMixedLiveAccountConfigCarriesNotificationsSingleAccount(t *testing.T) {
	root := notifyRoot("a")
	cfg := mixedLiveAccountConfig(root, "a", nil)
	if cfg.RPCChannels["tg"] == nil || cfg.RPCChannels["we"] == nil || cfg.Webhook["entry"] == nil {
		t.Fatalf("notifications missing: %v", cfg.RPCChannels)
	}
	cfg.RPCChannels["we"]["type"] = "changed"
	cfg.Webhook["entry"]["content"] = "changed"
	if root.RPCChannels["we"]["type"] != "wework" || root.Webhook["entry"]["content"] != "x" {
		t.Fatal("runtime config shares maps with the root config")
	}
}

func TestMixedLiveAccountConfigTelegramOnlyOnFirstOfSeveralAccounts(t *testing.T) {
	root := notifyRoot("b", "a", "c")
	root.Accounts["c"].NoTrade = true
	first := mixedLiveAccountConfig(root, "a", nil)
	if first.RPCChannels["tg"] == nil || first.RPCChannels["we"] == nil {
		t.Fatalf("first account must keep every channel: %v", first.RPCChannels)
	}
	for _, name := range []string{"b", "c"} {
		other := mixedLiveAccountConfig(root, name, nil)
		if other.RPCChannels["tg"] != nil {
			t.Fatalf("account %s must not start a second telegram listener", name)
		}
		if other.RPCChannels["we"] == nil {
			t.Fatalf("account %s lost its outbound channel", name)
		}
	}
	if root.RPCChannels["tg"] == nil {
		t.Fatal("root modified")
	}
}
