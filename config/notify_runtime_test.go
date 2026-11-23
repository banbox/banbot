package config

import (
	"reflect"
	"testing"
)

func notifyTestRoot() *Config {
	return &Config{
		Name: "bot",
		RPCChannels: map[string]map[string]interface{}{
			"tg": {"type": "telegram", "token": "t", "msg_types": []string{"entry"}},
			"we": {"type": "wework", "corp_id": "c"},
		},
		Mail:    &MailConfig{Enable: true, Host: "h"},
		Webhook: map[string]map[string]string{"entry": {"content": "x"}},
		Accounts: map[string]*AccountConfig{
			"a": {RPCChannels: []map[string]interface{}{{"name": "tg"}, {"name": "we"}}},
			"b": {},
		},
	}
}

func TestCopyNotificationsIsDeepAndKeepsAllChannels(t *testing.T) {
	root := notifyTestRoot()
	dst := root.Clone()
	if dst.RPCChannels != nil || dst.Mail != nil {
		t.Fatal("Clone must keep omitting the notification sections")
	}
	if dropped := dst.CopyNotifications(root, "a", true); len(dropped) != 0 {
		t.Fatalf("dropped %v", dropped)
	}
	if !reflect.DeepEqual(dst.RPCChannels, root.RPCChannels) || *dst.Mail != *root.Mail || !reflect.DeepEqual(dst.Webhook, root.Webhook) {
		t.Fatal("sections not copied")
	}
	// Mutating the copy must not leak into the root.
	dst.RPCChannels["we"]["corp_id"] = "other"
	dst.RPCChannels["tg"]["msg_types"].([]string)[0] = "exit"
	dst.Webhook["entry"]["content"] = "y"
	dst.Mail.Host = "z"
	dst.Accounts["a"].RPCChannels[0]["name"] = "zz"
	if root.RPCChannels["we"]["corp_id"] != "c" || root.RPCChannels["tg"]["msg_types"].([]string)[0] != "entry" ||
		root.Webhook["entry"]["content"] != "x" || root.Mail.Host != "h" || root.Accounts["a"].RPCChannels[0]["name"] != "tg" {
		t.Fatal("copy shares memory with the root config")
	}
}

func TestCopyNotificationsWithoutInboundDropsTelegram(t *testing.T) {
	root := notifyTestRoot()
	dst := root.Clone()
	dropped := dst.CopyNotifications(root, "a", false)
	if len(dropped) != 1 || dropped[0] != "tg" {
		t.Fatalf("dropped %v", dropped)
	}
	if _, ok := dst.RPCChannels["tg"]; ok || dst.RPCChannels["we"] == nil {
		t.Fatalf("channels: %v", dst.RPCChannels)
	}
	refs := dst.Accounts["a"].RPCChannels
	if len(refs) != 1 || refs[0]["name"] != "we" {
		t.Fatalf("account refs to dropped channel must go too: %v", refs)
	}
	if len(root.Accounts["a"].RPCChannels) != 2 || root.RPCChannels["tg"] == nil {
		t.Fatal("root modified")
	}
}
