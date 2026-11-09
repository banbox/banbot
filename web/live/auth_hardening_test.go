package live

import (
	"testing"

	"github.com/banbox/banbot/config"
)

func TestSecureEqual(t *testing.T) {
	cases := []struct {
		a, b string
		want bool
	}{
		{"pwd", "pwd", true},
		{"pwd", "pwd2", false},
		{"pwd", "", false},
		{"", "", true},
		{"a", "A", false},
	}
	for _, c := range cases {
		if got := secureEqual(c.a, c.b); got != c.want {
			t.Fatalf("secureEqual(%q,%q)=%v want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestStartAPIServerRejectsEmptyJWTSecret(t *testing.T) {
	for _, secret := range []string{"", "   "} {
		cfg := &config.APIServerConfig{Enable: true, BindIPAddr: "127.0.0.1", Port: -1, JWTSecretKey: secret}
		server, err := startAPIServer(nil, cfg, nil)
		if err == nil || server != nil {
			t.Fatalf("secret %q: got server=%v err=%v; want config error", secret, server, err)
		}
	}
}
