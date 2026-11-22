package config

import "testing"

func TestParseScutilProxy(t *testing.T) {
	cases := []struct {
		name string
		out  string
		want string
	}{
		{"https enabled", "<dictionary> {\n  HTTPEnable : 1\n  HTTPPort : 7890\n  HTTPProxy : 127.0.0.1\n  HTTPSEnable : 1\n  HTTPSPort : 7891\n  HTTPSProxy : 127.0.0.1\n}\n", "http://127.0.0.1:7891"},
		{"only http enabled", "<dictionary> {\n  HTTPEnable : 1\n  HTTPPort : 8080\n  HTTPProxy : proxy.local\n  HTTPSEnable : 0\n}\n", "http://proxy.local:8080"},
		{"none enabled", "<dictionary> {\n  FTPPassive : 1\n  HTTPEnable : 0\n  HTTPSEnable : 0\n}\n", ""},
		{"enabled without host", "<dictionary> {\n  HTTPSEnable : 1\n  HTTPSPort : 7891\n}\n", ""},
	}
	for _, c := range cases {
		if got := parseScutilProxy(c.out); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
