package config

import (
	"fmt"
	"maps"
)

// CopyNotifications copies the notification sections of src (rpc_channels,
// mail, webhook and the rpc_channels references of the given account) into c
// as deep copies; nothing is shared with src afterwards. Config.Clone leaves
// these sections out on purpose (Desensitize and the web views rely on it), so
// per-account runtime configs opt in through this helper.
//
// When inbound is false every channel of type "telegram" (which opens a bot
// listener or a dashboard connection) is left out, together with the account
// references to it, and the removed names are returned.
func (c *Config) CopyNotifications(src *Config, account string, inbound bool) (dropped []string) {
	if c == nil || src == nil {
		return nil
	}
	c.RPCChannels = nil
	c.Mail = nil
	c.Webhook = cloneWebhook(src.Webhook)
	if src.Mail != nil {
		mail := *src.Mail
		c.Mail = &mail
	}
	skip := map[string]bool{}
	if len(src.RPCChannels) > 0 {
		c.RPCChannels = make(map[string]map[string]interface{}, len(src.RPCChannels))
		for name, item := range src.RPCChannels {
			if !inbound && fmt.Sprint(item["type"]) == "telegram" {
				skip[name] = true
				dropped = append(dropped, name)
				continue
			}
			c.RPCChannels[name] = cloneStringMap(item)
		}
	}
	acc := c.Accounts[account]
	if acc == nil || len(acc.RPCChannels) == 0 {
		return dropped
	}
	item := *acc
	item.RPCChannels = make([]map[string]interface{}, 0, len(acc.RPCChannels))
	for _, chl := range acc.RPCChannels {
		if skip[fmt.Sprint(chl["name"])] {
			continue
		}
		item.RPCChannels = append(item.RPCChannels, maps.Clone(cloneStringMap(chl)))
	}
	c.Accounts = maps.Clone(c.Accounts)
	c.Accounts[account] = &item
	return dropped
}
