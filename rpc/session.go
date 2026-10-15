package rpc

import (
	"encoding/json"
	"fmt"
	"maps"
	"sync"

	"github.com/banbox/banbot/btime"
	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/core"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/errs"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
)

type Session struct {
	Core     *core.State
	Clock    *btime.ClockState
	dataDir  string
	messages map[string]string
	config   *config.Config
	accounts map[string]*config.AccountConfig
	mu       sync.RWMutex
	channels []IWebHook
	started  bool
	closed   bool
	stopOnce sync.Once
	joinOnce sync.Once
	stopDone chan struct{}
	Orders   OrderManagerInterface
	Wallets  WalletInfoProvider
}

func NewSession(snapshot *config.Snapshot, accounts map[string]*config.AccountConfig) *Session {
	cfg := &config.Config{}
	if snapshot != nil && snapshot.View() != nil {
		cfg = snapshot.View()
	}
	session := &Session{config: cfg, accounts: maps.Clone(accounts), stopDone: make(chan struct{})}
	if snapshot != nil {
		session.dataDir = snapshot.DataDir
		if contents, err := config.ReadLangFileFrom(session.dataDir, cfg.ShowLangCode, "messages.json"); err == nil {
			_ = json.Unmarshal([]byte(contents), &session.messages)
		}
	}
	return session
}

func (s *Session) Start() *errs.Error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started || s.closed {
		return nil
	}
	channels, err := buildChannels(s.config, s.accounts, func(name string, item map[string]interface{}) *Telegram {
		return newTelegramWithSession(name, item, s)
	})
	if err != nil {
		return err
	}
	for _, channel := range channels {
		if email, ok := channel.(*Email); ok {
			mail := s.config.Mail
			if mail == nil || !mail.Enable {
				email.send = func(string, string, string) error { return fmt.Errorf("runtime SMTP is not configured") }
			} else {
				sender := utils2.NewMailSender(mail.Host, mail.Port, mail.Username, mail.Password)
				email.send = func(subject, body, recipient string) error {
					return sender.SendMail(mail.Username, []string{recipient}, subject, body, nil, false)
				}
			}
		}
	}
	s.channels, s.started = channels, true
	for _, channel := range channels {
		go channel.ConsumeForever()
	}
	return nil
}

func (s *Session) SendMsg(msg map[string]interface{}) {
	if s == nil {
		return
	}
	if err := s.Start(); err != nil {
		s.Core.Log().Error("start runtime notifications", zap.Error(err))
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	values := maps.Clone(msg)
	account := utils.GetMapVal(values, "account", "")
	name := s.config.Name
	if account != "" {
		name += "/" + account
	}
	values["name"] = name
	msgType := utils.GetMapVal(values, "type", "")
	template := s.config.Webhook[msgType]
	if template == nil {
		return
	}
	payload := make(map[string]string, len(template))
	for key, format := range template {
		payload[key] = utils2.FormatWithMap(format, values)
	}
	for _, channel := range s.channels {
		channel.SendMsg(msgType, account, payload)
	}
}

func (s *Session) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		channels := append([]IWebHook(nil), s.channels...)
		s.mu.Unlock()
		for _, channel := range channels {
			stopChannel(channel)
		}
		close(s.stopDone)
	})
}

func (s *Session) Join() {
	if s == nil {
		return
	}
	s.Stop()
	s.joinOnce.Do(func() {
		<-s.stopDone
		for _, channel := range s.channels {
			joinChannel(channel)
		}
	})
}

func (s *Session) Close() { s.Join() }
