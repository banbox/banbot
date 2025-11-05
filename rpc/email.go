package rpc

import (
	"fmt"
	utils2 "github.com/banbox/banbot/utils"
	"github.com/banbox/banexg/log"
	"github.com/banbox/banexg/utils"
	"go.uber.org/zap"
	"strings"
)

// Email 表示邮件推送渠道，实现了 IWebHook 接口的核心功能
// 其配置继承自 webHookItem，额外支持 touser 字段来指定接收邮箱地址
// 示例配置（位于 rpc_channels.* 节点下）：
//
//   [rpc_channels.email_notice]
//   type = "email"          # 渠道类型，对应 ChlType
//   touser = "foo@bar.com"  # 必填，收件人邮箱
//   msg_types = ["status", "exception"]
//   retry_delay = 30
//   min_intv_secs = 5
//
// 通过 SendMsg -> Queue -> doSendMsgs 的链路实现异步批量发送与失败重试
// 实际发信调用 utils.SendEmailTo 或 utils.SendEmail。
//
// 注意：使用前需先在业务启动流程中调用 utils.SetMailSender(...) 完成 SMTP 客户端配置。

type Email struct {
	*WebHook
	toUser string
}

// NewEmail 构造函数，基于通用 WebHook 创建 Email 发送实例
func NewEmail(name string, item map[string]interface{}) *Email {
	hook := NewWebHook(name, item)
	res := &Email{
		WebHook: hook,
		toUser:  utils.GetMapVal(item, "touser", ""),
	}
	if res.toUser == "" {
		panic(name + ": `touser` is required")
	}
	res.doSendMsgs = makeDoSendMsgEmail(res)
	return res
}

// makeDoSendMsgEmail 返回批量邮件发送函数，符合 WebHook.doSendMsgs 的签名要求
func makeDoSendMsgEmail(e *Email) func([]map[string]string) []map[string]string {
	return func(msgList []map[string]string) []map[string]string {
		var b strings.Builder
		for _, msg := range msgList {
			content, _ := msg["content"]
			if content == "" {
				log.Error("email get empty msg, skip")
				continue
			}
			b.WriteString(content)
			b.WriteString("\n==========\n\n")
		}
		body := b.String()
		subject := fmt.Sprintf("[%d]%s", len(msgList), body[:min(len(body), 50)])
		if err := utils2.SendEmailTo(subject, body, e.toUser); err != nil {
			log.Error("email send fail", zap.String("to", e.toUser), zap.Error(err))
			return msgList
		}
		return nil
	}
}
