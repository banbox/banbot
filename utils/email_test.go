package utils

import (
	"os"
	"testing"
)

// TestMailSender_SendMailWithAttachment 测试发送带附件的邮件
func TestMailSender_SendMailWithAttachment(t *testing.T) {
	// 创建 MailSender 实例
	from := os.Getenv("SMTP_FROM")
	password := os.Getenv("SMTP_PASSWORD")
	host := os.Getenv("SMTP_HOST")
	sender := NewMailSender(host, 465, from, password)

	// 准备测试数据
	to := []string{"phiilo@163.com"}
	subject := "Test Email with Attachment"
	body := "This is a test email with attachment."

	// 创建附件（使用内存内容）
	attachments := []Attachment{
		{
			Filename:    "test.txt",
			Content:     []byte("This is test attachment content"),
			ContentType: "text/plain",
		},
	}

	err := sender.SendMail(from, to, subject, body, attachments, false)
	if err != nil {
		t.Fatal(err)
	}
}
