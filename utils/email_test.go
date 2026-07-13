package utils

import (
	"os"
	"testing"
)

// TestMailSender_SendMailWithAttachment 测试发送带附件的邮件
func TestMailSender_SendMailWithAttachment(t *testing.T) {
	if os.Getenv("BANBOT_TEST_SMTP") != "1" {
		t.Skip("set BANBOT_TEST_SMTP=1 to run the external SMTP integration test")
	}

	from := os.Getenv("SMTP_FROM")
	password := os.Getenv("SMTP_PASSWORD")
	host := os.Getenv("SMTP_HOST")
	to := os.Getenv("SMTP_TO")
	if from == "" || password == "" || host == "" || to == "" {
		t.Fatal("SMTP_FROM, SMTP_PASSWORD, SMTP_HOST, and SMTP_TO are required")
	}
	sender := NewMailSender(host, 465, from, password)

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

	err := sender.SendMail(from, []string{to}, subject, body, attachments, false)
	if err != nil {
		t.Fatal(err)
	}
}
