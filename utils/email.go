package utils

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/banbox/banexg/log"
	"go.uber.org/zap"
	"mime"
	"net/smtp"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Attachment 表示邮件附件
type Attachment struct {
	// Filename 附件文件名（必填）
	Filename string
	// Content 附件内容（二选一：Content 或 FilePath）
	Content []byte
	// FilePath 附件文件路径（二选一：Content 或 FilePath）
	FilePath string
	// ContentType MIME类型，如果为空则自动检测
	ContentType string
}

// EmailTask 表示一个邮件发送任务
type EmailTask struct {
	From    string
	To      []string // 支持多个收件人
	Subject string
	Body    string
	// Attachments 附件列表（可选）
	Attachments []Attachment
	// IsHTML 是否为HTML格式邮件（默认false为纯文本）
	IsHTML bool
}

var (
	emailQueue     chan *EmailTask
	emailQueueOnce sync.Once
	// 默认队列大小
	defaultQueueSize = 1000
	mailSender       *MailSender
)

// StartEmailWorker 启动邮件发送worker
func StartEmailWorker(queueSize int) {
	emailQueueOnce.Do(func() {
		if queueSize <= 0 {
			queueSize = defaultQueueSize
		}
		emailQueue = make(chan *EmailTask, queueSize)

		go func() {
			log.Info("Email worker started")
			for task := range emailQueue {
				err := mailSender.SendMail(task.From, task.To, task.Subject, task.Body, task.Attachments, task.IsHTML)
				if err != nil {
					log.Error("Failed to send email from queue",
						zap.Error(err),
						zap.Strings("to", task.To),
						zap.String("subject", task.Subject))
				} else {
					log.Info("send email ok", zap.Strings("to", task.To),
						zap.String("subject", task.Subject))
				}
			}
		}()
	})
}

func SetMailSender(host string, port int, username, password string) {
	mailSender = NewMailSender(host, port, username, password)
}

// SendEmailTo sends an email to a single recipient
func SendEmailTo(subject, body string, toList ...string) error {
	if len(toList) == 0 {
		return fmt.Errorf("email recipient is required")
	}
	return SendEmail(&EmailTask{
		To:      toList,
		Subject: subject,
		Body:    body,
	})
}

// SendEmail sends an email with attachments
func SendEmail(task *EmailTask) error {
	if task == nil {
		return nil
	}
	// 确保邮件工作线程已经启动
	if emailQueue == nil {
		if mailSender == nil {
			return nil
		}
		StartEmailWorker(defaultQueueSize)
	}
	select {
	case emailQueue <- task:
		return nil
	default:
		log.Warn("Email queue is full, task dropped", zap.Strings("to", task.To), zap.String("subject", task.Subject))
		return fmt.Errorf("email queue is full")
	}
}

func (t *EmailTask) AttachFile(filePath string) error {
	_, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	t.Attachments = append(t.Attachments, Attachment{
		Filename: filepath.Base(filePath),
		FilePath: filePath,
	})
	return nil
}

func (t *EmailTask) AttachFileBy(filename string, content []byte, contentType string) {
	if len(content) == 0 {
		return
	}
	t.Attachments = append(t.Attachments, Attachment{
		Filename:    filename,
		Content:     content,
		ContentType: contentType,
	})
}

type MailSender struct {
	host     string
	port     int
	username string
	password string
	client   *smtp.Client
	mu       sync.Mutex
}

func NewMailSender(host string, port int, username, password string) *MailSender {
	return &MailSender{
		host:     host,
		port:     port,
		username: username,
		password: password,
	}
}

func (ms *MailSender) getClient() (*smtp.Client, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// 如果已有客户端且连接正常，直接返回
	if ms.client != nil {
		if err := ms.client.Noop(); err == nil {
			return ms.client, nil
		}
		// 连接已断开，关闭旧客户端
		ms.client.Close()
	}

	// 建立新连接
	tlsConfig := &tls.Config{
		InsecureSkipVerify: false,
		ServerName:         ms.host,
	}

	addr := fmt.Sprintf("%s:%d", ms.host, ms.port)
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("TLS dial failed: %v", err)
	}

	client, err := smtp.NewClient(conn, ms.host)
	if err != nil {
		return nil, fmt.Errorf("SMTP client creation failed: %v", err)
	}

	// 认证
	auth := smtp.PlainAuth("", ms.username, ms.password, ms.host)
	if err = client.Auth(auth); err != nil {
		return nil, fmt.Errorf("SMTP auth failed: %v", err)
	}

	ms.client = client
	return client, nil
}

func (ms *MailSender) SendMail(from string, to []string, subject, body string, attachments []Attachment, isHTML bool) error {
	if len(to) == 0 {
		return fmt.Errorf("no recipients specified")
	}

	client, err := ms.getClient()
	if err != nil {
		return err
	}
	if from == "" {
		from = ms.username
	}

	// 构建邮件消息
	message, err := ms.buildMessage(from, to, subject, body, attachments, isHTML)
	if err != nil {
		return fmt.Errorf("failed to build message: %v", err)
	}

	// 使用同一个客户端发送多封邮件
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err = client.Mail(from); err != nil {
		return fmt.Errorf("MAIL command failed: %v", err)
	}
	// 为每个收件人调用 RCPT 命令
	for _, recipient := range to {
		if err = client.Rcpt(recipient); err != nil {
			return fmt.Errorf("RCPT command failed for %s: %v", recipient, err)
		}
	}
	w, err := client.Data()
	if err != nil {
		return fmt.Errorf("DATA command failed: %v", err)
	}
	if _, err = w.Write(message); err != nil {
		return fmt.Errorf("message writing failed: %v", err)
	}
	if err = w.Close(); err != nil {
		return fmt.Errorf("message closing failed: %v", err)
	}

	return nil
}

// buildMessage 构建邮件消息（支持附件和HTML）
func (ms *MailSender) buildMessage(from string, to []string, subject, body string, attachments []Attachment, isHTML bool) ([]byte, error) {
	var builder strings.Builder

	// 邮件头
	builder.WriteString("From: " + from + "\r\n")
	builder.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	builder.WriteString("Subject: " + subject + "\r\n")
	builder.WriteString("MIME-Version: 1.0\r\n")

	// 如果没有附件，使用简单格式
	if len(attachments) == 0 {
		if isHTML {
			builder.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
		} else {
			builder.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
		}
		builder.WriteString("\r\n")
		builder.WriteString(body)
		builder.WriteString("\r\n")
		return []byte(builder.String()), nil
	}

	// 有附件时使用multipart/mixed
	boundary := "----=_Part_" + RandomStr(32)
	builder.WriteString("Content-Type: multipart/mixed; boundary=\"" + boundary + "\"\r\n")
	builder.WriteString("\r\n")

	// 邮件正文部分
	builder.WriteString("--" + boundary + "\r\n")
	if isHTML {
		builder.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
	} else {
		builder.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	}
	builder.WriteString("Content-Transfer-Encoding: 8bit\r\n")
	builder.WriteString("\r\n")
	builder.WriteString(body)
	builder.WriteString("\r\n")

	// 附件部分
	for _, att := range attachments {
		if err := ms.writeAttachment(&builder, boundary, att); err != nil {
			return nil, fmt.Errorf("failed to write attachment %s: %v", att.Filename, err)
		}
	}

	// 结束边界
	builder.WriteString("--" + boundary + "--\r\n")

	return []byte(builder.String()), nil
}

// writeAttachment 写入附件到消息
func (ms *MailSender) writeAttachment(builder *strings.Builder, boundary string, att Attachment) error {
	// 获取附件内容
	var content []byte
	var err error

	if len(att.Content) > 0 {
		content = att.Content
	} else if att.FilePath != "" {
		content, err = os.ReadFile(att.FilePath)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %v", att.FilePath, err)
		}
	} else {
		return fmt.Errorf("attachment %s has no content or file path", att.Filename)
	}

	// 检测Content-Type
	contentType := att.ContentType
	if contentType == "" {
		contentType = detectContentType(att.Filename)
	}

	// 写入附件头
	builder.WriteString("--" + boundary + "\r\n")
	builder.WriteString("Content-Type: " + contentType + "; name=\"" + att.Filename + "\"\r\n")
	builder.WriteString("Content-Transfer-Encoding: base64\r\n")
	builder.WriteString("Content-Disposition: attachment; filename=\"" + att.Filename + "\"\r\n")
	builder.WriteString("\r\n")

	// Base64编码内容
	encoded := base64.StdEncoding.EncodeToString(content)
	// 每76个字符换行（RFC 2045标准）
	for i := 0; i < len(encoded); i += 76 {
		end := i + 76
		if end > len(encoded) {
			end = len(encoded)
		}
		builder.WriteString(encoded[i:end])
		builder.WriteString("\r\n")
	}

	return nil
}

// detectContentType 检测文件的MIME类型
func detectContentType(filename string) string {
	// 首先尝试根据文件扩展名判断
	ext := filepath.Ext(filename)
	if ext != "" {
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			return mimeType
		}
	}

	// 默认使用application/octet-stream
	return "application/octet-stream"
}
