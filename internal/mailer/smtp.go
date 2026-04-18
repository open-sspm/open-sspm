package mailer

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"strings"
	"time"
)

type smtpMailer struct {
	config SMTPConfig
}

func NewSMTP(cfg SMTPConfig) (Mailer, error) {
	normalized, err := normalizeSMTPConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &smtpMailer{config: normalized}, nil
}

func (m *smtpMailer) Send(ctx context.Context, msg Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	payload, recipients, err := buildMessage(m.config, msg)
	if err != nil {
		return err
	}

	client, err := m.connect(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	if m.config.Username != "" {
		auth := smtp.PlainAuth("", m.config.Username, m.config.Password, m.config.Host)
		if err := client.Auth(auth); err != nil {
			return fmt.Errorf("smtp auth: %w", err)
		}
	}

	if err := client.Mail(m.config.FromAddress); err != nil {
		return fmt.Errorf("smtp from: %w", err)
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return fmt.Errorf("smtp recipient %q: %w", recipient, err)
		}
	}

	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp data: %w", err)
	}
	if _, err := writer.Write(payload); err != nil {
		_ = writer.Close()
		return fmt.Errorf("smtp write: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("smtp finalize: %w", err)
	}

	if err := client.Quit(); err != nil {
		return fmt.Errorf("smtp quit: %w", err)
	}
	return nil
}

func (m *smtpMailer) connect(ctx context.Context) (*smtp.Client, error) {
	hostPort := net.JoinHostPort(m.config.Host, fmt.Sprintf("%d", m.config.Port))
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: m.config.Host,
	}

	var (
		conn net.Conn
		err  error
	)

	switch m.config.TLSMode {
	case TLSModeTLS:
		dialer := &tls.Dialer{
			Config:    tlsConfig,
			NetDialer: &net.Dialer{},
		}
		conn, err = dialer.DialContext(ctx, "tcp", hostPort)
	case TLSModeStartTLS, TLSModePlain:
		dialer := &net.Dialer{}
		conn, err = dialer.DialContext(ctx, "tcp", hostPort)
	default:
		return nil, fmt.Errorf("unsupported SMTP TLS mode %q", m.config.TLSMode)
	}
	if err != nil {
		return nil, fmt.Errorf("smtp dial: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}

	client, err := smtp.NewClient(conn, m.config.Host)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("smtp client: %w", err)
	}

	if m.config.TLSMode == TLSModeStartTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			_ = client.Close()
			return nil, errors.New("smtp server does not advertise STARTTLS")
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("smtp starttls: %w", err)
		}
	}

	return client, nil
}

func validateSMTPConfig(cfg SMTPConfig) error {
	if strings.TrimSpace(cfg.Host) == "" {
		return errors.New("smtp host is required")
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return errors.New("smtp port must be between 1 and 65535")
	}
	if strings.TrimSpace(cfg.FromAddress) == "" {
		return errors.New("smtp from address is required")
	}
	if _, err := mail.ParseAddress(cfg.FromAddress); err != nil {
		return fmt.Errorf("smtp from address: %w", err)
	}
	switch strings.ToLower(strings.TrimSpace(cfg.TLSMode)) {
	case TLSModeStartTLS, TLSModeTLS, TLSModePlain:
	default:
		return fmt.Errorf("unsupported SMTP TLS mode %q", cfg.TLSMode)
	}
	if (strings.TrimSpace(cfg.Username) == "") != (cfg.Password == "") {
		return errors.New("smtp username and password must either both be set or both be empty")
	}
	if cfg.Username != "" && cfg.TLSMode == TLSModePlain && !plainAuthAllowsInsecureHost(cfg.Host) {
		return errors.New("smtp plain auth requires TLS or localhost")
	}
	if containsHeaderBreak(cfg.FromName) {
		return errors.New("smtp from name must not contain line breaks")
	}
	return nil
}

func normalizeSMTPConfig(cfg SMTPConfig) (SMTPConfig, error) {
	cfg.Host = strings.TrimSpace(cfg.Host)
	cfg.Username = strings.TrimSpace(cfg.Username)
	cfg.FromAddress = strings.TrimSpace(cfg.FromAddress)
	cfg.FromName = strings.TrimSpace(cfg.FromName)
	cfg.TLSMode = strings.ToLower(strings.TrimSpace(cfg.TLSMode))

	addr, err := mail.ParseAddress(cfg.FromAddress)
	if err != nil {
		return cfg, fmt.Errorf("smtp from address: %w", err)
	}
	cfg.FromAddress = addr.Address
	if cfg.FromName == "" && strings.TrimSpace(addr.Name) != "" {
		cfg.FromName = addr.Name
	}

	if err := validateSMTPConfig(cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func buildMessage(cfg SMTPConfig, msg Message) ([]byte, []string, error) {
	recipients, toHeader, err := normalizeRecipients(msg.To)
	if err != nil {
		return nil, nil, err
	}
	if containsHeaderBreak(msg.Subject) {
		return nil, nil, errors.New("email subject must not contain line breaks")
	}

	var buf bytes.Buffer
	if _, err := fmt.Fprintf(&buf, "Date: %s\r\n", time.Now().UTC().Format(time.RFC1123Z)); err != nil {
		return nil, nil, err
	}
	if _, err := fmt.Fprintf(&buf, "From: %s\r\n", (&mail.Address{
		Name:    cfg.FromName,
		Address: cfg.FromAddress,
	}).String()); err != nil {
		return nil, nil, err
	}
	if _, err := fmt.Fprintf(&buf, "To: %s\r\n", strings.Join(toHeader, ", ")); err != nil {
		return nil, nil, err
	}
	if subject := strings.TrimSpace(msg.Subject); subject != "" {
		if _, err := fmt.Fprintf(&buf, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", subject)); err != nil {
			return nil, nil, err
		}
	}
	if _, err := io.WriteString(&buf, "MIME-Version: 1.0\r\n"); err != nil {
		return nil, nil, err
	}
	if _, err := io.WriteString(&buf, "Content-Type: text/plain; charset=UTF-8\r\n"); err != nil {
		return nil, nil, err
	}
	if _, err := io.WriteString(&buf, "Content-Transfer-Encoding: quoted-printable\r\n\r\n"); err != nil {
		return nil, nil, err
	}

	qp := quotedprintable.NewWriter(&buf)
	if _, err := io.WriteString(qp, normalizeBody(msg.TextBody)); err != nil {
		_ = qp.Close()
		return nil, nil, err
	}
	if err := qp.Close(); err != nil {
		return nil, nil, err
	}

	return buf.Bytes(), recipients, nil
}

func normalizeRecipients(values []string) ([]string, []string, error) {
	if len(values) == 0 {
		return nil, nil, errors.New("email must include at least one recipient")
	}

	recipients := make([]string, 0, len(values))
	headers := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		addr, err := mail.ParseAddress(value)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid recipient address %q: %w", value, err)
		}
		recipients = append(recipients, addr.Address)
		headers = append(headers, addr.String())
	}
	if len(recipients) == 0 {
		return nil, nil, errors.New("email must include at least one recipient")
	}
	return recipients, headers, nil
}

func normalizeBody(body string) string {
	body = strings.ReplaceAll(body, "\r\n", "\n")
	body = strings.ReplaceAll(body, "\r", "\n")
	return strings.ReplaceAll(body, "\n", "\r\n")
}

func containsHeaderBreak(value string) bool {
	return strings.ContainsAny(value, "\r\n")
}

func plainAuthAllowsInsecureHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
}
