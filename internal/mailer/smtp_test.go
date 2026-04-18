package mailer

import (
	"context"
	"strings"
	"testing"
)

func TestNewSMTPRejectsInvalidConfig(t *testing.T) {
	_, err := NewSMTP(SMTPConfig{
		Host:        "",
		Port:        587,
		FromAddress: "noreply@example.com",
		TLSMode:     TLSModeStartTLS,
	})
	if err == nil {
		t.Fatalf("expected invalid SMTP config error")
	}
}

func TestNewSMTPRejectsPlainAuthWithoutTLSForRemoteHost(t *testing.T) {
	_, err := NewSMTP(SMTPConfig{
		Host:        "smtp.example.com",
		Port:        587,
		Username:    "mailer",
		Password:    "secret",
		FromAddress: "noreply@example.com",
		TLSMode:     TLSModePlain,
	})
	if err == nil {
		t.Fatalf("expected invalid SMTP auth config error")
	}
}

func TestNewSMTPNormalizesFromMailbox(t *testing.T) {
	m, err := NewSMTP(SMTPConfig{
		Host:        "smtp.example.com",
		Port:        587,
		FromAddress: "Open SSPM <noreply@example.com>",
		TLSMode:     TLSModeStartTLS,
	})
	if err != nil {
		t.Fatalf("NewSMTP() error = %v", err)
	}

	smtpMailer, ok := m.(*smtpMailer)
	if !ok {
		t.Fatalf("NewSMTP() returned %T, want *smtpMailer", m)
	}
	if got, want := smtpMailer.config.FromAddress, "noreply@example.com"; got != want {
		t.Fatalf("FromAddress = %q, want %q", got, want)
	}
	if got, want := smtpMailer.config.FromName, "Open SSPM"; got != want {
		t.Fatalf("FromName = %q, want %q", got, want)
	}
}

func TestBuildMessageFormatsHeadersAndRecipients(t *testing.T) {
	payload, recipients, err := buildMessage(SMTPConfig{
		Host:        "smtp.example.com",
		Port:        587,
		FromAddress: "noreply@example.com",
		FromName:    "Open SSPM",
		TLSMode:     TLSModeStartTLS,
	}, Message{
		To:       []string{"Alice <alice@example.com>", "bob@example.com"},
		Subject:  "Hello",
		TextBody: "Line one\nLine two",
	})
	if err != nil {
		t.Fatalf("buildMessage() error = %v", err)
	}

	if got, want := len(recipients), 2; got != want {
		t.Fatalf("len(recipients) = %d, want %d", got, want)
	}
	if got, want := recipients[0], "alice@example.com"; got != want {
		t.Fatalf("recipients[0] = %q, want %q", got, want)
	}
	if got, want := recipients[1], "bob@example.com"; got != want {
		t.Fatalf("recipients[1] = %q, want %q", got, want)
	}

	rendered := string(payload)
	for _, expected := range []string{
		"From: \"Open SSPM\" <noreply@example.com>\r\n",
		"To: \"Alice\" <alice@example.com>, <bob@example.com>\r\n",
		"Subject: Hello\r\n",
		"Content-Type: text/plain; charset=UTF-8\r\n",
		"Line one\r\nLine two",
	} {
		if !strings.Contains(rendered, expected) {
			t.Fatalf("rendered message missing %q\n%s", expected, rendered)
		}
	}
}

func TestNoopMailerSend(t *testing.T) {
	if err := NewNoop().Send(context.Background(), Message{
		To:       []string{"user@example.com"},
		Subject:  "ignored",
		TextBody: "ignored",
	}); err != nil {
		t.Fatalf("NewNoop().Send() error = %v", err)
	}
}
