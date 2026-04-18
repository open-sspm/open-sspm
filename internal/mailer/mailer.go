package mailer

import "context"

// Mailer sends plain-text transactional email messages.
type Mailer interface {
	Send(context.Context, Message) error
}

type Message struct {
	To       []string
	Subject  string
	TextBody string
}

type SMTPConfig struct {
	Host        string
	Port        int
	Username    string
	Password    string
	FromAddress string
	FromName    string
	TLSMode     string
}

const (
	TLSModeStartTLS = "starttls"
	TLSModeTLS      = "tls"
	TLSModePlain    = "plain"
)
