package mailer

import "context"

type noopMailer struct{}

func NewNoop() Mailer {
	return noopMailer{}
}

func (noopMailer) Send(context.Context, Message) error {
	return nil
}
