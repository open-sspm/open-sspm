package providers

import (
	"context"

	"github.com/open-sspm/open-sspm/internal/auth"
)

type Provider interface {
	Name() string
	Authenticate(ctx context.Context, email, password string) (auth.Principal, error)
}
