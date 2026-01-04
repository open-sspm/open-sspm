package providers

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

type PasswordProvider struct {
	Q *gen.Queries
}

func NewPasswordProvider(q *gen.Queries) *PasswordProvider {
	return &PasswordProvider{Q: q}
}

func (p *PasswordProvider) Name() string {
	return auth.MethodPassword
}

func (p *PasswordProvider) Authenticate(ctx context.Context, email, password string) (auth.Principal, error) {
	email = auth.NormalizeEmail(email)
	if email == "" || password == "" {
		return auth.Principal{}, auth.ErrInvalidCredentials
	}

	user, err := p.Q.GetAuthUserByEmail(ctx, email)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return auth.Principal{}, auth.ErrInvalidCredentials
		}
		return auth.Principal{}, err
	}
	if !user.IsActive {
		return auth.Principal{}, auth.ErrInvalidCredentials
	}

	match, err := auth.ComparePassword(password, user.PasswordHash)
	if err != nil {
		return auth.Principal{}, err
	}
	if !match {
		return auth.Principal{}, auth.ErrInvalidCredentials
	}

	return auth.Principal{
		UserID: user.ID,
		Email:  user.Email,
		Role:   user.Role,
		Method: auth.MethodPassword,
	}, nil
}
