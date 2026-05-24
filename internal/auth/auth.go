package auth

import "github.com/open-sspm/open-sspm/internal/normalize"

const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"

	MethodPassword = "password"
)

type Principal struct {
	UserID int64
	Email  string
	Role   string // "admin" or "viewer"
	Method string // "password" now; "oidc" later
}

func (p Principal) IsAdmin() bool {
	return p.Role == RoleAdmin
}

func NormalizeEmail(email string) string {
	return normalize.Email(email)
}
