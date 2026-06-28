package auth

import (
	"net/mail"
	"strings"
)

const (
	RoleAdmin  = "admin"
	RoleViewer = "viewer"
)

type Principal struct {
	UserID int64
	Email  string
	Role   string // "admin" or "viewer"
}

func (p Principal) IsAdmin() bool {
	return p.Role == RoleAdmin
}

func NormalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// IsValidEmail reports whether email parses as a single RFC 5322 address with a
// non-empty local-part and a domain that contains a dot. Callers should pass an
// already-normalized email (see NormalizeEmail).
func IsValidEmail(email string) bool {
	normalized := NormalizeEmail(email)
	if normalized == "" {
		return false
	}
	addr, err := mail.ParseAddress(normalized)
	if err != nil {
		return false
	}
	if addr.Address != normalized {
		return false
	}
	at := -1
	for i := len(addr.Address) - 1; i >= 0; i-- {
		if addr.Address[i] == '@' {
			at = i
			break
		}
	}
	if at <= 0 || at == len(addr.Address)-1 {
		return false
	}
	domain := addr.Address[at+1:]
	dot := false
	for i := 0; i < len(domain); i++ {
		if domain[i] == '.' && i > 0 && i < len(domain)-1 {
			dot = true
			break
		}
	}
	return dot
}
