package entra

import (
	"strings"
)

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func looksLikeEmail(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if strings.Contains(s, " ") {
		return false
	}
	return strings.Contains(s, "@")
}

func preferredEmail(u User) string {
	if v := stringValue(u.GetMail()); looksLikeEmail(v) {
		return v
	}
	if strings.EqualFold(stringValue(u.GetUserType()), "Guest") {
		for _, v := range u.GetOtherMails() {
			if v = strings.TrimSpace(v); looksLikeEmail(v) {
				return v
			}
		}
	}
	if v := stringValue(u.GetUserPrincipalName()); looksLikeEmail(v) {
		return v
	}
	for _, v := range u.GetOtherMails() {
		if v = strings.TrimSpace(v); looksLikeEmail(v) {
			return v
		}
	}
	for _, v := range u.GetProxyAddresses() {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if idx := strings.IndexByte(v, ':'); idx > 0 {
			v = v[idx+1:]
		}
		if looksLikeEmail(v) {
			return v
		}
	}
	return ""
}
