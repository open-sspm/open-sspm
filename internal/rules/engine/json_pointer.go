package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// getByJSONPointer resolves RFC 6901 JSON Pointers against JSON-like values
// (maps/slices produced by encoding/json).
//
// It returns (value, true, nil) when the pointer resolves successfully, and
// (nil, false, nil) when any path segment is missing.
func getByJSONPointer(doc any, pointer string) (any, bool, error) {
	if pointer == "" {
		return doc, true, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, false, fmt.Errorf("invalid json pointer %q", pointer)
	}

	current := doc
	parts := strings.Split(pointer, "/")[1:]
	for _, rawPart := range parts {
		part, err := unescapeJSONPointerToken(rawPart)
		if err != nil {
			return nil, false, err
		}

		switch node := current.(type) {
		case map[string]any:
			v, ok := node[part]
			if !ok {
				return nil, false, nil
			}
			current = v
		case []any:
			if part == "-" {
				return nil, false, nil
			}
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(node) {
				return nil, false, nil
			}
			current = node[i]
		default:
			return nil, false, nil
		}
	}

	return current, true, nil
}

func unescapeJSONPointerToken(token string) (string, error) {
	if strings.IndexByte(token, '~') == -1 {
		return token, nil
	}

	var b strings.Builder
	b.Grow(len(token))
	for i := 0; i < len(token); i++ {
		ch := token[i]
		if ch != '~' {
			b.WriteByte(ch)
			continue
		}
		if i+1 >= len(token) {
			return "", fmt.Errorf("invalid json pointer escape %q", token)
		}
		next := token[i+1]
		switch next {
		case '0':
			b.WriteByte('~')
		case '1':
			b.WriteByte('/')
		default:
			return "", fmt.Errorf("invalid json pointer escape %q", token)
		}
		i++
	}
	return b.String(), nil
}
