package registry

func DecodeNormalizedConfig[T any](raw []byte, decode func([]byte) (T, error), normalize func(T) T) (any, error) {
	cfg, err := decode(raw)
	if err != nil {
		return nil, err
	}
	return normalize(cfg), nil
}
