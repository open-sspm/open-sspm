package configstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/db/gen"
)

const (
	ConnectorSecretVersionAES256GCM int16 = 1
	connectorSecretNonceSize              = 12
)

const (
	secretNameToken              = "token"
	secretNameAPIKey             = "api_key"
	secretNameAppKey             = "app_key"
	secretNameClientSecret       = "client_secret"
	secretNameServiceAccountJSON = "service_account_json"
	secretNameSecretAccessKey    = "secret_access_key"
	secretNameSessionToken       = "session_token"
	secretNameAppRoleSecretID    = "approle_secret_id"
)

var ErrConnectorSecretKeyRequired = errors.New("connector secret key is required")

type SecretValues map[string]string

type ResolvedConnectorConfig struct {
	Row            gen.ConnectorConfig
	PublicConfig   []byte
	ResolvedConfig []byte
	SecretValues   SecretValues
}

type SecretCipher struct {
	key []byte
}

type Store struct {
	pool   *pgxpool.Pool
	q      *gen.Queries
	cipher *SecretCipher
}

func NewSecretCipher(key []byte) (*SecretCipher, error) {
	if len(key) == 0 {
		return nil, nil
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("connector secret key must be 32 bytes, got %d", len(key))
	}
	return &SecretCipher{key: append([]byte(nil), key...)}, nil
}

func (c *SecretCipher) Encrypt(kind, secretName, plaintext string) ([]byte, []byte, int16, error) {
	if c == nil {
		return nil, nil, 0, connectorSecretKeyRequiredError()
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("create connector secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("create connector secret gcm: %w", err)
	}
	nonce := make([]byte, connectorSecretNonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, 0, fmt.Errorf("generate connector secret nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plaintext), secretAAD(kind, secretName))
	return ciphertext, nonce, ConnectorSecretVersionAES256GCM, nil
}

func (c *SecretCipher) Decrypt(kind, secretName string, ciphertext, nonce []byte, version int16) (string, error) {
	if c == nil {
		return "", connectorSecretKeyRequiredError()
	}
	if version != ConnectorSecretVersionAES256GCM {
		return "", fmt.Errorf("unsupported connector secret version %d", version)
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", fmt.Errorf("create connector secret cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create connector secret gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, secretAAD(kind, secretName))
	if err != nil {
		return "", fmt.Errorf("decrypt connector secret %s.%s: %w", strings.TrimSpace(kind), strings.TrimSpace(secretName), err)
	}
	return strings.TrimSpace(string(plaintext)), nil
}

func NewStore(pool *pgxpool.Pool, q *gen.Queries, key []byte) *Store {
	cipher, _ := NewSecretCipher(key)
	return &Store{
		pool:   pool,
		q:      q,
		cipher: cipher,
	}
}

func (s *Store) HasKey() bool {
	return s != nil && s.cipher != nil
}

func (s *Store) Bootstrap(ctx context.Context) error {
	if s == nil || s.pool == nil || s.q == nil {
		return errors.New("connector config store is not configured")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	qtx := s.q.WithTx(tx)
	rows, err := qtx.ListConnectorConfigsForUpdate(ctx)
	if err != nil {
		return err
	}
	secretRows, err := qtx.ListConnectorSecrets(ctx)
	if err != nil {
		return err
	}
	secretsByKind := groupConnectorSecretsByKind(secretRows)

	type pendingUpdate struct {
		kind         string
		publicRaw    []byte
		secretValues SecretValues
	}
	updates := make([]pendingUpdate, 0, len(rows))
	hasEncryptedSecrets := len(secretRows) > 0
	hasLegacySecrets := false

	for _, row := range rows {
		publicRaw, legacySecrets, err := SplitRawConfig(row.Kind, row.Config)
		if err != nil {
			return err
		}
		if len(legacySecrets) == 0 {
			continue
		}
		hasLegacySecrets = true

		storedSecrets, err := s.decryptConnectorSecretRows(row.Kind, secretsByKind[normalizeKind(row.Kind)])
		if err != nil {
			return err
		}
		updates = append(updates, pendingUpdate{
			kind:         row.Kind,
			publicRaw:    publicRaw,
			secretValues: mergeSecretValues(legacySecrets, storedSecrets),
		})
	}

	if !s.HasKey() {
		if hasEncryptedSecrets || hasLegacySecrets {
			return connectorSecretKeyRequiredError()
		}
		return tx.Commit(ctx)
	}

	for _, update := range updates {
		if _, err := qtx.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{
			Kind:   update.kind,
			Config: update.publicRaw,
		}); err != nil {
			return err
		}
		if err := s.replaceConnectorSecrets(ctx, qtx, update.kind, update.secretValues); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) ListResolvedConnectorConfigs(ctx context.Context) ([]ResolvedConnectorConfig, error) {
	if s == nil || s.q == nil {
		return nil, errors.New("connector config store is not configured")
	}
	rows, secretsByKind, err := s.ListConnectorConfigsWithSecretRows(ctx)
	if err != nil {
		return nil, err
	}
	return s.resolveRows(rows, secretsByKind)
}

func (s *Store) ListConnectorConfigsWithSecretRows(ctx context.Context) ([]gen.ConnectorConfig, map[string][]gen.ConnectorSecret, error) {
	if s == nil || s.q == nil {
		return nil, nil, errors.New("connector config store is not configured")
	}
	rows, err := s.q.ListConnectorConfigs(ctx)
	if err != nil {
		return nil, nil, err
	}
	secretRows, err := s.q.ListConnectorSecrets(ctx)
	if err != nil {
		return nil, nil, err
	}
	return rows, groupConnectorSecretsByKind(secretRows), nil
}

func (s *Store) GetResolvedConnectorConfig(ctx context.Context, kind string) (ResolvedConnectorConfig, error) {
	if s == nil || s.q == nil {
		return ResolvedConnectorConfig{}, errors.New("connector config store is not configured")
	}
	row, err := s.q.GetConnectorConfig(ctx, kind)
	if err != nil {
		return ResolvedConnectorConfig{}, err
	}
	return s.ResolveConnectorConfigRow(ctx, row)
}

func (s *Store) ResolveConnectorConfigRow(ctx context.Context, row gen.ConnectorConfig) (ResolvedConnectorConfig, error) {
	if s == nil || s.q == nil {
		return ResolvedConnectorConfig{}, errors.New("connector config store is not configured")
	}
	secretRows, err := s.q.ListConnectorSecretsByKind(ctx, row.Kind)
	if err != nil {
		return ResolvedConnectorConfig{}, err
	}
	return s.ResolveConnectorConfigRowWithSecretRows(row, secretRows)
}

func (s *Store) ResolveConnectorConfigRowWithSecretRows(row gen.ConnectorConfig, secretRows []gen.ConnectorSecret) (ResolvedConnectorConfig, error) {
	if s == nil {
		return ResolvedConnectorConfig{}, errors.New("connector config store is not configured")
	}
	return s.resolveRow(row, secretRows)
}

func (s *Store) SaveConnectorConfigTx(ctx context.Context, qtx *gen.Queries, kind string, cfg any) error {
	if s == nil || qtx == nil {
		return errors.New("connector config store is not configured")
	}
	publicCfg, secretValues, err := SplitConfig(kind, cfg)
	if err != nil {
		return err
	}
	publicRaw, err := EncodeConfig(publicCfg)
	if err != nil {
		return err
	}
	if len(secretValues) > 0 && !s.HasKey() {
		return connectorSecretKeyRequiredError()
	}
	if _, err := qtx.UpdateConnectorConfig(ctx, gen.UpdateConnectorConfigParams{
		Kind:   kind,
		Config: publicRaw,
	}); err != nil {
		return err
	}
	return s.replaceConnectorSecrets(ctx, qtx, kind, secretValues)
}

func (s *Store) resolveRows(rows []gen.ConnectorConfig, secretsByKind map[string][]gen.ConnectorSecret) ([]ResolvedConnectorConfig, error) {
	out := make([]ResolvedConnectorConfig, 0, len(rows))
	for _, row := range rows {
		resolved, err := s.resolveRow(row, secretsByKind[normalizeKind(row.Kind)])
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func (s *Store) resolveRow(row gen.ConnectorConfig, secretRows []gen.ConnectorSecret) (ResolvedConnectorConfig, error) {
	publicRaw, legacySecrets, err := SplitRawConfig(row.Kind, row.Config)
	if err != nil {
		return ResolvedConnectorConfig{}, err
	}
	storedSecrets, err := s.decryptConnectorSecretRows(row.Kind, secretRows)
	if err != nil {
		return ResolvedConnectorConfig{}, err
	}
	effectiveSecrets := mergeSecretValues(legacySecrets, storedSecrets)
	if !s.HasKey() && len(effectiveSecrets) > 0 {
		return ResolvedConnectorConfig{}, connectorSecretKeyRequiredError()
	}
	resolvedRaw, err := ResolveConfig(row.Kind, publicRaw, effectiveSecrets)
	if err != nil {
		return ResolvedConnectorConfig{}, err
	}
	return ResolvedConnectorConfig{
		Row:            row,
		PublicConfig:   publicRaw,
		ResolvedConfig: resolvedRaw,
		SecretValues:   effectiveSecrets,
	}, nil
}

func (s *Store) decryptConnectorSecretRows(kind string, rows []gen.ConnectorSecret) (SecretValues, error) {
	if len(rows) == 0 {
		return SecretValues{}, nil
	}
	if !s.HasKey() {
		return nil, connectorSecretKeyRequiredError()
	}
	out := make(SecretValues, len(rows))
	for _, row := range rows {
		value, err := s.cipher.Decrypt(kind, row.SecretName, row.Ciphertext, row.Nonce, row.Version)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(value) != "" {
			out[strings.TrimSpace(row.SecretName)] = strings.TrimSpace(value)
		}
	}
	return out, nil
}

func (s *Store) replaceConnectorSecrets(ctx context.Context, qtx *gen.Queries, kind string, secretValues SecretValues) error {
	if _, err := qtx.DeleteConnectorSecretsByKind(ctx, kind); err != nil {
		return err
	}
	if len(secretValues) == 0 {
		return nil
	}
	if !s.HasKey() {
		return connectorSecretKeyRequiredError()
	}
	for _, secretName := range sortedSecretNames(secretValues) {
		value := strings.TrimSpace(secretValues[secretName])
		if value == "" {
			continue
		}
		ciphertext, nonce, version, err := s.cipher.Encrypt(kind, secretName, value)
		if err != nil {
			return err
		}
		if _, err := qtx.UpsertConnectorSecret(ctx, gen.UpsertConnectorSecretParams{
			Kind:       kind,
			SecretName: secretName,
			Ciphertext: ciphertext,
			Nonce:      nonce,
			Version:    version,
		}); err != nil {
			return err
		}
	}
	return nil
}

func SplitRawConfig(kind string, raw []byte) ([]byte, SecretValues, error) {
	cfg, err := decodeTypedConfig(kind, raw)
	if err != nil {
		return nil, nil, err
	}
	publicCfg, secretValues, err := SplitConfig(kind, cfg)
	if err != nil {
		return nil, nil, err
	}
	publicRaw, err := EncodeConfig(publicCfg)
	if err != nil {
		return nil, nil, err
	}
	return publicRaw, secretValues, nil
}

func SplitConfig(kind string, cfg any) (any, SecretValues, error) {
	switch normalizeKind(kind) {
	case KindOkta:
		typed, ok := cfg.(OktaConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if token := strings.TrimSpace(typed.Token); token != "" {
			secrets[secretNameToken] = token
		}
		public.Token = ""
		return public, secrets, nil
	case KindGitHub:
		typed, ok := cfg.(GitHubConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if token := strings.TrimSpace(typed.Token); token != "" {
			secrets[secretNameToken] = token
		}
		public.Token = ""
		return public, secrets, nil
	case KindDatadog:
		typed, ok := cfg.(DatadogConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if apiKey := strings.TrimSpace(typed.APIKey); apiKey != "" {
			secrets[secretNameAPIKey] = apiKey
		}
		if appKey := strings.TrimSpace(typed.AppKey); appKey != "" {
			secrets[secretNameAppKey] = appKey
		}
		public.APIKey = ""
		public.AppKey = ""
		return public, secrets, nil
	case KindAWSIdentityCenter:
		typed, ok := cfg.(AWSIdentityCenterConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if secretAccessKey := strings.TrimSpace(typed.SecretAccessKey); secretAccessKey != "" {
			secrets[secretNameSecretAccessKey] = secretAccessKey
		}
		if sessionToken := strings.TrimSpace(typed.SessionToken); sessionToken != "" {
			secrets[secretNameSessionToken] = sessionToken
		}
		public.SecretAccessKey = ""
		public.SessionToken = ""
		return public, secrets, nil
	case KindEntra:
		typed, ok := cfg.(EntraConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if clientSecret := strings.TrimSpace(typed.ClientSecret); clientSecret != "" {
			secrets[secretNameClientSecret] = clientSecret
		}
		public.ClientSecret = ""
		return public, secrets, nil
	case KindGoogleWorkspace:
		typed, ok := cfg.(GoogleWorkspaceConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if serviceAccountJSON := strings.TrimSpace(typed.ServiceAccountJSON); serviceAccountJSON != "" {
			secrets[secretNameServiceAccountJSON] = serviceAccountJSON
		}
		public.ServiceAccountJSON = ""
		return public, secrets, nil
	case KindVault:
		typed, ok := cfg.(VaultConfig)
		if !ok {
			return nil, nil, typeMismatchError(kind)
		}
		typed = typed.Normalized()
		public := typed
		secrets := SecretValues{}
		if token := strings.TrimSpace(typed.Token); token != "" {
			secrets[secretNameToken] = token
		}
		if secretID := strings.TrimSpace(typed.AppRoleSecretID); secretID != "" {
			secrets[secretNameAppRoleSecretID] = secretID
		}
		public.Token = ""
		public.AppRoleSecretID = ""
		return public, secrets, nil
	default:
		return nil, nil, fmt.Errorf("unknown connector kind %q", kind)
	}
}

func ResolveConfig(kind string, publicRaw []byte, secretValues SecretValues) ([]byte, error) {
	switch normalizeKind(kind) {
	case KindOkta:
		cfg, err := DecodeOktaConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg.Token = strings.TrimSpace(secretValues[secretNameToken])
		return EncodeConfig(cfg.Normalized())
	case KindGitHub:
		cfg, err := DecodeGitHubConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg.Token = strings.TrimSpace(secretValues[secretNameToken])
		return EncodeConfig(cfg.Normalized())
	case KindDatadog:
		cfg, err := DecodeDatadogConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg.APIKey = strings.TrimSpace(secretValues[secretNameAPIKey])
		cfg.AppKey = strings.TrimSpace(secretValues[secretNameAppKey])
		return EncodeConfig(cfg.Normalized())
	case KindAWSIdentityCenter:
		cfg, err := DecodeAWSIdentityCenterConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg = cfg.Normalized()
		if cfg.AuthType == AWSIdentityCenterAuthTypeAccessKey {
			cfg.SecretAccessKey = strings.TrimSpace(secretValues[secretNameSecretAccessKey])
			cfg.SessionToken = strings.TrimSpace(secretValues[secretNameSessionToken])
		}
		return EncodeConfig(cfg.Normalized())
	case KindEntra:
		cfg, err := DecodeEntraConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg.ClientSecret = strings.TrimSpace(secretValues[secretNameClientSecret])
		return EncodeConfig(cfg.Normalized())
	case KindGoogleWorkspace:
		cfg, err := DecodeGoogleWorkspaceConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg = cfg.Normalized()
		if cfg.AuthType == GoogleWorkspaceAuthTypeServiceAccountJSON {
			cfg.ServiceAccountJSON = strings.TrimSpace(secretValues[secretNameServiceAccountJSON])
		}
		return EncodeConfig(cfg.Normalized())
	case KindVault:
		cfg, err := DecodeVaultConfig(publicRaw)
		if err != nil {
			return nil, err
		}
		cfg = cfg.Normalized()
		switch cfg.AuthType {
		case VaultAuthTypeToken:
			cfg.Token = strings.TrimSpace(secretValues[secretNameToken])
			cfg.AppRoleSecretID = ""
		case VaultAuthTypeAppRole:
			cfg.Token = ""
			cfg.AppRoleSecretID = strings.TrimSpace(secretValues[secretNameAppRoleSecretID])
		}
		return EncodeConfig(cfg.Normalized())
	default:
		return nil, fmt.Errorf("unknown connector kind %q", kind)
	}
}

func ResolveConfigWithSecretPresence(kind string, publicRaw []byte, presence map[string]bool) ([]byte, error) {
	return ResolveConfig(kind, publicRaw, placeholderSecretValues(kind, presence))
}

func SecretPresenceByKind(secretRows []gen.ConnectorSecret) map[string]map[string]bool {
	out := make(map[string]map[string]bool)
	for _, row := range secretRows {
		kind := normalizeKind(row.Kind)
		if _, ok := out[kind]; !ok {
			out[kind] = make(map[string]bool)
		}
		out[kind][strings.TrimSpace(row.SecretName)] = true
	}
	return out
}

func decodeTypedConfig(kind string, raw []byte) (any, error) {
	switch normalizeKind(kind) {
	case KindOkta:
		return DecodeOktaConfig(raw)
	case KindGitHub:
		return DecodeGitHubConfig(raw)
	case KindDatadog:
		return DecodeDatadogConfig(raw)
	case KindAWSIdentityCenter:
		return DecodeAWSIdentityCenterConfig(raw)
	case KindEntra:
		return DecodeEntraConfig(raw)
	case KindGoogleWorkspace:
		return DecodeGoogleWorkspaceConfig(raw)
	case KindVault:
		return DecodeVaultConfig(raw)
	default:
		return nil, fmt.Errorf("unknown connector kind %q", kind)
	}
}

func placeholderSecretValues(kind string, presence map[string]bool) SecretValues {
	out := SecretValues{}
	if presence == nil {
		return out
	}
	for secretName, exists := range presence {
		if !exists {
			continue
		}
		switch secretName {
		case secretNameServiceAccountJSON:
			out[secretName] = placeholderServiceAccountJSON()
		default:
			out[secretName] = "placeholder-secret"
		}
	}
	return out
}

func placeholderServiceAccountJSON() string {
	payload := map[string]string{
		"type":         "service_account",
		"project_id":   "placeholder-project",
		"private_key":  "-----BEGIN PRIVATE KEY-----\nplaceholder\n-----END PRIVATE KEY-----",
		"client_email": "placeholder@example.iam.gserviceaccount.com",
		"token_uri":    "https://oauth2.googleapis.com/token",
	}
	raw, _ := json.Marshal(payload)
	return string(raw)
}

func groupConnectorSecretsByKind(rows []gen.ConnectorSecret) map[string][]gen.ConnectorSecret {
	out := make(map[string][]gen.ConnectorSecret)
	for _, row := range rows {
		kind := normalizeKind(row.Kind)
		out[kind] = append(out[kind], row)
	}
	return out
}

func mergeSecretValues(base, override SecretValues) SecretValues {
	out := make(SecretValues)
	for secretName, value := range base {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out[strings.TrimSpace(secretName)] = trimmed
		}
	}
	for secretName, value := range override {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out[strings.TrimSpace(secretName)] = trimmed
		}
	}
	return out
}

func sortedSecretNames(secretValues SecretValues) []string {
	names := make([]string, 0, len(secretValues))
	for secretName := range secretValues {
		if strings.TrimSpace(secretName) == "" {
			continue
		}
		names = append(names, strings.TrimSpace(secretName))
	}
	sort.Strings(names)
	return names
}

func secretAAD(kind, secretName string) []byte {
	return []byte(normalizeKind(kind) + ":" + strings.TrimSpace(secretName))
}

func connectorSecretKeyRequiredError() error {
	return fmt.Errorf("%w: set CONNECTOR_SECRET_KEY or CONNECTOR_SECRET_KEY_FILE", ErrConnectorSecretKeyRequired)
}

func typeMismatchError(kind string) error {
	return fmt.Errorf("connector config type mismatch for %q", kind)
}

func normalizeKind(kind string) string {
	return strings.ToLower(strings.TrimSpace(kind))
}
