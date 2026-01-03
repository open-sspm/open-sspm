package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/open-sspm/open-sspm/internal/config"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/opensspm"
	"github.com/spf13/cobra"
)

var seedRulesCmd = &cobra.Command{
	Use:   "seed-rules",
	Short: "Seed benchmark rulesets and rules into the database.",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSeedRules(cmd.Context())
	},
}

func runSeedRules(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	q := gen.New(pool)

	loaded, err := loadRulesFromOpenSSPMDescriptor()
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	qtx := q.WithTx(tx)
	for _, ls := range loaded {
		if err := seedRuleset(ctx, qtx, ls); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	slog.Info("seeded rulesets", "count", len(loaded))
	return nil
}

func loadRulesFromOpenSSPMDescriptor() ([]LoadedRuleset, error) {
	desc, err := opensspm.DescriptorV1()
	if err != nil {
		return nil, err
	}

	out := make([]LoadedRuleset, 0, len(desc.Rulesets))
	seenKeys := make(map[string]string, len(desc.Rulesets))
	for _, compiled := range desc.Rulesets {
		doc := compiled.Object
		rs := doc.Ruleset

		rulesetKey := strings.TrimSpace(rs.Key)
		if rulesetKey == "" {
			return nil, fmt.Errorf("%s: missing ruleset.key", strings.TrimSpace(compiled.SourcePath))
		}
		if prevPath, ok := seenKeys[rulesetKey]; ok {
			return nil, fmt.Errorf("duplicate ruleset key %q in %s and %s", rulesetKey, prevPath, strings.TrimSpace(compiled.SourcePath))
		}
		seenKeys[rulesetKey] = strings.TrimSpace(compiled.SourcePath)

		defJSON, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("%s: marshal ruleset definition_json: %w", rulesetKey, err)
		}

		defHash := strings.TrimSpace(compiled.Hash)
		if defHash == "" {
			sum := sha256.Sum256(defJSON)
			defHash = hex.EncodeToString(sum[:])
		}

		sourceName := ""
		sourceVersion := ""
		sourceDate := ""
		if rs.Source != nil {
			sourceName = strings.TrimSpace(rs.Source.Name)
			sourceVersion = strings.TrimSpace(rs.Source.Version)
			sourceDate = strings.TrimSpace(rs.Source.Date)
		}

		scopeKind := strings.TrimSpace(string(rs.Scope.Kind))
		connectorKind := strings.TrimSpace(rs.Scope.ConnectorKind)

		status := strings.TrimSpace(rs.Status)
		if status == "" {
			status = "active"
		}

		seedRuleset := SeedRuleset{
			Key:            rulesetKey,
			Name:           strings.TrimSpace(rs.Name),
			Description:    strings.TrimSpace(rs.Description),
			Source:         sourceName,
			SourceVersion:  sourceVersion,
			SourceDate:     sourceDate,
			ScopeKind:      scopeKind,
			ConnectorKind:  connectorKind,
			Status:         status,
			DefinitionHash: defHash,
			DefinitionJson: defJSON,
		}

		seedRules := make([]SeedRule, 0, len(rs.Rules))
		for _, r := range rs.Rules {
			ruleJSON, err := json.Marshal(r)
			if err != nil {
				return nil, fmt.Errorf("%s/%s: marshal rule definition_json: %w", rulesetKey, strings.TrimSpace(r.Key), err)
			}

			requiredData := r.RequiredData
			if requiredData == nil {
				requiredData = []string{}
			}
			requiredDataJSON, err := json.Marshal(requiredData)
			if err != nil {
				return nil, fmt.Errorf("%s/%s: marshal required_data: %w", rulesetKey, strings.TrimSpace(r.Key), err)
			}

			expectedParamsJSON := []byte("{}")
			if r.Parameters != nil && r.Parameters.Defaults != nil {
				expectedParamsJSON, err = json.Marshal(r.Parameters.Defaults)
				if err != nil {
					return nil, fmt.Errorf("%s/%s: marshal expected_params: %w", rulesetKey, strings.TrimSpace(r.Key), err)
				}
			}

			ruleVersion := ""
			isActive := true
			if r.Lifecycle != nil {
				ruleVersion = strings.TrimSpace(r.Lifecycle.RuleVersion)
				if r.Lifecycle.IsActive != nil {
					isActive = *r.Lifecycle.IsActive
				}
			}

			seedRules = append(seedRules, SeedRule{
				Key:              strings.TrimSpace(r.Key),
				Title:            strings.TrimSpace(r.Title),
				Summary:          strings.TrimSpace(r.Summary),
				Category:         strings.TrimSpace(r.Category),
				Severity:         strings.TrimSpace(string(r.Severity)),
				MonitoringStatus: strings.TrimSpace(string(r.Monitoring.Status)),
				MonitoringReason: strings.TrimSpace(r.Monitoring.Reason),
				RequiredData:     requiredDataJSON,
				ExpectedParams:   expectedParamsJSON,
				RuleVersion:      ruleVersion,
				IsActive:         isActive,
				DefinitionJson:   ruleJSON,
			})
		}

		out = append(out, LoadedRuleset{
			Path:    strings.TrimSpace(compiled.SourcePath),
			Ruleset: seedRuleset,
			Rules:   seedRules,
		})
	}

	return out, nil
}

func seedRuleset(ctx context.Context, q *gen.Queries, ls LoadedRuleset) error {
	def := ls.Ruleset

	sourceDate, err := parseYYYYMMDD(def.SourceDate)
	if err != nil {
		return fmt.Errorf("%s: invalid source_date: %w", def.Key, err)
	}

	connectorKind := pgtype.Text{}
	switch strings.TrimSpace(def.ScopeKind) {
	case "connector_instance":
		connector := strings.TrimSpace(def.ConnectorKind)
		if connector == "" {
			return fmt.Errorf("%s: connector_kind is required for scope_kind=connector_instance", def.Key)
		}
		connectorKind = pgtype.Text{String: connector, Valid: true}
	case "global":
		connectorKind = pgtype.Text{}
	default:
		if connector := strings.TrimSpace(def.ConnectorKind); connector != "" {
			connectorKind = pgtype.Text{String: connector, Valid: true}
		}
	}

	rs, err := q.UpsertRuleset(ctx, gen.UpsertRulesetParams{
		Key:            def.Key,
		Name:           def.Name,
		Description:    def.Description,
		Source:         def.Source,
		SourceVersion:  def.SourceVersion,
		SourceDate:     sourceDate,
		ScopeKind:      def.ScopeKind,
		ConnectorKind:  connectorKind,
		Status:         def.Status,
		DefinitionHash: def.DefinitionHash,
		DefinitionJson: def.DefinitionJson,
	})
	if err != nil {
		return fmt.Errorf("upsert ruleset %s: %w", def.Key, err)
	}

	ruleKeys := make([]string, 0, len(ls.Rules))
	for _, r := range ls.Rules {
		if _, err := q.UpsertRule(ctx, gen.UpsertRuleParams{
			RulesetID:        rs.ID,
			Key:              r.Key,
			Title:            r.Title,
			Summary:          r.Summary,
			Category:         r.Category,
			Severity:         r.Severity,
			MonitoringStatus: r.MonitoringStatus,
			MonitoringReason: r.MonitoringReason,
			RequiredData:     r.RequiredData,
			ExpectedParams:   r.ExpectedParams,
			RuleVersion:      r.RuleVersion,
			IsActive:         r.IsActive,
			DefinitionJson:   r.DefinitionJson,
		}); err != nil {
			return fmt.Errorf("upsert rule %s: %w", r.Key, err)
		}

		ruleKeys = append(ruleKeys, r.Key)
	}

	if err := q.DeactivateRulesNotInKeys(ctx, gen.DeactivateRulesNotInKeysParams{
		RulesetID: rs.ID,
		RuleKeys:  ruleKeys,
	}); err != nil {
		return fmt.Errorf("deactivate missing rules for ruleset %s: %w", def.Key, err)
	}

	slog.Info("seeded ruleset", "key", def.Key, "rules", len(ls.Rules))
	return nil
}

func parseYYYYMMDD(v string) (pgtype.Date, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return pgtype.Date{}, nil
	}

	t, err := time.Parse("2006-01-02", v)
	if err != nil {
		return pgtype.Date{}, err
	}
	return pgtype.Date{Time: t, Valid: true}, nil
}

type SeedRuleset struct {
	Key            string
	Name           string
	Description    string
	Source         string
	SourceVersion  string
	SourceDate     string
	ScopeKind      string
	ConnectorKind  string
	Status         string
	DefinitionHash string
	DefinitionJson []byte
}

type SeedRule struct {
	Key              string
	Title            string
	Summary          string
	Category         string
	Severity         string
	MonitoringStatus string
	MonitoringReason string
	RequiredData     []byte
	ExpectedParams   []byte
	RuleVersion      string
	IsActive         bool
	DefinitionJson   []byte
}

type LoadedRuleset struct {
	Path    string
	Ruleset SeedRuleset
	Rules   []SeedRule
}
