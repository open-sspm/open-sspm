package okta

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/open-sspm/open-sspm/internal/connectors/configstore"
	"github.com/open-sspm/open-sspm/internal/records"
)

func CanonicalEventRecord(sourceName, channel string, event SystemLogEvent) (records.EventRecord, error) {
	raw := map[string]any{}
	if len(event.RawJSON) > 0 {
		if err := json.Unmarshal(event.RawJSON, &raw); err != nil {
			return records.EventRecord{}, fmt.Errorf("decode canonical Okta raw event %s: %w", event.ID, err)
		}
	}
	category := "okta.system_log"
	if signalKind, ok := DiscoverySignalKind(event); ok {
		category = "discovery." + signalKind
	} else if refreshKind, ok := StateRefreshSignalKind(event); ok {
		category = "state_refresh." + refreshKind
	}

	targets := make([]records.TargetRef, 0, 1)
	if strings.TrimSpace(event.AppID) != "" || strings.TrimSpace(event.AppName) != "" {
		targets = append(targets, records.TargetRef{
			Kind: "okta_app",
			ID:   strings.TrimSpace(event.AppID),
			Name: strings.TrimSpace(event.AppName),
			Envelope: map[string]any{
				"domain": strings.TrimSpace(event.AppDomain),
			},
		})
	}

	return records.EventRecord{
		Source: records.SourceRef{
			Kind: configstore.KindOkta,
			Name: sourceName,
		},
		Channel:         strings.TrimSpace(channel),
		ProviderEventID: strings.TrimSpace(event.ID),
		DedupeKeyValue:  "provider:" + strings.TrimSpace(event.ID),
		EventType:       strings.TrimSpace(event.EventType),
		Category:        category,
		Action:          strings.TrimSpace(event.EventType),
		OccurredAt:      event.Published,
		ObservedAt:      event.Published,
		Actor: records.ActorRef{
			Kind:        "okta_actor",
			ID:          strings.TrimSpace(event.ActorID),
			Email:       strings.ToLower(strings.TrimSpace(event.ActorEmail)),
			DisplayName: strings.TrimSpace(event.ActorName),
		},
		Targets: targets,
		Outcome: CanonicalOutcome(event.OutcomeResult),
		Envelope: map[string]any{
			"outcome_reason": strings.TrimSpace(event.OutcomeReason),
			"granted_scopes": event.GrantedScopes,
		},
		Raw: raw,
	}, nil
}

func CanonicalOutcome(result string) records.Outcome {
	switch strings.ToLower(strings.TrimSpace(result)) {
	case "success":
		return records.OutcomeSuccess
	case "failure":
		return records.OutcomeFailure
	default:
		return records.OutcomeUnknown
	}
}
