package discovery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/metrics"
)

type SourceRow struct {
	CanonicalKey     string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	SeenAt           time.Time
}

type EventRow struct {
	CanonicalKey     string
	SignalKind       string
	EventExternalID  string
	SourceAppID      string
	SourceAppName    string
	SourceAppDomain  string
	SourceVendorName string
	ActorExternalID  string
	ActorEmail       string
	ActorDisplayName string
	ObservedAt       time.Time
	Scopes           []string
	RawJSON          []byte
}

type WriteRowsParams struct {
	SourceKind string
	SourceName string
	RunID      int64
	Sources    []SourceRow
	Events     []EventRow
	Report     func(ProgressEvent)
}

type ProgressEvent struct {
	Source  string
	Stage   string
	Current int64
	Total   int64
	Message string
}

func WriteRows(ctx context.Context, q *gen.Queries, params WriteRowsParams) error {
	sourceKind := strings.TrimSpace(params.SourceKind)
	sourceName := strings.TrimSpace(params.SourceName)
	if q == nil {
		return fmt.Errorf("write discovery rows for %s/%s: queries is nil", sourceKind, sourceName)
	}

	total := len(params.Sources) + len(params.Events)
	report(params.Report, ProgressEvent{
		Source:  sourceKind,
		Stage:   "write-discovery",
		Current: 0,
		Total:   int64(total),
		Message: fmt.Sprintf("writing %d discovery records", total),
	})

	appMeta := map[string]AppMetadata{}
	firstSeenByKey := map[string]time.Time{}
	lastSeenByKey := map[string]time.Time{}
	addMeta := func(key string, seenAt time.Time, sample AppMetadata) {
		if key == "" {
			return
		}
		if _, ok := appMeta[key]; !ok {
			appMeta[key] = sample
			firstSeenByKey[key] = seenAt
			lastSeenByKey[key] = seenAt
			return
		}
		if seenAt.Before(firstSeenByKey[key]) {
			firstSeenByKey[key] = seenAt
		}
		if seenAt.After(lastSeenByKey[key]) {
			lastSeenByKey[key] = seenAt
		}
	}

	for _, source := range params.Sources {
		meta := BuildMetadata(CanonicalInput{
			SourceKind:       sourceKind,
			SourceName:       sourceName,
			SourceAppID:      source.SourceAppID,
			SourceAppName:    source.SourceAppName,
			SourceDomain:     source.SourceAppDomain,
			SourceVendorName: source.SourceVendorName,
		})
		meta.CanonicalKey = source.CanonicalKey
		addMeta(source.CanonicalKey, source.SeenAt, meta)
	}
	for _, event := range params.Events {
		meta := BuildMetadata(CanonicalInput{
			SourceKind:       sourceKind,
			SourceName:       sourceName,
			SourceAppID:      event.SourceAppID,
			SourceAppName:    event.SourceAppName,
			SourceDomain:     event.SourceAppDomain,
			SourceVendorName: event.SourceVendorName,
		})
		meta.CanonicalKey = event.CanonicalKey
		addMeta(event.CanonicalKey, event.ObservedAt, meta)
	}

	if len(appMeta) > 0 {
		canonicalKeys := make([]string, 0, len(appMeta))
		displayNames := make([]string, 0, len(appMeta))
		primaryDomains := make([]string, 0, len(appMeta))
		vendorNames := make([]string, 0, len(appMeta))
		firstSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))
		lastSeenAts := make([]pgtype.Timestamptz, 0, len(appMeta))
		for key, meta := range appMeta {
			firstSeenAt := firstSeenByKey[key]
			lastSeenAt := lastSeenByKey[key]
			canonicalKeys = append(canonicalKeys, key)
			displayNames = append(displayNames, meta.DisplayName)
			primaryDomains = append(primaryDomains, meta.Domain)
			vendorNames = append(vendorNames, meta.VendorName)
			firstSeenAts = append(firstSeenAts, pgTimestamptz(firstSeenAt))
			lastSeenAts = append(lastSeenAts, pgTimestamptz(lastSeenAt))
		}
		if _, err := q.UpsertSaaSAppsBulk(ctx, gen.UpsertSaaSAppsBulkParams{
			CanonicalKeys:  canonicalKeys,
			DisplayNames:   displayNames,
			PrimaryDomains: primaryDomains,
			VendorNames:    vendorNames,
			FirstSeenAts:   firstSeenAts,
			LastSeenAts:    lastSeenAts,
		}); err != nil {
			return fmt.Errorf("upsert saas apps: %w", err)
		}
	}

	written := 0
	if len(params.Sources) > 0 {
		if err := writeSourceRows(ctx, q, params, sourceKind, sourceName); err != nil {
			return err
		}
		written += len(params.Sources)
		report(params.Report, ProgressEvent{
			Source:  sourceKind,
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("sources %d/%d", written, total),
		})
	}

	if len(params.Events) > 0 {
		if err := writeEventRows(ctx, q, params, sourceKind, sourceName); err != nil {
			return err
		}
		written += len(params.Events)
		report(params.Report, ProgressEvent{
			Source:  sourceKind,
			Stage:   "write-discovery",
			Current: int64(written),
			Total:   int64(total),
			Message: fmt.Sprintf("events %d/%d", written, total),
		})
	}

	if written == 0 {
		report(params.Report, ProgressEvent{
			Source:  sourceKind,
			Stage:   "write-discovery",
			Current: 0,
			Total:   0,
			Message: "no discovery records to write",
		})
	}
	return nil
}

func writeSourceRows(ctx context.Context, q *gen.Queries, params WriteRowsParams, sourceKind, sourceName string) error {
	canonicalKeys := make([]string, 0, len(params.Sources))
	sourceAppIDs := make([]string, 0, len(params.Sources))
	sourceAppNames := make([]string, 0, len(params.Sources))
	sourceAppDomains := make([]string, 0, len(params.Sources))
	seenAts := make([]pgtype.Timestamptz, 0, len(params.Sources))
	for _, source := range params.Sources {
		canonicalKeys = append(canonicalKeys, source.CanonicalKey)
		sourceAppIDs = append(sourceAppIDs, source.SourceAppID)
		sourceAppNames = append(sourceAppNames, source.SourceAppName)
		sourceAppDomains = append(sourceAppDomains, source.SourceAppDomain)
		seenAts = append(seenAts, pgTimestamptz(source.SeenAt))
	}
	if _, err := q.UpsertSaaSAppSourcesBulkBySource(ctx, gen.UpsertSaaSAppSourcesBulkBySourceParams{
		SourceKind:       sourceKind,
		SourceName:       sourceName,
		SeenInRunID:      params.RunID,
		CanonicalKeys:    canonicalKeys,
		SourceAppIds:     sourceAppIDs,
		SourceAppNames:   sourceAppNames,
		SourceAppDomains: sourceAppDomains,
		SeenAts:          seenAts,
	}); err != nil {
		return fmt.Errorf("upsert saas app sources: %w", err)
	}
	return nil
}

func writeEventRows(ctx context.Context, q *gen.Queries, params WriteRowsParams, sourceKind, sourceName string) error {
	canonicalKeys := make([]string, 0, len(params.Events))
	signalKinds := make([]string, 0, len(params.Events))
	eventExternalIDs := make([]string, 0, len(params.Events))
	sourceAppIDs := make([]string, 0, len(params.Events))
	sourceAppNames := make([]string, 0, len(params.Events))
	sourceAppDomains := make([]string, 0, len(params.Events))
	actorExternalIDs := make([]string, 0, len(params.Events))
	actorEmails := make([]string, 0, len(params.Events))
	actorDisplayNames := make([]string, 0, len(params.Events))
	observedAts := make([]pgtype.Timestamptz, 0, len(params.Events))
	scopesJSONs := make([][]byte, 0, len(params.Events))
	rawJSONs := make([][]byte, 0, len(params.Events))
	ingestedBySignal := map[string]int{}
	for _, event := range params.Events {
		canonicalKeys = append(canonicalKeys, event.CanonicalKey)
		signalKinds = append(signalKinds, event.SignalKind)
		eventExternalIDs = append(eventExternalIDs, event.EventExternalID)
		sourceAppIDs = append(sourceAppIDs, event.SourceAppID)
		sourceAppNames = append(sourceAppNames, event.SourceAppName)
		sourceAppDomains = append(sourceAppDomains, event.SourceAppDomain)
		actorExternalIDs = append(actorExternalIDs, event.ActorExternalID)
		actorEmails = append(actorEmails, event.ActorEmail)
		actorDisplayNames = append(actorDisplayNames, event.ActorDisplayName)
		observedAts = append(observedAts, pgTimestamptz(event.ObservedAt))
		scopesJSONs = append(scopesJSONs, ScopesJSON(event.Scopes))
		rawJSONs = append(rawJSONs, normalizeJSON(event.RawJSON))
		ingestedBySignal[event.SignalKind]++
	}
	if _, err := q.UpsertSaaSAppEventsBulkBySource(ctx, gen.UpsertSaaSAppEventsBulkBySourceParams{
		SourceKind:        sourceKind,
		SourceName:        sourceName,
		SeenInRunID:       params.RunID,
		CanonicalKeys:     canonicalKeys,
		SignalKinds:       signalKinds,
		EventExternalIds:  eventExternalIDs,
		SourceAppIds:      sourceAppIDs,
		SourceAppNames:    sourceAppNames,
		SourceAppDomains:  sourceAppDomains,
		ActorExternalIds:  actorExternalIDs,
		ActorEmails:       actorEmails,
		ActorDisplayNames: actorDisplayNames,
		ObservedAts:       observedAts,
		ScopesJsons:       scopesJSONs,
		RawJsons:          rawJSONs,
	}); err != nil {
		return fmt.Errorf("upsert saas app events: %w", err)
	}
	for signalKind, count := range ingestedBySignal {
		metrics.DiscoveryEventsIngestedTotal.WithLabelValues(sourceKind, signalKind).Add(float64(count))
	}
	return nil
}

func report(report func(ProgressEvent), event ProgressEvent) {
	if report != nil {
		report(event)
	}
}

func pgTimestamptz(t time.Time) pgtype.Timestamptz {
	if t.IsZero() {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func normalizeJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}
