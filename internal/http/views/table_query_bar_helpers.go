package views

import (
	"strconv"
	"strings"

	"github.com/open-sspm/open-sspm/internal/http/querystate"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func SearchOnlyBasicListQueryBar(query querystate.BasicListQuery, placeholder string) viewmodels.TableQueryBarData {
	return viewmodels.TableQueryBarData{
		Search: tableQuerySearch(query.Q, placeholder),
	}
}

func ActiveInactiveStateTableQueryBar(query querystate.BasicListQuery, placeholder string) viewmodels.TableQueryBarData {
	options := []viewmodels.TableFilterOption{
		tableQueryOption("active", "Active", tableQueryControl("state", "active")),
		tableQueryOption("inactive", "Inactive", tableQueryControl("state", "inactive")),
	}

	field := tableQueryField(
		"state",
		"State",
		viewmodels.TableFilterFieldKindState,
		[]string{"state"},
		query.State,
		query.State == "",
		options,
	)

	data := viewmodels.TableQueryBarData{
		Search: tableQuerySearch(query.Q, placeholder),
		Fields: []viewmodels.TableFilterField{field},
	}

	if query.State != "" {
		data.ActiveChips = append(data.ActiveChips, tableQueryChip("state", chipLabelForField(field.Label, tableQueryOptionLabel(field.Options, query.State))))
		data.HiddenControls = append(data.HiddenControls, tableQueryControl("state", query.State))
	}

	return data
}

func IdentitiesTableQueryBar(data viewmodels.IdentitiesViewData) viewmodels.TableQueryBarData {
	query := data.Query

	sourceOptions := make([]viewmodels.TableFilterOption, 0, len(data.Sources))
	for _, source := range data.Sources {
		sourceOptions = append(sourceOptions, tableQueryOption(source.SourceKind, source.Label, tableQueryControl("source_kind", source.SourceKind)))
	}

	sourceNameOptions := make([]viewmodels.TableFilterOption, 0, len(data.SourceNameOptions))
	for _, source := range data.SourceNameOptions {
		sourceNameOptions = append(sourceNameOptions, tableQueryOption(source.SourceName, source.SourceName, tableQueryControl("source_name", source.SourceName)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("row_state", "Attention", viewmodels.TableFilterFieldKindSingleSelect, []string{"row_state"}, query.RowState, query.RowState == "", []viewmodels.TableFilterOption{
			tableQueryOption("action_required", "Needs action", tableQueryControl("row_state", "action_required")),
			tableQueryOption("review", "Review", tableQueryControl("row_state", "review")),
			tableQueryOption("healthy", "Healthy", tableQueryControl("row_state", "healthy")),
		}),
		tableQueryField("activity_state", "Activity", viewmodels.TableFilterFieldKindSingleSelect, []string{"activity_state"}, query.ActivityState, query.ActivityState == "", []viewmodels.TableFilterOption{
			tableQueryOption("recent", "Seen < 30d", tableQueryControl("activity_state", "recent")),
			tableQueryOption("aging", "30-89d", tableQueryControl("activity_state", "aging")),
			tableQueryOption("stale", "90d+", tableQueryControl("activity_state", "stale")),
			tableQueryOption("never_seen", "Never seen", tableQueryControl("activity_state", "never_seen")),
		}),
		tableQueryField("privileged", "Privileged access", viewmodels.TableFilterFieldKindBoolean, []string{"privileged"}, boolActiveValue(query.PrivilegedOnly), !query.PrivilegedOnly, []viewmodels.TableFilterOption{
			tableQueryOption("yes", "Yes", tableQueryControl("privileged", "1")),
		}),
		tableQueryField("status", "Status", viewmodels.TableFilterFieldKindSingleSelect, []string{"status"}, query.Status, query.Status == "", []viewmodels.TableFilterOption{
			tableQueryOption("active", "Active", tableQueryControl("status", "active")),
			tableQueryOption("suspended", "Suspended", tableQueryControl("status", "suspended")),
			tableQueryOption("orphaned", "Orphaned", tableQueryControl("status", "orphaned")),
			tableQueryOption("deleted", "Deleted", tableQueryControl("status", "deleted")),
			tableQueryOption("unknown", "Unknown", tableQueryControl("status", "unknown")),
		}),
		tableQueryField("source_kind", "Source", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_kind"}, query.Source.Kind, query.Source.Kind == "", sourceOptions),
		tableQueryField("source_name", "Source name", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_name"}, query.Source.Name, query.Source.Name == "", sourceNameOptions),
		tableQueryField("identity_type", "Type", viewmodels.TableFilterFieldKindSingleSelect, []string{"identity_type"}, query.IdentityType, query.IdentityType == "", []viewmodels.TableFilterOption{
			tableQueryOption("human", "Human", tableQueryControl("identity_type", "human")),
			tableQueryOption("service", "Service", tableQueryControl("identity_type", "service")),
			tableQueryOption("bot", "Bot", tableQueryControl("identity_type", "bot")),
			tableQueryOption("unknown", "Unknown", tableQueryControl("identity_type", "unknown")),
		}),
		tableQueryField("managed_state", "Managed", viewmodels.TableFilterFieldKindSingleSelect, []string{"managed_state"}, query.ManagedState, query.ManagedState == "", []viewmodels.TableFilterOption{
			tableQueryOption("managed", "Managed", tableQueryControl("managed_state", "managed")),
			tableQueryOption("unmanaged", "Unmanaged", tableQueryControl("managed_state", "unmanaged")),
		}),
		tableQueryField("sort", "Sort", viewmodels.TableFilterFieldKindSort, []string{"sort_by", "sort_dir"}, identitySortValue(query.SortBy, query.SortDir), query.SortBy == "", identitySortOptions()),
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search name, email, or external ID"),
		fields,
		[]viewmodels.TableActiveChip{
			maybeChip(fields[0]),
			maybeChip(fields[1]),
			maybeBooleanChip(fields[2], query.PrivilegedOnly),
			maybeChip(fields[3]),
			maybeChip(fields[4]),
			maybeChip(fields[5]),
			maybeChip(fields[6]),
			maybeChip(fields[7]),
			maybeChip(fields[8]),
		},
		tableQueryControls(
			controlIfNotEmpty("row_state", query.RowState),
			controlIfNotEmpty("source_kind", query.Source.Kind),
			controlIfNotEmpty("source_name", query.Source.Name),
			controlIfNotEmpty("identity_type", query.IdentityType),
			controlIfNotEmpty("managed_state", query.ManagedState),
			controlIfTrue("privileged", query.PrivilegedOnly),
			controlIfNotEmpty("status", query.Status),
			controlIfNotEmpty("activity_state", query.ActivityState),
			controlIfNotEmpty("sort_by", query.SortBy),
			controlIfNotEmpty("sort_dir", sortDirWhenSet(query.SortBy, query.SortDir)),
		),
	)
}

func NonHumanAccessTableQueryBar(data viewmodels.NonHumanAccessViewData) viewmodels.TableQueryBarData {
	query := data.Query

	sourceOptions := make([]viewmodels.TableFilterOption, 0, len(data.Sources))
	for _, source := range data.Sources {
		sourceOptions = append(sourceOptions, tableQueryOption(source.SourceKind, source.Label, tableQueryControl("source_kind", source.SourceKind)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("source_kind", "Provider", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_kind"}, query.Source.Kind, query.Source.Kind == "", sourceOptions),
		tableQueryField("activity_state", "Activity", viewmodels.TableFilterFieldKindSingleSelect, []string{"activity_state"}, query.ActivityState, query.ActivityState == "", []viewmodels.TableFilterOption{
			tableQueryOption("recent", "Seen < 30d", tableQueryControl("activity_state", "recent")),
			tableQueryOption("aging", "30-89d", tableQueryControl("activity_state", "aging")),
			tableQueryOption("stale", "90d+", tableQueryControl("activity_state", "stale")),
			tableQueryOption("never_seen", "Never seen", tableQueryControl("activity_state", "never_seen")),
		}),
		tableQueryField("principal_type", "Type", viewmodels.TableFilterFieldKindSingleSelect, []string{"principal_type"}, query.PrincipalType, query.PrincipalType == "", []viewmodels.TableFilterOption{
			tableQueryOption("service", "Service", tableQueryControl("principal_type", "service")),
			tableQueryOption("bot", "Bot", tableQueryControl("principal_type", "bot")),
			tableQueryOption("app", "App only", tableQueryControl("principal_type", "app")),
		}),
		tableQueryField("governance_state", "Governance", viewmodels.TableFilterFieldKindSingleSelect, []string{"governance_state"}, query.GovernanceState, query.GovernanceState == "", []viewmodels.TableFilterOption{
			tableQueryOption("unreviewed", "Unreviewed", tableQueryControl("governance_state", "unreviewed")),
			tableQueryOption("in_review", "In review", tableQueryControl("governance_state", "in_review")),
			tableQueryOption("approved", "Approved", tableQueryControl("governance_state", "approved")),
			tableQueryOption("action_required", "Action required", tableQueryControl("governance_state", "action_required")),
			tableQueryOption("ticketed", "Ticketed", tableQueryControl("governance_state", "ticketed")),
		}),
		tableQueryField("freshness_state", "Freshness", viewmodels.TableFilterFieldKindSingleSelect, []string{"freshness_state"}, query.FreshnessState, query.FreshnessState == "", []viewmodels.TableFilterOption{
			tableQueryOption("current", "Current", tableQueryControl("freshness_state", "current")),
			tableQueryOption("stale", "Stale", tableQueryControl("freshness_state", "stale")),
			tableQueryOption("unknown", "Unknown", tableQueryControl("freshness_state", "unknown")),
		}),
		tableQueryField("risk_level", "Risk", viewmodels.TableFilterFieldKindSingleSelect, []string{"risk_level"}, query.RiskLevel, query.RiskLevel == "", []viewmodels.TableFilterOption{
			tableQueryOption("critical", "Critical", tableQueryControl("risk_level", "critical")),
			tableQueryOption("high", "High", tableQueryControl("risk_level", "high")),
			tableQueryOption("medium", "Medium", tableQueryControl("risk_level", "medium")),
			tableQueryOption("low", "Low", tableQueryControl("risk_level", "low")),
		}),
		tableQueryField("owner_presence", "Owner", viewmodels.TableFilterFieldKindSingleSelect, []string{"owner_presence"}, query.OwnerPresence, query.OwnerPresence == "", []viewmodels.TableFilterOption{
			tableQueryOption("owned", "Owned", tableQueryControl("owner_presence", "owned")),
			tableQueryOption("unknown", "Unknown owner", tableQueryControl("owner_presence", "unknown")),
		}),
		tableQueryField("sort", "Sort", viewmodels.TableFilterFieldKindSort, []string{"sort_by", "sort_dir"}, nonHumanAccessSortValue(query.SortBy, query.SortDir), query.SortBy == "", nonHumanAccessSortOptions()),
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search principal, external ID, or owner"),
		fields,
		[]viewmodels.TableActiveChip{
			maybeChip(fields[0]),
			maybeChip(fields[1]),
			maybeChip(fields[2]),
			maybeChip(fields[3]),
			maybeChip(fields[4]),
			maybeChip(fields[5]),
			maybeChip(fields[6]),
			maybeChip(fields[7]),
		},
		tableQueryControls(
			controlIfNotEmpty("source_kind", query.Source.Kind),
			controlIfNotEmpty("principal_type", query.PrincipalType),
			controlIfNotEmpty("owner_presence", query.OwnerPresence),
			controlIfNotEmpty("governance_state", query.GovernanceState),
			controlIfNotEmpty("risk_level", query.RiskLevel),
			controlIfNotEmpty("activity_state", query.ActivityState),
			controlIfNotEmpty("freshness_state", query.FreshnessState),
			controlIfNotEmpty("sort_by", query.SortBy),
			controlIfNotEmpty("sort_dir", sortDirWhenSet(query.SortBy, query.SortDir)),
		),
	)
}

func CredentialsTableQueryBar(data viewmodels.CredentialsViewData) viewmodels.TableQueryBarData {
	query := data.Query
	activePreset := activeCredentialsPreset(query)
	suppressed := suppressedCredentialFieldIDs(activePreset)

	sourceOptions := make([]viewmodels.TableFilterOption, 0, len(data.Sources))
	for _, source := range data.Sources {
		sourceOptions = append(sourceOptions, tableQueryOption(source.SourceKind, source.Label, tableQueryControl("source_kind", source.SourceKind)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("source_kind", "Source", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_kind"}, query.Source.Kind, query.Source.Kind == "", sourceOptions),
		tableQueryField("preset", "Preset", viewmodels.TableFilterFieldKindPreset, []string{"credential_kind", "status", "risk_level", "expiry_state", "expires_in_days"}, activePreset, activePreset == "", credentialPresetOptions()),
		tableQueryField("credential_kind", "Credential kind", viewmodels.TableFilterFieldKindSingleSelect, []string{"credential_kind"}, query.CredentialKind, fieldPickerVisible(query.CredentialKind, suppressed["credential_kind"]), []viewmodels.TableFilterOption{
			tableQueryOption("entra_client_secret", "Entra client secret", tableQueryControl("credential_kind", "entra_client_secret")),
			tableQueryOption("entra_certificate", "Entra certificate", tableQueryControl("credential_kind", "entra_certificate")),
			tableQueryOption("github_deploy_key", "GitHub deploy key", tableQueryControl("credential_kind", "github_deploy_key")),
			tableQueryOption("github_pat_request", "GitHub PAT request", tableQueryControl("credential_kind", "github_pat_request")),
			tableQueryOption("github_pat_fine_grained", "GitHub fine-grained PAT", tableQueryControl("credential_kind", "github_pat_fine_grained")),
		}),
		tableQueryField("status", "Status", viewmodels.TableFilterFieldKindSingleSelect, []string{"status"}, query.Status, fieldPickerVisible(query.Status, suppressed["status"]), []viewmodels.TableFilterOption{
			tableQueryOption("active", "Active", tableQueryControl("status", "active")),
			tableQueryOption("pending_approval", "Pending approval", tableQueryControl("status", "pending_approval")),
			tableQueryOption("approved", "Approved", tableQueryControl("status", "approved")),
			tableQueryOption("inactive", "Inactive", tableQueryControl("status", "inactive")),
			tableQueryOption("revoked", "Revoked", tableQueryControl("status", "revoked")),
			tableQueryOption("expired", "Expired", tableQueryControl("status", "expired")),
		}),
		tableQueryField("risk_level", "Risk", viewmodels.TableFilterFieldKindSingleSelect, []string{"risk_level"}, query.RiskLevel, fieldPickerVisible(query.RiskLevel, suppressed["risk_level"]), []viewmodels.TableFilterOption{
			tableQueryOption("critical", "Critical", tableQueryControl("risk_level", "critical")),
			tableQueryOption("high", "High", tableQueryControl("risk_level", "high")),
			tableQueryOption("medium", "Medium", tableQueryControl("risk_level", "medium")),
			tableQueryOption("low", "Low", tableQueryControl("risk_level", "low")),
		}),
		tableQueryField("expiry_state", "Expiry state", viewmodels.TableFilterFieldKindSingleSelect, []string{"expiry_state"}, query.ExpiryState, fieldPickerVisible(query.ExpiryState, suppressed["expiry_state"]), []viewmodels.TableFilterOption{
			tableQueryOption("active", "Not expired", tableQueryControl("expiry_state", "active")),
			tableQueryOption("expired", "Expired", tableQueryControl("expiry_state", "expired")),
		}),
		tableQueryField("expires_in_days", "Expires in", viewmodels.TableFilterFieldKindSingleSelect, []string{"expires_in_days"}, positiveValue(query.ExpiresInDays), fieldPickerVisible(positiveValue(query.ExpiresInDays), suppressed["expires_in_days"]), []viewmodels.TableFilterOption{
			tableQueryOption("7", "7 days", tableQueryControl("expires_in_days", "7")),
			tableQueryOption("30", "30 days", tableQueryControl("expires_in_days", "30")),
			tableQueryOption("90", "90 days", tableQueryControl("expires_in_days", "90")),
		}),
	}

	chips := []viewmodels.TableActiveChip{}
	if chip := maybeChip(fields[0]); chip.FieldID != "" {
		chips = append(chips, chip)
	}
	if chip := maybeChip(fields[1]); chip.FieldID != "" {
		chips = append(chips, chip)
	}
	for _, field := range fields[2:] {
		if suppressed[field.ID] {
			continue
		}
		if chip := maybeChip(field); chip.FieldID != "" {
			chips = append(chips, chip)
		}
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search by name, external ID, asset, creator, or approver"),
		fields,
		chips,
		tableQueryControls(
			controlIfNotEmpty("source_kind", query.Source.Kind),
			controlIfNotEmpty("credential_kind", query.CredentialKind),
			controlIfNotEmpty("status", query.Status),
			controlIfNotEmpty("risk_level", query.RiskLevel),
			controlIfNotEmpty("expiry_state", query.ExpiryState),
			controlIfPositive("expires_in_days", query.ExpiresInDays),
		),
	)
}

func AppsTableQueryBar(data viewmodels.AppsViewData) viewmodels.TableQueryBarData {
	query := data.Query

	statusOptions := make([]viewmodels.TableFilterOption, 0, len(data.StatusOptions))
	for _, status := range data.StatusOptions {
		statusOptions = append(statusOptions, tableQueryOption(status, fallbackHumanized(status), tableQueryControl("status", status)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("integration", "Integration", viewmodels.TableFilterFieldKindSingleSelect, []string{"integration"}, query.Integration, query.Integration == "", []viewmodels.TableFilterOption{
			tableQueryOption("connected", "Connected", tableQueryControl("integration", "connected")),
			tableQueryOption("not_connected", "Not connected", tableQueryControl("integration", "not_connected")),
		}),
		tableQueryField("status", "Status", viewmodels.TableFilterFieldKindSingleSelect, []string{"status"}, query.Status, query.Status == "", statusOptions),
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search apps..."),
		fields,
		[]viewmodels.TableActiveChip{maybeChip(fields[0]), maybeChip(fields[1])},
		tableQueryControls(
			controlIfNotEmpty("integration", query.Integration),
			controlIfNotEmpty("status", query.Status),
		),
	)
}

func DiscoveryAppsTableQueryBar(data viewmodels.DiscoveryAppsViewData) viewmodels.TableQueryBarData {
	query := data.Query

	sourceOptions := make([]viewmodels.TableFilterOption, 0, len(data.SourceOptions))
	for _, source := range data.SourceOptions {
		sourceOptions = append(sourceOptions, tableQueryOption(source.SourceKind, source.Label, tableQueryControl("source_kind", source.SourceKind)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("source_kind", "Source", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_kind"}, query.Source.Kind, query.Source.Kind == "", sourceOptions),
		tableQueryField("managed_state", "Managed state", viewmodels.TableFilterFieldKindSingleSelect, []string{"managed_state"}, query.ManagedState, query.ManagedState == "", []viewmodels.TableFilterOption{
			tableQueryOption("managed", "Managed", tableQueryControl("managed_state", "managed")),
			tableQueryOption("unmanaged", "Unmanaged", tableQueryControl("managed_state", "unmanaged")),
		}),
		tableQueryField("risk_level", "Risk", viewmodels.TableFilterFieldKindSingleSelect, []string{"risk_level"}, query.RiskLevel, query.RiskLevel == "", []viewmodels.TableFilterOption{
			tableQueryOption("critical", "Critical", tableQueryControl("risk_level", "critical")),
			tableQueryOption("high", "High", tableQueryControl("risk_level", "high")),
			tableQueryOption("medium", "Medium", tableQueryControl("risk_level", "medium")),
			tableQueryOption("low", "Low", tableQueryControl("risk_level", "low")),
		}),
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search name, domain, vendor, or key"),
		fields,
		[]viewmodels.TableActiveChip{maybeChip(fields[0]), maybeChip(fields[1]), maybeChip(fields[2])},
		tableQueryControls(
			controlIfNotEmpty("source_kind", query.Source.Kind),
			controlIfNotEmpty("managed_state", query.ManagedState),
			controlIfNotEmpty("risk_level", query.RiskLevel),
		),
	)
}

func ConnectedAppsTableQueryBar(data viewmodels.ConnectedAppsViewData) viewmodels.TableQueryBarData {
	query := data.Query

	field := tableQueryField("governance_state", "Governance", viewmodels.TableFilterFieldKindSingleSelect, []string{"governance_state"}, query.GovernanceState, query.GovernanceState == "", []viewmodels.TableFilterOption{
		tableQueryOption("unreviewed", "Unreviewed", tableQueryControl("governance_state", "unreviewed")),
		tableQueryOption("in_review", "In review", tableQueryControl("governance_state", "in_review")),
		tableQueryOption("approved", "Approved", tableQueryControl("governance_state", "approved")),
		tableQueryOption("action_required", "Action required", tableQueryControl("governance_state", "action_required")),
		tableQueryOption("ticketed", "Ticketed", tableQueryControl("governance_state", "ticketed")),
	})

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search app name or client ID"),
		[]viewmodels.TableFilterField{field},
		[]viewmodels.TableActiveChip{maybeChip(field)},
		tableQueryControls(
			tableQueryControl("source_kind", "google_workspace"),
			tableQueryControl("asset_kind", "google_oauth_client"),
			controlIfNotEmpty("governance_state", query.GovernanceState),
		),
	)
}

func AppAssetsTableQueryBar(data viewmodels.AppAssetsViewData) viewmodels.TableQueryBarData {
	query := data.Query

	sourceOptions := make([]viewmodels.TableFilterOption, 0, len(data.Sources))
	for _, source := range data.Sources {
		sourceOptions = append(sourceOptions, tableQueryOption(source.SourceKind, source.Label, tableQueryControl("source_kind", source.SourceKind)))
	}

	fields := []viewmodels.TableFilterField{
		tableQueryField("source_kind", "Source", viewmodels.TableFilterFieldKindSingleSelect, []string{"source_kind"}, query.Source.Kind, query.Source.Kind == "", sourceOptions),
		tableQueryField("asset_kind", "Asset kind", viewmodels.TableFilterFieldKindSingleSelect, []string{"asset_kind"}, query.AssetKind, query.AssetKind == "", []viewmodels.TableFilterOption{
			tableQueryOption("entra_application", "Entra application", tableQueryControl("asset_kind", "entra_application")),
			tableQueryOption("entra_service_principal", "Entra service principal", tableQueryControl("asset_kind", "entra_service_principal")),
			tableQueryOption("github_app_installation", "GitHub app installation", tableQueryControl("asset_kind", "github_app_installation")),
			tableQueryOption("google_oauth_client", "Google OAuth client", tableQueryControl("asset_kind", "google_oauth_client")),
			tableQueryOption("vault_auth_mount", "Vault auth mount", tableQueryControl("asset_kind", "vault_auth_mount")),
			tableQueryOption("vault_secrets_mount", "Vault secrets mount", tableQueryControl("asset_kind", "vault_secrets_mount")),
			tableQueryOption("vault_auth_role", "Vault auth role", tableQueryControl("asset_kind", "vault_auth_role")),
		}),
	}

	return tableQueryBarData(
		tableQuerySearch(query.Q, "Search by name, external ID, or parent ID"),
		fields,
		[]viewmodels.TableActiveChip{maybeChip(fields[0]), maybeChip(fields[1])},
		tableQueryControls(
			controlIfNotEmpty("source_kind", query.Source.Kind),
			controlIfNotEmpty("asset_kind", query.AssetKind),
		),
	)
}

func tableQuerySearch(value, placeholder string) viewmodels.TableQuerySearch {
	return viewmodels.TableQuerySearch{
		Name:        "q",
		Value:       strings.TrimSpace(value),
		Placeholder: strings.TrimSpace(placeholder),
	}
}

func tableQueryField(id, label string, kind viewmodels.TableFilterFieldKind, inputNames []string, activeValue string, pickerVisible bool, options []viewmodels.TableFilterOption) viewmodels.TableFilterField {
	if len(options) == 0 {
		pickerVisible = false
	}
	return viewmodels.TableFilterField{
		ID:            strings.TrimSpace(id),
		Label:         strings.TrimSpace(label),
		Kind:          kind,
		InputNames:    append([]string(nil), inputNames...),
		Options:       append([]viewmodels.TableFilterOption(nil), options...),
		ActiveValue:   strings.TrimSpace(activeValue),
		PickerVisible: pickerVisible,
	}
}

func tableQueryOption(value, label string, controls ...viewmodels.TableFilterControl) viewmodels.TableFilterOption {
	return viewmodels.TableFilterOption{
		Value:    strings.TrimSpace(value),
		Label:    strings.TrimSpace(label),
		Controls: tableQueryControls(controls...),
	}
}

func tableQueryControl(name, value string) viewmodels.TableFilterControl {
	return viewmodels.TableFilterControl{
		Name:  strings.TrimSpace(name),
		Value: strings.TrimSpace(value),
	}
}

func tableQueryControls(controls ...viewmodels.TableFilterControl) []viewmodels.TableFilterControl {
	out := make([]viewmodels.TableFilterControl, 0, len(controls))
	for _, control := range controls {
		name := strings.TrimSpace(control.Name)
		if name == "" {
			continue
		}
		out = append(out, viewmodels.TableFilterControl{
			Name:  name,
			Value: strings.TrimSpace(control.Value),
		})
	}
	return out
}

func tableQueryBarData(search viewmodels.TableQuerySearch, fields []viewmodels.TableFilterField, chips []viewmodels.TableActiveChip, controls []viewmodels.TableFilterControl) viewmodels.TableQueryBarData {
	outChips := make([]viewmodels.TableActiveChip, 0, len(chips))
	for _, chip := range chips {
		if chip.FieldID == "" {
			continue
		}
		outChips = append(outChips, chip)
	}

	return viewmodels.TableQueryBarData{
		Search:         search,
		Fields:         fields,
		ActiveChips:    outChips,
		HiddenControls: controls,
	}
}

func maybeChip(field viewmodels.TableFilterField) viewmodels.TableActiveChip {
	if field.ActiveValue == "" {
		return viewmodels.TableActiveChip{}
	}
	return tableQueryChip(field.ID, chipLabelForField(field.Label, tableQueryOptionLabel(field.Options, field.ActiveValue)))
}

func maybeBooleanChip(field viewmodels.TableFilterField, active bool) viewmodels.TableActiveChip {
	if !active {
		return viewmodels.TableActiveChip{}
	}
	return tableQueryChip(field.ID, chipLabelForField(field.Label, tableQueryOptionLabel(field.Options, field.ActiveValue)))
}

func tableQueryChip(fieldID, label string) viewmodels.TableActiveChip {
	fieldID = strings.TrimSpace(fieldID)
	label = strings.TrimSpace(label)
	if fieldID == "" || label == "" {
		return viewmodels.TableActiveChip{}
	}
	return viewmodels.TableActiveChip{
		FieldID: fieldID,
		Label:   label,
	}
}

func tableQueryOptionLabel(options []viewmodels.TableFilterOption, value string) string {
	value = strings.TrimSpace(value)
	for _, option := range options {
		if option.Value == value {
			return option.Label
		}
	}
	return fallbackHumanized(value)
}

func chipLabelForField(fieldLabel, optionLabel string) string {
	fieldLabel = strings.TrimSpace(fieldLabel)
	optionLabel = strings.TrimSpace(optionLabel)
	if fieldLabel == "" {
		return optionLabel
	}
	if optionLabel == "" {
		return fieldLabel
	}
	return fieldLabel + ": " + optionLabel
}

func controlIfNotEmpty(name, value string) viewmodels.TableFilterControl {
	if strings.TrimSpace(value) == "" {
		return viewmodels.TableFilterControl{}
	}
	return tableQueryControl(name, value)
}

func controlIfPositive(name string, value int) viewmodels.TableFilterControl {
	if value <= 0 {
		return viewmodels.TableFilterControl{}
	}
	return tableQueryControl(name, strconv.Itoa(value))
}

func controlIfTrue(name string, value bool) viewmodels.TableFilterControl {
	if !value {
		return viewmodels.TableFilterControl{}
	}
	return tableQueryControl(name, "1")
}

func boolActiveValue(value bool) string {
	if value {
		return "yes"
	}
	return ""
}

func positiveValue(value int) string {
	if value <= 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func fieldPickerVisible(activeValue string, suppressed bool) bool {
	if suppressed {
		return true
	}
	return strings.TrimSpace(activeValue) == ""
}

func sortDirWhenSet(sortBy, sortDir string) string {
	if strings.TrimSpace(sortBy) == "" {
		return ""
	}
	return strings.TrimSpace(sortDir)
}

func activeCredentialsPreset(query querystate.CredentialsQuery) string {
	switch {
	case query.RiskLevel == "critical" && query.CredentialKind == "" && query.Status == "" && query.ExpiryState == "" && query.ExpiresInDays == 0:
		return "critical_risk"
	case query.RiskLevel == "high" && query.CredentialKind == "" && query.Status == "" && query.ExpiryState == "" && query.ExpiresInDays == 0:
		return "high_risk"
	case query.Status == "" && query.RiskLevel == "" && query.ExpiryState == "active" && query.ExpiresInDays == 30 && query.CredentialKind == "":
		return "expiring_30d"
	case query.Status == "pending_approval" && query.CredentialKind == "" && query.RiskLevel == "" && query.ExpiryState == "" && query.ExpiresInDays == 0:
		return "pending_approval"
	default:
		return ""
	}
}

func suppressedCredentialFieldIDs(activePreset string) map[string]bool {
	switch activePreset {
	case "critical_risk", "high_risk":
		return map[string]bool{
			"credential_kind": true,
			"status":          true,
			"risk_level":      true,
			"expiry_state":    true,
			"expires_in_days": true,
		}
	case "expiring_30d":
		return map[string]bool{
			"credential_kind": true,
			"risk_level":      true,
			"status":          true,
			"expiry_state":    true,
			"expires_in_days": true,
		}
	case "pending_approval":
		return map[string]bool{
			"credential_kind": true,
			"status":          true,
			"risk_level":      true,
			"expiry_state":    true,
			"expires_in_days": true,
		}
	default:
		return map[string]bool{}
	}
}

func credentialPresetOptions() []viewmodels.TableFilterOption {
	return []viewmodels.TableFilterOption{
		tableQueryOption(
			"critical_risk",
			"Critical risk",
			tableQueryControl("risk_level", "critical"),
		),
		tableQueryOption(
			"high_risk",
			"High risk",
			tableQueryControl("risk_level", "high"),
		),
		tableQueryOption(
			"expiring_30d",
			"Expiring <= 30d",
			tableQueryControl("expiry_state", "active"),
			tableQueryControl("expires_in_days", "30"),
		),
		tableQueryOption(
			"pending_approval",
			"Pending approval",
			tableQueryControl("status", "pending_approval"),
		),
	}
}

func identitySortValue(sortBy, sortDir string) string {
	sortBy = strings.TrimSpace(sortBy)
	sortDir = strings.TrimSpace(sortDir)
	if sortBy == "" {
		return ""
	}
	if sortDir == "" {
		sortDir = "desc"
	}
	return sortBy + "_" + sortDir
}

func identitySortOptions() []viewmodels.TableFilterOption {
	return []viewmodels.TableFilterOption{
		tableQueryOption("identity_asc", "Identity (A-Z)", tableQueryControl("sort_by", "identity"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("identity_desc", "Identity (Z-A)", tableQueryControl("sort_by", "identity"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("identity_type_asc", "Type (A-Z)", tableQueryControl("sort_by", "identity_type"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("identity_type_desc", "Type (Z-A)", tableQueryControl("sort_by", "identity_type"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("managed_desc", "Managed first", tableQueryControl("sort_by", "managed"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("managed_asc", "Managed last", tableQueryControl("sort_by", "managed"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("source_type_asc", "Source (A-Z)", tableQueryControl("sort_by", "source_type"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("source_type_desc", "Source (Z-A)", tableQueryControl("sort_by", "source_type"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("linked_sources_desc", "More linked sources", tableQueryControl("sort_by", "linked_sources"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("linked_sources_asc", "Fewer linked sources", tableQueryControl("sort_by", "linked_sources"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("privileged_roles_desc", "More privileged roles", tableQueryControl("sort_by", "privileged_roles"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("privileged_roles_asc", "Fewer privileged roles", tableQueryControl("sort_by", "privileged_roles"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("status_asc", "Status (A-Z)", tableQueryControl("sort_by", "status"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("status_desc", "Status (Z-A)", tableQueryControl("sort_by", "status"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("last_seen_desc", "Last seen (newest)", tableQueryControl("sort_by", "last_seen"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("last_seen_asc", "Last seen (oldest)", tableQueryControl("sort_by", "last_seen"), tableQueryControl("sort_dir", "asc")),
	}
}

func nonHumanAccessSortValue(sortBy, sortDir string) string {
	sortBy = strings.TrimSpace(sortBy)
	sortDir = strings.TrimSpace(sortDir)
	if sortBy == "" {
		return ""
	}
	if sortDir == "" {
		sortDir = "desc"
	}
	return sortBy + "_" + sortDir
}

func nonHumanAccessSortOptions() []viewmodels.TableFilterOption {
	return []viewmodels.TableFilterOption{
		tableQueryOption("principal_asc", "Principal (A-Z)", tableQueryControl("sort_by", "principal"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("principal_desc", "Principal (Z-A)", tableQueryControl("sort_by", "principal"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("principal_type_asc", "Type (A-Z)", tableQueryControl("sort_by", "principal_type"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("principal_type_desc", "Type (Z-A)", tableQueryControl("sort_by", "principal_type"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("source_asc", "Source (A-Z)", tableQueryControl("sort_by", "source"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("source_desc", "Source (Z-A)", tableQueryControl("sort_by", "source"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("owner_asc", "Owner (A-Z)", tableQueryControl("sort_by", "owner"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("owner_desc", "Owner (Z-A)", tableQueryControl("sort_by", "owner"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("governance_asc", "Governance (A-Z)", tableQueryControl("sort_by", "governance"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("governance_desc", "Governance (Z-A)", tableQueryControl("sort_by", "governance"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("risk_desc", "Highest risk", tableQueryControl("sort_by", "risk"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("risk_asc", "Lowest risk", tableQueryControl("sort_by", "risk"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("last_seen_desc", "Last seen (newest)", tableQueryControl("sort_by", "last_seen"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("last_seen_asc", "Last seen (oldest)", tableQueryControl("sort_by", "last_seen"), tableQueryControl("sort_dir", "asc")),
		tableQueryOption("freshness_desc", "Freshest first", tableQueryControl("sort_by", "freshness"), tableQueryControl("sort_dir", "desc")),
		tableQueryOption("freshness_asc", "Stalest first", tableQueryControl("sort_by", "freshness"), tableQueryControl("sort_dir", "asc")),
	}
}
