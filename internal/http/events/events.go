// Package events centralizes the names of htmx bus events that flow between
// handlers and templates. Import from both sides so a typo can't silently
// break the wiring.
package events

const (
	Toast                     = "osspm:toast"
	SettingsUsersChanged      = "osspm:settings-users-changed"
	ConnectorsChanged         = "osspm:connectors-changed"
	ConnectorHealthChanged    = "osspm:connector-health-changed"
	IdentityResolutionChanged = "osspm:identity-resolution-changed"
	FindingsRulesetChanged    = "osspm:findings-ruleset-changed"
	FindingsRuleChanged       = "osspm:findings-rule-changed"
	DataSyncChanged           = "osspm:data-sync-changed"
)

// DebounceMS is the standard debounce applied when listening for a bus event
// from body. Picked once so future tuning is a single-file change.
const DebounceMS = 100

// BusTrigger returns the hx-trigger string for listening to one of the
// osspm:* events from body with the standard debounce.
func BusTrigger(event string) string {
	return event + " from:body delay:100ms"
}

// BusTriggers joins several BusTrigger strings with the comma-space the
// htmx trigger syntax expects.
func BusTriggers(eventNames ...string) string {
	if len(eventNames) == 0 {
		return ""
	}
	out := make([]byte, 0, len(eventNames)*32)
	for i, name := range eventNames {
		if i > 0 {
			out = append(out, ',', ' ')
		}
		out = append(out, name...)
		out = append(out, ' ', 'f', 'r', 'o', 'm', ':', 'b', 'o', 'd', 'y', ' ', 'd', 'e', 'l', 'a', 'y', ':', '1', '0', '0', 'm', 's')
	}
	return string(out)
}
