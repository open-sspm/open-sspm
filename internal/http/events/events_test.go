package events

import "testing"

func TestBusTrigger(t *testing.T) {
	if got := BusTrigger(DataSyncChanged); got != "osspm:data-sync-changed from:body delay:100ms" {
		t.Fatalf("BusTrigger mismatch: %q", got)
	}
}

func TestBusTriggers(t *testing.T) {
	got := BusTriggers(DataSyncChanged, ConnectorsChanged)
	want := "osspm:data-sync-changed from:body delay:100ms, osspm:connectors-changed from:body delay:100ms"
	if got != want {
		t.Fatalf("BusTriggers mismatch:\n got=%q\nwant=%q", got, want)
	}
}

func TestBusTriggersEmpty(t *testing.T) {
	if got := BusTriggers(); got != "" {
		t.Fatalf("empty BusTriggers should be empty string, got %q", got)
	}
}
