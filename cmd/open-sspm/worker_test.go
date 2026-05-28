package main

import "testing"

func TestWorkerLaneByName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{name: "", wantName: "full"},
		{name: "full", wantName: "full"},
		{name: " FULL ", wantName: "full"},
		{name: "discovery", wantName: "discovery"},
		{name: "event-inbox", wantName: "event-inbox"},
		{name: " EVENT-INBOX ", wantName: "event-inbox"},
		{name: "tail", wantName: "tail"},
		{name: "evaluator", wantName: "evaluator"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lane, err := workerLaneByName(tt.name)
			if err != nil {
				t.Fatalf("workerLaneByName(%q) err = %v", tt.name, err)
			}
			if got := lane.Name(); got != tt.wantName {
				t.Fatalf("lane name = %q, want %q", got, tt.wantName)
			}
		})
	}
}

func TestWorkerLaneByNameRejectsUnknownLane(t *testing.T) {
	t.Parallel()

	if _, err := workerLaneByName("unknown"); err == nil {
		t.Fatal("workerLaneByName(unknown) err = nil, want error")
	}
}
