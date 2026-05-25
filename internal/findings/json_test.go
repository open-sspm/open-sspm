package findings

import "testing"

func TestJSONObjectReturnsEmptyObjectForInvalidInput(t *testing.T) {
	for _, raw := range [][]byte{
		nil,
		[]byte(""),
		[]byte("not json"),
		[]byte("null"),
		[]byte("[]"),
	} {
		if got := JSONObject(raw); got == nil || len(got) != 0 {
			t.Fatalf("JSONObject(%q) = %#v, want empty object", string(raw), got)
		}
	}
}

func TestJSONObjectReturnsParsedObject(t *testing.T) {
	got := JSONObject([]byte(`{"status":"open"}`))
	if got["status"] != "open" {
		t.Fatalf("JSONObject() status = %#v, want open", got["status"])
	}
}
