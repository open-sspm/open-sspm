package oktaingest

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisInboxQueueRoundTripDedupesIDs(t *testing.T) {
	srv := miniredis.RunT(t)
	q, err := NewRedisInboxQueue("redis://"+srv.Addr()+"/0", "test")
	if err != nil {
		t.Fatalf("NewRedisInboxQueue(): %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	ctx := context.Background()
	if err := q.Enqueue(ctx, []int64{3, 1, 3, 0, -1, 2}); err != nil {
		t.Fatalf("Enqueue(): %v", err)
	}
	if depth, err := q.Depth(ctx); err != nil || depth != 3 {
		t.Fatalf("Depth() = %d, %v; want 3, nil", depth, err)
	}

	got, err := q.Dequeue(ctx, 10, 0)
	if err != nil {
		t.Fatalf("Dequeue(): %v", err)
	}
	want := []int64{3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("Dequeue() = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Dequeue() = %#v, want %#v", got, want)
		}
	}
	if depth, err := q.Depth(ctx); err != nil || depth != 0 {
		t.Fatalf("Depth() after dequeue = %d, %v; want 0, nil", depth, err)
	}
}
