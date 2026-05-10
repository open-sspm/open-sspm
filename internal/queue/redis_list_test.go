package queue

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisListQueueEnqueueDequeueFIFO(t *testing.T) {
	srv := miniredis.RunT(t)
	q, err := NewRedisListQueue("redis://"+srv.Addr()+"/0", "test")
	if err != nil {
		t.Fatalf("NewRedisListQueue(): %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	ctx := context.Background()
	if err := q.Enqueue(ctx, "okta_push", []string{"1", "2", "3"}); err != nil {
		t.Fatalf("Enqueue(): %v", err)
	}

	got, err := q.Dequeue(ctx, "okta_push", 2, 0)
	if err != nil {
		t.Fatalf("Dequeue(): %v", err)
	}
	if len(got) != 2 || got[0] != "1" || got[1] != "2" {
		t.Fatalf("Dequeue() = %#v, want [1 2]", got)
	}

	got, err = q.Dequeue(ctx, "okta_push", 2, 0)
	if err != nil {
		t.Fatalf("Dequeue() second: %v", err)
	}
	if len(got) != 1 || got[0] != "3" {
		t.Fatalf("Dequeue() second = %#v, want [3]", got)
	}
}

func TestRedisListQueueDequeueEmpty(t *testing.T) {
	srv := miniredis.RunT(t)
	q, err := NewRedisListQueue("redis://"+srv.Addr()+"/0", "test")
	if err != nil {
		t.Fatalf("NewRedisListQueue(): %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	got, err := q.Dequeue(context.Background(), "okta_push", 10, 0)
	if err != nil {
		t.Fatalf("Dequeue(): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Dequeue() = %#v, want empty", got)
	}
}

func TestRedisListQueueBlockingDequeueHonorsTimeout(t *testing.T) {
	srv := miniredis.RunT(t)
	q, err := NewRedisListQueue("redis://"+srv.Addr()+"/0", "test")
	if err != nil {
		t.Fatalf("NewRedisListQueue(): %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	got, err := q.Dequeue(ctx, "okta_push", 1, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("Dequeue(): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Dequeue() = %#v, want empty", got)
	}
}
