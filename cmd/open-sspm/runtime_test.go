package main

import (
	"context"
	"testing"
	"time"

	"github.com/open-sspm/open-sspm/internal/config"
)

func TestOpenOktaPushInboxQueueFallsBackWhenRedisUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	q, err := openOktaPushInboxQueue(ctx, config.Config{
		QueueBackend:   config.QueueBackendRedis,
		RedisURL:       "redis://127.0.0.1:1/0",
		RedisKeyPrefix: "test",
	})
	if err != nil {
		t.Fatalf("openOktaPushInboxQueue() error = %v", err)
	}
	if q != nil {
		t.Fatal("openOktaPushInboxQueue() returned a queue for unavailable Redis")
	}
}
