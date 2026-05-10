package oktaingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/open-sspm/open-sspm/internal/queue"
)

const oktaPushInboxQueueName = "okta_push_inbox"

type InboxQueue interface {
	Enqueue(context.Context, []int64) error
	Dequeue(context.Context, int32, time.Duration) ([]int64, error)
	Close() error
}

type RedisInboxQueue struct {
	queue *queue.RedisListQueue
}

func NewRedisInboxQueue(redisURL, keyPrefix string) (*RedisInboxQueue, error) {
	q, err := queue.NewRedisListQueue(redisURL, keyPrefix)
	if err != nil {
		return nil, err
	}
	return &RedisInboxQueue{queue: q}, nil
}

func (q *RedisInboxQueue) Ping(ctx context.Context) error {
	if q == nil || q.queue == nil {
		return errors.New("okta push redis queue is not configured")
	}
	return q.queue.Ping(ctx)
}

func (q *RedisInboxQueue) Close() error {
	if q == nil || q.queue == nil {
		return nil
	}
	return q.queue.Close()
}

func (q *RedisInboxQueue) Enqueue(ctx context.Context, ids []int64) error {
	if q == nil || q.queue == nil {
		return errors.New("okta push redis queue is not configured")
	}
	values := make([]string, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		values = append(values, strconv.FormatInt(id, 10))
	}
	// Deduplication is per enqueue batch only; Postgres claim-by-ID keeps
	// repeated Redis wake-ups idempotent across deliveries.
	return q.queue.Enqueue(ctx, oktaPushInboxQueueName, values)
}

func (q *RedisInboxQueue) Depth(ctx context.Context) (int64, error) {
	if q == nil || q.queue == nil {
		return 0, errors.New("okta push redis queue is not configured")
	}
	return q.queue.Len(ctx, oktaPushInboxQueueName)
}

func (q *RedisInboxQueue) Dequeue(ctx context.Context, limit int32, wait time.Duration) ([]int64, error) {
	if q == nil || q.queue == nil {
		return nil, errors.New("okta push redis queue is not configured")
	}
	values, err := q.queue.Dequeue(ctx, oktaPushInboxQueueName, int(limit), wait)
	if err != nil && len(values) == 0 {
		return nil, err
	}
	ids := make([]int64, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode okta push inbox queue id %q: %w", value, err)
		}
		if id <= 0 {
			return nil, fmt.Errorf("decode okta push inbox queue id %q: id must be positive", value)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
