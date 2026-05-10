package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisListQueue struct {
	client    *redis.Client
	keyPrefix string
}

func NewRedisListQueue(rawURL, keyPrefix string) (*RedisListQueue, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("redis url is required")
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	keyPrefix = normalizeRedisKeyPart(keyPrefix)
	if keyPrefix == "" {
		keyPrefix = "open-sspm"
	}
	return &RedisListQueue{
		client:    redis.NewClient(opts),
		keyPrefix: keyPrefix,
	}, nil
}

func (q *RedisListQueue) Ping(ctx context.Context) error {
	if q == nil || q.client == nil {
		return errors.New("redis queue is not configured")
	}
	if err := q.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (q *RedisListQueue) Close() error {
	if q == nil || q.client == nil {
		return nil
	}
	return q.client.Close()
}

func (q *RedisListQueue) Enqueue(ctx context.Context, name string, values []string) error {
	if q == nil || q.client == nil {
		return errors.New("redis queue is not configured")
	}
	name = normalizeRedisKeyPart(name)
	if name == "" {
		return errors.New("redis queue name is required")
	}
	if len(values) == 0 {
		return nil
	}
	if err := q.client.RPush(ctx, q.key(name), values).Err(); err != nil {
		return fmt.Errorf("enqueue redis queue %q: %w", name, err)
	}
	return nil
}

func (q *RedisListQueue) Dequeue(ctx context.Context, name string, limit int, wait time.Duration) ([]string, error) {
	if q == nil || q.client == nil {
		return nil, errors.New("redis queue is not configured")
	}
	name = normalizeRedisKeyPart(name)
	if name == "" {
		return nil, errors.New("redis queue name is required")
	}
	if limit <= 0 {
		limit = 1
	}

	key := q.key(name)
	first, err := q.dequeueOne(ctx, key, wait)
	if err != nil {
		return nil, err
	}
	if first == "" {
		return nil, nil
	}
	out := []string{first}
	if limit == 1 {
		return out, nil
	}

	more, err := q.client.LPopCount(ctx, key, limit-1).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return out, fmt.Errorf("dequeue redis queue %q: %w", name, err)
	}
	out = append(out, more...)
	return out, nil
}

func (q *RedisListQueue) Len(ctx context.Context, name string) (int64, error) {
	if q == nil || q.client == nil {
		return 0, errors.New("redis queue is not configured")
	}
	name = normalizeRedisKeyPart(name)
	if name == "" {
		return 0, errors.New("redis queue name is required")
	}
	n, err := q.client.LLen(ctx, q.key(name)).Result()
	if err != nil {
		return 0, fmt.Errorf("read redis queue %q length: %w", name, err)
	}
	return n, nil
}

func (q *RedisListQueue) dequeueOne(ctx context.Context, key string, wait time.Duration) (string, error) {
	if wait > 0 {
		values, err := q.client.BLPop(ctx, wait, key).Result()
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("dequeue redis queue: %w", err)
		}
		if len(values) < 2 {
			return "", nil
		}
		return strings.TrimSpace(values[1]), nil
	}

	value, err := q.client.LPop(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("dequeue redis queue: %w", err)
	}
	return strings.TrimSpace(value), nil
}

func (q *RedisListQueue) key(name string) string {
	return q.keyPrefix + ":" + name
}

func normalizeRedisKeyPart(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, ":")
	return value
}
