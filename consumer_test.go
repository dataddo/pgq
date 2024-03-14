package pgq

import (
	"context"
	"testing"
	"time"

	"golang.org/x/sync/semaphore"

	"go.dataddo.com/pgq/internal/require"
)

func TestConsumer_generateQuery(t *testing.T) {
	type args struct {
		queueName string
		opts      []ConsumerOption
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "simple",
			args: args{queueName: "testing_queue"},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND consumed_count < :max_consume_count AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
		{
			name: "scanInterval 12 hours",

			args: args{
				queueName: "testing_queue",
				opts: []ConsumerOption{
					WithHistoryLimit(12 * time.Hour),
				},
			},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE created_at >= CURRENT_TIMESTAMP - (:history_limit)::interval AND created_at < CURRENT_TIMESTAMP AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND consumed_count < :max_consume_count AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
		{
			name: "scn interval 12 hours abd max consumed count limit disabled",

			args: args{
				queueName: "testing_queue",
				opts: []ConsumerOption{
					WithHistoryLimit(12 * time.Hour),
					WithMaxConsumeCount(0),
				},
			},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE created_at >= CURRENT_TIMESTAMP - (:history_limit)::interval AND created_at < CURRENT_TIMESTAMP AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
		{
			name: "with metadata condition",
			args: args{queueName: "testing_queue"},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND consumed_count < :max_consume_count AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
		{
			name: "scanInterval 12 hours with metadata condition",
			args: args{
				queueName: "testing_queue",
				opts: []ConsumerOption{
					WithHistoryLimit(12 * time.Hour),
				},
			},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE created_at >= CURRENT_TIMESTAMP - (:history_limit)::interval AND created_at < CURRENT_TIMESTAMP AND (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND consumed_count < :max_consume_count AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
		{
			name: "with negative metadata condition",
			args: args{queueName: "testing_queue"},
			want: "UPDATE \"testing_queue\" SET locked_until = :locked_until, started_at = CURRENT_TIMESTAMP, consumed_count = consumed_count+1 WHERE id IN (SELECT id FROM \"testing_queue\" WHERE (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP) AND consumed_count < :max_consume_count AND processed_at IS NULL ORDER BY consumed_count ASC, created_at ASC LIMIT :limit FOR UPDATE SKIP LOCKED) RETURNING id, payload, metadata, consumed_count, locked_until",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(nil, tt.args.queueName, nil, tt.args.opts...)
			require.NoError(t, err)
			got := c.generateQuery()
			require.Equal(t, tt.want, got.String())
		})
	}
}

func TestAcquireMaxFromSemaphore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	const size int64 = 10

	w := semaphore.NewWeighted(size)

	acquired, err := acquireMaxFromSemaphore(ctx, w, size)
	require.NoError(t, err)
	require.Equal(t, size, acquired)

	const released1 int64 = 3
	w.Release(released1)
	acquired, err = acquireMaxFromSemaphore(ctx, w, size)
	require.NoError(t, err)
	require.Equal(t, released1, acquired)

	const released2 int64 = 1
	go func() {
		time.Sleep(time.Millisecond)
		w.Release(released2)
	}()
	acquired, err = acquireMaxFromSemaphore(ctx, w, size)
	require.NoError(t, err)
	require.Equal(t, released2, acquired)

	acquired, err = acquireMaxFromSemaphore(ctx, w, size)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int64(0), acquired)
}
