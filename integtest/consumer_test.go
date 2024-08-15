package integtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
	"go.opentelemetry.io/otel/metric/noop"

	. "go.dataddo.com/pgq"
	"go.dataddo.com/pgq/internal/pg"
	"go.dataddo.com/pgq/internal/require"
	"go.dataddo.com/pgq/x/schema"
)

func TestConsumer_Run_graceful_shutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	db := openDB(t)
	queueName := t.Name()
	_, _ = db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
	_, err := db.ExecContext(ctx, schema.GenerateCreateTableQuery(queueName))
	t.Cleanup(func() {
		db := openDB(t)
		_, err := db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)
	})
	require.NoError(t, err)
	publisher := NewPublisher(db)

	msgIDs, err := publisher.Publish(ctx, queueName,
		&MessageOutgoing{Metadata: Metadata{"foo": "bar"}, Payload: json.RawMessage(`{"foo":"bar"}`)},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgIDs))
	msgID := msgIDs[0]

	// consumer
	handler := &slowHandler{}
	consumer, err := NewConsumer(db, queueName, handler,
		WithLogger(slog.New(slog.NewTextHandler(&tbWriter{tb: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		WithLockDuration(time.Hour),
		WithPollingInterval(time.Second),
		WithMaxParallelMessages(1),
		WithInvalidMessageCallback(func(_ context.Context, _ InvalidMessage, err error) {
			require.NoError(t, err)
		}),
		WithMetrics(noop.Meter{}),
	)
	require.NoError(t, err)

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumeCancel()
	err = consumer.Run(consumeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = db.Close()
	require.NoError(t, err)

	// evaluate
	query := fmt.Sprintf(
		`SELECT locked_until, consumed_count FROM %s WHERE id = $1`,
		pg.QuoteIdentifier(queueName),
	)
	db = openDB(t)
	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err)
	})
	row := db.QueryRowContext(ctx, query, msgID)
	var (
		lockedUntil    sql.NullTime
		processedCount int
	)
	err = row.Scan(&lockedUntil, &processedCount)
	require.NoError(t, err)
	require.Equal(t, false, lockedUntil.Valid)
	require.Equal(t, 1, processedCount)
}

func TestConsumer_Run_MetadataFilter_Equal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	db := openDB(t)
	queueName := t.Name()
	_, _ = db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
	_, err := db.ExecContext(ctx, schema.GenerateCreateTableQuery(queueName))
	t.Cleanup(func() {
		db := openDB(t)
		_, err := db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)
	})
	require.NoError(t, err)
	publisher := NewPublisher(db)

	msgIDs, err := publisher.Publish(ctx, queueName,
		&MessageOutgoing{Metadata: Metadata{"baz": "quux"}, Payload: json.RawMessage(`{"baz":"queex"}`)},
	)

	require.NoError(t, err)
	require.Equal(t, 1, len(msgIDs))

	simpleMsgIDs, err := publisher.Publish(ctx, queueName,
		&MessageOutgoing{Metadata: Metadata{"foo": "bar"}, Payload: json.RawMessage(`{"foo":"bar"}`)},
	)

	require.NoError(t, err)
	require.Equal(t, 1, len(simpleMsgIDs))

	// consumer
	handler := &regularHandler{}
	consumer, err := NewConsumer(db, queueName, handler,
		WithLogger(slog.New(slog.NewTextHandler(&tbWriter{tb: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		WithLockDuration(time.Hour),
		WithPollingInterval(time.Second),
		WithMaxParallelMessages(1),
		WithMetadataFilter(&MetadataFilter{Key: "baz", Operation: OpEqual, Value: "quux"}),
		WithInvalidMessageCallback(func(_ context.Context, _ InvalidMessage, err error) {
			require.NoError(t, err)
		}),
		WithMetrics(noop.Meter{}),
	)
	require.NoError(t, err)

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumeCancel()
	err = consumer.Run(consumeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = db.Close()
	require.NoError(t, err)

	// evaluate
	query := fmt.Sprintf(
		`SELECT count(1) FROM %s WHERE processed_at is null`,
		pg.QuoteIdentifier(queueName),
	)
	db = openDB(t)
	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err)
	})
	row := db.QueryRowContext(ctx, query)
	var (
		msgCount int
	)
	err = row.Scan(&msgCount)
	require.NoError(t, err)
	require.Equal(t, 1, msgCount)

}

func TestConsumer_Run_MetadataFilter_NotEqual(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	db := openDB(t)
	queueName := t.Name()
	_, _ = db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
	_, err := db.ExecContext(ctx, schema.GenerateCreateTableQuery(queueName))
	t.Cleanup(func() {
		db := openDB(t)
		_, err := db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)
	})
	require.NoError(t, err)
	publisher := NewPublisher(db)

	msgIDs, err := publisher.Publish(ctx, queueName,
		&MessageOutgoing{Metadata: Metadata{"baz": "quux"}, Payload: json.RawMessage(`{"baz":"queex"}`)},
	)

	require.NoError(t, err)
	require.Equal(t, 1, len(msgIDs))

	simpleMsgIDs, err := publisher.Publish(ctx, queueName,
		&MessageOutgoing{Metadata: Metadata{"foo": "bar"}, Payload: json.RawMessage(`{"foo":"bar"}`)},
	)

	require.NoError(t, err)
	require.Equal(t, 1, len(simpleMsgIDs))

	// consumer
	handler := &regularHandler{}
	consumer, err := NewConsumer(db, queueName, handler,
		WithLogger(slog.New(slog.NewTextHandler(&tbWriter{tb: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))),
		WithLockDuration(time.Hour),
		WithPollingInterval(time.Second),
		WithMaxParallelMessages(1),
		WithMetadataFilter(&MetadataFilter{Key: "baz", Operation: OpNotEqual, Value: "quux"}),
		WithInvalidMessageCallback(func(_ context.Context, _ InvalidMessage, err error) {
			require.NoError(t, err)
		}),
		WithMetrics(noop.Meter{}),
	)
	require.NoError(t, err)

	consumeCtx, consumeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer consumeCancel()
	err = consumer.Run(consumeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = db.Close()
	require.NoError(t, err)

	// evaluate
	query := fmt.Sprintf(
		`SELECT count(1) FROM %s WHERE processed_at is null and metadata->>'baz' = 'quux'`,
		pg.QuoteIdentifier(queueName),
	)
	db = openDB(t)
	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err)
	})
	row := db.QueryRowContext(ctx, query)
	var (
		msgCount int
	)
	err = row.Scan(&msgCount)
	require.NoError(t, err)
	require.Equal(t, 1, msgCount)

}

func openDB(t *testing.T) *sql.DB {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok {
		t.Skip("Skipping integration test, TEST_POSTGRES_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err)
	})
	ensureUUIDExtension(t, db)
	return db
}

func ensureUUIDExtension(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
		DO $$ 
		BEGIN
		  IF current_setting('server_version_num')::int < 130000 THEN
		    -- If PostgreSQL version is less than 13, enable pgcrypto
		    CREATE EXTENSION IF NOT EXISTS pgcrypto;
		  END IF;
		END $$;
	`)
	require.NoError(t, err)
}

type (
	slowHandler    struct{}
	regularHandler struct{}
)

func (s *regularHandler) HandleMessage(ctx context.Context, _ *MessageIncoming) (processed bool, err error) {
	<-ctx.Done()
	return MessageProcessed, nil
}

func (s *slowHandler) HandleMessage(ctx context.Context, _ *MessageIncoming) (processed bool, err error) {
	<-ctx.Done()
	return MessageNotProcessed, ctx.Err()
}

type tbWriter struct {
	tb testing.TB
}

func (w *tbWriter) Write(p []byte) (n int, err error) {
	w.tb.Log(string(p))
	return len(p), nil
}
