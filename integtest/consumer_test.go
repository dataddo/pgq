package integtest

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
		&MessageOutgoing{Metadata: Metadata{
			"foo":               "bar",
			"pgq:lock_duration": "00:15:00", // 15 minutes
		}, Payload: json.RawMessage(`{"foo":"bar"}`)},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgIDs))
	msgID := msgIDs[0]

	logger := slog.New(slog.NewTextHandler(&tbWriter{tb: t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler := &slowHandler{
		log: logger,
	}
	consumer, err := NewConsumer(db, queueName, handler,
		WithLogger(logger),
		WithLockDuration(time.Hour),
		WithMessageProcessingReserveDuration(time.Second),
		WithAckTimeout(time.Second),
		WithPollingInterval(time.Second),
		WithMaxParallelMessages(1),
		WithInvalidMessageCallback(func(_ context.Context, _ InvalidMessage, err error) {
			require.NoError(t, err)
		}),
		WithMetrics(noop.Meter{}),
	)
	require.NoError(t, err)

	consumeCtx, consumeCancel := context.WithTimeoutCause(ctx, 10*time.Second,
		errors.New("consumer.Run timeout"),
	)
	defer consumeCancel()
	err = consumer.Run(consumeCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	err = db.Close()
	require.NoError(t, err)

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

type slowHandler struct {
	log *slog.Logger
}

func (s *slowHandler) HandleMessage(ctx context.Context, _ *MessageIncoming) (processed bool, err error) {
	defer func(now time.Time) {
		s.log.InfoContext(ctx, "slowHandler.HandleMessage finished",
			"processed", processed,
			"err", err,
			"duration", time.Since(now),
		)
	}(time.Now())
	if deadline, ok := ctx.Deadline(); ok {
		s.log.InfoContext(ctx, "slowHandler.HandleMessage deadline", "deadline", deadline)
	}
	<-ctx.Done()
	return MessageNotProcessed, context.Cause(ctx)
}

type tbWriter struct {
	tb testing.TB
}

func (w *tbWriter) Write(p []byte) (n int, err error) {
	w.tb.Log(string(p))
	return len(p), nil
}
