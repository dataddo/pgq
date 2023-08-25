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
	"go.dataddo.com/pgq/internal/sqlschema"
)

func TestConsumer_Run_graceful_shutdown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	db := openDB(t)
	queueName := t.Name()
	queueSchema := sqlschema.Queue{Name: queueName}
	_, _ = db.ExecContext(ctx, queueSchema.DropQuery())
	_, err := db.ExecContext(ctx, queueSchema.CreateQuery())
	t.Cleanup(func() {
		db := openDB(t)
		_, err := db.ExecContext(ctx, queueSchema.DropQuery())
		require.NoError(t, err)
		err = db.Close()
		require.NoError(t, err)
	})
	require.NoError(t, err)
	publisher := NewPublisher(db)
	msgIDs, err := publisher.Publish(ctx, queueName,
		NewMessage(Metadata{"foo": "bar"}, json.RawMessage(`{"foo":"bar"}`)),
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(msgIDs))
	msgID := msgIDs[0]

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

	query := fmt.Sprintf(
		`SELECT locked_until, consumed_count FROM %s WHERE id = $1`,
		pgutils.QuoteIdentifier(queueName),
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
	return db
}

type slowHandler struct{}

func (s *slowHandler) HandleMessage(ctx context.Context, _ Message) (processed bool, err error) {
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
