package pgq_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"go.dataddo.com/pgq"
	"go.opentelemetry.io/otel/metric/noop"
)

var db *sql.DB

func ExampleNewConsumer() {
	slogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, err := pgq.NewConsumer(db, "queue_name", &Handler{},
		pgq.WithLockDuration(10*time.Minute),
		pgq.WithPollingInterval(500*time.Millisecond),
		pgq.WithAckTimeout(5*time.Second),
		pgq.WithMaxParallelMessages(42),
		pgq.WithMetrics(noop.Meter{}),
		pgq.WithHistoryLimit(24*time.Hour),
		pgq.WithLogger(slogger),
		pgq.WithInvalidMessageCallback(func(ctx context.Context, msg pgq.InvalidMessage, err error) {
			// message payload and/or metadata are not JSON object.
			// The message will be discarded.
			slogger.Warn("invalid message",
				"error", err,
				"msg.id", msg.ID,
			)
		}),
	)
	_, _ = c, err
}

func ExampleNewPublisher() {
	hostname, _ := os.Hostname()
	p := pgq.NewPublisher(db,
		pgq.WithMetaInjectors(
			pgq.StaticMetaInjector(pgq.Metadata{"publisher-id": hostname}),
		),
	)
	_ = p
}
