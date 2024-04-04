package pgq

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/semaphore"

	"go.dataddo.com/pgq/internal/pg"
	"go.dataddo.com/pgq/internal/query"
)

type fatalError struct {
	Err error
}

func (e fatalError) Error() string {
	return fmt.Sprintf("pgq consumer fatal error: %v", e.Err)
}

func (e fatalError) Unwrap() error { return e.Err }

const (
	// MessageProcessed signals that message was processed and shouldn't be processed
	// again. If processed with an error, it is expected permanent, and new run would
	// result in the same error.
	MessageProcessed = true
	// MessageNotProcessed signals that message wasn't processed and can be processed
	// again. The error interrupting processing is considered temporary.
	MessageNotProcessed = false
)

// MessageHandler handles message received from queue. Returning false means
// message wasn't processed correctly and shouldn't be acknowledged. Error
// contains additional information.
//
// Possible combinations:
//
//	// | processed | err        | description                                          |
//	// | --------- | ---------- | ---------------------------------------------------- |
//	// | false     | <nil>      | missing failure info, but the message can be retried |
//	// | false     | some error | not processed, because of some error, can be retried |
//	// | true      | <nil>      | processed, no error.                                 |
//	// | true      | some error | processed, ended with error. Don't retry!            |
type MessageHandler interface {
	HandleMessage(context.Context, *MessageIncoming) (processed bool, err error)
}

// MessageHandlerFunc is MessageHandler implementation by simple function.
type MessageHandlerFunc func(context.Context, *MessageIncoming) (processed bool, err error)

// HandleMessage calls self. It also implements MessageHandler interface.
func (fn MessageHandlerFunc) HandleMessage(ctx context.Context, msg *MessageIncoming) (processed bool, err error) {
	return fn(ctx, msg)
}

// consumerConfig contains consumer configuration.
type consumerConfig struct {
	// LockDuration is the maximal duration for how long the message remains locked for other consumers.
	LockDuration time.Duration
	// PollingInterval defines how frequently consumer checks the queue for new messages.
	PollingInterval time.Duration
	// AckTimeout is the timeout for updating the message status when message is processed.
	AckTimeout time.Duration
	// MaxParallelMessages sets how many jobs can single consumer process simultaneously.
	MaxParallelMessages int
	// Metrics define prometheus parameters.
	Metrics metric.Meter
	// InvalidMessageCallback defines what should happen to messages which are identified as invalid.
	// Such messages usually have missing or malformed required fields.
	InvalidMessageCallback InvalidMessageCallback
	// HistoryLimit means how long in history you want to search for unprocessed messages
	// If not set, it will look for message in the whole table.
	// You may set this value when using partitioned table to search just in partitions you are interested in
	HistoryLimit time.Duration
	// MaxConsumeCount is the maximal number of times a message can be consumed before it is ignored.
	// This is a safety mechanism to prevent failure infinite loops when a message causes unhandled panic, OOM etc.
	MaxConsumeCount uint

	// MsgProcessingReserveDuration is the duration for which the message is reserved for handling result state.
	MessageProcessingReserveDuration time.Duration

	MetadataFilters []MetadataFilter

	Logger *slog.Logger
}

var noopLogger = slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(math.MaxInt)}))

var defaultConsumerConfig = consumerConfig{
	LockDuration:                     time.Hour,
	PollingInterval:                  5 * time.Second,
	AckTimeout:                       1 * time.Second,
	MessageProcessingReserveDuration: 1 * time.Second,
	MaxParallelMessages:              1,
	InvalidMessageCallback:           func(context.Context, InvalidMessage, error) {},
	Metrics:                          noop.Meter{},
	MaxConsumeCount:                  3,
	Logger:                           noopLogger,
}

// InvalidMessageCallback defines what should happen to messages which are identified as invalid.
// Such messages usually have missing or malformed required fields.
type InvalidMessageCallback func(ctx context.Context, msg InvalidMessage, err error)

// InvalidMessage is definition of invalid message used as argument for InvalidMessageCallback by Consumer.
type InvalidMessage struct {
	ID       string
	Metadata json.RawMessage
	Payload  json.RawMessage
}

// Consumer is the preconfigured subscriber of the write input messages
type Consumer struct {
	db        *sqlx.DB
	queueName string
	cfg       consumerConfig
	handler   MessageHandler
	metrics   *metrics
	sem       *semaphore.Weighted
}

// ConsumerOption applies option to consumerConfig.
type ConsumerOption func(c *consumerConfig)

// WithLockDuration sets the maximal duration for how long the message remains
// locked for other consumers.
func WithLockDuration(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.LockDuration = d
	}
}

// WithPollingInterval sets how frequently consumer checks the queue for new
// messages.
func WithPollingInterval(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.PollingInterval = d
	}
}

// WithAckTimeout sets the timeout for updating the message status when message
// is processed.
func WithAckTimeout(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.AckTimeout = d
	}
}

// WithMaxParallelMessages sets how many jobs can single consumer process
// simultaneously.
func WithMaxParallelMessages(n int) ConsumerOption {
	return func(c *consumerConfig) {
		c.MaxParallelMessages = n
	}
}

// WithMetrics sets metrics meter. Default is noop.Meter{}.
func WithMetrics(m metric.Meter) ConsumerOption {
	return func(c *consumerConfig) {
		c.Metrics = m
	}
}

// WithMessageProcessingReserveDuration sets the duration for which the message is reserved for handling result state.
func WithMessageProcessingReserveDuration(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.MessageProcessingReserveDuration = d
	}
}

// WithInvalidMessageCallback sets callback for invalid messages.
func WithInvalidMessageCallback(fn InvalidMessageCallback) ConsumerOption {
	return func(c *consumerConfig) {
		c.InvalidMessageCallback = fn
	}
}

// WithHistoryLimit sets how long in history you want to search for unprocessed
// messages (default is no limit). If not set, it will look for message in the
// whole table. You may set this value when using partitioned table to search
// just in partitions you are interested in.
func WithHistoryLimit(d time.Duration) ConsumerOption {
	return func(c *consumerConfig) {
		c.HistoryLimit = d
	}
}

// WithMaxConsumeCount sets the maximal number of times a message can be consumed before it is ignored.
// Unhandled SIGKILL or uncaught panic, OOM error etc. could lead to consumer failure infinite loop.
// Setting this value to greater than 0 will prevent happening this loop.
// Setting this to value 0 disableS this safe mechanism.
func WithMaxConsumeCount(max uint) ConsumerOption {
	return func(c *consumerConfig) {
		c.MaxConsumeCount = max
	}
}

// WithLogger sets logger. Default is no logging.
func WithLogger(logger *slog.Logger) ConsumerOption {
	return func(c *consumerConfig) {
		c.Logger = logger
	}
}

// MetadataFilter is a filter for metadata. Right now support only direct matching of key/value
type (
	MetadataOperation string

	MetadataFilter struct {
		Key       string
		Operation MetadataOperation
		Value     string
	}
)

const (
	OpEqual    MetadataOperation = "="
	OpNotEqual MetadataOperation = "<>"
)

func WithMetadataFilter(filter *MetadataFilter) ConsumerOption {
	return func(c *consumerConfig) {
		filters := c.MetadataFilters
		if filter == nil {
			filters = make([]MetadataFilter, 0, 1)
		}

		c.MetadataFilters = append(filters, *filter)
	}
}

// NewConsumer creates Consumer with proper settings
func NewConsumer(db *sqlx.DB, queueName string, handler MessageHandler, opts ...ConsumerOption) (*Consumer, error) {
	config := defaultConsumerConfig
	for _, opt := range opts {
		opt(&config)
	}
	metrics, err := prepareProcessMetric(queueName, config.Metrics)
	if err != nil {
		return nil, errors.Wrap(err, "registering metrics")
	}
	sem := semaphore.NewWeighted(int64(config.MaxParallelMessages))
	return &Consumer{
		db:        db,
		queueName: queueName,
		cfg:       config,
		handler:   handler,
		metrics:   metrics,
		sem:       sem,
	}, nil
}

type metrics struct {
	jobsCounter             metric.Int64Counter
	failedProcessingCounter metric.Int64Counter
}

func prepareProcessMetric(queueName string, meter metric.Meter) (*metrics, error) {
	queueName = strings.ReplaceAll(queueName, "/", "_")
	// '_total' suffix is added to all counters by default by OpenTelemetry.
	jobsCounter, err := meter.Int64Counter(
		fmt.Sprintf("pgq_%s_processed_jobs", queueName),
		metric.WithDescription("Total number of processed jobs. The label 'resolution' says how the job was handled."),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	failedProcessingCounter, err := meter.Int64Counter(
		fmt.Sprintf("pgq_%s_failed_job_processing", queueName),
		metric.WithDescription("Total number of errors during marking a job as processed. Example is a failed job ACK. This metric signals a chance of inconsistencies in the queue."),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &metrics{
		jobsCounter:             jobsCounter,
		failedProcessingCounter: failedProcessingCounter,
	}, nil
}

// Run consumes messages until the context isn't cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	c.cfg.Logger.InfoContext(ctx, "starting consumption...",
		"inputQueue", c.queueName,
	)
	if err := c.verifyTable(ctx); err != nil {
		return errors.Wrap(err, "verifying table")
	}
	query, err := c.generateQuery()
	if err != nil {
		return errors.Wrap(err, "generating query")
	}

	var wg sync.WaitGroup
	defer wg.Wait() // wait for handlers to finish
	for {
		msgs, err := c.consumeMessages(ctx, query)
		if err != nil {
			if errors.As(err, &fatalError{}) {
				return errors.Wrapf(err, "consuming from PostgreSQL queue %s", c.queueName)
			}
			c.cfg.Logger.InfoContext(ctx, "pgq: consume failed, will retry")
			continue
		}
		wg.Add(len(msgs))
		for _, msg := range msgs {
			go func(msg *MessageIncoming) {
				defer wg.Done()
				defer c.sem.Release(1)
				c.handleMessage(ctx, msg)
			}(msg)
		}
	}
}

func (c *Consumer) verifyTable(ctx context.Context) error {
	// --- (1) ----
	// Validate the queue mandatory fields
	err := ValidateFields(ctx, c.db, c.queueName)
	if err != nil {
		return errors.Wrap(err, "error validating queue mandatory fields")
	}

	// --- (2) ----
	// Validate the queue mandatory indexes
	err = ValidateIndexes(ctx, c.db, c.queueName)
	if err != nil {
		return errors.Wrap(err, "error validating queue mandatory indexes")
	}

	return nil
}

func (c *Consumer) generateQuery() (*query.Builder, error) {
	qb := query.NewBuilder()

	qb.WriteString(`UPDATE `)
	qb.WriteString(pg.QuoteIdentifier(c.queueName))
	qb.WriteString(` SET locked_until = :locked_until`)
	qb.WriteString(`, started_at = CURRENT_TIMESTAMP`)
	qb.WriteString(`, consumed_count = consumed_count+1`)
	qb.WriteString(` WHERE id IN (`)
	{
		qb.WriteString(`SELECT id FROM `)
		qb.WriteString(pg.QuoteIdentifier(c.queueName))
		qb.WriteString(` WHERE`)
		if c.cfg.HistoryLimit > 0 {
			qb.WriteString(` created_at >= CURRENT_TIMESTAMP - (:history_limit)::interval AND`)
			qb.WriteString(` created_at < CURRENT_TIMESTAMP AND`)
		}
		qb.WriteString(` (locked_until IS NULL OR locked_until < CURRENT_TIMESTAMP)`)
		if c.cfg.MaxConsumeCount > 0 {
			qb.WriteString(` AND consumed_count < :max_consume_count`)
		}

		if c.cfg.MetadataFilters != nil && len(c.cfg.MetadataFilters) > 0 {
			for i, filter := range c.cfg.MetadataFilters {
				if len(filter.Operation) == 0 {
					return nil, fatalError{Err: fmt.Errorf("metadata filter operation is empty")}
				}

				qb.WriteString(fmt.Sprintf(" AND metadata->>:metadata_key_%d %s :metadata_value_%d", i, filter.Operation, i))
			}
		}

		qb.WriteString(` AND processed_at IS NULL`)
		qb.WriteString(` ORDER BY consumed_count ASC, created_at ASC`)
		qb.WriteString(` LIMIT :limit`)
		qb.WriteString(` FOR UPDATE SKIP LOCKED`)
	}
	qb.WriteString(`) RETURNING id, payload, metadata, consumed_count, locked_until`)

	return qb, nil
}

func (c *Consumer) handleMessage(ctx context.Context, msg *MessageIncoming) {
	ctx, cancel := context.WithDeadline(ctx, msg.Deadline)
	defer cancel()

	ctxTimeout, cancel := prepareCtxTimeout()
	defer cancel()
	// TODO configurable Propagator
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(msg.Metadata)
	ctx = propagator.Extract(ctx, carrier)

	ctx, span := otel.Tracer("pgq").Start(ctx, "HandleMessage")
	defer span.End()
	span.SetAttributes(
		attribute.String("messageId", msg.id.String()),
		attribute.String("queueName", c.queueName),
	)

	processed, err := c.handler.HandleMessage(ctx, msg)
	if !processed {
		reason := "unknown"
		if err != nil {
			span.RecordError(err)
			reason = err.Error()
		}
		span.SetStatus(codes.Ok, "Message Nacked")
		if err := msg.nack(ctxTimeout(c.cfg.AckTimeout), reason); err != nil {
			c.cfg.Logger.ErrorContext(ctx, "pgq: nack failed",
				"error", err.Error(),
				"ackTimeout", c.cfg.AckTimeout,
				"reason", reason,
				"msg.metadata", msg.Metadata,
			)
		}
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Message Discarded")
		discardReason := err.Error()
		if err := msg.discard(ctxTimeout(c.cfg.AckTimeout), discardReason); err != nil {
			c.cfg.Logger.ErrorContext(ctx, "pgq: discard failed",
				"error", err,
				"ackTimeout", c.cfg.AckTimeout,
				"reason", discardReason,
				"msg.metadata", msg.Metadata,
			)
		}
		return
	}
	span.SetStatus(codes.Ok, "Message Acked")
	if err := msg.ack(ctxTimeout(c.cfg.AckTimeout)); err != nil {
		c.cfg.Logger.ErrorContext(ctx, "pgq: ack failed",
			"error", err,
			"ackTimeout", c.cfg.AckTimeout,
			"msg.metadata", msg.Metadata,
		)
	}
}

func prepareCtxTimeout() (func(td time.Duration) context.Context, context.CancelFunc) {
	parent, cancel := context.WithCancel(context.Background())
	fn := func(td time.Duration) context.Context {
		// ctx will be released by parent cancellation
		ctx, _ := context.WithTimeout(parent, td)
		return ctx
	}
	return fn, cancel
}

func (c *Consumer) consumeMessages(ctx context.Context, query *query.Builder) ([]*MessageIncoming, error) {
	for {
		maxMsg, err := acquireMaxFromSemaphore(ctx, c.sem, int64(c.cfg.MaxParallelMessages))
		if err != nil {
			return nil, fatalError{Err: errors.WithStack(err)}
		}
		msgs, err := c.tryConsumeMessages(ctx, query, maxMsg)
		if err != nil {
			c.sem.Release(maxMsg)
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, errors.WithStack(err)
			}
			select {
			case <-ctx.Done():
				return nil, fatalError{Err: ctx.Err()}
			case <-time.After(c.cfg.PollingInterval):
				continue
			}
		}
		// release unused resources
		c.sem.Release(maxMsg - int64(len(msgs)))
		return msgs, nil
	}
}

type pgMessage struct {
	ID          pgtype.UUID
	Payload     pgtype.JSONB
	Metadata    pgtype.JSONB
	Attempt     pgtype.Int4
	LockedUntil pgtype.Timestamptz
}

func (c *Consumer) tryConsumeMessages(ctx context.Context, query *query.Builder, limit int64) (_ []*MessageIncoming, err error) {
	tx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		// TODO not necessary fatal, network could wiggle.
		return nil, fatalError{Err: errors.WithStack(err)}
	}
	defer func() {
		txRollbackErr := tx.Rollback()
		if errors.Is(txRollbackErr, sql.ErrTxDone) {
			return
		}
		if txRollbackErr != nil {
			c.cfg.Logger.ErrorContext(ctx, "pgq: rollback failed",
				"error", txRollbackErr.Error(),
				"rollbackReason", err,
			)
			return
		}
	}()

	lockedUntil := time.Now().Add(c.cfg.LockDuration)
	namedParams := map[string]interface{}{
		"locked_until": lockedUntil,
		"limit":        limit,
	}

	if query.HasParam("history_limit") {
		var scanInterval pgtype.Interval
		// time.Duration doesn't ever fail
		_ = scanInterval.Set(c.cfg.HistoryLimit)

		namedParams["history_limit"] = scanInterval
	}

	if query.HasParam("max_consume_count") {
		namedParams["max_consume_count"] = c.cfg.MaxConsumeCount
	}

	if c.cfg.MetadataFilters != nil && len(c.cfg.MetadataFilters) > 0 {
		for i, filter := range c.cfg.MetadataFilters {
			namedParams[fmt.Sprintf("metadata_key_%d", i)] = filter.Key
			namedParams[fmt.Sprintf("metadata_value_%d", i)] = filter.Value
		}
	}

	queryString, err := query.Build(namedParams)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	rows, err := sqlx.NamedQueryContext(ctx, tx, queryString, namedParams)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()

	var msgs []*MessageIncoming
	for rows.Next() {
		msg, err := c.parseRow(ctx, rows)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	if len(msgs) == 0 {
		return nil, sql.ErrNoRows
	}
	if err := tx.Commit(); err != nil {
		return nil, errors.Wrap(err, "commit message consumption")
	}
	return msgs, nil
}

func (c *Consumer) parseRow(ctx context.Context, rows *sqlx.Rows) (*MessageIncoming, error) {
	var pgMsg pgMessage
	if err := rows.Scan(
		&pgMsg.ID,
		&pgMsg.Payload,
		&pgMsg.Metadata,
		&pgMsg.Attempt,
		&pgMsg.LockedUntil,
	); err != nil {
		if isErrorCode(err, undefinedTableErrCode, undefinedColumnErrCode) {
			return nil, fatalError{Err: err}
		}
		return nil, errors.Wrap(err, "retrieving message")
	}
	msg, err := c.finishParsing(pgMsg)
	if err != nil {
		c.cfg.Logger.ErrorContext(ctx, "reading message", c.logFields(pgMsg, err)...)
		c.discardInvalidMsg(ctx, pgMsg.ID, err)
		go c.cfg.InvalidMessageCallback(ctx, InvalidMessage{
			ID:       uuid.UUID(pgMsg.ID.Bytes).String(),
			Metadata: pgMsg.Metadata.Bytes,
			Payload:  pgMsg.Payload.Bytes,
		}, err)
		return nil, errors.WithStack(err)
	}
	return msg, nil
}

func (c *Consumer) logFields(msg pgMessage, err error) []any {
	entry := []any{
		"msg.id", uuid.UUID(msg.ID.Bytes).String(),
		"msg.metadata", json.RawMessage(msg.Metadata.Bytes),
	}
	if err != nil {
		entry = append(entry, []any{
			"error", err,
			"msg.payload", json.RawMessage(msg.Payload.Bytes),
		})
	}
	return entry
}

func (c *Consumer) discardInvalidMsg(ctx context.Context, id pgtype.UUID, err error) {
	ctxTimeout, cancel := prepareCtxTimeout()
	defer cancel()
	reason := err.Error()
	if err := c.discardMessage(c.db, id)(ctxTimeout(c.cfg.AckTimeout), reason); err != nil {
		c.cfg.Logger.ErrorContext(ctx, "pgq: discard failed",
			"error", err,
			"msg.id", id,
			"ackTimeout", c.cfg.AckTimeout,
			"reason", reason,
		)
		return
	}
}

func (c *Consumer) finishParsing(pgMsg pgMessage) (*MessageIncoming, error) {
	msg := &MessageIncoming{
		id:        uuid.UUID(pgMsg.ID.Bytes),
		once:      sync.Once{},
		ackFn:     c.ackMessage(c.db, pgMsg.ID),
		nackFn:    c.nackMessage(c.db, pgMsg.ID),
		discardFn: c.discardMessage(c.db, pgMsg.ID),
	}
	var err error
	msg.Payload, err = parsePayload(pgMsg)
	if err != nil {
		return msg, errors.Wrap(err, "parsing payload")
	}
	msg.Metadata, err = parseMetadata(pgMsg)
	if err != nil {
		return msg, errors.Wrap(err, "parsing metadata")
	}
	msg.Attempt = int(pgMsg.Attempt.Int)
	msg.maxConsumedCount = c.cfg.MaxConsumeCount
	msg.Deadline = pgMsg.LockedUntil.Time.Add(-c.cfg.AckTimeout).Add(-c.cfg.MessageProcessingReserveDuration)
	return msg, nil
}

func parsePayload(pgMsg pgMessage) (json.RawMessage, error) {
	if pgMsg.Payload.Status != pgtype.Present {
		return nil, errors.New("missing message payload")
	}
	if !isJSONObject(pgMsg.Payload.Bytes) {
		return nil, errors.New("payload is invalid JSON object")
	}
	return pgMsg.Payload.Bytes, nil
}

func parseMetadata(pgMsg pgMessage) (map[string]string, error) {
	if pgMsg.Metadata.Status != pgtype.Present {
		return map[string]string{}, nil
	}
	var m map[string]string
	if err := json.Unmarshal(pgMsg.Metadata.Bytes, &m); err != nil {
		if !isJSONObject(pgMsg.Metadata.Bytes) {
			return nil, errors.New("metadata is invalid JSON object")
		}
		return nil, errors.Wrap(err, "parsing metadata")
	}
	return m, nil
}

func isJSONObject(b json.RawMessage) bool {
	if !json.Valid(b) {
		return false
	}
	// remove insignificant characters.
	b = bytes.TrimLeftFunc(b, unicode.IsSpace)
	return bytes.HasPrefix(b, []byte{'{'})
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (c *Consumer) ackMessage(exec execer, msgID pgtype.UUID) func(ctx context.Context) error {
	query := `UPDATE ` + pg.QuoteIdentifier(c.queueName) + ` SET locked_until = NULL, processed_at = CURRENT_TIMESTAMP WHERE id = $1`
	return func(ctx context.Context) error {
		if _, err := exec.ExecContext(ctx, query, msgID); err != nil {
			c.metrics.failedProcessingCounter.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("resolution", "ack"),
					attribute.String("queue_name", c.queueName),
				),
			)
			return errors.WithStack(err)
		}
		c.metrics.jobsCounter.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("resolution", "ack"),
				attribute.String("queue_name", c.queueName),
			),
		)
		return nil
	}
}

func (c *Consumer) nackMessage(exec execer, msgID pgtype.UUID) func(ctx context.Context, reason string) error {
	query := `UPDATE ` + pg.QuoteIdentifier(c.queueName) + ` SET locked_until = NULL, error_detail = $2 WHERE id = $1`
	return func(ctx context.Context, reason string) error {
		if _, err := exec.ExecContext(ctx, query, msgID, reason); err != nil {
			c.metrics.failedProcessingCounter.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("resolution", "nack"),
					attribute.String("queue_name", c.queueName),
				),
			)
			return errors.WithStack(err)
		}
		c.metrics.jobsCounter.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("resolution", "nack"),
				attribute.String("queue_name", c.queueName),
			),
		)
		return nil
	}
}

func (c *Consumer) discardMessage(exec execer, msgID pgtype.UUID) func(ctx context.Context, reason string) error {
	query := `UPDATE ` + pg.QuoteIdentifier(c.queueName) + ` SET locked_until = NULL, processed_at = CURRENT_TIMESTAMP, error_detail = $2 WHERE id = $1`
	return func(ctx context.Context, reason string) error {
		if _, err := exec.ExecContext(ctx, query, msgID, reason); err != nil {
			c.metrics.failedProcessingCounter.Add(ctx, 1,
				metric.WithAttributes(
					attribute.String("resolution", "discard"),
					attribute.String("queue_name", c.queueName),
				),
			)
			return errors.WithStack(err)
		}
		c.metrics.jobsCounter.Add(ctx, 1,
			metric.WithAttributes(
				attribute.String("resolution", "discard"),
				attribute.String("queue_name", c.queueName),
			),
		)
		return nil
	}
}

// acquireMaxFromSemaphore acquires maximum possible weight. It blocks until resources are
// available or ctx is done. On success, returns acquired weight. On failure,
// returns ctx.Err() and leaves the semaphore unchanged.
//
// If ctx is already done, Acquire may still succeed without blocking.
func acquireMaxFromSemaphore(ctx context.Context, w *semaphore.Weighted, size int64) (int64, error) {
	for i := size; i > 1; i-- {
		if ok := w.TryAcquire(i); ok {
			// Same practise like in underlying library.
			// Acquired the semaphore after we were canceled.  Rather than trying to
			// fix up the queue, just pretend we didn't notice the cancellation.
			return i, nil
		}
	}
	if err := w.Acquire(ctx, 1); err != nil {
		return 0, err
	}
	return 1, nil
}
