package pgq

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Metadata is message Metadata definition.
type Metadata map[string]string

var (
	fieldCountPerMessageOutgoing int
	dbFieldsPerMessageOutgoing   []string
	dbFieldsString               string
)

func init() {
	var err error

	// Count fields in MessageOutgoing struct. Used for dynamically building the queries
	t := reflect.TypeOf(MessageOutgoing{})
	fieldCountPerMessageOutgoing = t.NumField()

	// Get field names in MessageOutgoing struct. Used for dynamically building the queries
	dbFieldsPerMessageOutgoing, err = buildColumnListFromTags(MessageOutgoing{})
	if err != nil {
		panic(err)
	}

	dbFieldsString = strings.Join(dbFieldsPerMessageOutgoing, ", ")
}

// NewMessage creates new message that satisfies Message interface.
func NewMessage(meta Metadata, payload json.RawMessage, attempt int, maxConsumedCount uint) *MessageIncoming {
	return &MessageIncoming{
		Metadata:         meta,
		Payload:          payload,
		Attempt:          attempt,
		maxConsumedCount: maxConsumedCount,
	}
}

// MessageOutgoing is a record to be inserted into table queue in Postgres
type MessageOutgoing struct {
	// ScheduledFor is the time when the message should be processed. If nil, the messages
	// gets processed immediately.
	ScheduledFor *time.Time `db:"scheduled_for"`
	// Payload is the message's Payload.
	Payload json.RawMessage `db:"payload"`
	// Metadata contains the message Metadata.
	Metadata Metadata `db:"metadata"`
}

// MessageIncoming is a record retrieved from table queue in Postgres
type MessageIncoming struct {
	// id is an unique identifier of message
	id uuid.UUID
	// Metadata contains the message Metadata.
	Metadata Metadata
	// Payload is the message's Payload.
	Payload json.RawMessage
	// Attempt number, counts from 1. It is incremented every time the message is
	// consumed.
	Attempt int
	// Deadline is the time when the message will be returned to the queue if not
	// finished. It is set by the queue when the message is consumed.
	Deadline time.Time

	maxConsumedCount uint
	// once ensures that the message will be finished only once. It's easier than
	// complicated SQL queries.
	once      sync.Once
	ackFn     func(ctx context.Context) error
	nackFn    func(context.Context, string) error
	discardFn func(context.Context, string) error
}

// LastAttempt returns true if the message is consumed for the last time
// according to maxConsumedCount settings. If the Consumer is not configured to
// limit the number of attempts setting WithMaxConsumeCount to zero, it always
// returns false.
func (m *MessageIncoming) LastAttempt() bool {
	if m.maxConsumedCount == 0 {
		return false
	}
	return m.Attempt >= int(m.maxConsumedCount)
}

// errMessageAlreadyFinished is error that is returned if message is being
// finished for second time. Example: when trying to ack after the nack has been already called.
var errMessageAlreadyFinished = errors.New("message already finished")

// ack positively acknowledges the message, and the message is marked as processed.
func (m *MessageIncoming) ack(ctx context.Context) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.ackFn(ctx)
	})
	return err
}

// nack does not the negative acknowledge of the message. The message is
// returned to the queue after nack and may be processed again.
func (m *MessageIncoming) nack(ctx context.Context, reason string) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.nackFn(ctx, reason)
	})
	return err
}

// discard removes the message from the queue completely. It's like ack, but it
// also records the reason why the message was discarded.
func (m *MessageIncoming) discard(ctx context.Context, reason string) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.discardFn(ctx, reason)
	})
	return err
}

// buildColumnListFromTags dynamically constructs a list of column names based on the `db` struct tags
// of any given struct. It returns a slice of strings containing the column names.
func buildColumnListFromTags(data interface{}) ([]string, error) {
	// Ensure that 'data' is a struct
	t := reflect.TypeOf(data)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("provided argument is not a struct")
	}

	var columns []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		dbTag := field.Tag.Get("db") // Get the value of the `db` tag
		if dbTag != "" {
			columns = append(columns, dbTag)
		}
	}

	return columns, nil
}
