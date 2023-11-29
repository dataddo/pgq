package pgq

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Metadata is message Metadata definition.
type Metadata map[string]string

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
	// Metadata contains the message Metadata.
	Metadata Metadata
	// Payload is the message's Payload.
	Payload json.RawMessage
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
