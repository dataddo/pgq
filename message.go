package pgq

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// Metadata is message metadata definition.
type Metadata map[string]string

// Message is an item retrieved from table queue in Postgres
type Message interface {
	// Metadata returns metadata of the message.
	Metadata() Metadata
	// Payload returns payload of the message.
	Payload() json.RawMessage
}

// NewMessage creates new message that satisfies Message interface.
func NewMessage(meta Metadata, payload json.RawMessage) Message {
	return &message{
		metadata: meta,
		payload:  payload,
	}
}

type message struct {
	// ID is an unique identifier of message
	id uuid.UUID
	// Metadata contains the message metadata.
	metadata Metadata
	// Payload is the message's payload.
	payload json.RawMessage
	// once ensures that the message will be finished only once. It's easier than
	// complicated SQL queries.
	once      sync.Once
	ackFn     func(ctx context.Context) error
	nackFn    func(context.Context, string) error
	discardFn func(context.Context, string) error
}

// Metadata returns metadata of the message.
// Metadata is the key/value map being stored as a json object in the db table.
// You may use it for passing any description data relevant to your message like labels etc.
func (m *message) Metadata() Metadata { return m.metadata }

// Payload returns payload of the message.
func (m *message) Payload() json.RawMessage { return m.payload }

// errMessageAlreadyFinished is error that is returned if message is being
// finished for second time. Example: when trying to ack after the nack has been already called.
var errMessageAlreadyFinished = errors.New("message already finished")

// ack positively acknowledges the message, and the message is marked as processed or removed from the queue
// what happens to after ack depends on the CleanupStrategy.
func (m *message) ack(ctx context.Context) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.ackFn(ctx)
	})
	return err
}

// nack does not the negative acknowledge of the message.
// The message is returned to the queue after nack and may be processed again.
func (m *message) nack(ctx context.Context, reason string) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.nackFn(ctx, reason)
	})
	return err
}

// discard removes the message from the queue completely
func (m *message) discard(ctx context.Context, reason string) error {
	err := errMessageAlreadyFinished
	m.once.Do(func() {
		err = m.discardFn(ctx, reason)
	})
	return err
}
