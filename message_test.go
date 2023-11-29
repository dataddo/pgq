package pgq

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestMessageIncoming_LastAttempt(t *testing.T) {
	type fields struct {
		id               uuid.UUID
		Metadata         Metadata
		Payload          json.RawMessage
		Attempt          int
		maxConsumedCount uint
		once             sync.Once
		ackFn            func(ctx context.Context) error
		nackFn           func(context.Context, string) error
		discardFn        func(context.Context, string) error
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "maxConsumedCount is 0",
			fields: fields{
				maxConsumedCount: 0,
			},
			want: false,
		},
		{
			name: "Attempt is less than maxConsumedCount",
			fields: fields{
				Attempt:          1,
				maxConsumedCount: 2,
			},
			want: false,
		},
		{
			name: "Attempt is equal to maxConsumedCount",
			fields: fields{
				Attempt:          2,
				maxConsumedCount: 2,
			},
			want: true,
		},
		{
			name: "Attempt is greater than maxConsumedCount",
			fields: fields{
				Attempt:          3,
				maxConsumedCount: 2,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MessageIncoming{
				id:               tt.fields.id,
				Metadata:         tt.fields.Metadata,
				Payload:          tt.fields.Payload,
				Attempt:          tt.fields.Attempt,
				maxConsumedCount: tt.fields.maxConsumedCount,
				once:             tt.fields.once,
				ackFn:            tt.fields.ackFn,
				nackFn:           tt.fields.nackFn,
				discardFn:        tt.fields.discardFn,
			}
			if got := m.LastAttempt(); got != tt.want {
				t.Errorf("LastAttempt() = %v, want %v", got, tt.want)
			}
		})
	}
}
