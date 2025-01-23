package pgq

import (
	"context"
	"testing"
	"time"

	"go.dataddo.com/pgq/internal/require"
)

func TestMessageIncoming_LastAttempt(t *testing.T) {
	type fields struct {
		Attempt          int
		maxConsumedCount uint
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
				Attempt:          tt.fields.Attempt,
				maxConsumedCount: tt.fields.maxConsumedCount,
			}
			if got := m.LastAttempt(); got != tt.want {
				t.Errorf("LastAttempt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMessageIncoming_SetDeadline(t *testing.T) {
	m := &MessageIncoming{
		Deadline: time.Date(9999, 0, 0, 0, 0, 0, 0, time.UTC),
		updateLockedUntilFn: func(ctx context.Context, t time.Time) error {
			return nil
		},
	}
	ctx := context.Background()
	ctx, err := m.SetDeadline(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	ctx, err = m.SetDeadline(ctx, time.Now())
	require.NoError(t, err)
	ctx, err = m.SetDeadline(ctx, time.Now())
	require.Error(t, err)
}
