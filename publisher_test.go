package pgq

import (
	"context"
	"encoding/json"
	"testing"

	"go.dataddo.com/pgq/internal/require"
)

func Test_buildInsertQuery(t *testing.T) {
	type args struct {
		queue    string
		msgCount int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "single message",
			args: args{
				queue:    "queue",
				msgCount: 1,
			},
			want: `INSERT INTO "queue" (payload, metadata) VALUES ($1,$2) RETURNING "id"`,
		},
		{
			name: "multiple messages",
			args: args{
				queue:    "queue",
				msgCount: 3,
			},
			want: `INSERT INTO "queue" (payload, metadata) VALUES ($1,$2),($3,$4),($5,$6) RETURNING "id"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildInsertQuery(tt.args.queue, tt.args.msgCount); got != tt.want {
				t.Errorf("buildInsertQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_buildArgs(t *testing.T) {
	type args struct {
		ctx  context.Context
		msgs []Message
	}
	tests := []struct {
		name string
		args args
		want []any
	}{
		{
			name: "",
			args: args{
				ctx: context.Background(),
				msgs: []Message{
					NewMessage(Metadata{}, nil),
				},
			},
			want: []any{
				json.RawMessage(nil),
				Metadata{
					"foo": "bar",
				},
			},
		},
		{
			name: "",
			args: args{
				ctx: context.Background(),
				msgs: []Message{
					NewMessage(Metadata{}, nil),
				},
			},
			want: []any{
				json.RawMessage(nil),
				Metadata{
					"foo": "bar",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPublisher(nil, WithMetaInjectors(
				StaticMetaInjector(Metadata{"foo": "bar"}),
			))
			got := p.(*publisher).buildArgs(tt.args.ctx, tt.args.msgs)
			require.Equal(t, tt.want, got)
		})
	}
}
