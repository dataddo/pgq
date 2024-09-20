package integtest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgtype"

	"go.dataddo.com/pgq"
	pgutils "go.dataddo.com/pgq/internal/pg"
	"go.dataddo.com/pgq/internal/require"
	"go.dataddo.com/pgq/x/schema"
)

func TestPublisher(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	ctx := context.Background()

	type want struct {
		metadata pgtype.JSONB
		payload  pgtype.JSONB
	}
	tests := []struct {
		name          string
		msg           *pgq.MessageOutgoing
		publisherOpts []pgq.PublisherOption
		want          want
		wantErr       bool
	}{
		{
			name: "Select extra columns",
			msg: &pgq.MessageOutgoing{
				Metadata: pgq.Metadata{
					"test": "test_value",
				},
				Payload: json.RawMessage(`{"foo":"bar"}`),
			},
			publisherOpts: []pgq.PublisherOption{
				pgq.WithMetaInjectors(
					pgq.StaticMetaInjector(pgq.Metadata{"host": "localhost"}),
				),
			},
			want: want{
				metadata: pgtype.JSONB{Bytes: []byte(`{"host": "localhost", "test": "test_value"}`), Status: pgtype.Present},
				payload:  pgtype.JSONB{Bytes: []byte(`{"foo": "bar"}`), Status: pgtype.Present},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openDB(t)
			t.Cleanup(func() {
				db.Close()
			})
			queueName := t.Name()
			_, _ = db.Exec(ctx, schema.GenerateDropTableQuery(queueName))
			_, err := db.Exec(ctx, schema.GenerateCreateTableQuery(queueName))
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := db.Exec(ctx, schema.GenerateDropTableQuery(queueName))
				require.NoError(t, err)
			})
			d := pgq.NewPublisher(db, tt.publisherOpts...)
			msgIDs, err := d.Publish(ctx, queueName, tt.msg)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.Equal(t, 1, len(msgIDs))
			require.NoError(t, err)
			row := db.QueryRow(ctx,
				fmt.Sprintf(
					"SELECT id, metadata, payload FROM %s WHERE id = $1",
					pgutils.QuoteIdentifier(queueName),
				),
				msgIDs[0],
			)
			var (
				id       pgtype.UUID
				metadata pgtype.JSONB
				payload  pgtype.JSONB
			)
			err = row.Scan(&id, &metadata, &payload)
			require.NoError(t, err)
			require.Equal(t, [16]byte(msgIDs[0]), id.Bytes)
			require.Equal(t, tt.want.metadata.Status, metadata.Status)
			require.Equal(t, string(tt.want.metadata.Bytes), string(metadata.Bytes))
			require.Equal(t, tt.want.payload.Status, payload.Status)
			require.Equal(t, string(tt.want.payload.Bytes), string(payload.Bytes))
		})
	}
}
