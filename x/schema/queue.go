// Package schema is a place where to put general functions and constants relevant to the postgres table schema and pgq setup
package schema

import (
	"fmt"

	"go.dataddo.com/pgq/internal/pg"
)

// This map will defined all the required fields that the queue should have as well as the type of each one.
// It will be used the queue schema validation
var Fields = map[string]string{
	"id":             "UUID",
	"created_at":     "TIMESTAMPTZ",
	"started_at":     "TIMESTAMPTZ",
	"locked_until":   "TIMESTAMPTZ",
	"processed_at":   "TIMESTAMPTZ",
	"consumed_count": "INTEGER",
	"error_detail":   "TEXT",
	"payload":        "JSONB",
	"metadata":       "JSONB",
}

// GenerateCreateTableQuery returns the query for creating the queue table
func GenerateCreateTableQuery(queueName string) string {
	quotedTableName := pg.QuoteIdentifier(queueName)
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s
	(
		id             UUID        DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
		created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
		started_at     TIMESTAMPTZ                           NULL,
		locked_until   TIMESTAMPTZ                           NULL,
		processed_at   TIMESTAMPTZ                           NULL,
		consumed_count INTEGER     DEFAULT 0                 NOT NULL,
		error_detail   TEXT                                  NULL,
		payload        JSONB                                 NOT NULL,
		metadata       JSONB                                 NOT NULL
	);
	CREATE INDEX IF NOT EXISTS "%[2]s_created_at_idx" ON %[1]s (created_at);
	CREATE INDEX IF NOT EXISTS "%[2]s_processed_at_null_idx" ON %[1]s (processed_at) WHERE (processed_at IS NULL);
	`, quotedTableName, quotedTableName[1:len(quotedTableName)-1])
}

// GenerateDropTableQuery returns a postgres query for dropping the queue table
func GenerateDropTableQuery(queueName string) string {
	quotedTableName := pg.QuoteIdentifier(queueName)
	return `DROP TABLE IF EXISTS ` + quotedTableName
}
