package sqlschema

import (
	"fmt"

	"go.dataddo.com/pgq/internal/pg"
)

// Queue is a schema for a queue table
type Queue struct {
	Name string
}

// CreateQuery returns a query for creating a queue table
func (q *Queue) CreateQuery() string {
	quotedTableName := pg.QuoteIdentifier(q.Name)
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

// DropQuery returns a query for dropping a queue table
func (q *Queue) DropQuery() string {
	quotedTableName := pg.QuoteIdentifier(q.Name)
	return `DROP TABLE IF EXISTS ` + quotedTableName
}
