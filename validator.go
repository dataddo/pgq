package pgq

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

const columnSelect = `SELECT column_name
FROM information_schema.columns
WHERE table_catalog = CURRENT_CATALOG
  AND table_schema = CURRENT_SCHEMA
  AND table_name = $1
ORDER BY ordinal_position
`

const indexSelect = `
SELECT
    COUNT(DISTINCT a.attname) >= 2 AS index_exists
FROM
    pg_class t
JOIN
    pg_index ix ON t.oid = ix.indrelid
JOIN
    pg_class i ON i.oid = ix.indexrelid
JOIN
    pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE
    t.relkind IN ('r', 'p')
    AND t.relname = $1
    AND a.attname = ANY($2)
    AND ix.indisvalid;
`

var mandatoryFields = []string{
	"id",
	"locked_until",
	"processed_at",
	"consumed_count",
	"started_at",
	"payload",
	"metadata",
}

// A list of all the indexes that the queue should have. Each slice entrance is a slice itself
// that contains the fields that are used to create each index.
var mandatoryIndexes = []string{
	"created_at",
	"processed_at",
}

// ValidateFields checks if required fields exist
func ValidateFields(ctx context.Context, db *sqlx.DB, queueName string) error {
	// --- (1) ----
	// Recover the columns that the queue has
	columns, err := getColumnData(ctx, db, queueName)
	if err != nil {
		return err
	}

	// --- (2) ----
	// Run through each one of the recovered columns and validate if all the mandatory ones are included
	var missingColumns []string
	for _, mandatoryField := range mandatoryFields {
		if _, ok := columns[mandatoryField]; !ok {
			missingColumns = append(missingColumns, mandatoryField)
		}
		delete(columns, mandatoryField)
	}

	// If all the mandatory fields have been found then we don't need to return an error. However,
	// if there is at least one mandatory field missing in the schema then this queue is invalid.
	// TODO: Add some more logic to maybe indicate which field is the one that need to be included
	if len(missingColumns) > 1 {
		return errors.Errorf("some PGQ columns are missing: %v", missingColumns)
	}

	// TODO log extra columns in queue table or ignore them?
	// extraColumns := make([]string, 0, len(columns))
	// for k := range columns {
	//	extraColumns = append(extraColumns, k)
	// }
	// _ = extraColumns

	return nil
}

// ValidateIndexes checks if required indexes exist
func ValidateIndexes(ctx context.Context, db *sqlx.DB, queueName string) error {
	found, err := checkIndexData(ctx, db, queueName)
	if err != nil {
		return err
	}

	// Check if we found all the mandatory indexes were found. If even 1 is missing, then we return an error
	if !found {
		return errors.Errorf("some PGQ indexes are missing or invalid")
	}
	return nil
}

func getColumnData(ctx context.Context, db *sqlx.DB, queueName string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, columnSelect, queueName)
	if err != nil {
		return nil, errors.Wrap(err, "querying schema of queue table")
	}
	defer func() { _ = rows.Close() }()

	columns := make(map[string]struct{})
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, errors.Wrap(err, "reading schema row of queue table")
		}
		columns[s] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "reading schema of queue table")
	}
	return columns, nil
}

func checkIndexData(ctx context.Context, db *sqlx.DB, queueName string) (bool, error) {
	rows, err := db.QueryContext(ctx, indexSelect, queueName, mandatoryIndexes)
	if err != nil {
		return false, errors.Wrap(err, "querying index schema of queue table")
	}
	defer func() { _ = rows.Close() }()

	var allMandatoryColumnsAreIndexed bool
	for rows.Next() {
		if err := rows.Scan(&allMandatoryColumnsAreIndexed); err != nil {
			return false, errors.Wrap(err, "reading index schema row of queue table")
		}
	}
	if err := rows.Err(); err != nil {
		return false, errors.Wrap(err, "reading index schema of queue table")
	}
	if err := rows.Close(); err != nil {
		return false, errors.Wrap(err, "closing index schema query of queue table")
	}
	return allMandatoryColumnsAreIndexed, nil
}
