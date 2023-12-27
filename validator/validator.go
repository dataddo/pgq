package validator

import (
	"database/sql"
	"fmt"
	"strings"

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
    i.relname as index_name,
    array_to_string(array_agg(a.attname), ', ') as column_names
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
WHERE
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and t.relname = $1
GROUP BY
    t.relname,
    i.relname
ORDER BY
    t.relname,
    i.relname;
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
var mandatoryIndexes = [][]string{
	{"created_at"},
	{"processed_at"},
}

func ValidateFields(db *sql.DB, queueName string) error {
	// --- (1) ----
	// Recover the columns that the queue has
	columns, err := getColumnData(db, queueName)
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

// Validate if requiered indexes exist
func ValidateIndexes(db *sql.DB, queueName string) error {
	// --- (1) ----
	// Recover the indexes that the queue has
	indexes, err := getIndexData(db, queueName)
	if err != nil {
		return err
	}

	// --- (2) ----
	// Run through each one of the recovered indexes and see if the mandatory ones are included
	var matchedIndexes int
	for _, fields := range indexes {
		if isIndexFound(fields) {
			matchedIndexes++
		}
	}

	// Check if we found all the mandatory indexes were found. If even 1 is missing, then we return an error
	if matchedIndexes != len(mandatoryIndexes) {
		return errors.Errorf("some PGQ indexes are missing")
	}
	return nil
}

func getColumnData(db *sql.DB, queueName string) (map[string]struct{}, error) {
	rows, err := db.Query(columnSelect, queueName)
	if err != nil {
		return nil, errors.Wrap(err, "querying schema of queue table")
	}
	defer rows.Close()

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
	if err := rows.Close(); err != nil {
		return nil, errors.Wrap(err, "closing schema query of queue table")
	}
	if err != nil {
		return nil, fmt.Errorf("error recovering queue columns %v", err)
	}
	return columns, nil
}

func getIndexData(db *sql.DB, queueName string) (map[string][]string, error) {
	rows, err := db.Query(indexSelect, queueName)
	if err != nil {
		return nil, errors.Wrap(err, "querying index schema of queue table")
	}
	defer rows.Close()

	indexes := make(map[string][]string)
	for rows.Next() {
		var indexName, indexColumns string
		if err := rows.Scan(&indexName, &indexColumns); err != nil {
			return nil, errors.Wrap(err, "reading index schema row of queue table")
		}
		indexes[indexName] = strings.Split(indexColumns, ",")
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "reading index schema of queue table")
	}
	if err := rows.Close(); err != nil {
		return nil, errors.Wrap(err, "closing index schema query of queue table")
	}
	if err != nil {
		return nil, fmt.Errorf("error recovering queue indexes %v", err)
	}
	return indexes, nil
}

func isIndexFound(columns []string) bool {
	for _, mandatoryIndexColumns := range mandatoryIndexes {
		if unorderedEqual(columns, mandatoryIndexColumns) {
			return true
		}
	}
	return false
}

func unorderedEqual(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	exists := make(map[string]bool)
	for _, value := range first {
		exists[value] = true
	}
	for _, value := range second {
		if !exists[value] {
			return false
		}
	}
	return true
}
