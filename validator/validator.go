package validator

import (
	"database/sql"
	"fmt"

	"go.dataddo.com/pgq/x/schema"
)

const columnSelect = "SELECT column_name FROM information_schema.columns where table_name = $1; "

func Validate(db *sql.DB, queueName string) error {
	// --- (1) ----
	// Recover the columns that current has
	columns, err := getColumnData(db, queueName)
	if err != nil {
		return err
	}

	// --- (2) ----
	// Run through each one of the recovered columns and validate if all the mandatory ones are included
	var columnsFound int
	for _, column := range columns {
		if _, found := schema.Fields[column]; found {
			// Mandatory field found
			columnsFound++
		}
	}

	// If all the mandatory fields have been found then we don't need to return an error. However,
	// if there is at least one mandatory field missing in the schema then this queue is invalid.
	// TODO: Add some more logic to maybe indicate which field is the one that need to be included
	if columnsFound != len(schema.Fields) {
		return fmt.Errorf("error validating queue: mandatory fields missing from schema")
	}

	// --- (3) ----
	// Validate if requiered indexes exist
	// TODO
	// TODO

	return nil
}

func getColumnData(db *sql.DB, queueName string) ([]string, error) {
	rows, err := db.Query(columnSelect, queueName)
	if err != nil {
		return nil, fmt.Errorf("error recovering queue columns %v", err)
	}
	defer rows.Close()

	var ret []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		ret = append(ret, columnName)
	}
	return ret, nil
}
