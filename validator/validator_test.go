package validator

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"go.dataddo.com/pgq/internal/pg"
	"go.dataddo.com/pgq/internal/require"
	"go.dataddo.com/pgq/x/schema"
)

func TestValidator_ValidateFieldsCorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))

	defer db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))

	// Create the new queue
	_, err := db.ExecContext(ctx, schema.GenerateCreateTableQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateFields(db, queueName)

	// Assert
	require.NoError(t, err)
}

func TestValidator_ValidateFieldsIncorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	defer db.ExecContext(ctx, schema.GenerateDropTableQuery(queueName))

	// Create the new incorrect queue
	_, err := db.ExecContext(ctx, generateInvalidQueueQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateFields(db, queueName)

	// Assert
	require.Error(t, err)
}

func generateRandomString(length int) string {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// TODO: This was recovered from the consumer_test.go file. We can make a common testing package and add all these common
// functionalities will be included
func openDB(t *testing.T) *sql.DB {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok {
		t.Skip("Skipping integration test, TEST_POSTGRES_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := db.Close()
		require.NoError(t, err)
	})
	ensureUUIDExtension(t, db)
	return db
}

func ensureUUIDExtension(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
		DO $$ 
		BEGIN
		  IF current_setting('server_version_num')::int < 130000 THEN
		    -- If PostgreSQL version is less than 13, enable pgcrypto
		    CREATE EXTENSION IF NOT EXISTS pgcrypto;
		  END IF;
		END $$;
	`)
	require.NoError(t, err)
}

func generateInvalidQueueQuery(queueName string) string {
	quotedTableName := pg.QuoteIdentifier(queueName)
	return fmt.Sprintf(`	CREATE TABLE IF NOT EXISTS %[1]s
	(
		id            UUID        DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
		created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
		started_at     TIMESTAMPTZ                           NULL,
		description    TEXT  								 NULL,
		name		   TEXT 								 NULL
	);
	`, quotedTableName, quotedTableName[1:len(quotedTableName)-1])
}
