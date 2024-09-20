package pgq

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"go.dataddo.com/pgq/internal/pg"
	"go.dataddo.com/pgq/internal/require"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestValidator_ValidateFieldsCorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})

	// Create the new queue
	_, err := db.Exec(ctx, generateCreateTableQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateFields(ctx, db, queueName)

	// Assert
	require.NoError(t, err)
}

func TestValidator_ValidateFieldsCorrectSchemaPartitionedTable(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})

	// Create the new queue
	_, err := db.Exec(ctx, generateCreateTablePartitionedQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateFields(ctx, db, queueName)

	// Assert
	require.NoError(t, err)
}

func TestValidator_ValidateFieldsIncorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})
	// Create the new incorrect queue
	_, err := db.Exec(ctx, generateInvalidQueueQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateFields(ctx, db, queueName)

	// Assert
	require.Error(t, err)
}

func TestValidator_ValidateIndexesCorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})

	// Create the new queue
	_, err := db.Exec(ctx, generateCreateTableQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateIndexes(ctx, db, queueName)

	// Assert
	require.NoError(t, err)
}

func TestValidator_ValidateIndexesCorrectSchema_CompositeIndexes(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})
	// Create the new queue
	_, err := db.Exec(ctx, generateCreateTableQueryCompositeIndex(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateIndexes(ctx, db, queueName)

	// Assert
	require.NoError(t, err)
}

func TestValidator_ValidateIndexesIncorrectSchema(t *testing.T) {
	// --- (1) ----
	// Arrange
	ctx := context.Background()
	db := openDB(t)
	queueName := fmt.Sprintf("TestQueue_%s", generateRandomString(10))
	t.Cleanup(func() {
		_, err := db.Exec(ctx, generateDropTableQuery(queueName))
		require.NoError(t, err)
	})
	// Create the new incorrect queue
	_, err := db.Exec(ctx, generateInvalidQueueQuery(queueName))
	require.NoError(t, err)

	// --- (2) ----
	// Act: Validate queue
	err = ValidateIndexes(ctx, db, queueName)

	// Assert
	require.Error(t, err)
}

// TODO: This was recovered from the consumer_test.go file. We can make a common testing package and add all these common
// functionalities will be included
func openDB(t *testing.T) *pgxpool.Pool {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok {
		t.Skip("Skipping integration test, TEST_POSTGRES_DSN is not set")
	}
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	db, err := pgxpool.NewWithConfig(context.Background(), config)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})
	ensureUUIDExtension(t, db)
	return db
}

func ensureUUIDExtension(t *testing.T, db *pgxpool.Pool) {
	_, err := db.Exec(context.Background(), `
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

func generateRandomString(length int) string {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(b)
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

func generateCreateTableQuery(queueName string) string {
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

func generateCreateTableQueryCompositeIndex(queueName string) string {
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
	CREATE INDEX IF NOT EXISTS "%[2]s_processed_at_null_idx" ON %[1]s (consumed_count, processed_at) WHERE (processed_at IS NULL);
	`, quotedTableName, quotedTableName[1:len(quotedTableName)-1])
}

func generateCreateTablePartitionedQuery(queueName string) string {
	quotedTableName := pg.QuoteIdentifier(queueName)
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s
	(
		id             UUID        DEFAULT gen_random_uuid() NOT NULL,
		created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
		started_at     TIMESTAMPTZ                           NULL,
		locked_until   TIMESTAMPTZ                           NULL,
		processed_at   TIMESTAMPTZ                           NULL,
		consumed_count INTEGER     DEFAULT 0                 NOT NULL,
		error_detail   TEXT                                  NULL,
		payload        JSONB                                 NOT NULL,
		metadata       JSONB                                 NOT NULL
	) PARTITION BY RANGE (created_at);
	CREATE TABLE "%[2]s_y2024m02" PARTITION OF %[1]s FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
	CREATE INDEX IF NOT EXISTS "%[2]s_created_at_idx" ON %[1]s (created_at);
	CREATE INDEX IF NOT EXISTS "%[2]s_processed_at_null_idx" ON %[1]s (processed_at) WHERE (processed_at IS NULL);
	`, quotedTableName, quotedTableName[1:len(quotedTableName)-1])
}

func generateDropTableQuery(queueName string) string {
	quotedTableName := pg.QuoteIdentifier(queueName)
	return `DROP TABLE IF EXISTS ` + quotedTableName
}
