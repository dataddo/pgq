module go.dataddo.com/pgq

go 1.21

require (
	github.com/google/uuid v1.5.0
	github.com/jackc/pgtype v1.14.1
	github.com/jmoiron/sqlx v1.3.5
	github.com/lib/pq v1.10.2
	github.com/pkg/errors v0.9.1
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/metric v1.21.0
	golang.org/x/sync v0.6.0
)

require (
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	go.opentelemetry.io/otel/trace v1.21.0 // indirect
)

// Test dependencies
require github.com/jackc/pgx/v4 v4.18.1

// dependencies from github.com/jackc/pgx/v4 v4.18.1, that's used only in tests.
require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.2 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/text v0.12.0 // indirect
)
