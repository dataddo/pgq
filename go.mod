module go.dataddo.com/pgq

go 1.22.0

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgtype v1.14.4
	github.com/jmoiron/sqlx v1.4.0
	github.com/pkg/errors v0.9.1
	go.opentelemetry.io/otel v1.34.0
	go.opentelemetry.io/otel/metric v1.34.0
	golang.org/x/sync v0.11.0
)

require (
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/trace v1.34.0 // indirect
)

// Test dependencies
require github.com/jackc/pgx/v4 v4.18.2

// Prevent forcing someones to use a vulnerable version of pgx/v4
// https://devhub.checkmarx.com/cve-details/CVE-2024-27289/
exclude github.com/jackc/pgx/v4 v4.18.3

// dependencies from github.com/jackc/pgx/v4 v4.18.2, that's used only in tests.
require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/crypto v0.26.0 // indirect
	golang.org/x/text v0.17.0 // indirect
)
