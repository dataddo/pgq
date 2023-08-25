package pgq

import (
	"errors"
	"slices"
)

// block contains used error codes, which are used in the code.
const (
	undefinedTableErrCode  = "42P01"
	undefinedColumnErrCode = "42703"
)

type pgError interface {
	SQLState() string
}

// legacyPGError is an interface used by previous versions of github.com/lib/pq.
// It is provided only to support legacy code. New code should use the pgError
// interface.
type legacyPGError interface {
	Error() string
	Fatal() bool
	Get(k byte) (v string)
}

//var (
//	_ pgError       = (*pgconn.PgError)(nil)
//	_ pgError       = (*pq.Error)(nil)
//	_ legacyPGError = (*pq.Error)(nil)
//)

func isErrorCode(err error, codes ...string) bool {
	var pgErr pgError
	if ok := errors.As(err, &pgErr); !ok {
		var legacyErr legacyPGError
		if ok := errors.As(err, &legacyErr); !ok {
			return false
		}
		return slices.Contains(codes, legacyErr.Get('C'))
	}
	return slices.Contains(codes, pgErr.SQLState())
}
