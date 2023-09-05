package pg

import (
	"strconv"
	"strings"
)

// StmtParams is a helper for generating prepared statement parameters,
// i.e. $1, $2, $3, ...
type StmtParams struct {
	counter int
}

// Next returns next parameter.
func (p *StmtParams) Next() string {
	p.counter++
	return "$" + strconv.Itoa(p.counter)
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//	tblname := "my_table"
//	data := "my_data"
//	quoted := pq.QuoteIdentifier(tblname)
//	err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
//
// It's a copy of the function from github.com/lib/pq.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
