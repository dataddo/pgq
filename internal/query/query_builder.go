package query

import (
	"fmt"
	"regexp"
	"strings"
)

// Builder is a helper for building SQL queries with named parameters (jackc/pgx/v5 style for the moment)
type Builder struct {
	query  strings.Builder
	params []string
}

var (
	tagRe = regexp.MustCompile(`@{1,2}(\w+)`)
)

func NewBuilder() *Builder {
	return &Builder{}
}

// WriteString appends the provided string to the query and extracts any parameters from it.
func (qb *Builder) WriteString(part string) {
	params := getParams(part)

	qb.params = append(qb.params, params...)

	qb.query.WriteString(part)
}

// HasParam checks whether the Builder has a parameter of the given name.
func (qb *Builder) HasParam(name string) bool {
	for _, paramName := range qb.params {
		if paramName == name {
			return true
		}
	}
	return false
}

func (qb *Builder) String() string {
	return qb.query.String()
}

// Build returns the query string with the parameters replaced by the values from the provided map.
func (qb *Builder) Build(params map[string]interface{}) (string, error) {
	// Validate that the params map includes all parameter names from qb.params
	for _, paramName := range qb.params {
		if _, exists := params[paramName]; !exists {
			return "", fmt.Errorf("missing parameter: %s", paramName)
		}
	}

	queryString := qb.String()

	return queryString, nil
}

// getParams extracts tags formatted as :tagName from the provided line, ignoring type casting patterns like ::interval.
func getParams(line string) []string {

	matches := tagRe.FindAllStringSubmatch(line, -1)
	if matches == nil {
		return nil
	}

	var params []string
	for _, match := range matches {
		// Check the preceding character(s) in the match to filter out type casting
		if len(match) > 0 && !strings.HasPrefix(match[0], "::") {
			params = append(params, match[1])
		}
	}
	return params
}
