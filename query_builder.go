package pgq

import (
	"fmt"
	"regexp"
	"strings"
)

type QueryBuilder struct {
	query  strings.Builder
	params []string
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) WriteString(part string) {
	params := getParams(part)

	qb.params = append(qb.params, params...)

	qb.query.WriteString(part)
}

// HasParam checks whether the QueryBuilder has a parameter of the given name.
func (qb *QueryBuilder) HasParam(name string) bool {
	for _, paramName := range qb.params {
		if paramName == name {
			return true
		}
	}
	return false
}
func (qb *QueryBuilder) Build(params map[string]interface{}) (string, error) {
	// Validate that the params map includes all parameter names from qb.params
	for _, paramName := range qb.params {
		if _, exists := params[paramName]; !exists {
			return "", fmt.Errorf("missing parameter: %s", paramName)
		}
	}

	queryString := qb.query.String()

	return queryString, nil
}

// getParams extracts tags formatted as :tagName from the provided line, ignoring type casting patterns like ::interval.
func getParams(line string) []string {
	re := regexp.MustCompile(`:{1,2}(\w+)`)
	matches := re.FindAllStringSubmatch(line, -1)
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
