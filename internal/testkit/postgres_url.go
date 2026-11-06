package testkit

import (
	"net/url"
	"strings"
)

// PostgresQuery parses PostgreSQL URI parameters, where + is literal, not a space.
func PostgresQuery(rawQuery string) (url.Values, error) {
	return url.ParseQuery(strings.ReplaceAll(rawQuery, "+", "%2B"))
}

// EncodePostgresQuery percent-encodes spaces for libpq-compatible URI parsers.
func EncodePostgresQuery(values url.Values) string {
	return strings.ReplaceAll(values.Encode(), "+", "%20")
}
