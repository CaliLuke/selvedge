//go:build integration

package selvedge

import "testing"

func TestPostgresDescribeStatus(t *testing.T) { s14TestDescribeStatus(t, s14PostgresFactory(t)) }
