//go:build integration

package selvedge

import "testing"

func TestPostgresActivityOptionsResolution(t *testing.T) {
	testActivityOptionsResolution(t, createTestAppWithPostgres)
}
func TestPostgresActivityOptionsRecorded(t *testing.T) {
	testActivityOptionsRecorded(t, createTestAppWithPostgres)
}

func TestPostgresActivityOptionsNormalized(t *testing.T) {
	testActivityOptionsNormalized(t, createTestAppWithPostgres)
}
