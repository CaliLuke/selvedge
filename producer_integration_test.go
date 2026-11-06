//go:build integration

package selvedge

import (
	"testing"

	"github.com/CaliLuke/selvedge/internal/testkit"
)

func TestProducerSeparateWorker_Postgres(t *testing.T) {
	producerSeparateWorker(t, createTestAppWithPostgres)
}
func TestProducerUncertainSubmission_Postgres(t *testing.T) {
	producerUncertainSubmission(t, createTestAppWithPostgres)
}
func TestProducerRejectsInvalidInput_Postgres(t *testing.T) {
	producerRejectsInvalidInput(t, createTestAppWithPostgres)
}
func TestProducerShutdownDrainsOperations_Postgres(t *testing.T) {
	producerShutdownDrainsOperations(t, createTestAppWithPostgres)
}

func TestProducerInitializationCancellationDuringMigration_Postgres(t *testing.T) {
	producerInitializationCancellationDuringMigration(t, createTestAppWithPostgres)
}
func TestProducerChildIdentity_Postgres(t *testing.T) {
	producerChildIdentity(t, createTestAppWithPostgres)
}

func TestProducerMigrationOwnership_Postgres(t *testing.T) {
	url, cleanup := testkit.PostgresDatabase(t)
	defer cleanup()
	producerMigrationOwnership(t, url)
}
