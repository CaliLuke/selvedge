//go:build integration

package selvedge

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/notify"

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

// TestProducerNotifiesIdlePostgresWorker observes the LISTEN callback directly;
// neither dispatcher polling nor the process-local bus can satisfy the assertion.
func TestProducerNotifiesIdlePostgresWorker(t *testing.T) {
	worker, cleanup := createTestAppWithPostgres(t,
		WithWorkflowResumptionInterval(time.Hour), WithTimerCheckInterval(time.Hour), WithMaxPollInterval(time.Hour))
	t.Cleanup(cleanup)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, worker.Start(ctx))
	received := make(chan string, 1)
	worker.notifyListener.OnNotification(notify.ChannelWorkflowResumable, func(_ context.Context, _ notify.NotifyChannel, payload string) {
		select {
		case received <- payload:
		default:
		}
	})
	require.Eventually(t, worker.notifyListener.IsActive, 5*time.Second, time.Millisecond)

	address, err := url.Parse(worker.config.databaseURL)
	require.NoError(t, err)
	query := address.Query()
	query.Set("application_name", "producer_notification_test")
	address.RawQuery = query.Encode()
	// Distinct literal DSNs also prevent the local notification bus reaching the worker.
	require.NotEqual(t, worker.config.databaseURL, address.String())
	producer, err := OpenProducer(ctx, WithDatabase(address.String()), WithAutoMigrate(false))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, producer.Shutdown(context.Background())) })
	id, err := producer.StartWorkflowByName(ctx, "notification_only", nil)
	require.NoError(t, err)
	select {
	case payload := <-received:
		notification, err := notify.ParseWorkflowNotification(payload)
		require.NoError(t, err)
		require.Equal(t, id, notification.InstanceID)
	case <-ctx.Done():
		t.Fatal("producer did not notify the listening PostgreSQL worker")
	}
}
