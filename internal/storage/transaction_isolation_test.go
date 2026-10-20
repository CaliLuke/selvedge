package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionContextDoesNotRedirectAnotherDatabase(t *testing.T) {
	testTransactionContextDoesNotRedirectAnotherDatabase(t, sqliteHistoryFactory)
}

func testTransactionContextDoesNotRedirectAnotherDatabase(t *testing.T, factory historyFactory) {
	first := factory(t)
	second := factory(t)
	ctx := context.Background()
	tx, err := first.BeginTransaction(ctx)
	require.NoError(t, err)
	defer first.RollbackTransaction(tx)
	require.NoError(t, first.CreateInstance(tx, &WorkflowInstance{InstanceID: "first", WorkflowName: "test", Status: StatusRunning}))

	// Context propagation must not switch the second store's target database.
	require.NoError(t, second.CreateInstance(tx, &WorkflowInstance{InstanceID: "second", WorkflowName: "test", Status: StatusRunning}))
	require.NoError(t, first.RollbackTransaction(tx))
	instance, err := second.GetInstance(ctx, "second")
	require.NoError(t, err)
	require.NotNil(t, instance, "the second database's write must not join the first database's transaction")
	instance, err = first.GetInstance(ctx, "first")
	require.NoError(t, err)
	require.Nil(t, instance)
}
