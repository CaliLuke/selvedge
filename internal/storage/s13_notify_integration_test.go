//go:build integration

package storage

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func s13PostgresDSN(t *testing.T, s *PostgresStorage) string {
	t.Helper()
	conn, err := s.DB().Conn(context.Background())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	var dsn string
	require.NoError(t, conn.Raw(func(c any) error { dsn = c.(*stdlib.Conn).Conn().Config().ConnString(); return nil }))
	return dsn
}
func TestLocalNotifyKeyedByDatabase_Postgres(t *testing.T) {
	s, cleanup := setupPostgresContainer(t)
	defer cleanup()
	dsn := s13PostgresDSN(t, s)
	s13LocalNotify(t, s, dsn, dsn)
}
func TestCloseNotifies(t *testing.T) {
	s, cleanup := setupPostgresContainer(t)
	defer cleanup()
	s.SetNotifyEnabled(true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, s13PostgresDSN(t, s))
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.Background()) }()
	_, err = conn.Exec(ctx, "LISTEN workflow_closed")
	require.NoError(t, err)
	require.NoError(t, s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "closed-notify", WorkflowName: "s13_close", Status: StatusRunning, InputData: []byte(`null`)}))
	grant, acquired, err := s.AcquireLease(ctx, "closed-notify", "owner", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, s.CloseInstance(ctx, Fence{InstanceID: "closed-notify", Owner: "owner", Epoch: grant.Epoch}, StatusCompleted, []byte(`"done"`), nil))
	notification, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	require.Equal(t, "workflow_closed", notification.Channel)
	require.Equal(t, "closed-notify", notification.Payload)
	instance, err := s.GetInstance(ctx, "closed-notify")
	require.NoError(t, err)
	require.Equal(t, StatusCompleted, instance.Status)
}
