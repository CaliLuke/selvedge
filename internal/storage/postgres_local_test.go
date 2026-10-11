//go:build pglocal

package storage

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// Run with:
//   SELVEDGE_TEST_POSTGRES_URL="postgres://selvedge:selvedge@localhost:5432/selvedge_test?sslmode=disable" \
//     go test -tags=pglocal -count=1 -v ./internal/storage/ -run TestPGLocal

func pgLocalURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("SELVEDGE_TEST_POSTGRES_URL")
	if url == "" {
		t.Fatal("SELVEDGE_TEST_POSTGRES_URL must be set")
	}
	return url
}

func pgLocalSetup(t *testing.T) *PostgresStorage {
	t.Helper()
	store, err := NewPostgresStorage(pgLocalURL(t))
	if err != nil {
		t.Fatalf("NewPostgresStorage: %v", err)
	}
	ctx := context.Background()
	if err := InitializeTestSchemaPostgres(ctx, store); err != nil {
		store.Close()
		t.Fatalf("InitializeTestSchemaPostgres: %v", err)
	}
	pgLocalCleanup(t, store)
	return store
}

func pgLocalCleanup(t *testing.T, s *PostgresStorage) {
	t.Helper()
	ctx := context.Background()
	tables := []string{
		"channel_message_claims",
		"channel_delivery_cursors",
		"channel_messages",
		"channel_subscriptions",
		"workflow_timer_subscriptions",
		"workflow_history",
		"workflow_instances",
		"workflow_definitions",
		"system_locks",
	}
	for _, tbl := range tables {
		if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE "+tbl+" RESTART IDENTITY CASCADE"); err != nil {
			t.Logf("truncate %s: %v (ignoring)", tbl, err)
		}
	}
}

// insertInstance inserts a row directly with a chosen framework value.
// Bypasses CreateInstance to allow framework='python'.
func pgLocalInsertInstance(t *testing.T, s *PostgresStorage, id, name, framework string, status WorkflowStatus) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	// Insert workflow_definitions first (FK)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workflow_definitions (workflow_name, source_hash, source_code, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING
	`, name, "test-hash", "// test", now)
	if err != nil {
		t.Fatalf("insert workflow_definitions: %v", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO workflow_instances (
			instance_id, workflow_name, framework, status, input_data, source_hash,
			started_at, updated_at, owner_service
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, 'test')
	`, id, name, framework, string(status), "{}", "test-hash", now)
	if err != nil {
		t.Fatalf("insert workflow_instances: %v", err)
	}
}

func TestPGLocal_AllFrameworks(t *testing.T) {
	s := pgLocalSetup(t)
	defer s.Close()
	ctx := context.Background()
	for _, f := range []string{"go", "python"} {
		if err := s.CreateInstance(ctx, &WorkflowInstance{InstanceID: f, WorkflowName: "test", Framework: f, Status: StatusRunning}); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := s.FindDispatchable(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("dispatchable: %v", ids)
	}
}
func TestPGLocal_AcquireLeaseConcurrent(t *testing.T) {
	s := pgLocalSetup(t)
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "race", WorkflowName: "test", Status: StatusRunning}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan bool, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.AcquireLease(ctx, "race", "worker", time.Second)
			if err != nil {
				t.Error(err)
			}
			results <- ok
		}()
	}
	wg.Wait()
	close(results)
	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("winners: %d", wins)
	}
}
func TestPGLocal_AcquireLeaseHonorsContext(t *testing.T) {
	s := pgLocalSetup(t)
	defer s.Close()
	ctx := context.Background()
	if err := s.CreateInstance(ctx, &WorkflowInstance{InstanceID: "locked", WorkflowName: "test", Status: StatusRunning}); err != nil {
		t.Fatal(err)
	}
	tx, err := s.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SELECT instance_id FROM workflow_instances WHERE instance_id='locked' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, ok, err := s.AcquireLease(call, "locked", "worker", time.Second)
	if ok || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire: %v, %v", ok, err)
	}
}
