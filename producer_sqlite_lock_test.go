package selvedge

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProducerSQLiteInitializationHonorsContextWhileLocked(t *testing.T) {
	for _, stage := range []string{"wal", "migration"} {
		for _, interruption := range []string{"deadline", "cancel"} {
			t.Run(stage+"/"+interruption, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "locked.db")
				if stage == "migration" {
					p, err := OpenProducer(context.Background(), WithDatabase(path))
					require.NoError(t, err)
					require.NoError(t, p.Shutdown(context.Background()))
				}
				blocker, err := sql.Open("sqlite", path)
				require.NoError(t, err)
				defer blocker.Close()
				blocker.SetMaxOpenConns(1)
				_, err = blocker.Exec("BEGIN EXCLUSIVE")
				require.NoError(t, err)
				defer blocker.Exec("ROLLBACK")

				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				wantErr := context.DeadlineExceeded
				if interruption == "cancel" {
					cancel()
					ctx, cancel = context.WithCancel(context.Background())
					timer := time.AfterFunc(50*time.Millisecond, cancel)
					defer timer.Stop()
					wantErr = context.Canceled
				}
				defer cancel()
				start := time.Now()
				p, err := OpenProducer(ctx, WithDatabase(path), WithAutoMigrate(stage == "migration"))
				elapsed := time.Since(start)
				if p != nil {
					defer p.Shutdown(context.Background())
				}
				require.Nil(t, p)
				require.ErrorIs(t, err, wantErr)
				require.Less(t, elapsed, time.Second, "initialization must not wait for SQLite's five-second busy timeout")
			})
		}
	}
}
