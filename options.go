package selvedge

import (
	"io/fs"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
)

// Option configures an App.
type Option func(*appConfig)

// appConfig holds the configuration for an App.
type appConfig struct {
	lease                   replay.LeaseConfig
	clock                   func() time.Time
	maxConcurrentActivities int
	activityPollInterval    time.Duration
	// Database
	databaseURL string

	// Service identity
	serviceName string
	workerID    string

	// PostgreSQL LISTEN/NOTIFY
	useListenNotify      *bool         // nil = auto-detect, true = force enable, false = force disable
	notifyReconnectDelay time.Duration // Delay before reconnecting after connection failure

	// Background task intervals
	timerCheckInterval         time.Duration
	workflowResumptionInterval time.Duration

	// Concurrency control
	maxConcurrentResumptions int
	maxConcurrentTimers      int

	// Batch sizes for background tasks
	maxWorkflowsPerBatch int
	maxTimersPerBatch    int

	// Leader election configuration
	leaderHeartbeatInterval time.Duration
	leaderLeaseDuration     time.Duration

	// Hooks
	hooks hooks.WorkflowHooks

	// Shutdown
	shutdownTimeout time.Duration

	// Migration settings
	autoMigrate  bool  // Whether to run migrations on startup (default: true)
	migrationsFS fs.FS // Optional custom migrations filesystem
}

// defaultConfig returns the default configuration.
func defaultConfig() *appConfig {
	return &appConfig{
		databaseURL:                "file:selvedge.db",
		maxConcurrentActivities:    10,
		activityPollInterval:       time.Second,
		lease:                      replay.DefaultLeaseConfig(),
		serviceName:                "selvedge-service",
		useListenNotify:            nil, // auto-detect
		notifyReconnectDelay:       60 * time.Second,
		timerCheckInterval:         10 * time.Second,
		workflowResumptionInterval: 1 * time.Second,
		maxConcurrentResumptions:   10,
		maxConcurrentTimers:        10,
		maxWorkflowsPerBatch:       100,
		maxTimersPerBatch:          100,
		leaderHeartbeatInterval:    15 * time.Second,
		leaderLeaseDuration:        45 * time.Second,
		shutdownTimeout:            30 * time.Second,
		hooks:                      &hooks.NoOpHooks{},
		autoMigrate:                true,
	}
}

// WithDatabase sets the database connection URL.
// Supported formats:
//   - SQLite: "file:path/to/db.db" or "sqlite://path/to/db.db"
//   - PostgreSQL: "postgres://user:pass@host:port/dbname"
func WithDatabase(url string) Option {
	return func(c *appConfig) {
		c.databaseURL = url
	}
}

// WithAutoMigrate enables or disables automatic database migrations on startup.
//
// When enabled (default), selvedge will automatically apply pending dbmate-compatible
// migrations from the embedded internal/migrations/sql/ directory during App.Start().
//
// This is compatible with the dbmate CLI tool and uses the same schema_migrations
// table for tracking applied migrations.
//
// Set to false if you prefer to manage migrations manually using dbmate CLI:
//
//	dbmate -d internal/migrations/sql/sqlite up
func WithAutoMigrate(enabled bool) Option {
	return func(c *appConfig) {
		c.autoMigrate = enabled
	}
}

// WithMigrationsFS sets a custom filesystem for database migrations.
//
// By default, selvedge uses embedded migrations from internal/migrations/sql/.
// Use this option to provide custom migrations from a different source.
//
// The filesystem should contain subdirectories for each database type:
//   - sqlite/
//   - postgresql/
//
// Each subdirectory should contain .sql files in dbmate format with
// -- migrate:up and -- migrate:down sections.
func WithMigrationsFS(migrationsFS fs.FS) Option {
	return func(c *appConfig) {
		c.migrationsFS = migrationsFS
	}
}

// WithServiceName sets the service name for identification.
func WithServiceName(name string) Option {
	return func(c *appConfig) {
		c.serviceName = name
	}
}

// WithWorkerID sets a custom worker ID.
// If not set, a UUID will be generated.
func WithWorkerID(id string) Option {
	return func(c *appConfig) {
		c.workerID = id
	}
}

// WithLease configures execution leases. Start rejects unsafe configurations.
func WithLease(ttl, renewInterval, margin, maxSkew time.Duration) Option {
	return func(c *appConfig) {
		c.lease = replay.LeaseConfig{TTL: ttl, RenewInterval: renewInterval, Margin: margin, MaxSkew: maxSkew}
	}
}

// WithTimerCheckInterval sets the interval for checking expired timers.
func WithTimerCheckInterval(d time.Duration) Option {
	return func(c *appConfig) {
		c.timerCheckInterval = d
	}
}

// WithHooks sets the workflow lifecycle hooks.
func WithHooks(h hooks.WorkflowHooks) Option {
	return func(c *appConfig) {
		c.hooks = h
	}
}

// WithShutdownTimeout sets the timeout for graceful shutdown.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *appConfig) {
		c.shutdownTimeout = d
	}
}

// WithWorkflowResumptionInterval sets the interval for the workflow resumption task.
// This task finds runnable workflows and expired leases, then dispatches them. This is essential for load balancing in multi-worker environments.
// Default: 1 second.
func WithWorkflowResumptionInterval(d time.Duration) Option {
	return func(c *appConfig) {
		c.workflowResumptionInterval = d
	}
}

// WithMaxConcurrentResumptions sets the maximum number of concurrent workflow resumptions.
// This limits goroutine spawning in background tasks to prevent resource exhaustion.
// Default: 10.
func WithMaxConcurrentResumptions(n int) Option {
	return func(c *appConfig) {
		if n > 0 {
			c.maxConcurrentResumptions = n
		}
	}
}

// WithMaxConcurrentTimers sets the maximum number of concurrent timer handlers.
// Default: 10.
func WithMaxConcurrentTimers(n int) Option {
	return func(c *appConfig) {
		if n > 0 {
			c.maxConcurrentTimers = n
		}
	}
}

// WithLeaderHeartbeatInterval sets the interval for leader election heartbeat.
// The leader will renew its lease at this interval.
// Default: 15 seconds.
func WithLeaderHeartbeatInterval(d time.Duration) Option {
	return func(c *appConfig) {
		if d > 0 {
			c.leaderHeartbeatInterval = d
		}
	}
}

// WithLeaderLeaseDuration sets the duration for which a leader holds its lease.
// Should be at least 3x the heartbeat interval to allow for missed heartbeats.
// Default: 45 seconds.
func WithLeaderLeaseDuration(d time.Duration) Option {
	return func(c *appConfig) {
		if d > 0 {
			c.leaderLeaseDuration = d
		}
	}
}

// WithListenNotify configures PostgreSQL LISTEN/NOTIFY usage.
// - nil (default): auto-detect based on database URL (enabled for PostgreSQL)
// - true: force enable (fails if not PostgreSQL)
// - false: force disable (use polling only)
func WithListenNotify(enabled *bool) Option {
	return func(c *appConfig) {
		c.useListenNotify = enabled
	}
}

// WithNotifyReconnectDelay sets the delay before reconnecting
// after a LISTEN/NOTIFY connection failure. Default: 60 seconds.
func WithNotifyReconnectDelay(d time.Duration) Option {
	return func(c *appConfig) {
		c.notifyReconnectDelay = d
	}
}

// WithMaxWorkflowsPerBatch sets the maximum number of workflows
// to process per polling cycle. Default: 100.
func WithMaxWorkflowsPerBatch(n int) Option {
	return func(c *appConfig) {
		if n > 0 {
			c.maxWorkflowsPerBatch = n
		}
	}
}

// WithMaxTimersPerBatch sets the maximum number of timers
// to process per polling cycle. Default: 100.
func WithMaxTimersPerBatch(n int) Option {
	return func(c *appConfig) {
		if n > 0 {
			c.maxTimersPerBatch = n
		}
	}
}

// WithMaxConcurrentActivities bounds the number of background activity attempts.
func WithMaxConcurrentActivities(n int) Option {
	return func(c *appConfig) {
		if n > 0 {
			c.maxConcurrentActivities = n
		}
	}
}

// WithActivityPollInterval sets the base activity poll interval before adaptive backoff.
func WithActivityPollInterval(d time.Duration) Option {
	return func(c *appConfig) {
		if d > 0 {
			c.activityPollInterval = d
		}
	}
}
