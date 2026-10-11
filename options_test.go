package selvedge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/CaliLuke/selvedge/hooks"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	// Database defaults
	assert.Equal(t, "file:selvedge.db", cfg.databaseURL)

	// Service identity defaults
	assert.Equal(t, "selvedge-service", cfg.serviceName)
	assert.Empty(t, cfg.workerID) // UUID generated at runtime

	// Feature defaults

	// Background task interval defaults
	assert.Equal(t, 10*time.Second, cfg.timerCheckInterval)
	assert.Equal(t, 30*time.Second, cfg.eventTimeoutInterval)
	assert.Equal(t, 5*time.Second, cfg.messageCheckInterval)
	assert.Equal(t, 300*time.Second, cfg.channelCleanupInterval)
	assert.Equal(t, 24*time.Hour, cfg.channelMessageRetention)
	assert.Equal(t, 1*time.Second, cfg.workflowResumptionInterval)

	// Concurrency defaults
	assert.Equal(t, 10, cfg.maxConcurrentResumptions)
	assert.Equal(t, 10, cfg.maxConcurrentTimers)
	assert.Equal(t, 10, cfg.maxConcurrentMessages)

	// Hooks default
	assert.IsType(t, &hooks.NoOpHooks{}, cfg.hooks)

	// Shutdown default
	assert.Equal(t, 30*time.Second, cfg.shutdownTimeout)
}

func TestWithDatabase(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "sqlite file path",
			url:  "file:test.db",
		},
		{
			name: "sqlite simple path",
			url:  "test.db",
		},
		{
			name: "sqlite memory",
			url:  ":memory:",
		},
		{
			name: "postgres",
			url:  "postgres://user:pass@localhost:5432/dbname",
		},
		{
			name: "empty string",
			url:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithDatabase(tt.url)(cfg)
			assert.Equal(t, tt.url, cfg.databaseURL)
		})
	}
}

func TestWithAutoMigrate(t *testing.T) {
	// WithAutoMigrate is now deprecated and is a no-op
	// This test just verifies that it doesn't panic
	cfg := defaultConfig()
	WithAutoMigrate(true)(cfg)
	WithAutoMigrate(false)(cfg)
	// No assertion needed - option is now a no-op for backwards compatibility
}

func TestWithServiceName(t *testing.T) {
	tests := []struct {
		name        string
		serviceName string
	}{
		{
			name:        "custom service name",
			serviceName: "my-workflow-service",
		},
		{
			name:        "empty string",
			serviceName: "",
		},
		{
			name:        "with special characters",
			serviceName: "service-v1.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithServiceName(tt.serviceName)(cfg)
			assert.Equal(t, tt.serviceName, cfg.serviceName)
		})
	}
}

func TestWithWorkerID(t *testing.T) {
	tests := []struct {
		name     string
		workerID string
	}{
		{
			name:     "custom worker ID",
			workerID: "worker-1",
		},
		{
			name:     "UUID style",
			workerID: "550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name:     "empty string",
			workerID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithWorkerID(tt.workerID)(cfg)
			assert.Equal(t, tt.workerID, cfg.workerID)
		})
	}
}

func TestWithTimerCheckInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "5 seconds",
			interval: 5 * time.Second,
		},
		{
			name:     "10 seconds",
			interval: 10 * time.Second,
		},
		{
			name:     "1 minute",
			interval: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithTimerCheckInterval(tt.interval)(cfg)
			assert.Equal(t, tt.interval, cfg.timerCheckInterval)
		})
	}
}

func TestWithEventTimeoutInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "15 seconds",
			interval: 15 * time.Second,
		},
		{
			name:     "30 seconds",
			interval: 30 * time.Second,
		},
		{
			name:     "1 minute",
			interval: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithEventTimeoutInterval(tt.interval)(cfg)
			assert.Equal(t, tt.interval, cfg.eventTimeoutInterval)
		})
	}
}

func TestWithMessageCheckInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "1 second",
			interval: 1 * time.Second,
		},
		{
			name:     "5 seconds",
			interval: 5 * time.Second,
		},
		{
			name:     "10 seconds",
			interval: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithMessageCheckInterval(tt.interval)(cfg)
			assert.Equal(t, tt.interval, cfg.messageCheckInterval)
		})
	}
}

func TestWithChannelCleanupInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "1 minute",
			interval: 1 * time.Minute,
		},
		{
			name:     "5 minutes",
			interval: 5 * time.Minute,
		},
		{
			name:     "1 hour",
			interval: 1 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithChannelCleanupInterval(tt.interval)(cfg)
			assert.Equal(t, tt.interval, cfg.channelCleanupInterval)
		})
	}
}

func TestWithChannelMessageRetention(t *testing.T) {
	tests := []struct {
		name      string
		retention time.Duration
	}{
		{
			name:      "1 hour",
			retention: 1 * time.Hour,
		},
		{
			name:      "24 hours",
			retention: 24 * time.Hour,
		},
		{
			name:      "7 days",
			retention: 7 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithChannelMessageRetention(tt.retention)(cfg)
			assert.Equal(t, tt.retention, cfg.channelMessageRetention)
		})
	}
}

func TestWithWorkflowResumptionInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{
			name:     "500 milliseconds",
			interval: 500 * time.Millisecond,
		},
		{
			name:     "1 second",
			interval: 1 * time.Second,
		},
		{
			name:     "5 seconds",
			interval: 5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithWorkflowResumptionInterval(tt.interval)(cfg)
			assert.Equal(t, tt.interval, cfg.workflowResumptionInterval)
		})
	}
}

func TestWithMaxConcurrentResumptions(t *testing.T) {
	tests := []struct {
		name         string
		n            int
		wantN        int
		shouldChange bool
	}{
		{
			name:         "positive value",
			n:            20,
			wantN:        20,
			shouldChange: true,
		},
		{
			name:         "value of 1",
			n:            1,
			wantN:        1,
			shouldChange: true,
		},
		{
			name:         "zero value ignored",
			n:            0,
			wantN:        10, // default
			shouldChange: false,
		},
		{
			name:         "negative value ignored",
			n:            -5,
			wantN:        10, // default
			shouldChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithMaxConcurrentResumptions(tt.n)(cfg)
			assert.Equal(t, tt.wantN, cfg.maxConcurrentResumptions)
		})
	}
}

func TestWithMaxConcurrentTimers(t *testing.T) {
	tests := []struct {
		name         string
		n            int
		wantN        int
		shouldChange bool
	}{
		{
			name:         "positive value",
			n:            15,
			wantN:        15,
			shouldChange: true,
		},
		{
			name:         "value of 1",
			n:            1,
			wantN:        1,
			shouldChange: true,
		},
		{
			name:         "zero value ignored",
			n:            0,
			wantN:        10, // default
			shouldChange: false,
		},
		{
			name:         "negative value ignored",
			n:            -3,
			wantN:        10, // default
			shouldChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithMaxConcurrentTimers(tt.n)(cfg)
			assert.Equal(t, tt.wantN, cfg.maxConcurrentTimers)
		})
	}
}

func TestWithMaxConcurrentMessages(t *testing.T) {
	tests := []struct {
		name         string
		n            int
		wantN        int
		shouldChange bool
	}{
		{
			name:         "positive value",
			n:            25,
			wantN:        25,
			shouldChange: true,
		},
		{
			name:         "value of 1",
			n:            1,
			wantN:        1,
			shouldChange: true,
		},
		{
			name:         "zero value ignored",
			n:            0,
			wantN:        10, // default
			shouldChange: false,
		},
		{
			name:         "negative value ignored",
			n:            -10,
			wantN:        10, // default
			shouldChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithMaxConcurrentMessages(tt.n)(cfg)
			assert.Equal(t, tt.wantN, cfg.maxConcurrentMessages)
		})
	}
}

func TestWithShutdownTimeout(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{
			name:    "10 seconds",
			timeout: 10 * time.Second,
		},
		{
			name:    "30 seconds",
			timeout: 30 * time.Second,
		},
		{
			name:    "1 minute",
			timeout: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithShutdownTimeout(tt.timeout)(cfg)
			assert.Equal(t, tt.timeout, cfg.shutdownTimeout)
		})
	}
}

type mockHooks struct {
	hooks.NoOpHooks
}

func TestWithHooks(t *testing.T) {
	tests := []struct {
		name  string
		hooks hooks.WorkflowHooks
	}{
		{
			name:  "NoOpHooks",
			hooks: &hooks.NoOpHooks{},
		},
		{
			name:  "custom hooks",
			hooks: &mockHooks{},
		},
		{
			name:  "nil hooks",
			hooks: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			WithHooks(tt.hooks)(cfg)
			assert.Equal(t, tt.hooks, cfg.hooks)
		})
	}
}

func TestOptionChaining(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
		check   func(t *testing.T, cfg *appConfig)
	}{
		{
			name: "database and worker",
			options: []Option{
				WithDatabase("postgres://localhost/test"),
				WithWorkerID("worker-1"),
			},
			check: func(t *testing.T, cfg *appConfig) {
				assert.Equal(t, "postgres://localhost/test", cfg.databaseURL)
				assert.Equal(t, "worker-1", cfg.workerID)
			},
		},
		{
			name: "timing configuration",
			options: []Option{
				WithTimerCheckInterval(5 * time.Second),
				WithEventTimeoutInterval(1 * time.Minute),
			},
			check: func(t *testing.T, cfg *appConfig) {
				assert.Equal(t, 5*time.Second, cfg.timerCheckInterval)
				assert.Equal(t, 1*time.Minute, cfg.eventTimeoutInterval)
			},
		},
		{
			name: "concurrency configuration",
			options: []Option{
				WithMaxConcurrentResumptions(20),
				WithMaxConcurrentTimers(15),
				WithMaxConcurrentMessages(25),
			},
			check: func(t *testing.T, cfg *appConfig) {
				assert.Equal(t, 20, cfg.maxConcurrentResumptions)
				assert.Equal(t, 15, cfg.maxConcurrentTimers)
				assert.Equal(t, 25, cfg.maxConcurrentMessages)
			},
		},
		{
			name: "full configuration",
			options: []Option{
				WithAutoMigrate(false),
				WithServiceName("test-service"),
				WithWorkerID("worker-test"),
				WithShutdownTimeout(1 * time.Minute),
			},
			check: func(t *testing.T, cfg *appConfig) {
				assert.False(t, cfg.autoMigrate)
				assert.Equal(t, "test-service", cfg.serviceName)
				assert.Equal(t, "worker-test", cfg.workerID)
				assert.Equal(t, 1*time.Minute, cfg.shutdownTimeout)
			},
		},
		{
			name: "override same option",
			options: []Option{
				WithDatabase("first.db"),
				WithDatabase("second.db"),
				WithDatabase("final.db"),
			},
			check: func(t *testing.T, cfg *appConfig) {
				assert.Equal(t, "final.db", cfg.databaseURL)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			for _, opt := range tt.options {
				opt(cfg)
			}
			tt.check(t, cfg)
		})
	}
}
