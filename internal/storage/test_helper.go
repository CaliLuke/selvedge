// Package storage provides the storage layer for Selvedge.
package storage

import (
	"context"
)

// testSchema is the schema SQL for tests.
// This is derived from schema/db/migrations/sqlite.
const testSchemaSQLite = `
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now')),
    description TEXT NOT NULL
);

-- Workflow definitions (source code storage)
CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    source_code TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (workflow_name, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_definitions_name ON workflow_definitions(workflow_name);
CREATE INDEX IF NOT EXISTS idx_definitions_hash ON workflow_definitions(source_hash);

-- Workflow instances with distributed locking support
CREATE TABLE IF NOT EXISTS workflow_instances (
    instance_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL DEFAULT '',
    owner_service TEXT NOT NULL DEFAULT '',
    framework TEXT NOT NULL DEFAULT 'go',
    status TEXT NOT NULL DEFAULT 'running',
    current_activity_id TEXT,
    continued_from TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    input_data TEXT NOT NULL,
    output_data TEXT,
    locked_by TEXT,
    locked_at TEXT,
    lock_timeout_seconds INTEGER,
    lock_expires_at TEXT,
    CONSTRAINT valid_status CHECK (
        status IN ('running', 'completed', 'failed', 'waiting_for_event', 'waiting_for_timer', 'waiting_for_message', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);
CREATE INDEX IF NOT EXISTS idx_instances_workflow ON workflow_instances(workflow_name);
CREATE INDEX IF NOT EXISTS idx_instances_owner ON workflow_instances(owner_service);
CREATE INDEX IF NOT EXISTS idx_instances_framework ON workflow_instances(framework);
CREATE INDEX IF NOT EXISTS idx_instances_locked ON workflow_instances(locked_by, locked_at);
CREATE INDEX IF NOT EXISTS idx_instances_lock_expires ON workflow_instances(lock_expires_at);
CREATE INDEX IF NOT EXISTS idx_instances_updated ON workflow_instances(updated_at);
CREATE INDEX IF NOT EXISTS idx_instances_hash ON workflow_instances(source_hash);
CREATE INDEX IF NOT EXISTS idx_instances_continued_from ON workflow_instances(continued_from);
CREATE INDEX IF NOT EXISTS idx_instances_resumable ON workflow_instances(status, locked_by);

-- Workflow execution history (for deterministic replay)
CREATE TABLE IF NOT EXISTS workflow_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    data_type TEXT NOT NULL DEFAULT 'json',
    event_data TEXT,
    event_data_binary BLOB,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_activity UNIQUE (instance_id, activity_id)
);

CREATE INDEX IF NOT EXISTS idx_history_instance ON workflow_history(instance_id, activity_id);
CREATE INDEX IF NOT EXISTS idx_history_created ON workflow_history(created_at);

-- Timer subscriptions (for wait_timer)
CREATE TABLE IF NOT EXISTS workflow_timer_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    activity_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_timer UNIQUE (instance_id, timer_id)
);

CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_expires ON workflow_timer_subscriptions(expires_at);
CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_instance ON workflow_timer_subscriptions(instance_id);

-- Channel messages (persistent message queue)
CREATE TABLE IF NOT EXISTS channel_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    data_type TEXT NOT NULL,
    data TEXT,
    data_binary BLOB,
    metadata TEXT,
    published_at TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT valid_data_type CHECK (data_type IN ('json', 'binary')),
    CONSTRAINT data_type_consistency CHECK (
        (data_type = 'json' AND data IS NOT NULL AND data_binary IS NULL) OR
        (data_type = 'binary' AND data IS NULL AND data_binary IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel ON channel_messages(channel, published_at);
CREATE INDEX IF NOT EXISTS idx_channel_messages_id ON channel_messages(id);

-- Channel subscriptions
CREATE TABLE IF NOT EXISTS channel_subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    mode TEXT NOT NULL,
    activity_id TEXT,
    cursor_message_id INTEGER,
    timeout_at TEXT,
    subscribed_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT valid_mode CHECK (mode IN ('broadcast')),
    CONSTRAINT unique_instance_channel UNIQUE (instance_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_channel ON channel_subscriptions(channel);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_instance ON channel_subscriptions(instance_id);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_waiting ON channel_subscriptions(channel, activity_id);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_timeout ON channel_subscriptions(timeout_at);

-- Channel delivery cursors (broadcast mode: track who read what)
CREATE TABLE IF NOT EXISTS channel_delivery_cursors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    channel TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    last_delivered_id INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_channel_instance UNIQUE (channel, instance_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_delivery_cursors_channel ON channel_delivery_cursors(channel);

-- System locks (for coordinating background tasks across pods)
CREATE TABLE IF NOT EXISTS system_locks (
    lock_name TEXT PRIMARY KEY,
    locked_by TEXT,
    locked_at TEXT,
    lock_expires_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_system_locks_expires ON system_locks(lock_expires_at);
`

// InitializeTestSchema applies the test schema to an SQLiteStorage.
// This is intended for use in tests only.
func InitializeTestSchema(ctx context.Context, s *SQLiteStorage) error {
	_, err := s.DB().ExecContext(ctx, testSchemaSQLite)
	return err
}

// InitializeTestSchemaForStorage initializes the test schema for any Storage interface.
// This is useful for external test packages.
func InitializeTestSchemaForStorage(ctx context.Context, s Storage) error {
	if sqliteStorage, ok := s.(*SQLiteStorage); ok {
		return InitializeTestSchema(ctx, sqliteStorage)
	}
	if pgStorage, ok := s.(*PostgresStorage); ok {
		return InitializeTestSchemaPostgres(ctx, pgStorage)
	}
	return nil
}

// testSchemaPostgres is the schema SQL for PostgreSQL tests.
// Derived from schema/db/migrations/postgresql, without workflow_definitions FK.
const testSchemaPostgres = `
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW(),
    description TEXT NOT NULL
);

-- Workflow definitions (source code storage)
CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    source_code TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_name, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_definitions_name ON workflow_definitions(workflow_name);
CREATE INDEX IF NOT EXISTS idx_definitions_hash ON workflow_definitions(source_hash);

-- Workflow instances with distributed locking support
CREATE TABLE IF NOT EXISTS workflow_instances (
    instance_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL DEFAULT '',
    owner_service TEXT NOT NULL DEFAULT '',
    framework TEXT NOT NULL DEFAULT 'go',
    status TEXT NOT NULL DEFAULT 'running',
    current_activity_id TEXT,
    continued_from TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    input_data TEXT NOT NULL,
    output_data TEXT,
    locked_by TEXT,
    locked_at TIMESTAMP,
    lock_timeout_seconds INTEGER,
    lock_expires_at TIMESTAMP,
    CONSTRAINT valid_status CHECK (
        status IN ('running', 'completed', 'failed', 'waiting_for_event', 'waiting_for_timer', 'waiting_for_message', 'cancelled')
    )
);

CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);
CREATE INDEX IF NOT EXISTS idx_instances_workflow ON workflow_instances(workflow_name);
CREATE INDEX IF NOT EXISTS idx_instances_owner ON workflow_instances(owner_service);
CREATE INDEX IF NOT EXISTS idx_instances_framework ON workflow_instances(framework);
CREATE INDEX IF NOT EXISTS idx_instances_locked ON workflow_instances(locked_by, locked_at);
CREATE INDEX IF NOT EXISTS idx_instances_lock_expires ON workflow_instances(lock_expires_at);
CREATE INDEX IF NOT EXISTS idx_instances_updated ON workflow_instances(updated_at);
CREATE INDEX IF NOT EXISTS idx_instances_hash ON workflow_instances(source_hash);
CREATE INDEX IF NOT EXISTS idx_instances_continued_from ON workflow_instances(continued_from);
CREATE INDEX IF NOT EXISTS idx_instances_resumable ON workflow_instances(status, locked_by);

-- Workflow execution history (for deterministic replay)
CREATE TABLE IF NOT EXISTS workflow_history (
    id SERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    data_type TEXT NOT NULL DEFAULT 'json',
    event_data TEXT,
    event_data_binary BYTEA,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_activity UNIQUE (instance_id, activity_id)
);

CREATE INDEX IF NOT EXISTS idx_history_instance ON workflow_history(instance_id, activity_id);
CREATE INDEX IF NOT EXISTS idx_history_created ON workflow_history(created_at);

-- Timer subscriptions (for wait_timer)
CREATE TABLE IF NOT EXISTS workflow_timer_subscriptions (
    id SERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    activity_id TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_timer UNIQUE (instance_id, timer_id)
);

CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_expires ON workflow_timer_subscriptions(expires_at);
CREATE INDEX IF NOT EXISTS idx_timer_subscriptions_instance ON workflow_timer_subscriptions(instance_id);

-- Channel messages (persistent message queue)
CREATE TABLE IF NOT EXISTS channel_messages (
    id SERIAL PRIMARY KEY,
    channel TEXT NOT NULL,
    message_id TEXT NOT NULL UNIQUE,
    data_type TEXT NOT NULL,
    data TEXT,
    data_binary BYTEA,
    metadata TEXT,
    published_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_data_type CHECK (data_type IN ('json', 'binary')),
    CONSTRAINT data_type_consistency CHECK (
        (data_type = 'json' AND data IS NOT NULL AND data_binary IS NULL) OR
        (data_type = 'binary' AND data IS NULL AND data_binary IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_channel_messages_channel ON channel_messages(channel, published_at);
CREATE INDEX IF NOT EXISTS idx_channel_messages_id ON channel_messages(id);

-- Channel subscriptions
CREATE TABLE IF NOT EXISTS channel_subscriptions (
    id SERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    mode TEXT NOT NULL,
    activity_id TEXT,
    cursor_message_id INTEGER,
    timeout_at TIMESTAMP,
    subscribed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT valid_mode CHECK (mode IN ('broadcast')),
    CONSTRAINT unique_instance_channel UNIQUE (instance_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_channel ON channel_subscriptions(channel);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_instance ON channel_subscriptions(instance_id);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_waiting ON channel_subscriptions(channel, activity_id);
CREATE INDEX IF NOT EXISTS idx_channel_subscriptions_timeout ON channel_subscriptions(timeout_at);

-- Channel delivery cursors (broadcast mode: track who read what)
CREATE TABLE IF NOT EXISTS channel_delivery_cursors (
    id SERIAL PRIMARY KEY,
    channel TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    last_delivered_id INTEGER NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_channel_instance UNIQUE (channel, instance_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_delivery_cursors_channel ON channel_delivery_cursors(channel);

-- System locks (for coordinating background tasks across pods)
CREATE TABLE IF NOT EXISTS system_locks (
    lock_name TEXT PRIMARY KEY,
    locked_by TEXT,
    locked_at TIMESTAMP,
    lock_expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_system_locks_expires ON system_locks(lock_expires_at);
`

// InitializeTestSchemaPostgres applies the test schema to a PostgresStorage.
// This is intended for use in tests only.
func InitializeTestSchemaPostgres(ctx context.Context, s *PostgresStorage) error {
	_, err := s.DB().ExecContext(ctx, testSchemaPostgres)
	return err
}
