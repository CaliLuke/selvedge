-- migrate:up

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
    cancel_requested_at TIMESTAMP NULL,
    cancel_reason TEXT NULL,
    lease_owner TEXT NULL,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_expires_ms BIGINT NULL,
    runnable BOOLEAN NOT NULL DEFAULT FALSE,
    instance_id TEXT PRIMARY KEY,
    workflow_name TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    owner_service TEXT NOT NULL,
    framework TEXT NOT NULL DEFAULT 'go',
    status TEXT NOT NULL DEFAULT 'running',
    current_activity_id TEXT,
    continued_from TEXT,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    input_data TEXT NOT NULL,
    output_data TEXT,
    CONSTRAINT valid_status CHECK (
        status IN ('running', 'completed', 'failed', 'cancelled')
    ),
    FOREIGN KEY (continued_from) REFERENCES workflow_instances(instance_id),
    FOREIGN KEY (workflow_name, source_hash) REFERENCES workflow_definitions(workflow_name, source_hash)
);

CREATE INDEX IF NOT EXISTS idx_instances_status ON workflow_instances(status);
CREATE INDEX IF NOT EXISTS idx_instances_workflow ON workflow_instances(workflow_name);
CREATE INDEX IF NOT EXISTS idx_instances_owner ON workflow_instances(owner_service);
CREATE INDEX IF NOT EXISTS idx_instances_framework ON workflow_instances(framework);
CREATE INDEX IF NOT EXISTS idx_instances_updated ON workflow_instances(updated_at);
CREATE INDEX IF NOT EXISTS idx_instances_hash ON workflow_instances(source_hash);
CREATE INDEX IF NOT EXISTS idx_instances_continued_from ON workflow_instances(continued_from);

-- Workflow execution history (for deterministic replay)
CREATE TABLE IF NOT EXISTS workflow_history (
    id SERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL,
    activity_id TEXT NOT NULL,
    seq BIGINT NULL,
    event_type TEXT NOT NULL,
    command TEXT NOT NULL DEFAULT '',
    data_type TEXT NOT NULL DEFAULT 'json',
    event_data TEXT,
    delivered_seq BIGINT NULL,
    reservation_data TEXT,
    event_data_binary BYTEA,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    FOREIGN KEY (instance_id) REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    CONSTRAINT unique_instance_activity UNIQUE (instance_id, activity_id),
    CONSTRAINT unique_instance_seq UNIQUE (instance_id, seq),
    CONSTRAINT history_delivered_has_value CHECK (delivered_seq IS NULL OR event_data IS NOT NULL),
    CONSTRAINT history_delivered_after_issue CHECK (delivered_seq IS NULL OR seq IS NULL OR delivered_seq >= seq),
    CONSTRAINT history_seq_required CHECK (seq IS NOT NULL OR activity_id LIKE '$%')
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

-- Per-instance signal inbox
CREATE TABLE IF NOT EXISTS workflow_signals (
    id BIGSERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    payload TEXT NOT NULL,
    sent_at TIMESTAMP NOT NULL DEFAULT NOW(),
    consumed_key TEXT NULL,
    delivered_seq BIGINT NULL,
    consumed_seq BIGINT NULL,
    CONSTRAINT signal_consumed_position CHECK ((consumed_key IS NULL) = (consumed_seq IS NULL)),
    CONSTRAINT signal_consumed_after_delivery CHECK (consumed_seq IS NULL OR (delivered_seq IS NOT NULL AND delivered_seq <= consumed_seq)),
    CONSTRAINT unique_consumed UNIQUE (instance_id, consumed_key)
);
CREATE INDEX IF NOT EXISTS idx_signals_delivery ON workflow_signals(instance_id, delivered_seq);
CREATE INDEX IF NOT EXISTS idx_signals_next ON workflow_signals(instance_id, channel, id);

-- Durable activity inputs and attempt claims
CREATE TABLE IF NOT EXISTS workflow_activity_tasks (
    id BIGSERIAL PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES workflow_instances(instance_id) ON DELETE CASCADE,
    activity_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    activity_name TEXT NOT NULL,
    input_data TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    claim_owner TEXT NULL,
    claim_epoch BIGINT NOT NULL DEFAULT 0,
    claim_expires_ms BIGINT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_activity_task UNIQUE (instance_id, activity_id),
    CONSTRAINT valid_task_status CHECK (status IN ('queued','running','completed','cancelled')),
    FOREIGN KEY (instance_id, activity_id) REFERENCES workflow_history(instance_id, activity_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_activity_tasks_claim ON workflow_activity_tasks(status, claim_expires_ms, activity_name);

-- System locks (for coordinating background tasks across pods)
CREATE TABLE IF NOT EXISTS system_locks (
    lock_name TEXT PRIMARY KEY,
    locked_by TEXT,
    locked_at TIMESTAMP,
    lock_expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_system_locks_expires ON system_locks(lock_expires_at);

CREATE INDEX IF NOT EXISTS idx_instances_dispatch ON workflow_instances(status, runnable, lease_expires_ms);

-- migrate:down

DROP TABLE IF EXISTS workflow_activity_tasks;

DROP TABLE IF EXISTS workflow_signals;

DROP INDEX IF EXISTS idx_timer_subscriptions_instance;
DROP INDEX IF EXISTS idx_timer_subscriptions_expires;
DROP TABLE IF EXISTS workflow_timer_subscriptions;

DROP INDEX IF EXISTS idx_history_created;
DROP INDEX IF EXISTS idx_history_instance;
DROP TABLE IF EXISTS workflow_history;

DROP INDEX IF EXISTS idx_instances_continued_from;
DROP INDEX IF EXISTS idx_instances_hash;
DROP INDEX IF EXISTS idx_instances_updated;
DROP INDEX IF EXISTS idx_instances_dispatch;
DROP INDEX IF EXISTS idx_instances_framework;
DROP INDEX IF EXISTS idx_instances_owner;
DROP INDEX IF EXISTS idx_instances_workflow;
DROP INDEX IF EXISTS idx_instances_status;
DROP TABLE IF EXISTS workflow_instances;

DROP INDEX IF EXISTS idx_definitions_hash;
DROP INDEX IF EXISTS idx_definitions_name;
DROP TABLE IF EXISTS workflow_definitions;
