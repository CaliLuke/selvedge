package selvedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/semaphore"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/migrations"
	"github.com/CaliLuke/selvedge/internal/notify"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

// App is the main entry point for Selvedge.
// It manages workflow execution, storage, and background tasks.
type App struct {
	config  *appConfig
	storage storage.Storage
	hooks   hooks.WorkflowHooks
	engine  *replay.Engine

	// PostgreSQL LISTEN/NOTIFY listener (if enabled)
	notifyListener *notify.Listener

	// Registered workflows
	workflows   map[string]any
	workflowsMu sync.RWMutex

	// Workflow runners (workflow name → runner function)
	workflowRunners   map[string]replay.WorkflowRunner
	workflowRunnersMu sync.RWMutex

	// Background task management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Concurrency control semaphores
	resumptionSem *semaphore.Weighted
	timerSem      *semaphore.Weighted
	messageSem    *semaphore.Weighted

	// NOTIFY wake event channels for adaptive backoff
	resumeWakeEvent chan struct{}

	// Leader election state
	isLeader           atomic.Bool
	leaderCtx          context.Context
	leaderCancel       context.CancelFunc
	leaderTasksRunning bool
	leaderMu           sync.Mutex

	// State
	initialized bool
	running     bool
	mu          sync.Mutex
}

// NewApp creates a new Selvedge application.
func NewApp(opts ...Option) *App {
	config := defaultConfig()
	for _, opt := range opts {
		opt(config)
	}

	// Generate worker ID if not set
	if config.workerID == "" {
		config.workerID = uuid.New().String()
	}

	return &App{
		config:          config,
		hooks:           config.hooks,
		workflows:       make(map[string]any),
		workflowRunners: make(map[string]replay.WorkflowRunner),
		resumptionSem:   semaphore.NewWeighted(int64(config.maxConcurrentResumptions)),
		timerSem:        semaphore.NewWeighted(int64(config.maxConcurrentTimers)),
		messageSem:      semaphore.NewWeighted(int64(config.maxConcurrentMessages)),
		resumeWakeEvent: make(chan struct{}, 1),
	}
}

// Start initializes the application and starts background tasks.
func (a *App) Start(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.running {
		return fmt.Errorf("app already running")
	}

	// Create cancellable context
	a.ctx, a.cancel = context.WithCancel(ctx)

	// Initialize storage
	if err := a.initStorage(); err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Start background tasks
	a.startBackgroundTasks()

	a.running = true
	a.initialized = true
	return nil
}

// Shutdown gracefully shuts down the application.
func (a *App) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	if !a.running {
		a.mu.Unlock()
		return nil
	}
	a.running = false
	a.mu.Unlock()

	// Stop LISTEN/NOTIFY listener if enabled
	if a.notifyListener != nil {
		if err := a.notifyListener.Stop(ctx); err != nil {
			slog.Debug("error stopping LISTEN/NOTIFY listener", "error", err)
		}
	}

	// Cancel background tasks
	if a.cancel != nil {
		a.cancel()
	}

	// Wait for background tasks with timeout
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All tasks completed
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out")
	case <-time.After(a.config.shutdownTimeout):
		return fmt.Errorf("shutdown timed out after %v", a.config.shutdownTimeout)
	}

	// Executions have finished committing their lease releases. Wake other
	// workers once so shutdown abandons do not wait for idle polling backoff.
	if notifier, ok := a.storage.(interface{ NotifyRunnable(context.Context) }); ok {
		notifier.NotifyRunnable(ctx)
	}

	// Release the leader lock if we held it. Without this, a fresh
	// process starting after a graceful shutdown has to wait the full
	// leader-lease TTL (default 45s) before it can become leader and
	// resume timer and event checks. The TTL is the safety
	// net for crash scenarios; clean shutdowns should release the
	// lock symmetrically with TryAcquireSystemLock.
	//
	// Background tasks have already stopped at this point so
	// a.isLeader is stable.
	if a.isLeader.Load() && a.storage != nil {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := a.storage.ReleaseSystemLock(releaseCtx, "selvedge_leader", a.config.workerID); err != nil {
			slog.Debug("failed to release leader lock on shutdown", "error", err)
		}
		cancel()
		a.isLeader.Store(false)
	}

	// Close storage
	if a.storage != nil {
		if err := a.storage.Close(); err != nil {
			return fmt.Errorf("failed to close storage: %w", err)
		}
	}

	return nil
}

// initStorage initializes the storage backend.
func (a *App) initStorage() error {
	if err := a.config.lease.Validate(); err != nil {
		return err
	}
	if a.config.databaseURL == "" {
		a.config.databaseURL = "selvedge.db"
	}

	// Determine storage type based on URL
	url := a.config.databaseURL
	isPostgres := len(url) >= 8 && url[:8] == "postgres"

	switch {
	case isPostgres:
		// PostgreSQL
		pgStorage, err := storage.NewPostgresStorage(url)
		if err != nil {
			return fmt.Errorf("failed to create PostgreSQL storage: %w", err)
		}
		a.storage = pgStorage

		// Configure LISTEN/NOTIFY if enabled
		if a.shouldEnableListenNotify(isPostgres) {
			pgStorage.SetNotifyEnabled(true)
			a.initNotifyListener(url)
		}
	default:
		// Default to SQLite
		sqliteStorage, err := storage.NewSQLiteStorage(url)
		if err != nil {
			return fmt.Errorf("failed to create SQLite storage: %w", err)
		}
		a.storage = sqliteStorage
	}

	// Run automatic migrations if enabled
	if a.config.autoMigrate {
		dbType, err := migrations.DetectDBType(url)
		if err != nil {
			slog.Warn("could not detect database type for migrations", "error", err)
		} else {
			// Use custom migrations FS if provided, otherwise use embedded migrations
			migrationsFS := a.config.migrationsFS
			if migrationsFS == nil {
				migrationsFS = EmbeddedMigrationsFS()
			}

			applied, err := migrations.ApplyMigrations(a.ctx, a.storage.DB(), dbType, migrationsFS)
			if err != nil {
				return fmt.Errorf("failed to apply migrations: %w", err)
			}
			if len(applied) > 0 {
				slog.Info("applied database migrations",
					"count", len(applied),
					"versions", applied,
					"db_type", dbType)
			}
		}
	}

	// Create the replay engine
	a.engine = replay.NewEngine(a.storage, a.hooks, a.config.workerID, errorCodec{})
	if err := a.engine.ConfigureLease(a.config.lease, nil); err != nil {
		return err
	}

	return nil
}

// shouldEnableListenNotify determines if LISTEN/NOTIFY should be enabled.
// Returns true if:
//   - useListenNotify is explicitly set to true, or
//   - useListenNotify is nil (auto-detect) and isPostgres is true
func (a *App) shouldEnableListenNotify(isPostgres bool) bool {
	if a.config.useListenNotify != nil {
		return *a.config.useListenNotify
	}
	// Auto-detect: enable for PostgreSQL by default
	return isPostgres
}

// initNotifyListener initializes the PostgreSQL LISTEN/NOTIFY listener.
func (a *App) initNotifyListener(connString string) {
	a.notifyListener = notify.NewListener(
		connString,
		notify.WithReconnectDelay(a.config.notifyReconnectDelay),
	)

	// Register notification handlers
	a.notifyListener.OnNotification(notify.ChannelWorkflowResumable, a.handleWorkflowResumableNotify)
	a.notifyListener.OnNotification(notify.ChannelTimerExpired, a.handleTimerExpiredNotify)
	a.notifyListener.OnNotification(notify.ChannelChannelMessage, a.handleChannelMessageNotify)

	slog.Info("PostgreSQL LISTEN/NOTIFY configured",
		"reconnect_delay", a.config.notifyReconnectDelay)
}

// handleWorkflowResumableNotify handles workflow resumable notifications.
func (a *App) handleWorkflowResumableNotify(ctx context.Context, channel notify.NotifyChannel, payload string) {
	n, err := notify.ParseWorkflowNotification(payload)
	if err != nil {
		slog.Debug("failed to parse workflow notification", "payload", payload, "error", err)
		return
	}

	slog.Debug("received workflow resumable notification", "instance_id", n.InstanceID)

	a.wakeDispatcher()
}

// handleTimerExpiredNotify handles timer expired notifications.
func (a *App) handleTimerExpiredNotify(ctx context.Context, channel notify.NotifyChannel, payload string) {
	// Timer notifications are informational - the timer check loop will pick them up
	// We just log for debugging
	slog.Debug("received timer notification", "payload", payload)
}

// handleChannelMessageNotify handles channel message notifications.
func (a *App) handleChannelMessageNotify(ctx context.Context, channel notify.NotifyChannel, payload string) {
	n, err := notify.ParseChannelMessageNotification(payload)
	if err != nil {
		slog.Debug("failed to parse channel message notification", "payload", payload, "error", err)
		return
	}

	slog.Debug("received channel message notification",
		"channel_name", n.ChannelName, "message_id", n.MessageID, "target", n.TargetInstanceID)

	// Trigger immediate message delivery
	a.wakeWaitingChannelSubscribersForNotify(ctx, n.ChannelName, n.TargetInstanceID)
}

// wakeWaitingChannelSubscribersForNotify triggers message delivery for waiting subscribers.
func (a *App) wakeWaitingChannelSubscribersForNotify(ctx context.Context, channelName, targetInstanceID string) {
	if a.storage == nil {
		return
	}

	// If targeted message, only wake the target subscriber
	if targetInstanceID != "" {
		a.deliverToWaitingSubscriber(ctx, targetInstanceID)
		return
	}

	// Get waiting subscribers for this channel
	subs, err := a.storage.GetChannelSubscribersWaiting(ctx, channelName)
	if err != nil {
		slog.Debug("error getting waiting subscribers", "channel", channelName, "error", err)
		return
	}

	for _, sub := range subs {
		if ctx.Err() != nil {
			break
		}
		a.deliverToWaitingSubscriber(ctx, sub.InstanceID)
	}
}

// deliverToWaitingSubscriber attempts to deliver messages to a single waiting subscriber.
func (a *App) deliverToWaitingSubscriber(ctx context.Context, instanceID string) {
	if err := a.storage.MarkRunnable(ctx, instanceID); err != nil {
		slog.Debug("failed to wake subscriber", "error", err)
	}
	a.wakeDispatcher()
}

// addJitter adds random jitter (±25%) to a duration to prevent thundering herd.
// This helps distribute load when multiple workers start simultaneously.
func addJitter(d time.Duration) time.Duration {
	const jitterPercent = 0.25
	// Generate a factor between (1 - jitterPercent) and (1 + jitterPercent)
	factor := 1.0 + jitterPercent*(2*rand.Float64()-1)
	return time.Duration(float64(d) * factor)
}

// calculateBackoff computes exponential backoff with max cap.
// Used for adaptive polling when no work is available.
func calculateBackoff(base time.Duration, consecutiveEmpty int, maxBackoff time.Duration) time.Duration {
	if consecutiveEmpty == 0 {
		return base
	}
	exp := min(consecutiveEmpty, 5) // Cap exponent to prevent overflow
	backoff := time.Duration(float64(base) * math.Pow(2, float64(exp)))
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

// addJitterFraction adds random jitter as a fraction of the duration.
// jitterFraction is the maximum fraction of d to add (e.g., 0.3 for ±30%).
func addJitterFraction(d time.Duration, jitterFraction float64) time.Duration {
	jitter := time.Duration(rand.Float64() * float64(d) * jitterFraction)
	return d + jitter
}

// startBackgroundTasks starts all background goroutines.
func (a *App) startBackgroundTasks() {
	// Start PostgreSQL LISTEN/NOTIFY listener if configured
	if a.notifyListener != nil {
		if err := a.notifyListener.Start(a.ctx); err != nil {
			slog.Error("failed to start LISTEN/NOTIFY listener", "error", err)
		}
	}

	// Leader election loop (all workers participate)
	a.wg.Add(1)
	go a.runLeaderElection()

	// Workflow resumption (all workers - uses execution leases for coordination)
	a.wg.Add(1)
	go a.runWorkflowResumption()

	// Note: Leader-only tasks (timer check, event timeout, channel timeout,
	// event cleanup) are started dynamically
	// when this worker becomes the leader via runLeaderElection().
}

// runLeaderElection periodically tries to become the leader.
// Only the leader runs certain background tasks to reduce DB load.
func (a *App) runLeaderElection() {
	defer a.wg.Done()

	// Try to acquire leadership immediately on startup
	a.tryAcquireLeadership()

	ticker := time.NewTicker(addJitter(a.config.leaderHeartbeatInterval))
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			a.stopLeaderTasks()
			return
		case <-ticker.C:
			a.tryAcquireLeadership()
		}
	}
}

// tryAcquireLeadership attempts to acquire or renew leadership.
func (a *App) tryAcquireLeadership() {
	if a.storage == nil {
		return
	}

	wasLeader := a.isLeader.Load()

	acquired, err := a.storage.TryAcquireSystemLock(
		a.ctx,
		"selvedge_leader",
		a.config.workerID,
		a.config.leaderLeaseDuration,
	)
	if err != nil {
		slog.Error("leader election error", "error", err)
		return
	}

	if acquired && !wasLeader {
		// Became leader - start leader-only tasks
		slog.Info("became leader", "worker_id", a.config.workerID)
		a.isLeader.Store(true)
		a.startLeaderOnlyTasks()
	} else if !acquired && wasLeader {
		// Lost leadership - stop leader-only tasks
		slog.Info("lost leadership", "worker_id", a.config.workerID)
		a.isLeader.Store(false)
		a.stopLeaderTasks()
	}
}

// startLeaderOnlyTasks starts tasks that should only run on the leader.
func (a *App) startLeaderOnlyTasks() {
	a.leaderMu.Lock()
	defer a.leaderMu.Unlock()

	if a.leaderTasksRunning {
		return
	}

	// Create a cancellable context for leader tasks
	a.leaderCtx, a.leaderCancel = context.WithCancel(a.ctx)
	a.leaderTasksRunning = true

	// Timer check
	a.wg.Add(1)
	go a.runTimerCheckWithContext(a.leaderCtx)

	// Event timeout check
	a.wg.Add(1)
	go a.runEventTimeoutCheckWithContext(a.leaderCtx)

	// Channel timeout check
	a.wg.Add(1)
	go a.runChannelTimeoutCheckWithContext(a.leaderCtx)

	// Channel message cleanup
	a.wg.Add(1)
	go a.runChannelCleanupWithContext(a.leaderCtx)

	slog.Debug("started leader-only tasks")
}

// stopLeaderTasks stops all leader-only tasks.
func (a *App) stopLeaderTasks() {
	a.leaderMu.Lock()
	defer a.leaderMu.Unlock()

	if !a.leaderTasksRunning {
		return
	}

	if a.leaderCancel != nil {
		a.leaderCancel()
		a.leaderCancel = nil
	}
	a.leaderTasksRunning = false

	slog.Debug("stopped leader-only tasks")
}

// runTimerCheckWithContext periodically checks for expired timers with a cancellable context.
func (a *App) runTimerCheckWithContext(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(addJitter(a.config.timerCheckInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.checkExpiredTimers(); err != nil {
				slog.Error("error checking expired timers", "error", err)
			}
		}
	}
}

// checkExpiredTimers processes expired timers.
func (a *App) checkExpiredTimers() error {
	if a.storage == nil {
		return nil
	}

	timers, err := a.storage.FindExpiredTimers(a.ctx, a.config.maxTimersPerBatch)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for _, timer := range timers {
		if a.ctx.Err() != nil {
			break
		}

		if err := a.timerSem.Acquire(a.ctx, 1); err != nil {
			break
		}

		wg.Add(1)
		go func(timer *storage.TimerSubscription) {
			defer wg.Done()
			defer a.timerSem.Release(1)

			if err := a.handleExpiredTimer(timer); err != nil {
				slog.Error("error handling timer", "timer_id", timer.TimerID, "error", err)
			}
		}(timer)
	}

	wg.Wait()
	return nil
}

// handleExpiredTimer handles a single expired timer.
func (a *App) handleExpiredTimer(timer *storage.TimerSubscription) error {
	instance, err := a.storage.GetInstance(a.ctx, timer.InstanceID)
	if err != nil {
		return err
	}
	workflowName := ""
	if instance != nil {
		workflowName = instance.WorkflowName
	}
	if err := a.storage.FireTimer(a.ctx, timer); err != nil {
		return err
	}
	a.config.hooks.OnTimerFired(a.ctx, hooks.TimerFiredInfo{
		InstanceID: timer.InstanceID, WorkflowName: workflowName, TimerID: timer.TimerID, FiredAt: time.Now(),
	})
	a.wakeDispatcher()
	return nil
}

// runEventTimeoutCheckWithContext periodically checks for event timeouts with a cancellable context.
func (a *App) runEventTimeoutCheckWithContext(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(addJitter(a.config.eventTimeoutInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.checkEventTimeouts(); err != nil {
				slog.Error("error checking event timeouts", "error", err)
			}
		}
	}
}

// checkEventTimeouts is deprecated - event timeouts are now handled through
// the channel subscription system via checkChannelTimeouts.
// This function now delegates to checkChannelTimeouts for backward compatibility.
func (a *App) checkEventTimeouts() error {
	// Events are now delivered via the channel system.
	// Timeouts are handled by checkChannelTimeouts.
	return a.checkChannelTimeouts()
}

// registerWorkflowWithRunner registers a workflow with its runner function.
// The runner bridges the generic workflow to the replay engine.
func (a *App) registerWorkflowWithRunner(name string, workflow any, runner replay.WorkflowRunner) {
	a.workflowsMu.Lock()
	a.workflows[name] = workflow
	a.workflowsMu.Unlock()

	a.workflowRunnersMu.Lock()
	a.workflowRunners[name] = runner
	a.workflowRunnersMu.Unlock()
}

// startWorkflow starts a new workflow instance.
func (a *App) startWorkflow(ctx context.Context, workflowName string, input any, opts *startOptions) (string, error) {
	// Generate instance ID if not provided
	instanceID := opts.instanceID
	if instanceID == "" {
		instanceID = uuid.New().String()
	}

	// Serialize input
	inputData, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("failed to serialize input: %w", err)
	}

	// Create instance in storage
	now := time.Now()
	instance := &storage.WorkflowInstance{
		InstanceID:   instanceID,
		WorkflowName: workflowName,
		Status:       storage.StatusRunning,
		InputData:    inputData,
		StartedAt:    now,
		UpdatedAt:    now,
	}

	if a.storage != nil {
		if err := a.storage.CreateInstance(ctx, instance); err != nil {
			return "", fmt.Errorf("failed to create instance: %w", err)
		}
	}

	// Call hook
	a.hooks.OnWorkflowStart(ctx, hooks.WorkflowStartInfo{
		InstanceID:   instanceID,
		WorkflowName: workflowName,
		InputData:    input,
		StartTime:    now,
	})

	a.wakeDispatcher()

	return instanceID, nil
}

// resumeWorkflow dispatches a registered workflow through the lease protocol.
func (a *App) resumeWorkflow(ctx context.Context, instanceID string) error {
	// Get the instance to find the workflow name
	instance, err := a.storage.GetInstance(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("failed to get instance: %w", err)
	}
	if instance == nil {
		return fmt.Errorf("instance %s not found", instanceID)
	}

	// Get the runner for this workflow
	a.workflowRunnersMu.RLock()
	runner, ok := a.workflowRunners[instance.WorkflowName]
	a.workflowRunnersMu.RUnlock()

	if !ok {
		return fmt.Errorf("workflow %s not registered", instance.WorkflowName)
	}

	return a.engine.Run(ctx, instanceID, runner)
}

// GetInstance retrieves a workflow instance by ID.
func (a *App) GetInstance(ctx context.Context, instanceID string) (*storage.WorkflowInstance, error) {
	if a.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	instance, err := a.storage.GetInstance(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	if instance == nil {
		return nil, &WorkflowNotFoundError{InstanceID: instanceID}
	}

	return instance, nil
}

// FindInstances searches for workflow instances with input filters.
// This is a convenience method for searching workflows by their input data.
//
// The inputFilters parameter maps JSON paths to expected values:
//   - "customer.id" -> "12345" matches input like {"customer": {"id": "12345"}}
//   - "status" -> "active" matches input like {"status": "active"}
//
// Values are compared with exact match. Supported value types:
//   - string: Exact string match
//   - int/float64: Numeric comparison
//   - bool: Boolean comparison
//   - nil: Matches null values
//
// Example:
//
//	instances, err := app.FindInstances(ctx, map[string]any{
//	    "order.customer_id": "cust_123",
//	    "order.status": "pending",
//	})
func (a *App) FindInstances(ctx context.Context, inputFilters map[string]any) ([]*storage.WorkflowInstance, error) {
	return a.FindInstancesWithOptions(ctx, storage.ListInstancesOptions{
		InputFilters: inputFilters,
	})
}

// FindInstancesWithOptions searches for workflow instances with full options.
// Use this when you need pagination, status filters, or other advanced options
// in addition to input filters.
func (a *App) FindInstancesWithOptions(ctx context.Context, opts storage.ListInstancesOptions) ([]*storage.WorkflowInstance, error) {
	if a.storage == nil {
		return nil, fmt.Errorf("storage not initialized")
	}

	result, err := a.storage.ListInstances(ctx, opts)
	if err != nil {
		return nil, err
	}

	return result.Instances, nil
}

// cancelWorkflow durably requests cancellation and wakes dispatch.
func (a *App) cancelWorkflow(ctx context.Context, instanceID, reason string) error {
	if a.storage == nil {
		return fmt.Errorf("storage not initialized")
	}
	status, err := a.storage.RequestCancel(ctx, instanceID, reason)
	if errors.Is(err, storage.ErrInstanceNotFound) {
		return &WorkflowNotFoundError{InstanceID: instanceID}
	}
	if err != nil {
		return err
	}
	switch status {
	case storage.StatusCompleted:
		return ErrWorkflowAlreadyCompleted
	case storage.StatusFailed:
		return ErrWorkflowAlreadyFailed
	case storage.StatusRunning:
		a.wakeDispatcher()
	}
	return nil
}

// DeliverEvent delivers an event to workflows waiting for eventType via WaitEvent.
// It records the event in the existing envelope format and wakes waiting workflows.
func (a *App) DeliverEvent(ctx context.Context, eventType, source string, data any) error {
	eventID := uuid.New().String()

	// Call OnEventReceived hook
	a.hooks.OnEventReceived(ctx, hooks.EventReceivedInfo{
		EventType:   eventType,
		EventID:     eventID,
		EventSource: source,
		InstanceID:  "", // Not known at this point
	})

	return a.deliverEventToWaitingWorkflows(ctx, eventID, eventType, source, data)
}

// deliverEventToWaitingWorkflows delivers an event to workflows waiting for it.
// WaitEvent subscribes to the event type using the internal delivery primitive.
func (a *App) deliverEventToWaitingWorkflows(ctx context.Context, eventID, eventType, source string, data any) error {
	if a.storage == nil {
		return nil
	}

	// Prepare event data in ReceivedEvent format
	eventData := map[string]any{
		"id":          eventID,
		"type":        eventType,
		"source":      source,
		"specversion": "1.0",
	}
	if data != nil {
		eventData["data"] = data
	}

	dataJSON, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to serialize event data: %w", err)
	}

	channelName := eventType

	// Publish to channel using eventType as channel name
	_, err = a.storage.PublishToChannel(ctx, channelName, dataJSON, nil)
	if err != nil {
		return fmt.Errorf("failed to publish event to channel: %w", err)
	}

	// Wake up waiting subscribers using the unified channel system
	// The durable input has already marked subscribers runnable.
	a.wakeWaitingChannelSubscribers(ctx, channelName)

	return nil
}

// wakeWaitingChannelSubscribers delivers a message to waiting channel subscribers.
func (a *App) wakeWaitingChannelSubscribers(ctx context.Context, channelName string) {
	subs, err := a.storage.GetChannelSubscribersWaiting(ctx, channelName)
	if err != nil {
		return
	}
	for _, sub := range subs {
		a.deliverToWaitingSubscriber(ctx, sub.InstanceID)
	}
}

// Storage returns the storage instance for advanced usage.
func (a *App) Storage() storage.Storage {
	return a.storage
}

// WorkerID returns the worker ID.
func (a *App) WorkerID() string {
	return a.config.workerID
}

// ========================================
// Channel Message Handling
// ========================================

// runChannelTimeoutCheckWithContext periodically checks for channel subscription timeouts with a cancellable context.
func (a *App) runChannelTimeoutCheckWithContext(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(addJitter(a.config.messageCheckInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.checkChannelTimeouts(); err != nil {
				slog.Error("error checking channel timeouts", "error", err)
			}
		}
	}
}

// checkChannelTimeouts handles expired channel subscriptions.
func (a *App) checkChannelTimeouts() error {
	if a.storage == nil {
		return nil
	}

	// Check for expired channel subscriptions
	expiredSubs, err := a.storage.FindExpiredChannelSubscriptions(a.ctx, a.config.maxMessagesPerBatch)
	if err != nil {
		return fmt.Errorf("failed to find expired channel subscriptions: %w", err)
	}

	var wg sync.WaitGroup
	for _, sub := range expiredSubs {
		if a.ctx.Err() != nil {
			break
		}

		if err := a.messageSem.Acquire(a.ctx, 1); err != nil {
			break
		}

		wg.Add(1)
		go func(sub *storage.ChannelSubscription) {
			defer wg.Done()
			defer a.messageSem.Release(1)

			if err := a.handleChannelTimeout(sub); err != nil {
				slog.Error("error handling channel timeout", "instance_id", sub.InstanceID, "error", err)
			}
		}(sub)
	}

	wg.Wait()
	return nil
}

// handleChannelTimeout handles a timed-out channel subscription.
// Instead of failing the workflow directly, we record the timeout error in history
// and set the workflow to 'running' so it can be resumed and handle the error gracefully.
func (a *App) handleChannelTimeout(sub *storage.ChannelSubscription) error {
	// Call OnEventTimeout hook
	instance, _ := a.storage.GetInstance(a.ctx, sub.InstanceID)
	workflowName := ""
	if instance != nil {
		workflowName = instance.WorkflowName
	}
	a.hooks.OnEventTimeout(a.ctx, hooks.EventTimeoutInfo{
		InstanceID:   sub.InstanceID,
		WorkflowName: workflowName,
		EventType:    sub.Channel,
	})

	// Record timeout error in history using the activity_id from the subscription
	// This allows Receive to return the timeout error when the workflow resumes
	activityID := sub.ActivityID
	if activityID == "" {
		activityID = fmt.Sprintf("receive_%s:timeout", sub.Channel)
	}

	timeoutErr := &ChannelMessageTimeoutError{
		InstanceID:  sub.InstanceID,
		ChannelName: sub.Channel,
	}
	errJSON, _ := json.Marshal(timeoutErr.Error())

	historyEvent := &storage.HistoryEvent{
		InstanceID: sub.InstanceID,
		ActivityID: activityID,
		EventType:  storage.HistoryMessageTimeout,
		Command:    "receive:" + sub.Channel,
		EventData:  errJSON,
		DataType:   "json",
	}
	if _, err := a.storage.RecordHistory(a.ctx, nil, historyEvent); err != nil {
		return fmt.Errorf("failed to record timeout error: %w", err)
	}

	a.wakeDispatcher()
	return a.storage.MarkRunnable(a.ctx, sub.InstanceID)
}

// ========================================
// Workflow Resumption (Load Balancing)
// ========================================

// runWorkflowResumption periodically checks for workflows ready to be resumed.
// This task dispatches runnable workflows and recovers expired leases. This is essential for load
// balancing in multi-worker environments - any worker can pick up and resume
// the workflow, not just the one that delivered the message.
//
// Uses adaptive backoff to reduce DB load when no work is available.
func (a *App) runWorkflowResumption() {
	defer a.wg.Done()

	consecutiveEmpty := 0
	const maxBackoff = 60 * time.Second

	for {
		select {
		case <-a.ctx.Done():
			return
		default:
		}

		count, err := a.resumeResumableWorkflows()
		switch {
		case err != nil:
			slog.Error("error resuming workflows", "error", err)
			consecutiveEmpty++
		case count == 0:
			consecutiveEmpty++
		default:
			consecutiveEmpty = 0
		}

		// Calculate adaptive backoff
		backoff := calculateBackoff(a.config.workflowResumptionInterval, consecutiveEmpty, maxBackoff)
		waitTime := addJitterFraction(backoff, 0.3)

		// Wait with optional NOTIFY wake
		select {
		case <-a.resumeWakeEvent:
			consecutiveEmpty = 0
			slog.Debug("workflow resumption woken by NOTIFY")
		case <-time.After(waitTime):
		case <-a.ctx.Done():
			return
		}
	}
}

// resumeResumableWorkflows finds and resumes workflows that are ready.
// Returns the number of registered workflows dispatched and any error that occurred.
func (a *App) resumeResumableWorkflows() (int, error) {
	if a.storage == nil {
		return 0, nil
	}

	// Find runnable instances with free leases and orphaned expired leases.
	a.workflowRunnersMu.RLock()
	names := make([]string, 0, len(a.workflowRunners))
	for name := range a.workflowRunners {
		names = append(names, name)
	}
	a.workflowRunnersMu.RUnlock()
	workflows, err := a.storage.FindDispatchable(a.ctx, a.config.maxWorkflowsPerBatch, names...)
	if err != nil {
		return 0, fmt.Errorf("failed to find resumable workflows: %w", err)
	}

	if len(workflows) == 0 {
		return 0, nil
	}

	dispatched := 0
	for _, wf := range workflows {
		// Check context before acquiring semaphore
		if a.ctx.Err() != nil {
			break
		}

		// Acquire semaphore (blocks if at capacity)
		if err := a.resumptionSem.Acquire(a.ctx, 1); err != nil {
			// Context cancelled
			break
		}

		dispatched++
		a.wg.Add(1)
		go func(wf string) {
			defer a.wg.Done()
			defer a.resumptionSem.Release(1)

			// Error is expected when another worker picks up the workflow first
			if err := a.resumeWorkflow(a.ctx, wf); err == nil {
				a.wakeDispatcher()
			}
		}(wf)
	}

	return dispatched, nil
}

// ========================================
// Channel Message Cleanup
// ========================================

// runChannelCleanupWithContext periodically cleans up old channel messages with a cancellable context.
func (a *App) runChannelCleanupWithContext(ctx context.Context) {
	defer a.wg.Done()

	ticker := time.NewTicker(addJitter(a.config.channelCleanupInterval))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Leader runs directly (no singleton runner needed when using leader election)
			if err := a.cleanupChannelMessages(); err != nil {
				slog.Error("error cleaning up channel messages", "error", err)
			}
		}
	}
}

// cleanupChannelMessages removes old channel messages.
func (a *App) cleanupChannelMessages() error {
	if a.storage == nil {
		return nil
	}

	if err := a.storage.CleanupOldChannelMessages(a.ctx, a.config.channelMessageRetention); err != nil {
		return fmt.Errorf("failed to cleanup channel messages: %w", err)
	}

	return nil
}

func (a *App) wakeDispatcher() {
	select {
	case a.resumeWakeEvent <- struct{}{}:
	default:
	}
}
