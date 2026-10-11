package selvedge

import (
	"context"
	"testing"
	"time"

	"github.com/CaliLuke/selvedge/hooks"
	"github.com/CaliLuke/selvedge/internal/replay"
	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestReceiveGoEnvelopeOnly(t *testing.T) {
	testReceiveGoEnvelopeOnly(t, sqliteSchemaBackend)
}

func testReceiveGoEnvelopeOnly(t *testing.T, factory schemaBackendFactory) {
	t.Helper()
	for _, tc := range []struct {
		name, envelope string
		wantError      bool
	}{
		{"go", `{"id":7,"channel_name":"messages","data":"value","created_at":"2026-10-11T00:00:00Z"}`, false},
		{"foreign", `{"id":"uuid","channel":"messages","data":"value","published_at":"2026-10-11T00:00:00Z"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := factory(t)
			ctx := context.Background()
			if err := b.initialize(ctx); err != nil {
				t.Fatal(err)
			}
			if err := b.store.CreateInstance(ctx, &storage.WorkflowInstance{InstanceID: "receive", WorkflowName: "receiver", Status: storage.StatusRunning, InputData: []byte(`{}`), StartedAt: time.Now(), UpdatedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			if err := b.store.AppendHistory(ctx, &storage.HistoryEvent{InstanceID: "receive", ActivityID: "receive_messages:1", EventType: storage.HistoryChannelMessageReceived, DataType: "json", EventData: []byte(tc.envelope)}); err != nil {
				t.Fatal(err)
			}
			engine := replay.NewEngine(b.store, &hooks.NoOpHooks{}, "worker", errorCodec{})
			err := engine.ResumeWorkflow(ctx, "receive", func(ec *replay.ExecutionContext) (any, error) {
				msg, err := receiveEvent[string](NewWorkflowContextFromExecution(ec), "messages")
				if err != nil {
					return nil, err
				}
				if msg.ID != 7 || msg.ChannelName != "messages" || msg.Data != "value" || !msg.CreatedAt.Equal(time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)) {
					t.Errorf("wrong replayed Go envelope: %+v", msg)
				}
				return msg.Data, nil
			})
			if (err != nil) != tc.wantError {
				t.Fatalf("receive error = %v, wantError %v", err, tc.wantError)
			}
		})
	}
}
