package selvedge

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

type s13Pause struct {
	RunID, Reason, RequestedBy string
	Labels                     map[string]string
}
type s13Resume struct {
	RunID, Notes, RequestedBy string
	Labels                    map[string]string
}
type s13Clarification struct{ RunID, ID, Answer, AnswerRef string }
type s13ToolResult struct {
	Name, ToolCallID string
	Result           json.RawMessage
}
type s13ToolResults struct {
	RunID, ID string
	Results   []*s13ToolResult
}
type s13Confirmation struct {
	RunID, ID string
	Approved  bool
	Labels    map[string]string
}
type s13TypedAnswer struct {
	RunID, ID, PayloadRef string
	Payload               json.RawMessage
}
type s13Signals struct {
	Pause         *s13Pause
	Resume        *s13Resume
	Clarification *s13Clarification
	Tools         *s13ToolResults
	Confirmation  *s13Confirmation
	Typed         *s13TypedAnswer
}

var s13Channels = []string{"loom_mcp.runtime.pause", "loom_mcp.runtime.resume", "loom_mcp.runtime.provide.clarification", "loom_mcp.runtime.provide.toolresults", "loom_mcp.runtime.provide.confirmation", "loom_mcp.runtime.provide.typed_input"}

func s13ReceiveSignals(ctx *WorkflowContext) (s13Signals, error) {
	var result s13Signals
	var err error
	result.Pause, err = Receive[*s13Pause](ctx, s13Channels[0])
	if err != nil {
		return result, err
	}
	result.Resume, err = Receive[*s13Resume](ctx, s13Channels[1])
	if err != nil {
		return result, err
	}
	result.Clarification, err = Receive[*s13Clarification](ctx, s13Channels[2])
	if err != nil {
		return result, err
	}
	result.Tools, err = Receive[*s13ToolResults](ctx, s13Channels[3])
	if err != nil {
		return result, err
	}
	result.Confirmation, err = Receive[*s13Confirmation](ctx, s13Channels[4])
	if err != nil {
		return result, err
	}
	result.Typed, err = Receive[*s13TypedAnswer](ctx, s13Channels[5])
	return result, err
}
func TestChannelsHaveSeparateCursors_LoomChannels(t *testing.T) {
	s13SeparateChannels(t, createTestApp)
}
func s13SeparateChannels(t *testing.T, factory replayAppFactory) {
	h := newTypedReplayHarness(t, factory)
	client := s13ClientFor(t, h.app)
	RegisterWorkflow(h.app, DefineWorkflow("s13_channels", func(ctx *WorkflowContext, _ string) ([]s13Signals, error) {
		first, err := s13ReceiveSignals(ctx)
		if err != nil {
			return nil, err
		}
		second, err := s13ReceiveSignals(ctx)
		return []s13Signals{first, second}, err
	}))
	id, err := StartWorkflowByName(h.app.ctx, client, "s13_channels", "")
	require.NoError(t, err)
	expected := s13Signals{
		Pause:         &s13Pause{RunID: id, Reason: "operator", RequestedBy: "user", Labels: map[string]string{"label": "value"}},
		Resume:        &s13Resume{RunID: id, Notes: "continue", RequestedBy: "user"},
		Clarification: &s13Clarification{RunID: id, ID: "question", Answer: "answer", AnswerRef: "sha256:answer"},
		Tools:         &s13ToolResults{RunID: id, ID: "external", Results: []*s13ToolResult{{Name: "echo", ToolCallID: "call", Result: json.RawMessage(`{"ok":true}`)}}},
		Confirmation:  &s13Confirmation{RunID: id, ID: "confirmation", Approved: true},
		Typed:         &s13TypedAnswer{RunID: id, ID: "typed", PayloadRef: "sha256:typed", Payload: json.RawMessage(`{"answer":[1,true]}`)},
	}
	values := []any{expected.Pause, expected.Resume, expected.Clarification, expected.Tools, expected.Confirmation, expected.Typed}
	for _, i := range []int{5, 1, 3, 0, 4, 2} {
		require.NoError(t, client.Signal(h.app.ctx, id, s13Channels[i], values[i]))
	}
	for _, i := range []int{3, 2, 4, 1, 0, 5} {
		require.NoError(t, client.Signal(h.app.ctx, id, s13Channels[i], nil))
	}
	require.NoError(t, h.RunOnce(id))
	result := s13Wait[[]s13Signals](t, client, id)
	require.NoError(t, result.Error)
	require.Equal(t, []s13Signals{expected, {}}, result.Output)
	var pending int
	require.NoError(t, client.storage.DB().QueryRow("SELECT count(*) FROM workflow_signals WHERE consumed_key IS NULL").Scan(&pending))
	require.Zero(t, pending)
}
