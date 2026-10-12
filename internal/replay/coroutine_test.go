package replay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoroutineHandoffAndClose(t *testing.T) {
	ec := NewExecutionContext(context.Background(), "coroutine")
	deferStarted, finishDefer := make(chan struct{}), make(chan struct{})
	// Ensure a failed assertion cannot leave the test coroutine blocked.
	defer close(finishDefer)
	var recovered any
	var closeCommandErr error
	var afterYield bool
	wait := Wait{Seq: 1, Kind: WaitSignal, Key: "#1"}
	co := startCoroutine(ec, func(ec *ExecutionContext) (any, error) {
		defer func() {
			recovered = recover()
			_, closeCommandErr = ec.Issue("activity:deferred", "")
			close(deferStarted)
			<-finishDefer
		}()
		cmd, err := ec.Issue("receive:messages", "")
		if err != nil {
			return nil, err
		}
		defer cmd.End()
		err = ec.Yield(wait)
		afterYield = true
		return nil, err
	})
	outcome := <-co.outcomes
	require.Equal(t, &wait, outcome.wait)
	require.EqualValues(t, 1, ec.Position())
	require.False(t, ec.running.Load())
	_, err := ec.Issue("activity:foreign", "")
	var detail *NondeterminismError
	require.ErrorAs(t, err, &detail)
	require.Equal(t, "command issued off the workflow coroutine", detail.Reason)
	closed := make(chan struct{})
	go func() { co.closeBlocked(ec); close(closed) }()
	<-deferStarted
	require.Nil(t, recovered)
	require.ErrorIs(t, closeCommandErr, errExecutionClosed)
	require.Equal(t, detail, ec.Fatal(), "closure commands must not change the failure")
	select {
	case <-closed:
		t.Fatal("close returned before the workflow defer finished")
	default:
	}
	// Unblock using a send; the deferred close remains safe on assertion failure.
	finishDefer <- struct{}{}
	<-closed
	require.False(t, afterYield)
	require.Nil(t, co.closeErr)
	require.False(t, ec.inCommand.Load())
}

func TestCoroutineDoneRejectsLaterCommand(t *testing.T) {
	ec := NewExecutionContext(context.Background(), "done")
	co := startCoroutine(ec, func(*ExecutionContext) (any, error) { return "done", nil })
	outcome := <-co.outcomes
	<-co.exited
	require.Nil(t, outcome.wait)
	require.Equal(t, "done", outcome.result)
	require.NoError(t, outcome.err)
	_, err := ec.Issue("activity:late", "")
	var detail *NondeterminismError
	require.ErrorAs(t, err, &detail)
	require.Equal(t, "command issued off the workflow coroutine", detail.Reason)
	require.Zero(t, ec.Position())
}
