package selvedge

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/CaliLuke/selvedge/internal/storage"
)

func TestDecodeRecordedInvalidJSON(t *testing.T) {
	for _, payload := range []string{"", "{", `{"A":"wrong"}`} {
		t.Run(payload, func(t *testing.T) {
			event := &storage.HistoryEvent{InstanceID: "instance", ActivityID: "key", EventData: []byte(payload)}
			value, err := decodeRecorded[struct{ A int }](event)
			require.Zero(t, value)
			var decodeErr *ReplayDecodeError
			require.ErrorAs(t, err, &decodeErr)
			require.Equal(t, "instance", decodeErr.InstanceID)
			require.Equal(t, "key", decodeErr.Key)
			require.NotEmpty(t, decodeErr.GoType)
			require.NotNil(t, decodeErr.Unwrap())
		})
	}
}

func TestDecodeRecordedAllowsUnknownFields(t *testing.T) {
	value, err := decodeRecorded[struct{ A int }](&storage.HistoryEvent{EventData: []byte(`{"A":3,"future_field":true}`)})
	require.NoError(t, err)
	require.Equal(t, 3, value.A)
}

func TestDecodeRecordedNull(t *testing.T) {
	value, err := decodeRecorded[*int64](&storage.HistoryEvent{EventData: []byte(`null`)})
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestDecodeRecordedInterfaceErrorType(t *testing.T) {
	_, err := decodeRecorded[any](&storage.HistoryEvent{EventData: []byte(`{`)})
	var decodeErr *ReplayDecodeError
	require.ErrorAs(t, err, &decodeErr)
	require.Equal(t, "interface {}", decodeErr.GoType)
	var syntaxErr *json.SyntaxError
	require.ErrorAs(t, err, &syntaxErr)
}
