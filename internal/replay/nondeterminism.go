package replay

import "fmt"

// NondeterminismError identifies the first divergent command position.
type NondeterminismError struct {
	Seq      int64  `json:"seq"`
	Key      string `json:"key"`
	Recorded string `json:"recorded"`
	Issued   string `json:"issued"`
	Reason   string `json:"reason"`
}

func (e *NondeterminismError) Error() string {
	return fmt.Sprintf("nondeterminism at position %d (key %q): recorded %q, issued %q: %s", e.Seq, e.Key, e.Recorded, e.Issued, e.Reason)
}
func (e *NondeterminismError) Is(target error) bool { return target == ErrDeterminismViolation }
