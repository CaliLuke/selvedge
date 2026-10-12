package replay

import "fmt"

// NondeterminismError identifies the first divergent command position.
type NondeterminismError struct {
	Seq                           int64
	Key, Recorded, Issued, Reason string
}

func (e *NondeterminismError) Error() string {
	return fmt.Sprintf("nondeterminism at position %d (key %q): recorded %q, issued %q: %s", e.Seq, e.Key, e.Recorded, e.Issued, e.Reason)
}
func (e *NondeterminismError) Is(target error) bool { return target == ErrDeterminismViolation }
