package replay

import "github.com/CaliLuke/selvedge/internal/storage"

// Delivered reads only this execution's position-indexed view. The overlay is
// visible exclusively during a live Await predicate evaluation.
func (ec *ExecutionContext) Delivered(key string) bool {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if seq := ec.deliveries[key]; seq > 0 && seq <= ec.seq {
		return true
	}
	if ec.overlay != nil {
		for _, k := range ec.overlay.HistoryKeys {
			if k == key {
				return true
			}
		}
	}
	return false
}
func signalVisible(p storage.SignalPosition, q int64) bool {
	return p.DeliveredSeq > 0 && p.DeliveredSeq <= q && (p.ConsumedSeq == 0 || p.ConsumedSeq > q)
}
func (ec *ExecutionContext) PendingSignals(channel string) int {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	n := 0
	for _, p := range ec.signals {
		if p.Channel == channel && signalVisible(p, ec.seq) {
			n++
		}
	}
	if ec.overlay != nil {
		for _, p := range ec.overlay.Signals {
			if p.Channel == channel {
				n++
			}
		}
	}
	return n
}
func (ec *ExecutionContext) NextDeliveredSignal(channel string) (int64, bool) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	var id int64
	for _, p := range ec.signals {
		if p.Channel == channel && signalVisible(p, ec.seq) && (id == 0 || p.ID < id) {
			id = p.ID
		}
	}
	return id, id != 0
}
func (ec *ExecutionContext) ApplyDelivery(seq int64, b storage.DeliveryBatch) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	for _, key := range b.HistoryKeys {
		if ec.deliveries[key] == 0 {
			ec.deliveries[key] = seq
		}
	}
	for _, p := range b.Signals {
		if _, ok := ec.signals[p.ID]; !ok {
			ec.signals[p.ID] = storage.SignalPosition{ID: p.ID, Channel: p.Channel, DeliveredSeq: seq}
		}
	}
}
func (ec *ExecutionContext) ConsumeDeliveredSignal(id, seq int64) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	p := ec.signals[id]
	p.ConsumedSeq = seq
	ec.signals[id] = p
}

// WithDeliveryCandidate evaluates fn against a temporary delivery overlay. Even
// a panic clears it before the coroutine reports the failure. A nil batch runs
// the W2 pre-check with the same predicate guard and no overlay.
func (ec *ExecutionContext) WithDeliveryCandidate(b *storage.DeliveryBatch, fn func() bool) bool {
	ec.mu.Lock()
	ec.overlay = b
	ec.mu.Unlock()
	ec.inPredicate.Store(true)
	defer func() { ec.inPredicate.Store(false); ec.mu.Lock(); ec.overlay = nil; ec.mu.Unlock() }()
	return fn()
}
