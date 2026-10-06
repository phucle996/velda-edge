package metrics

import (
	"sync/atomic"
)

// Snapshot represents a point-in-time view of server metrics.
type Snapshot struct {
	TotalRequests uint64 `json:"total_requests"`
	TotalBytesIn  uint64 `json:"total_bytes_in"`
	TotalBytesOut uint64 `json:"total_bytes_out"`
	ActiveConns   int64  `json:"active_conns"`
}

// Tracker provides atomic telemetry counters across all transports.
type Tracker struct {
	totalRequests uint64
	totalBytesIn  uint64
	totalBytesOut uint64
	activeConns   int64
}

// NewTracker creates an initialized Tracker.
func NewTracker() *Tracker {
	return &Tracker{}
}

// IncRequests increments total request count.
func (t *Tracker) IncRequests() {
	atomic.AddUint64(&t.totalRequests, 1)
}

// AddBytesIn adds received bytes to total.
func (t *Tracker) AddBytesIn(n uint64) {
	atomic.AddUint64(&t.totalBytesIn, n)
}

// AddBytesOut adds transmitted bytes to total.
func (t *Tracker) AddBytesOut(n uint64) {
	atomic.AddUint64(&t.totalBytesOut, n)
}

// IncConns increments active connection gauge.
func (t *Tracker) IncConns() {
	atomic.AddInt64(&t.activeConns, 1)
}

// DecConns decrements active connection gauge.
func (t *Tracker) DecConns() {
	atomic.AddInt64(&t.activeConns, -1)
}

// Snapshot returns the current metric values.
func (t *Tracker) Snapshot() Snapshot {
	return Snapshot{
		TotalRequests: atomic.LoadUint64(&t.totalRequests),
		TotalBytesIn:  atomic.LoadUint64(&t.totalBytesIn),
		TotalBytesOut: atomic.LoadUint64(&t.totalBytesOut),
		ActiveConns:   atomic.LoadInt64(&t.activeConns),
	}
}
