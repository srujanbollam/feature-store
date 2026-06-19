package metrics

import (
	"sort"
	"sync"
	"time"
)

// Recorder tracks request latencies in memory and computes percentiles
// on demand. It keeps only the most recent maxSamples entries so memory
// usage stays bounded under sustained traffic.
type Recorder struct {
	mu         sync.Mutex
	durations  []float64 // milliseconds
	maxSamples int
}

// NewRecorder creates a Recorder that retains up to maxSamples latencies.
func NewRecorder(maxSamples int) *Recorder {
	return &Recorder{
		durations:  make([]float64, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// Record stores one request's duration.
func (r *Recorder) Record(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ms := float64(d.Microseconds()) / 1000.0

	if len(r.durations) >= r.maxSamples {
		// Drop the oldest sample to make room — keeps memory bounded.
		r.durations = r.durations[1:]
	}
	r.durations = append(r.durations, ms)
}

// Snapshot is a point-in-time view of latency percentiles, in milliseconds.
type Snapshot struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	P100  float64 `json:"p100_ms"`
}

// Snapshot computes current percentiles from recorded samples.
func (r *Recorder) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := len(r.durations)
	if n == 0 {
		return Snapshot{}
	}

	sorted := make([]float64, n)
	copy(sorted, r.durations)
	sort.Float64s(sorted)

	return Snapshot{
		Count: n,
		P50:   percentile(sorted, 50),
		P95:   percentile(sorted, 95),
		P99:   percentile(sorted, 99),
		P100:  percentile(sorted, 100),
	}
}

// percentile returns the value at the given percentile (0-100) from an
// already-sorted slice.
func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if n == 1 {
		return sorted[0]
	}

	index := (p / 100.0) * float64(n-1)
	lower := int(index)
	upper := lower + 1
	if upper >= n {
		return sorted[n-1]
	}

	// Linear interpolation between the two nearest ranks for accuracy
	// when the exact percentile falls between two samples.
	weight := index - float64(lower)
	return sorted[lower] + weight*(sorted[upper]-sorted[lower])
}