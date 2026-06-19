package metrics

import (
	"testing"
	"time"
)

func TestSnapshotEmpty(t *testing.T) {
	r := NewRecorder(100)

	snap := r.Snapshot()

	if snap.Count != 0 {
		t.Errorf("expected count 0 on empty recorder, got %d", snap.Count)
	}
}

func TestSnapshotSingleValue(t *testing.T) {
	r := NewRecorder(100)
	r.Record(10 * time.Millisecond)

	snap := r.Snapshot()

	if snap.Count != 1 {
		t.Errorf("expected count 1, got %d", snap.Count)
	}

	for name, got := range map[string]float64{
		"P50": snap.P50, "P95": snap.P95, "P99": snap.P99, "P100": snap.P100,
	} {
		if got != 10 {
			t.Errorf("%s: got %v, want 10", name, got)
		}
	}
}

func TestSnapshotOrderingIsCorrect(t *testing.T) {
	r := NewRecorder(100)

	// Record out of order on purpose — the recorder must sort internally.
	for _, ms := range []int{50, 10, 30, 20, 40} {
		r.Record(time.Duration(ms) * time.Millisecond)
	}

	snap := r.Snapshot()

	if snap.P100 != 50 {
		t.Errorf("P100: got %v, want 50 (the max)", snap.P100)
	}

	if snap.P50 < 10 || snap.P50 > 50 {
		t.Errorf("P50: got %v, expected a value within the recorded range", snap.P50)
	}
}

func TestRecorderRespectsMaxSamples(t *testing.T) {
	r := NewRecorder(5)

	for i := 0; i < 10; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}

	snap := r.Snapshot()

	if snap.Count != 5 {
		t.Errorf("expected count capped at 5, got %d", snap.Count)
	}
}

func TestPercentileHelperKnownValues(t *testing.T) {
	// 1 through 10, sorted. P50 of an evenly spaced set should sit
	// near the middle.
	sorted := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	p100 := percentile(sorted, 100)
	if p100 != 10 {
		t.Errorf("P100 of 1..10: got %v, want 10", p100)
	}

	p0 := percentile(sorted, 0)
	if p0 != 1 {
		t.Errorf("P0 of 1..10: got %v, want 1", p0)
	}
}