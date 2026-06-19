package ml

import (
	"testing"

	"feature-store/store"
)

func newTestPipeline(t *testing.T) (*Pipeline, *store.FeatureStore) {
	t.Helper()

	path := t.TempDir() + "/test.db"
	fs, err := store.NewFeatureStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() { fs.Close() })

	return NewPipeline(fs), fs
}

func TestIngestNormalizesAndStores(t *testing.T) {
	pipeline, fs := newTestPipeline(t)

	raw := RawFeatures{
		UserID: "test-user",
		Age:    50,    // mid-point of 0-100 range -> should normalize to 0.5
		Income: 100000, // mid-point of 0-200000 range -> should normalize to 0.5
		Clicks: 500,    // mid-point of 0-1000 range -> should normalize to 0.5
	}

	if err := pipeline.Ingest(raw); err != nil {
		t.Fatalf("Ingest returned error: %v", err)
	}

	got, err := fs.Get("feature:test-user:age")
	if err != nil {
		t.Fatalf("expected age feature to be stored: %v", err)
	}

	if got != "0.5000" {
		t.Errorf("normalized age: got %q, want %q", got, "0.5000")
	}
}

func TestNormalizeBounds(t *testing.T) {
	cases := []struct {
		name  string
		value float64
		b     bounds
		want  float64
	}{
		{"minimum value", 0, bounds{min: 0, max: 100}, 0},
		{"maximum value", 100, bounds{min: 0, max: 100}, 1},
		{"midpoint", 50, bounds{min: 0, max: 100}, 0.5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalize(tc.value, tc.b)
			if got != tc.want {
				t.Errorf("normalize(%v, %v): got %v, want %v", tc.value, tc.b, got, tc.want)
			}
		})
	}
}