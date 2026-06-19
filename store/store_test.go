package store

import (
	"os"
	"testing"
)

// newTestStore creates a temporary store and registers cleanup so the
// underlying file is removed after the test finishes.
func newTestStore(t *testing.T) *FeatureStore {
	t.Helper()

	path := t.TempDir() + "/test.db"

	fs, err := NewFeatureStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	t.Cleanup(func() {
		fs.Close()
		os.Remove(path)
	})

	return fs
}

func TestSetAndGet(t *testing.T) {
	fs := newTestStore(t)

	if err := fs.Set("key1", "value1"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	got, err := fs.Get("key1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if got != "value1" {
		t.Errorf("got %q, want %q", got, "value1")
	}
}

func TestGetMissingKey(t *testing.T) {
	fs := newTestStore(t)

	_, err := fs.Get("does-not-exist")
	if err == nil {
		t.Error("expected an error for a missing key, got nil")
	}
}

func TestDelete(t *testing.T) {
	fs := newTestStore(t)

	if err := fs.Set("key1", "value1"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	if err := fs.Delete("key1"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	_, err := fs.Get("key1")
	if err == nil {
		t.Error("expected an error after deleting key1, got nil")
	}
}

func TestOverwriteExistingKey(t *testing.T) {
	fs := newTestStore(t)

	fs.Set("key1", "first")
	fs.Set("key1", "second")

	got, err := fs.Get("key1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}

	if got != "second" {
		t.Errorf("got %q, want %q", got, "second")
	}
}