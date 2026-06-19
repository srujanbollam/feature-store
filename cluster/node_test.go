package cluster

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeStore is a minimal in-memory implementation of FeatureStore,
// used so these tests don't need a real BoltDB file.
type fakeStore struct {
	data map[string]string
}

func newFakeStore() *fakeStore {
	return &fakeStore{data: make(map[string]string)}
}

func (f *fakeStore) Set(key, value string) error {
	f.data[key] = value
	return nil
}

func (f *fakeStore) Get(key string) (string, error) {
	return f.data[key], nil
}

func (f *fakeStore) Delete(key string) error {
	delete(f.data, key)
	return nil
}

func TestReplicateSucceedsOnFirstAttempt(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	peerAddr := server.Listener.Addr().String()
	node := NewNode("leader", "localhost:9000", true, []string{peerAddr}, newFakeStore(),"")

	if err := node.Set("key1", "value1"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	// Replication happens in a goroutine; give it a moment to complete.
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&requestCount) != 1 {
		t.Errorf("expected exactly 1 replication request, got %d", requestCount)
	}

	if node.PendingCount(peerAddr) != 0 {
		t.Errorf("expected no pending commands after a successful replication, got %d",
			node.PendingCount(peerAddr))
	}
}

func TestReplicateQueuesAfterAllRetriesFail(t *testing.T) {
	// An address nothing is listening on — every request will fail.
	unreachablePeer := "127.0.0.1:1"

	node := NewNode("leader", "localhost:9000", true, []string{unreachablePeer}, newFakeStore(),"")

	if err := node.Set("key1", "value1"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	// Wait for all retries (200ms + 400ms backoff) to finish.
	time.Sleep(1 * time.Second)

	if node.PendingCount(unreachablePeer) != 1 {
		t.Errorf("expected 1 pending command after exhausting retries, got %d",
			node.PendingCount(unreachablePeer))
	}
}

func TestReplayPendingClearsQueueOnSuccess(t *testing.T) {
	var requestCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	peerAddr := server.Listener.Addr().String()
	node := NewNode("leader", "localhost:9000", true, []string{peerAddr}, newFakeStore(),"")

	// Manually queue a pending command, simulating a previously failed replication.
	node.pendingMu.Lock()
	node.pending[peerAddr] = []ReplicateCommand{{Action: "set", Key: "k", Value: "v"}}
	node.pendingMu.Unlock()

	node.ReplayPending(peerAddr)

	if node.PendingCount(peerAddr) != 0 {
		t.Errorf("expected pending queue to be empty after a successful replay, got %d",
			node.PendingCount(peerAddr))
	}

	if atomic.LoadInt32(&requestCount) != 1 {
		t.Errorf("expected exactly 1 replay request, got %d", requestCount)
	}
}


func TestPendingQueueSurvivesRestart(t *testing.T) {
	logPath := t.TempDir() + "/pending.log"
	unreachablePeer := "127.0.0.1:1"

	// First "run": queue a write that fails to replicate.
	node1 := NewNode("leader", "localhost:9000", true, []string{unreachablePeer}, newFakeStore(), logPath)
	if err := node1.Set("key1", "value1"); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}
	time.Sleep(1 * time.Second) // wait for retries to exhaust

	if node1.PendingCount(unreachablePeer) != 1 {
		t.Fatalf("expected 1 pending command before restart, got %d", node1.PendingCount(unreachablePeer))
	}

	// Simulate a restart: construct a brand new Node pointed at the same log file.
	node2 := NewNode("leader", "localhost:9000", true, []string{unreachablePeer}, newFakeStore(), logPath)

	if node2.PendingCount(unreachablePeer) != 1 {
		t.Errorf("expected pending command to be restored after restart, got %d",
			node2.PendingCount(unreachablePeer))
	}
}