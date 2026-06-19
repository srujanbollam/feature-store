package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// FeatureStore is the subset of store behavior the cluster package
// depends on. Defined here (rather than importing the store package
// directly) keeps this package easy to test in isolation.
type FeatureStore interface {
	Set(key, value string) error
	Get(key string) (string, error)
	Delete(key string) error
}

// Node represents a single server in the cluster. It wraps a local
// store and, if it is the leader, propagates writes to its peers.
type Node struct {
	ID       string
	Address  string
	IsLeader bool
	Peers    []string

	store      FeatureStore
	mu         sync.Mutex
	httpClient *http.Client

	// pendingMu guards pending, which holds commands that failed to
	// replicate to a specific peer after all retries were exhausted.
	// A recovered peer can be caught up from this log instead of
	// silently staying stale.
	pendingMu sync.Mutex
	pending   map[string][]ReplicateCommand // peer address -> missed commands
}

// NewNode creates a Node backed by the given store.
func NewNode(id, address string, isLeader bool, peers []string, s FeatureStore) *Node {
	return &Node{
		ID:       id,
		Address:  address,
		IsLeader: isLeader,
		Peers:    peers,
		store:    s,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
		pending: make(map[string][]ReplicateCommand),
	}
}

// ReplicateCommand is the payload sent from leader to followers to
// replay a write.
type ReplicateCommand struct {
	Action string `json:"action"` // "set" or "delete"
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
}

// Set writes a key locally and, if this node is the leader, replicates
// the write to all peers.
func (n *Node) Set(key, value string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.store.Set(key, value); err != nil {
		return err
	}

	if n.IsLeader {
		n.replicate(ReplicateCommand{Action: "set", Key: key, Value: value})
	}

	return nil
}

// Get reads a key locally. Any node can serve reads.
func (n *Node) Get(key string) (string, error) {
	return n.store.Get(key)
}

// Delete removes a key locally and, if this node is the leader,
// replicates the delete to all peers.
func (n *Node) Delete(key string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.store.Delete(key); err != nil {
		return err
	}

	if n.IsLeader {
		n.replicate(ReplicateCommand{Action: "delete", Key: key})
	}

	return nil
}

// replicate sends cmd to every peer concurrently, retrying transient
// failures with backoff. If a peer is still unreachable after all
// retries, the command is recorded in the pending log for that peer
// so it can be replayed once the peer recovers.
func (n *Node) replicate(cmd ReplicateCommand) {
	for _, peer := range n.Peers {
		go n.replicateToPeerWithRetry(peer, cmd)
	}
}

const (
	maxReplicationAttempts = 3
	initialBackoff         = 200 * time.Millisecond
)

func (n *Node) replicateToPeerWithRetry(peer string, cmd ReplicateCommand) {
	backoff := initialBackoff

	for attempt := 1; attempt <= maxReplicationAttempts; attempt++ {
		if n.sendReplicateRequest(peer, cmd) {
			return // success
		}

		if attempt < maxReplicationAttempts {
			time.Sleep(backoff)
			backoff *= 2 // exponential backoff: 200ms, 400ms, ...
		}
	}

	// All retries exhausted — record this as a missed write for the peer.
	fmt.Printf("replication to %s failed after %d attempts, queued for catch-up: %s %s\n",
		peer, maxReplicationAttempts, cmd.Action, cmd.Key)

	n.pendingMu.Lock()
	n.pending[peer] = append(n.pending[peer], cmd)
	n.pendingMu.Unlock()
}

func (n *Node) sendReplicateRequest(peer string, cmd ReplicateCommand) bool {
	body, err := json.Marshal(cmd)
	if err != nil {
		return false
	}

	url := fmt.Sprintf("http://%s/internal/replicate", peer)
	resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// ApplyReplicated applies a command received from the leader. Called
// by followers when they receive a /internal/replicate request.
func (n *Node) ApplyReplicated(cmd ReplicateCommand) error {
	switch cmd.Action {
	case "set":
		return n.store.Set(cmd.Key, cmd.Value)
	case "delete":
		return n.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("unknown replication action: %s", cmd.Action)
	}
}

// PendingCount returns the number of missed writes still queued for
// the given peer. Useful for the status endpoint and for debugging.
func (n *Node) PendingCount(peer string) int {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()
	return len(n.pending[peer])
}

// ReplayPending re-attempts every missed write queued for peer. Call
// this once a previously-unreachable peer is confirmed healthy again
// (e.g. after a successful health check).
func (n *Node) ReplayPending(peer string) {
	n.pendingMu.Lock()
	commands := n.pending[peer]
	n.pending[peer] = nil
	n.pendingMu.Unlock()

	for _, cmd := range commands {
		if !n.sendReplicateRequest(peer, cmd) {
			// Still unreachable — put it back in the queue.
			n.pendingMu.Lock()
			n.pending[peer] = append(n.pending[peer], cmd)
			n.pendingMu.Unlock()
		}
	}
}