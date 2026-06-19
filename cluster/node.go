package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"feature-store/store"
)

// Node represents a single server in the cluster. It wraps a local
// store and, if it is the leader, propagates writes to its peers.
type Node struct {
	ID       string
	Address  string
	IsLeader bool
	Peers    []string

	store      *store.FeatureStore
	mu         sync.Mutex
	httpClient *http.Client
}

// NewNode creates a Node backed by the given store.
func NewNode(id, address string, isLeader bool, peers []string, s *store.FeatureStore) *Node {
	return &Node{
		ID:       id,
		Address:  address,
		IsLeader: isLeader,
		Peers:    peers,
		store:    s,
		httpClient: &http.Client{
			Timeout: 2 * time.Second,
		},
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

// replicate sends cmd to every peer concurrently. Failures are logged
// by the caller-visible error return of each goroutine being dropped —
// in a production system these would feed a retry queue; here we keep
// it simple and log failures to stdout via the caller's logger.
func (n *Node) replicate(cmd ReplicateCommand) {
	body, _ := json.Marshal(cmd)

	for _, peer := range n.Peers {
		go func(addr string) {
			url := fmt.Sprintf("http://%s/internal/replicate", addr)
			resp, err := n.httpClient.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				fmt.Printf("replication to %s failed: %v\n", addr, err)
				return
			}
			defer resp.Body.Close()
		}(peer)
	}
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