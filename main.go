package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"feature-store/api"
	"feature-store/cluster"
	"feature-store/ml"
	"feature-store/store"
)

func main() {
	nodeID := getEnv("NODE_ID", "node1")
	port := getEnv("PORT", "8080")
	isLeader := getEnv("LEADER", "false") == "true"
	peers := parsePeers(getEnv("PEERS", ""))

	dbPath := fmt.Sprintf("/app/data/%s.db", nodeID)

	log.Printf("starting %s (leader=%v) on :%s", nodeID, isLeader, port)

	fs, err := store.NewFeatureStore(dbPath)
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer fs.Close()

	pendingLogPath := fmt.Sprintf("/app/data/%s-pending.log", nodeID)
	node := cluster.NewNode(nodeID, "localhost:"+port, isLeader, peers, fs, pendingLogPath)
	pipeline := ml.NewPipeline(fs)

	server := api.NewServer(node, pipeline)
	if err := server.Start(port); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}

// parsePeers splits a comma-separated peer list, e.g. "localhost:8082,localhost:8083"
func parsePeers(raw string) []string {
	if raw == "" {
		return []string{}
	}
	return strings.Split(raw, ",")
}