# Feature Store

A distributed key-value feature store for machine learning, built from scratch in Go. It demonstrates three areas together: an embedded database layer, leader-follower replication across nodes, and an ML feature ingestion pipeline — with production-style observability (latency percentiles) and failure handling (replication retries) built in rather than bolted on.

## What it does

In ML systems, models need preprocessed inputs ("features") served quickly and consistently. This project implements a minimal version of that pattern, similar in spirit to Uber's Michelangelo or Feast:

- Raw data comes in via an `/ingest` endpoint, gets normalized (min-max scaling), and is stored
- Any stored feature can be read by key via a REST API
- Writes go to a leader node and are replicated to follower nodes automatically
- If a follower is temporarily unreachable, the leader retries with backoff, then queues the write and replays it once the follower recovers
- Every request's latency is tracked, with P50/P95/P99/P100 exposed live

## Architecture
                ┌─────────────────────┐
                │   Python ML Client   │
                │  (fetches features)  │
                └──────────┬───────────┘
                           │ HTTP
                           ▼
                ┌─────────────────────┐
                │   Node 1 (Leader)    │
                │      :8081           │
                │                      │
                │  REST API (Gin)      │
                │  Latency middleware  │
                │  Feature pipeline    │
                │  BoltDB storage      │
                └──────────┬───────────┘
                           │
            replicate (retry + backoff)
                           │
          ┌────────────────┴────────────────┐
          ▼                                  ▼
┌─────────────────────┐          ┌─────────────────────┐

│  Node 2 (Follower)   │          │  Node 3 (Follower)   │

│      :8082           │          │      :8083           │

│  BoltDB storage      │          │  BoltDB storage      │

└─────────────────────┘          └─────────────────────┘

## What's technically interesting here

**Replication failure handling.** If a follower is down during a write, the leader doesn't just drop it. It retries up to 3 times with exponential backoff (200ms, 400ms), and if still unreachable, queues the write in memory. A background watcher checks peer health every 5 seconds and replays queued writes once a peer recovers. This is tested in `cluster/node_test.go` using a fake HTTP server and an intentionally unreachable address — not just demonstrated manually.

**Latency percentiles, not averages.** Every request's duration is recorded and exposed as P50/P95/P99/P100 via `/metrics/latency`, computed with linear interpolation between ranks. Averages hide tail latency; percentiles surface it. The math is verified in `metrics/metrics_test.go`, including edge cases (empty data, single sample, out-of-order input).

**Leader-only writes, enforced server-side.** Followers reject writes with `403 Forbidden` rather than silently accepting and diverging from the leader.

## Tech stack

| Layer | Technology |
|---|---|
| Language | Go |
| HTTP framework | Gin |
| Embedded storage | BoltDB (bbolt) |
| Containerization | Docker, Docker Compose |
| ML client | Python 3 |

## Running it

### Option 1 — Docker (recommended, one command)

```bash
docker compose up --build
```

This starts all 3 nodes, networked together, with the leader on `localhost:8081`.

### Option 2 — manual (3 terminals)

```bash
NODE_ID=node1 PORT=8081 LEADER=true PEERS=localhost:8082,localhost:8083 go run main.go
```
```bash
NODE_ID=node2 PORT=8082 LEADER=false go run main.go
```
```bash
NODE_ID=node3 PORT=8083 LEADER=false go run main.go
```

### View the dashboard
http://localhost:8081

Shows live node status, role (leader/follower), and latency percentiles.

## API reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/features/:key` | Read a feature value (any node) |
| PUT | `/features/:key` | Write a feature value (leader only) |
| DELETE | `/features/:key` | Delete a feature (leader only) |
| POST | `/ingest` | Submit raw data for normalization and storage (leader only) |
| GET | `/metrics/latency` | Current P50/P95/P99/P100 latency, in milliseconds |
| GET | `/status` | This node's role, address, and peers |
| GET | `/health` | Liveness check |

### Example

```bash
curl -X POST http://localhost:8081/ingest \
  -H "Content-Type: application/json" \
  -d '{"user_id":"123","age":25,"income":50000,"clicks":142}'

curl http://localhost:8083/features/feature:123:age
```

### Python ML client

```bash
python ml_client.py
```

Fetches normalized features from the store and runs a simple mock prediction.

## Running tests

```bash
go test ./...
```

Covers:
- `store` — persistence, overwrites, missing keys
- `metrics` — percentile correctness, bounded sample window, edge cases
- `ml` — normalization correctness at boundary values
- `cluster` — replication success path, retry-and-queue on failure, replay-on-recovery

## Known limitations

This is a learning project, not a production system. Specifically:

- **No automated leader election.** The leader is set via an environment variable at startup. If the leader node dies, there is currently no failover — a real implementation would need a consensus protocol (Raft is the standard choice).
- **Replication is asynchronous and best-effort**, not strongly consistent. A client could read stale data from a follower immediately after a write to the leader, before replication completes.
- - ~~The pending-write queue is in-memory~~ — **fixed**: queued writes are now persisted to an append-only log on disk (`cluster/node.go`) and restored automatically on startup, verified by `TestPendingQueueSurvivesRestart`.
- **Bounds for normalization are hardcoded** per field rather than computed from real data distributions.

## Possible extensions

- Raft-based leader election for automatic failover
- Persist the pending replication queue to disk
- gRPC instead of HTTP for node-to-node communication
- Prometheus metrics export alongside the custom latency tracker