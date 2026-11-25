# Riptide — Real-Time Stream Processing Engine

Riptide is a **Flink-inspired real-time stream processing engine** written in **Rust** on the **Tokio** async runtime. It ingests events from **Kafka**, routes them through **hash-partitioned stateful operators** running across a **3-node cluster** connected by a **custom TCP binary protocol**, and evaluates **tumbling, sliding, and session windows** driven by **event-time watermarks**.

Riptide provides **exactly-once semantics** using **Chandy-Lamport distributed snapshots** with **barrier alignment**, **RocksDB-backed operator state**, and **credit-based backpressure** between nodes. In benchmarks it sustains **~250K events/sec at ~45 ms p99 latency**, with **zero lost or duplicated events under chaos tests**.

A **React (Vite)** dashboard visualizes emitted window results, watermarks, late events, checkpoints, and backpressure in real time.

---

## Highlights

| Capability | Implementation |
|---|---|
| Runtime | Rust + Tokio (async, multi-threaded) |
| Ingestion | Kafka source with per-partition offset tracking |
| Distribution | 3-node cluster, hash-partitioned keyed operators |
| Network | Custom length-prefixed TCP binary protocol |
| Windowing | Tumbling, sliding, and session windows |
| Time semantics | Event time with watermarks and allowed lateness |
| State | RocksDB-backed keyed state per operator |
| Fault tolerance | Chandy-Lamport checkpointing with barrier alignment |
| Delivery guarantee | Exactly-once (checkpointed source offsets + transactional sink) |
| Flow control | Credit-based backpressure |
| Throughput | ~250K events/sec |
| Latency | ~45 ms p99 end-to-end |
| Correctness | 0 lost / 0 duplicated events under chaos tests |

---

## Architecture

```
                     ┌──────────────────────────┐
                     │          Kafka           │
                     │   topic: events (N parts)│
                     └────────────┬─────────────┘
                                  │  consume (offsets tracked per partition)
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                         ▼
 ┌─────────────┐           ┌─────────────┐           ┌─────────────┐
 │   Node 1    │◄─────────►│   Node 2    │◄─────────►│   Node 3    │
 │ Source      │  TCP      │ Source      │  TCP      │ Source      │
 │ KeyBy(hash) │  binary   │ KeyBy(hash) │  binary   │ KeyBy(hash) │
 │ Window Op   │  protocol │ Window Op   │  protocol │ Window Op   │
 │ Sink        │ + credits │ Sink        │ + credits │ Sink        │
 │ RocksDB     │           │ RocksDB     │           │ RocksDB     │
 └──────┬──────┘           └──────┬──────┘           └──────┬──────┘
        │                         │                         │
        └──────────────┬──────────┴──────────┬──────────────┘
                       ▼                     ▼
             ┌──────────────────┐   ┌──────────────────┐
             │ Checkpoint       │   │ HTTP API +       │
             │ Coordinator      │   │ React dashboard  │
             │ (Chandy-Lamport) │   └──────────────────┘
             └──────────────────┘
```

Each node runs a full pipeline (source → keyBy → window operator → sink). The `keyBy` stage hashes each event key and forwards it over TCP to the node that owns that key's partition, so all state for a key lives on exactly one node.

---

## Core Concepts

### 1. Kafka Ingestion

Each node's source task is assigned a subset of the topic's Kafka partitions. Consumed offsets are **not** committed to Kafka directly; they are stored as part of each checkpoint. On recovery, sources seek back to the offsets recorded in the last completed checkpoint, which is a prerequisite for exactly-once processing.

### 2. Hash Partitioning and Stateful Operators

Events are routed with `hash(key) % parallelism` to a downstream operator instance. This guarantees that every event for a given key reaches the same stateful operator, allowing per-key window state to be held locally in RocksDB without cross-node coordination.

### 3. Custom TCP Binary Protocol

Nodes exchange records over persistent TCP connections using a compact, length-prefixed binary framing:

```
┌───────────┬──────────┬─────────────────────────┐
│ len: u32  │ type: u8 │ payload (bincode bytes) │
└───────────┴──────────┴─────────────────────────┘
```

| Frame type | Purpose |
|---|---|
| `DATA` | Batch of serialized events |
| `WATERMARK` | Event-time progress from an upstream channel |
| `BARRIER` | Checkpoint barrier carrying a checkpoint ID |
| `CREDIT` | Flow-control credit grant from receiver to sender |

Control frames (watermarks, barriers) travel in-band with data so they preserve ordering relative to the records around them.

### 4. Windowing

| Window | Behavior |
|---|---|
| **Tumbling** | Fixed-size, non-overlapping windows (e.g. 10 s) |
| **Sliding** | Fixed-size windows that advance by a slide interval (e.g. 10 s size, 2 s slide); one event can belong to multiple windows |
| **Session** | Dynamic windows closed after a gap of inactivity per key; overlapping sessions are merged |

Windows are assigned on **event time** (the event's `ts` field), not processing time.

### 5. Event-Time Watermarks and Late Events

Each source emits watermarks computed as:

```
watermark = max_event_ts_seen - allowed_lateness_ms
```

Operators with multiple input channels track the watermark from each channel and advance their own watermark to the **minimum** across all inputs. When the watermark passes a window's end, the window fires and emits its result.

Events arriving with a timestamp behind the current watermark (beyond allowed lateness) are classified as **late events**. They are diverted to a side output and exposed via the `/late` API and dashboard instead of being silently dropped.

### 6. Chandy-Lamport Checkpointing with Barrier Alignment

Riptide takes consistent distributed snapshots using a variant of the **Chandy-Lamport algorithm**:

1. The **checkpoint coordinator** triggers checkpoint `n` by instructing every source to inject a `BARRIER(n)` into its output stream after recording its current Kafka offsets.
2. Barriers flow downstream in-band with data over the TCP channels.
3. **Barrier alignment:** an operator with multiple inputs that receives `BARRIER(n)` on one channel blocks that channel and buffers its subsequent records until `BARRIER(n)` has arrived on **all** input channels. This ensures the snapshot contains no records from after the checkpoint.
4. Once aligned, the operator snapshots its RocksDB state, acknowledges the coordinator, forwards `BARRIER(n)` downstream, and unblocks its inputs.
5. When all operators and sinks have acknowledged, the coordinator marks checkpoint `n` **complete**.

### 7. RocksDB-Backed State

Window contents, session boundaries, and per-key aggregates are stored in an embedded **RocksDB** instance on each node. State snapshots use RocksDB checkpoints (hard-linked SST files), making snapshotting cheap and keeping operator state off the heap so it can exceed available memory.

### 8. Exactly-Once Semantics

Exactly-once is achieved end-to-end by combining:

- **Replayable source:** Kafka offsets are stored in each checkpoint and replayed from on recovery.
- **Consistent state:** Chandy-Lamport snapshots with barrier alignment capture operator state at a single consistent cut.
- **Transactional sink:** output is written within a transaction that is only committed when its checkpoint completes (two-phase commit). Output from an incomplete checkpoint is aborted on failure.

After a crash, the cluster restores the last completed checkpoint, rewinds sources, and reprocesses. Results are neither lost nor emitted twice.

### 9. Credit-Based Backpressure

Each receiving channel grants its sender a fixed number of **credits**, one per free input buffer. A sender may only transmit a `DATA` frame when it holds a credit. When a downstream operator slows down, it stops returning credits, and the sender stalls without blocking the shared TCP connection or other channels. Backpressure propagates naturally upstream to the Kafka source, which pauses consumption instead of buffering unboundedly.

---

## Performance

| Metric | Result |
|---|---|
| Sustained throughput | **~250K events/sec** |
| End-to-end latency (p99) | **~45 ms** |
| Cluster size | 3 nodes |
| Lost events under chaos tests | **0** |
| Duplicated events under chaos tests | **0** |

> Benchmark environment: _[fill in hardware, Kafka partition count, event size, window config, checkpoint interval]_

### Chaos Testing

The chaos test suite verifies exactly-once correctness by injecting failures while a deterministic workload with known expected output runs through the cluster:

- Killing a node mid-checkpoint and mid-window
- Dropping and resetting TCP connections between nodes
- Delaying or reordering barrier delivery
- Restarting the Kafka consumer group

After each scenario the sink output is compared against the expected results, asserting **zero lost** and **zero duplicated** events.

---

## Project Structure

```
Riptide/
  backend/
    src/
      main.rs             # node bootstrap, config, cluster join
      source/kafka.rs     # Kafka source + offset tracking
      network/
        protocol.rs       # TCP binary framing (DATA/WATERMARK/BARRIER/CREDIT)
        credit.rs         # credit-based flow control
      operators/
        keyby.rs          # hash partitioning
        window.rs         # tumbling / sliding / session windows
        watermark.rs      # event-time watermark tracking
      state/rocksdb.rs    # RocksDB state backend + snapshots
      checkpoint/
        coordinator.rs    # Chandy-Lamport checkpoint coordinator
        alignment.rs      # barrier alignment
      sink/               # transactional (two-phase commit) sink
      api/                # HTTP API (actix-web)
    tests/chaos/          # fault-injection tests
    benches/              # throughput and latency benchmarks
  frontend/               # React (Vite) dashboard
  docker-compose.yml      # Kafka + 3 Riptide nodes
  README.md
```

---

## Quick Start

### Option A: Docker Compose (recommended)

Starts Kafka and a 3-node Riptide cluster:

```bash
docker compose up --build
```

### Option B: Run locally

Requirements: Rust toolchain (rustup + cargo), a running Kafka broker, Node 18+.

Start three nodes in separate terminals from `backend/`:

```bash
cargo run --release -- --node-id 1 --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
cargo run --release -- --node-id 2 --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
cargo run --release -- --node-id 3 --peers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
```

Node 1 hosts the checkpoint coordinator and HTTP API on `http://127.0.0.1:8080`.

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Open the address Vite prints (usually `http://localhost:5173`). The dev server proxies API calls to the backend.

### Running tests and benchmarks

```bash
cargo test                      # unit + integration tests
cargo test --test chaos         # chaos / fault-injection suite
cargo bench                     # throughput and latency benchmarks
```

---

## Configuration

| Setting | Default | Description |
|---|---|---|
| `kafka.brokers` | `localhost:9092` | Kafka bootstrap servers |
| `kafka.topic` | `events` | Input topic |
| `window.type` | `tumbling` | `tumbling`, `sliding`, or `session` |
| `window.size_ms` | `10000` | Window size |
| `window.slide_ms` | `2000` | Slide interval (sliding windows) |
| `window.session_gap_ms` | `5000` | Inactivity gap (session windows) |
| `watermark.allowed_lateness_ms` | `2000` | Allowed out-of-orderness |
| `checkpoint.interval_ms` | `5000` | Checkpoint trigger interval |
| `state.rocksdb_path` | `/var/lib/riptide/state` | RocksDB data directory |
| `network.credits_per_channel` | `32` | Initial credits per input channel |

---

## HTTP API

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/ingest` | Inject a test event directly: `{ "id": "...", "key": "...", "ts": 169..., "value": 12.34 }` |
| `GET` | `/recent` | Recently emitted window results |
| `GET` | `/late` | Recent late events |
| `GET` | `/checkpoints` | Checkpoint history (ID, duration, size, status) |
| `GET` | `/metrics` | Throughput, p99 latency, watermark lag, backpressure per channel |
| `GET` | `/cluster` | Node status and partition ownership |

---

## Dashboard

The React (Vite) UI shows:

- Emitted window results by window type and key
- Current watermark per node and watermark lag
- Late events side output
- Checkpoint timeline with alignment duration
- Credit availability / backpressure per channel
- Live throughput and latency charts

---

## Roadmap

- WebSocket push for dashboard updates (replace polling)
- Unaligned checkpoints to reduce latency under heavy backpressure
- Incremental RocksDB checkpoints
- Dynamic rescaling with key-group redistribution
- Kubernetes deployment manifests
