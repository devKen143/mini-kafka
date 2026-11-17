# mini-kafka

A Kafka-style log broker in Rust (tokio). Topics are split into partitions; each partition is a segmented, checksummed, append-only log on disk. Consumer groups share partitions and track committed offsets.

## Features
- **Storage**: segment files with CRC-32 per record, sparse offset index, crash recovery (torn tail truncated), size-based rolling and retention
- **Partitioning**: FNV-1a key hashing (stable per key), round-robin for unkeyed records
- **Consumer groups**: range assignment, rebalance on join/leave, 10 s session timeout, durable committed offsets, lag
- **Long polling**: `fetch`/`poll` with `wait_ms` wake as soon as a producer appends
- **Protocol**: one JSON object per line over TCP

## Usage
```bash
cargo run --release -- broker --data ./data --bind 127.0.0.1:9092
cargo run --release -- create orders --partitions 3
cargo run --release -- produce orders --key user-1 "order #1" "order #2"
cargo run --release -- consume orders --group billing --follow
cargo run --release -- describe orders --group billing
```

### Protocol
```json
{"op":"create_topic","topic":"orders","partitions":3}
{"op":"produce","topic":"orders","key":"user-1","value":"hello"}
{"op":"fetch","topic":"orders","partition":0,"offset":0,"max":100,"wait_ms":1000}
{"op":"poll","group":"billing","topic":"orders","member":"m1","max":100,"wait_ms":5000}
{"op":"commit","group":"billing","topic":"orders","partition":0,"offset":42}
{"op":"committed","group":"billing","topic":"orders"}
```
Other ops: `list_topics`, `describe`, `produce_batch`, `join`, `leave`.

## Tests
`cargo test` covers log recovery, segment rolling, retention, key affinity, rebalancing, restart durability and end-to-end group consumption over TCP.

## License
MIT
