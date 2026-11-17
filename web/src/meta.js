export default {
  name: 'mini-kafka',
  stack: 'Rust · tokio · append-only logs',
  tagline: 'A Kafka-style log broker: partitioned topics on segmented, checksummed logs, with consumer groups, committed offsets and long polling.',
  repo: 'https://github.com/devKen143/mini-kafka',
  description:
    'Producers append to topics; each topic is split into partitions and each partition is an append-only log on disk. Consumers in a group divide the partitions between them, read from their committed offsets and commit as they go — so a crashed consumer picks up exactly where its group left off.',
  features: [
    'Segmented logs with CRC-32 per record, a sparse offset index and crash recovery that truncates torn writes',
    'Size-based segment rolling and retention',
    'Key → partition hashing (FNV-1a) so one key always keeps its order; unkeyed records round-robin',
    'Consumer groups with range assignment, rebalancing on join/leave and session timeouts',
    'Committed offsets journalled to disk and compacted on restart; per-partition lag reporting',
    'Long-poll fetch that wakes the instant a producer appends',
    'JSON-lines TCP protocol — you can talk to it with netcat',
  ],
  demoNote: 'A live model of the broker logic. Produce keyed messages, add consumers to the group, and poll to watch offsets, assignment and lag change.',
  architecture: [
    { title: 'log.rs', text: 'Records are [len][crc][offset][timestamp][key][value]. Reads binary-search the sparse index, seek, then scan forward.' },
    { title: 'broker.rs', text: 'Topics map to Vec<Mutex<PartitionLog>>. Group membership is a BTreeMap of members to last heartbeat; partition p goes to member p % n.' },
    { title: 'server.rs', text: 'tokio TCP server. Each line is a tagged JSON request (produce, fetch, poll, commit, …). Long polls wait on a tokio Notify.' },
    { title: 'Durability', text: 'Every record is checksummed. On startup each segment is scanned, the index rebuilt, and a partial trailing record cut off.' },
  ],
  usage: `git clone https://github.com/devKen143/mini-kafka && cd mini-kafka
cargo run --release -- broker --data ./data

cargo run --release -- create orders --partitions 3
cargo run --release -- produce orders --key user-1 "order #1" "order #2"
cargo run --release -- consume orders --group billing --follow
cargo run --release -- describe orders --group billing

# raw protocol
echo '{"op":"produce","topic":"orders","value":"hi"}' | nc 127.0.0.1 9092`,
}
