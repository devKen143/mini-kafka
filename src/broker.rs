//! Topics, partitions, key-based partitioning, consumer groups and committed offsets.

use crate::log::{LogConfig, PartitionLog, Record};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

pub const SESSION_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Mutex<PartitionLog>>,
    rr: AtomicUsize,
    pub notify: Notify,
}

#[derive(Default)]
struct Group {
    members: BTreeMap<String, Instant>,
    generation: u64,
}

pub struct Broker {
    dir: PathBuf,
    cfg: LogConfig,
    topics: RwLock<HashMap<String, Arc<Topic>>>,
    offsets: Mutex<HashMap<(String, String, u32), u64>>,
    groups: Mutex<HashMap<(String, String), Group>>,
}

#[derive(Debug)]
pub enum BrokerError {
    UnknownTopic(String),
    BadPartition(u32),
    Invalid(String),
    Io(io::Error),
}

impl std::fmt::Display for BrokerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerError::UnknownTopic(t) => write!(f, "unknown topic '{t}'"),
            BrokerError::BadPartition(p) => write!(f, "partition {p} does not exist"),
            BrokerError::Invalid(m) => write!(f, "{m}"),
            BrokerError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl From<io::Error> for BrokerError {
    fn from(e: io::Error) -> Self {
        BrokerError::Io(e)
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

/// FNV-1a: stable across runs/platforms so a key always lands on the same partition.
pub fn partition_for(key: &[u8], n: usize) -> usize {
    let h = key.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| (h ^ b as u64).wrapping_mul(0x0100_0000_01b3));
    (h % n as u64) as usize
}

fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 200 && s.chars().all(|c| c.is_ascii_alphanumeric() || "._-".contains(c)) && s != "." && s != ".."
}

pub struct PartitionInfo {
    pub partition: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub bytes: u64,
    pub segments: usize,
}

impl Broker {
    pub fn open(dir: impl Into<PathBuf>, cfg: LogConfig) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(dir.join("topics"))?;
        let mut topics = HashMap::new();
        for e in fs::read_dir(dir.join("topics"))? {
            let e = e?;
            let name = e.file_name().to_string_lossy().to_string();
            let mut parts: Vec<u32> = fs::read_dir(e.path())?
                .filter_map(|p| p.ok()?.file_name().to_str()?.parse().ok())
                .collect();
            parts.sort_unstable();
            let logs = parts.iter().map(|p| PartitionLog::open(e.path().join(p.to_string()), cfg).map(Mutex::new)).collect::<io::Result<Vec<_>>>()?;
            if !logs.is_empty() {
                topics.insert(name.clone(), Arc::new(Topic { name, partitions: logs, rr: AtomicUsize::new(0), notify: Notify::new() }));
            }
        }
        let b = Broker { dir, cfg, topics: RwLock::new(topics), offsets: Mutex::new(HashMap::new()), groups: Mutex::new(HashMap::new()) };
        b.load_offsets()?;
        Ok(b)
    }

    fn offsets_path(&self) -> PathBuf {
        self.dir.join("consumer_offsets.jsonl")
    }

    /// Replays the offsets journal (last write wins) and compacts it.
    fn load_offsets(&self) -> io::Result<()> {
        let path = self.offsets_path();
        let mut map = self.offsets.lock().unwrap();
        if let Ok(f) = fs::File::open(&path) {
            for line in io::BufReader::new(f).lines() {
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&line?) else { continue };
                if let (Some(g), Some(t), Some(p), Some(o)) = (v["g"].as_str(), v["t"].as_str(), v["p"].as_u64(), v["o"].as_u64()) {
                    map.insert((g.into(), t.into(), p as u32), o);
                }
            }
        }
        let tmp = path.with_extension("tmp");
        let mut f = fs::File::create(&tmp)?;
        for ((g, t, p), o) in map.iter() {
            writeln!(f, "{}", serde_json::json!({"g": g, "t": t, "p": p, "o": o}))?;
        }
        f.sync_all()?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn topic(&self, name: &str) -> Result<Arc<Topic>> {
        self.topics.read().unwrap().get(name).cloned().ok_or_else(|| BrokerError::UnknownTopic(name.into()))
    }

    pub fn topic_names(&self) -> Vec<(String, usize)> {
        let mut v: Vec<_> = self.topics.read().unwrap().values().map(|t| (t.name.clone(), t.partitions.len())).collect();
        v.sort();
        v
    }

    /// Creates a topic; idempotent if it already exists with any partition count.
    pub fn create_topic(&self, name: &str, partitions: u32) -> Result<bool> {
        if !valid_name(name) {
            return Err(BrokerError::Invalid(format!("invalid topic name '{name}'")));
        }
        if partitions == 0 || partitions > 1024 {
            return Err(BrokerError::Invalid("partitions must be 1..=1024".into()));
        }
        let mut topics = self.topics.write().unwrap();
        if topics.contains_key(name) {
            return Ok(false);
        }
        let base = self.dir.join("topics").join(name);
        let logs = (0..partitions).map(|p| PartitionLog::open(base.join(p.to_string()), self.cfg).map(Mutex::new)).collect::<io::Result<Vec<_>>>()?;
        topics.insert(name.into(), Arc::new(Topic { name: name.into(), partitions: logs, rr: AtomicUsize::new(0), notify: Notify::new() }));
        Ok(true)
    }

    /// Appends one record. Keyed records hash to a fixed partition, unkeyed ones round-robin.
    pub fn produce(&self, topic: &str, key: Option<String>, value: String, partition: Option<u32>) -> Result<(u32, u64)> {
        let t = self.topic(topic)?;
        let n = t.partitions.len();
        let p = match (partition, &key) {
            (Some(p), _) if (p as usize) < n => p as usize,
            (Some(p), _) => return Err(BrokerError::BadPartition(p)),
            (None, Some(k)) => partition_for(k.as_bytes(), n),
            (None, None) => t.rr.fetch_add(1, Ordering::Relaxed) % n,
        };
        let offset = t.partitions[p].lock().unwrap().append(key.map(String::into_bytes), value.into_bytes(), now_ms())?;
        t.notify.notify_waiters();
        Ok((p as u32, offset))
    }

    pub fn fetch(&self, topic: &str, partition: u32, offset: u64, max: usize) -> Result<(Vec<Record>, u64)> {
        let t = self.topic(topic)?;
        let log = t.partitions.get(partition as usize).ok_or(BrokerError::BadPartition(partition))?.lock().unwrap();
        Ok((log.read(offset, max)?, log.next_offset()))
    }

    pub fn describe(&self, topic: &str) -> Result<Vec<PartitionInfo>> {
        let t = self.topic(topic)?;
        Ok(t.partitions
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let l = p.lock().unwrap();
                PartitionInfo { partition: i as u32, start_offset: l.start_offset(), end_offset: l.next_offset(), bytes: l.size_bytes(), segments: l.segment_count() }
            })
            .collect())
    }

    pub fn commit(&self, group: &str, topic: &str, partition: u32, offset: u64) -> Result<()> {
        if !valid_name(group) {
            return Err(BrokerError::Invalid("invalid group name".into()));
        }
        let mut f = OpenOptions::new().create(true).append(true).open(self.offsets_path())?;
        writeln!(f, "{}", serde_json::json!({"g": group, "t": topic, "p": partition, "o": offset}))?;
        self.offsets.lock().unwrap().insert((group.into(), topic.into(), partition), offset);
        Ok(())
    }

    pub fn committed(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        self.offsets.lock().unwrap().get(&(group.into(), topic.into(), partition)).copied()
    }

    /// Registers/heartbeats `member` and returns (generation, assigned partitions).
    /// Partitions are range-assigned over the sorted list of live members.
    pub fn join(&self, group: &str, topic: &str, member: &str) -> Result<(u64, Vec<u32>)> {
        let n = self.topic(topic)?.partitions.len() as u32;
        let mut groups = self.groups.lock().unwrap();
        let g = groups.entry((group.into(), topic.into())).or_default();
        let now = Instant::now();
        let before = g.members.len();
        g.members.retain(|_, seen| now.duration_since(*seen) < SESSION_TIMEOUT);
        let is_new = g.members.insert(member.into(), now).is_none();
        if is_new || g.members.len() != before {
            g.generation += 1; // membership changed: rebalance
        }
        let members: Vec<&String> = g.members.keys().collect();
        let idx = members.iter().position(|m| m.as_str() == member).unwrap() as u32;
        let count = members.len() as u32;
        let assigned = (0..n).filter(|p| p % count == idx).collect();
        Ok((g.generation, assigned))
    }

    pub fn leave(&self, group: &str, topic: &str, member: &str) {
        if let Some(g) = self.groups.lock().unwrap().get_mut(&(group.into(), topic.into())) {
            if g.members.remove(member).is_some() {
                g.generation += 1;
            }
        }
    }

    pub fn group_members(&self, group: &str, topic: &str) -> Vec<String> {
        self.groups.lock().unwrap().get(&(group.into(), topic.into())).map(|g| g.members.keys().cloned().collect()).unwrap_or_default()
    }

    /// Fetches from every assigned partition starting at the committed offset.
    /// Waits up to `wait` for new data if nothing is available (long polling).
    pub async fn poll(&self, group: &str, topic: &str, member: &str, max: usize, wait: Duration) -> Result<(u64, Vec<(u32, Record)>)> {
        let t = self.topic(topic)?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = t.notify.notified();
            let (generation, parts) = self.join(group, topic, member)?;
            let mut out = Vec::new();
            for p in parts {
                let from = self.committed(group, topic, p).unwrap_or(0);
                let (recs, _) = self.fetch(topic, p, from, max.saturating_sub(out.len()))?;
                out.extend(recs.into_iter().map(|r| (p, r)));
                if out.len() >= max {
                    break;
                }
            }
            if !out.is_empty() || tokio::time::Instant::now() >= deadline {
                return Ok((generation, out));
            }
            let _ = tokio::time::timeout_at(deadline, notified).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_stick_to_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let b = Broker::open(dir.path(), LogConfig::default()).unwrap();
        b.create_topic("orders", 4).unwrap();
        let p1 = b.produce("orders", Some("user-42".into()), "a".into(), None).unwrap().0;
        for _ in 0..10 {
            assert_eq!(b.produce("orders", Some("user-42".into()), "b".into(), None).unwrap().0, p1);
        }
        let unkeyed: std::collections::HashSet<u32> = (0..8).map(|_| b.produce("orders", None, "x".into(), None).unwrap().0).collect();
        assert_eq!(unkeyed.len(), 4);
    }

    #[test]
    fn group_assignment_rebalances() {
        let dir = tempfile::tempdir().unwrap();
        let b = Broker::open(dir.path(), LogConfig::default()).unwrap();
        b.create_topic("t", 6).unwrap();
        let (g1, a) = b.join("g", "t", "alice").unwrap();
        assert_eq!(a, vec![0, 1, 2, 3, 4, 5]);
        let (g2, bob) = b.join("g", "t", "bob").unwrap();
        let (_, alice) = b.join("g", "t", "alice").unwrap();
        assert!(g2 > g1);
        assert_eq!(alice, vec![0, 2, 4]);
        assert_eq!(bob, vec![1, 3, 5]);
        b.leave("g", "t", "bob");
        assert_eq!(b.join("g", "t", "alice").unwrap().1.len(), 6);
    }

    #[test]
    fn offsets_and_topics_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let b = Broker::open(dir.path(), LogConfig::default()).unwrap();
            b.create_topic("events", 2).unwrap();
            b.produce("events", None, "hello".into(), Some(1)).unwrap();
            b.commit("g", "events", 1, 1).unwrap();
            b.commit("g", "events", 1, 1).unwrap();
        }
        let b = Broker::open(dir.path(), LogConfig::default()).unwrap();
        assert_eq!(b.topic_names(), vec![("events".to_string(), 2)]);
        assert_eq!(b.committed("g", "events", 1), Some(1));
        assert_eq!(b.fetch("events", 1, 0, 10).unwrap().0[0].value, b"hello");
    }

    #[test]
    fn rejects_bad_names() {
        let dir = tempfile::tempdir().unwrap();
        let b = Broker::open(dir.path(), LogConfig::default()).unwrap();
        assert!(b.create_topic("../etc", 1).is_err());
        assert!(b.create_topic("ok", 0).is_err());
    }
}
