//! JSON-lines protocol over TCP. One request object per line, one response object per line.

use crate::broker::Broker;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    CreateTopic { topic: String, #[serde(default = "one")] partitions: u32 },
    ListTopics,
    Describe { topic: String },
    Produce { topic: String, key: Option<String>, value: String, partition: Option<u32> },
    ProduceBatch { topic: String, records: Vec<BatchRecord> },
    Fetch { topic: String, partition: u32, offset: u64, #[serde(default = "hundred")] max: usize, #[serde(default)] wait_ms: u64 },
    Join { group: String, topic: String, member: String },
    Poll { group: String, topic: String, member: String, #[serde(default = "hundred")] max: usize, #[serde(default)] wait_ms: u64 },
    Commit { group: String, topic: String, partition: u32, offset: u64 },
    Committed { group: String, topic: String },
    Leave { group: String, topic: String, member: String },
}

#[derive(Deserialize)]
pub struct BatchRecord {
    pub key: Option<String>,
    pub value: String,
}

fn one() -> u32 {
    1
}
fn hundred() -> usize {
    100
}

fn rec_json(partition: Option<u32>, r: &crate::log::Record) -> Value {
    let mut v = json!({
        "offset": r.offset,
        "timestamp": r.timestamp,
        "key": r.key.as_ref().map(|k| String::from_utf8_lossy(k).into_owned()),
        "value": String::from_utf8_lossy(&r.value),
    });
    if let Some(p) = partition {
        v["partition"] = json!(p);
    }
    v
}

pub async fn handle(b: &Broker, req: Request) -> Result<Value, String> {
    let e = |e: crate::broker::BrokerError| e.to_string();
    let wait = |ms: u64| Duration::from_millis(ms.min(30_000));
    Ok(match req {
        Request::CreateTopic { topic, partitions } => json!({"created": b.create_topic(&topic, partitions).map_err(e)?}),
        Request::ListTopics => {
            json!({"topics": b.topic_names().into_iter().map(|(n, p)| json!({"name": n, "partitions": p})).collect::<Vec<_>>()})
        }
        Request::Describe { topic } => {
            let parts = b.describe(&topic).map_err(e)?;
            json!({"topic": topic, "partitions": parts.iter().map(|p| json!({
                "partition": p.partition, "start_offset": p.start_offset, "end_offset": p.end_offset,
                "bytes": p.bytes, "segments": p.segments,
            })).collect::<Vec<_>>()})
        }
        Request::Produce { topic, key, value, partition } => {
            let (p, o) = b.produce(&topic, key, value, partition).map_err(e)?;
            json!({"partition": p, "offset": o})
        }
        Request::ProduceBatch { topic, records } => {
            let mut out = Vec::new();
            for r in records {
                let (p, o) = b.produce(&topic, r.key, r.value, None).map_err(e)?;
                out.push(json!({"partition": p, "offset": o}));
            }
            json!({"results": out})
        }
        Request::Fetch { topic, partition, offset, max, wait_ms } => {
            let t = b.topic(&topic).map_err(e)?;
            let deadline = tokio::time::Instant::now() + wait(wait_ms);
            loop {
                let notified = t.notify.notified();
                let (recs, hw) = b.fetch(&topic, partition, offset, max.min(10_000)).map_err(e)?;
                if !recs.is_empty() || tokio::time::Instant::now() >= deadline {
                    break json!({"records": recs.iter().map(|r| rec_json(None, r)).collect::<Vec<_>>(), "high_watermark": hw});
                }
                let _ = tokio::time::timeout_at(deadline, notified).await;
            }
        }
        Request::Join { group, topic, member } => {
            let (gen, parts) = b.join(&group, &topic, &member).map_err(e)?;
            json!({"generation": gen, "partitions": parts, "members": b.group_members(&group, &topic)})
        }
        Request::Poll { group, topic, member, max, wait_ms } => {
            let (gen, recs) = b.poll(&group, &topic, &member, max.min(10_000), wait(wait_ms)).await.map_err(e)?;
            json!({"generation": gen, "records": recs.iter().map(|(p, r)| rec_json(Some(*p), r)).collect::<Vec<_>>()})
        }
        Request::Commit { group, topic, partition, offset } => {
            b.commit(&group, &topic, partition, offset).map_err(e)?;
            json!({"ok": true})
        }
        Request::Committed { group, topic } => {
            let parts = b.describe(&topic).map_err(e)?;
            json!({"offsets": parts.iter().map(|p| {
                let c = b.committed(&group, &topic, p.partition);
                json!({"partition": p.partition, "committed": c, "end_offset": p.end_offset, "lag": p.end_offset - c.unwrap_or(p.start_offset).min(p.end_offset)})
            }).collect::<Vec<_>>()})
        }
        Request::Leave { group, topic, member } => {
            b.leave(&group, &topic, &member);
            json!({"ok": true})
        }
    })
}

async fn connection(b: Arc<Broker>, stream: TcpStream) -> std::io::Result<()> {
    let (r, mut w) = stream.into_split();
    let mut lines = BufReader::new(r).lines();
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let resp = match serde_json::from_str::<Request>(&line) {
            Ok(req) => match handle(&b, req).await {
                Ok(v) => v,
                Err(msg) => json!({"error": msg}),
            },
            Err(err) => json!({"error": format!("bad request: {err}")}),
        };
        let mut out = serde_json::to_vec(&resp)?;
        out.push(b'\n');
        w.write_all(&out).await?;
    }
    Ok(())
}

pub async fn serve(b: Arc<Broker>, listener: TcpListener) -> std::io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true).ok();
        let b = b.clone();
        tokio::spawn(async move {
            let _ = connection(b, stream).await;
        });
    }
}
