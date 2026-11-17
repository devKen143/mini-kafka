//! A small async client for the JSON-lines protocol.

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub struct Client {
    lines: Lines<BufReader<OwnedReadHalf>>,
    w: OwnedWriteHalf,
}

impl Client {
    pub async fn connect(addr: &str) -> std::io::Result<Self> {
        let s = TcpStream::connect(addr).await?;
        s.set_nodelay(true).ok();
        let (r, w) = s.into_split();
        Ok(Client { lines: BufReader::new(r).lines(), w })
    }

    pub async fn request(&mut self, req: Value) -> Result<Value, String> {
        let mut line = serde_json::to_vec(&req).map_err(|e| e.to_string())?;
        line.push(b'\n');
        self.w.write_all(&line).await.map_err(|e| e.to_string())?;
        let resp = self.lines.next_line().await.map_err(|e| e.to_string())?.ok_or("connection closed")?;
        let v: Value = serde_json::from_str(&resp).map_err(|e| e.to_string())?;
        match v.get("error").and_then(Value::as_str) {
            Some(msg) => Err(msg.to_string()),
            None => Ok(v),
        }
    }

    pub async fn create_topic(&mut self, topic: &str, partitions: u32) -> Result<Value, String> {
        self.request(json!({"op": "create_topic", "topic": topic, "partitions": partitions})).await
    }

    pub async fn produce(&mut self, topic: &str, key: Option<&str>, value: &str) -> Result<(u32, u64), String> {
        let v = self.request(json!({"op": "produce", "topic": topic, "key": key, "value": value})).await?;
        Ok((v["partition"].as_u64().unwrap_or(0) as u32, v["offset"].as_u64().unwrap_or(0)))
    }

    pub async fn poll(&mut self, group: &str, topic: &str, member: &str, max: usize, wait_ms: u64) -> Result<Vec<Value>, String> {
        let v = self.request(json!({"op": "poll", "group": group, "topic": topic, "member": member, "max": max, "wait_ms": wait_ms})).await?;
        Ok(v["records"].as_array().cloned().unwrap_or_default())
    }

    pub async fn commit(&mut self, group: &str, topic: &str, partition: u32, offset: u64) -> Result<(), String> {
        self.request(json!({"op": "commit", "group": group, "topic": topic, "partition": partition, "offset": offset})).await.map(|_| ())
    }
}
