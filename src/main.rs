use mini_kafka::broker::Broker;
use mini_kafka::client::Client;
use mini_kafka::log::LogConfig;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

const USAGE: &str = "\
mini-kafka — a Kafka-style log broker

USAGE:
  mini-kafka broker [--data ./data] [--bind 127.0.0.1:9092] [--segment-bytes N] [--retention-bytes N]
  mini-kafka create <topic> [--partitions 3]
  mini-kafka topics
  mini-kafka describe <topic> [--group g]
  mini-kafka produce <topic> [--key k] [message ...]     (reads stdin lines if no message)
  mini-kafka consume <topic> --group <g> [--member id] [--follow]
  mini-kafka fetch <topic> --partition 0 [--offset 0] [--max 100]

  --server host:port (default 127.0.0.1:9092) applies to all client commands
";

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
}

/// Positional args after the subcommand, skipping `--flag value` pairs.
fn positional(args: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1;
    while i < args.len() {
        if args[i].starts_with("--") {
            i += if args[i] == "--follow" { 1 } else { 2 };
        } else {
            out.push(args[i].clone());
            i += 1;
        }
    }
    out
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(args).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let cmd = args.first().cloned().unwrap_or_default();
    let server = flag(&args, "--server").unwrap_or_else(|| "127.0.0.1:9092".into());
    let pos = positional(&args);
    let topic = || pos.first().cloned().ok_or_else(|| "missing topic".to_string());
    match cmd.as_str() {
        "broker" => {
            let data = flag(&args, "--data").unwrap_or_else(|| "./data".into());
            let bind = flag(&args, "--bind").unwrap_or_else(|| "127.0.0.1:9092".into());
            let mut cfg = LogConfig::default();
            if let Some(s) = flag(&args, "--segment-bytes") {
                cfg.segment_bytes = s.parse().map_err(|_| "bad --segment-bytes")?;
            }
            cfg.retention_bytes = flag(&args, "--retention-bytes").map(|s| s.parse()).transpose().map_err(|_| "bad --retention-bytes")?;
            let broker = Arc::new(Broker::open(&data, cfg).map_err(|e| e.to_string())?);
            let listener = tokio::net::TcpListener::bind(&bind).await.map_err(|e| e.to_string())?;
            println!("mini-kafka broker on {bind}, data in {data}, {} topic(s)", broker.topic_names().len());
            mini_kafka::server::serve(broker, listener).await.map_err(|e| e.to_string())
        }
        "create" => {
            let parts: u32 = flag(&args, "--partitions").unwrap_or_else(|| "3".into()).parse().map_err(|_| "bad --partitions")?;
            let v = Client::connect(&server).await.map_err(|e| e.to_string())?.create_topic(&topic()?, parts).await?;
            println!("{}", if v["created"].as_bool() == Some(true) { "created" } else { "already exists" });
            Ok(())
        }
        "topics" => {
            let v = Client::connect(&server).await.map_err(|e| e.to_string())?.request(json!({"op": "list_topics"})).await?;
            for t in v["topics"].as_array().into_iter().flatten() {
                println!("{}\t{} partition(s)", t["name"].as_str().unwrap_or(""), t["partitions"]);
            }
            Ok(())
        }
        "describe" => {
            let mut c = Client::connect(&server).await.map_err(|e| e.to_string())?;
            let t = topic()?;
            let v = c.request(json!({"op": "describe", "topic": t})).await?;
            println!("partition\tstart\tend\tbytes\tsegments");
            for p in v["partitions"].as_array().into_iter().flatten() {
                println!("{}\t\t{}\t{}\t{}\t{}", p["partition"], p["start_offset"], p["end_offset"], p["bytes"], p["segments"]);
            }
            if let Some(g) = flag(&args, "--group") {
                let v = c.request(json!({"op": "committed", "group": g, "topic": t})).await?;
                println!("\ngroup {g}: partition\tcommitted\tlag");
                for p in v["offsets"].as_array().into_iter().flatten() {
                    println!("{}\t\t{}\t\t{}", p["partition"], p["committed"], p["lag"]);
                }
            }
            Ok(())
        }
        "produce" => {
            let mut c = Client::connect(&server).await.map_err(|e| e.to_string())?;
            let t = topic()?;
            let key = flag(&args, "--key");
            let msgs: Vec<String> = pos[1..].to_vec();
            if !msgs.is_empty() {
                for m in msgs {
                    let (p, o) = c.produce(&t, key.as_deref(), &m).await?;
                    println!("{t}/{p}@{o}");
                }
            } else {
                let mut lines = BufReader::new(tokio::io::stdin()).lines();
                while let Some(l) = lines.next_line().await.map_err(|e| e.to_string())? {
                    let (p, o) = c.produce(&t, key.as_deref(), &l).await?;
                    eprintln!("{t}/{p}@{o}");
                }
            }
            Ok(())
        }
        "consume" => {
            let mut c = Client::connect(&server).await.map_err(|e| e.to_string())?;
            let t = topic()?;
            let group = flag(&args, "--group").ok_or("--group is required")?;
            let member = flag(&args, "--member").unwrap_or_else(|| format!("member-{}", std::process::id()));
            let follow = args.iter().any(|a| a == "--follow");
            loop {
                let recs = c.poll(&group, &t, &member, 500, if follow { 5_000 } else { 200 }).await?;
                if recs.is_empty() && !follow {
                    break;
                }
                let mut next: BTreeMap<u32, u64> = BTreeMap::new();
                for r in &recs {
                    let p = r["partition"].as_u64().unwrap_or(0) as u32;
                    let o = r["offset"].as_u64().unwrap_or(0);
                    println!("[{t}/{p}@{o}] {}{}", r["key"].as_str().map(|k| format!("{k}: ")).unwrap_or_default(), r["value"].as_str().unwrap_or(""));
                    next.insert(p, o + 1);
                }
                for (p, o) in next {
                    c.commit(&group, &t, p, o).await?;
                }
            }
            c.request(json!({"op": "leave", "group": group, "topic": t, "member": member})).await.ok();
            Ok(())
        }
        "fetch" => {
            let mut c = Client::connect(&server).await.map_err(|e| e.to_string())?;
            let p: u32 = flag(&args, "--partition").unwrap_or_else(|| "0".into()).parse().map_err(|_| "bad --partition")?;
            let o: u64 = flag(&args, "--offset").unwrap_or_else(|| "0".into()).parse().map_err(|_| "bad --offset")?;
            let max: usize = flag(&args, "--max").unwrap_or_else(|| "100".into()).parse().map_err(|_| "bad --max")?;
            let v = c.request(json!({"op": "fetch", "topic": topic()?, "partition": p, "offset": o, "max": max})).await?;
            for r in v["records"].as_array().into_iter().flatten() {
                println!("@{} {}", r["offset"], r["value"].as_str().unwrap_or(""));
            }
            println!("high watermark: {}", v["high_watermark"]);
            Ok(())
        }
        _ => {
            eprint!("{USAGE}");
            std::process::exit(2)
        }
    }
}
