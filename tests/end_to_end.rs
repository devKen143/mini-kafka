use mini_kafka::broker::Broker;
use mini_kafka::client::Client;
use mini_kafka::log::LogConfig;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};

async fn start() -> (String, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Arc::new(Broker::open(dir.path(), LogConfig { segment_bytes: 4096, ..Default::default() }).unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(mini_kafka::server::serve(broker, listener));
    (addr, dir)
}

#[tokio::test]
async fn produce_and_consume_with_groups() {
    let (addr, _dir) = start().await;
    let mut c = Client::connect(&addr).await.unwrap();
    c.create_topic("clicks", 3).await.unwrap();
    for i in 0..300 {
        c.produce("clicks", Some(&format!("user{}", i % 7)), &format!("click {i}")).await.unwrap();
    }

    // two members of one group split the partitions; together they see every message once
    let mut a = Client::connect(&addr).await.unwrap();
    let mut b = Client::connect(&addr).await.unwrap();
    a.request(json!({"op": "join", "group": "g", "topic": "clicks", "member": "a"})).await.unwrap();
    b.request(json!({"op": "join", "group": "g", "topic": "clicks", "member": "b"})).await.unwrap();
    let mut seen = std::collections::HashSet::new();
    for (client, member) in [(&mut a, "a"), (&mut b, "b")] {
        loop {
            let recs = client.poll("g", "clicks", member, 1000, 0).await.unwrap();
            if recs.is_empty() {
                break;
            }
            let mut last = std::collections::HashMap::new();
            for r in recs {
                assert!(seen.insert(r["value"].as_str().unwrap().to_string()), "duplicate delivery");
                last.insert(r["partition"].as_u64().unwrap() as u32, r["offset"].as_u64().unwrap());
            }
            for (p, o) in last {
                client.commit("g", "clicks", p, o + 1).await.unwrap();
            }
        }
    }
    assert_eq!(seen.len(), 300);

    let lag = c.request(json!({"op": "committed", "group": "g", "topic": "clicks"})).await.unwrap();
    assert!(lag["offsets"].as_array().unwrap().iter().all(|p| p["lag"] == 0));
}

#[tokio::test]
async fn long_poll_wakes_on_produce() {
    let (addr, _dir) = start().await;
    let mut c = Client::connect(&addr).await.unwrap();
    c.create_topic("live", 1).await.unwrap();
    let addr2 = addr.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        Client::connect(&addr2).await.unwrap().produce("live", None, "ping").await.unwrap();
    });
    let t0 = Instant::now();
    let v = c.request(json!({"op": "fetch", "topic": "live", "partition": 0, "offset": 0, "wait_ms": 5000})).await.unwrap();
    assert_eq!(v["records"][0]["value"], "ping");
    assert!(t0.elapsed() < Duration::from_secs(2), "fetch should return as soon as data arrives");
}

#[tokio::test]
async fn reports_errors() {
    let (addr, _dir) = start().await;
    let mut c = Client::connect(&addr).await.unwrap();
    assert!(c.produce("nope", None, "x").await.unwrap_err().contains("unknown topic"));
    assert!(c.request(json!({"op": "dance"})).await.unwrap_err().contains("bad request"));
}
