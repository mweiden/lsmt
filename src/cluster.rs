use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::future::join_all;
use murmur3::murmur3_32;
use reqwest::Client;

use crate::{Database, SqlEngine, query::QueryError};
use serde_json::{Value, json};
use sqlparser::ast::{ObjectType, Statement};
use tokio::{sync::RwLock, time::sleep};

/// Simple cluster management and request coordination.
///
/// This implementation provides a very lightweight rendition of a
/// peer-to-peer ring with configurable replication.  Nodes are
/// identified by their base HTTP address (e.g. `http://127.0.0.1:8080`).
/// Each node owns a number of virtual nodes on the ring in order to
/// balance load.  Requests are replicated to the selected peers based on
/// a Murmur3 hash of the incoming statement.
pub struct Cluster {
    db: Arc<Database>,
    ring: BTreeMap<u32, String>,
    rf: usize,
    client: Client,
    self_addr: String,
    health: Arc<RwLock<HashMap<String, Instant>>>,
}

impl Cluster {
    /// Create a new cluster coordinator.
    pub fn new(
        db: Arc<Database>,
        self_addr: String,
        mut peers: Vec<String>,
        vnodes: usize,
        rf: usize,
    ) -> Self {
        peers.push(self_addr.clone());
        let mut ring = BTreeMap::new();
        for node in peers.iter() {
            for v in 0..vnodes.max(1) {
                let token_key = format!("{}-{}", node, v);
                let mut cursor = Cursor::new(token_key.as_bytes());
                let token = murmur3_32(&mut cursor, 0).unwrap_or(0);
                ring.insert(token, node.clone());
            }
        }

        let client = Client::new();
        let mut initial = HashMap::new();
        for p in peers.iter() {
            if p != &self_addr {
                initial.insert(p.clone(), Instant::now());
            }
        }
        let health = Arc::new(RwLock::new(initial));
        let gossip_client = client.clone();
        let gossip_peers = peers.clone();
        let gossip_addr = self_addr.clone();
        let gossip_health = health.clone();
        tokio::spawn(async move {
            let mut idx = 0usize;
            loop {
                if gossip_peers.is_empty() {
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                idx = (idx + 1) % gossip_peers.len();
                let peer = &gossip_peers[idx];
                if peer != &gossip_addr {
                    if let Ok(resp) = gossip_client.get(format!("{}/health", peer)).send().await {
                        if resp.status().is_success() {
                            let _ = resp.bytes().await;
                            let mut map = gossip_health.write().await;
                            map.insert(peer.clone(), Instant::now());
                        }
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        });

        Self {
            db,
            ring,
            rf: rf.max(1),
            client,
            self_addr,
            health,
        }
    }

    pub fn replicas_for(&self, key: &str) -> Vec<String> {
        let mut cursor = Cursor::new(key.as_bytes());
        let token = murmur3_32(&mut cursor, 0).unwrap_or(0);
        let mut reps = Vec::new();
        for (_k, node) in self.ring.range(token..) {
            if !reps.contains(node) {
                reps.push(node.clone());
            }
            if reps.len() == self.rf {
                return reps;
            }
        }
        for (_k, node) in &self.ring {
            if !reps.contains(node) {
                reps.push(node.clone());
            }
            if reps.len() == self.rf {
                break;
            }
        }
        reps
    }

    pub async fn is_alive(&self, node: &str) -> bool {
        if node == self.self_addr {
            return true;
        }
        let map = self.health.read().await;
        map.get(node)
            .map(|t| t.elapsed() < Duration::from_secs(8))
            .unwrap_or(false)
    }

    pub async fn peer_health(&self) -> Vec<(String, bool)> {
        let map = self.health.read().await;
        map.iter()
            .map(|(peer, t)| (peer.clone(), t.elapsed() < Duration::from_secs(8)))
            .collect()
    }

    pub fn health_info(&self) -> Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let tokens: Vec<u32> = self
            .ring
            .iter()
            .filter_map(|(tok, node)| (node == &self.self_addr).then_some(*tok))
            .collect();
        json!({
            "node": self.self_addr,
            "timestamp": now,
            "tokens": tokens,
        })
    }

    /// Execute `sql` against the appropriate replicas.
    ///
    /// When `forwarded` is false the current node acts as the coordinator
    /// and forwards the statement to the replica nodes determined by the
    /// partition key.  Results from all replicas are unioned together and
    /// returned to the caller.  When `forwarded` is true the query is being
    /// handled on behalf of a peer and is executed locally without further
    /// replication.
    pub async fn execute(&self, sql: &str, forwarded: bool) -> Result<Option<Vec<u8>>, QueryError> {
        let engine = SqlEngine::new();
        if forwarded {
            let (ts, real_sql) = if let Some(rest) = sql.strip_prefix("--ts:") {
                if let Some(pos) = rest.find('\n') {
                    let ts = rest[..pos].parse().unwrap_or(0);
                    (ts, &rest[pos + 1..])
                } else {
                    (0, sql)
                }
            } else {
                (0, sql)
            };
            return engine.execute_with_ts(&self.db, real_sql, ts, true).await;
        }

        // Determine if the statement is a schema mutation that should be
        // broadcast to every node and whether it is a write.
        let mut broadcast = false;
        let mut is_write = false;
        let mut first_stmt: Option<Statement> = None;
        if let Ok(stmts) = engine.parse(sql) {
            if let Some(st) = stmts.first() {
                first_stmt = Some(st.clone());
            }
            broadcast = stmts.iter().all(|s| {
                matches!(
                    s,
                    Statement::CreateTable(_)
                        | Statement::Drop {
                            object_type: ObjectType::Table,
                            ..
                        }
                )
            });
            is_write = stmts.iter().any(|s| {
                matches!(
                    s,
                    Statement::Insert(_)
                        | Statement::Update { .. }
                        | Statement::Delete(_)
                        | Statement::CreateTable(_)
                        | Statement::Drop {
                            object_type: ObjectType::Table,
                            ..
                        }
                )
            });
        }

        let ts = if is_write {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_micros() as u64
        } else {
            0
        };

        // Work out the target replica set.
        let mut replicas: HashSet<String> = HashSet::new();
        if broadcast {
            replicas.extend(self.ring.values().cloned());
        } else {
            let keys = engine
                .partition_keys(&self.db, sql)
                .await
                .unwrap_or_else(|_| vec![sql.to_string()]);
            for key in keys {
                for node in self.replicas_for(&key) {
                    replicas.insert(node);
                }
            }
            if replicas.is_empty() {
                replicas.insert(self.self_addr.clone());
            }
        }

        let mut replicas: Vec<String> = replicas.into_iter().collect();
        let mut healthy: Vec<String> = Vec::new();
        for node in replicas.drain(..) {
            if self.is_alive(&node).await {
                healthy.push(node);
            }
        }
        if !broadcast && healthy.len() < self.rf {
            return Err(QueryError::Other("not enough healthy replicas".into()));
        }

        // Collect results from all replicas, performing last-write-wins per key.
        let mut rows: BTreeMap<String, (u64, String)> = BTreeMap::new();
        let mut others: BTreeSet<String> = BTreeSet::new();
        let mut last_err: Option<QueryError> = None;
        let mut row_count: u64 = 0;

        let sql_string = sql.to_string();
        let tasks: Vec<_> = healthy
            .into_iter()
            .map(|node| {
                let db = self.db.clone();
                let client = self.client.clone();
                let self_addr = self.self_addr.clone();
                let sql_clone = sql_string.clone();
                async move {
                    let payload = if ts > 0 {
                        format!("--ts:{}\n{}", ts, sql_clone.clone())
                    } else {
                        sql_clone.clone()
                    };
                    if node == self_addr {
                        let engine = SqlEngine::new();
                        engine.execute_with_ts(&db, &sql_clone, ts, true).await
                    } else {
                        let resp = client
                            .post(format!("{}/internal", node))
                            .body(payload)
                            .send()
                            .await;
                        match resp {
                            Ok(resp) => {
                                if resp.status().is_success() {
                                    match resp.bytes().await {
                                        Ok(bytes) => {
                                            if bytes.is_empty() {
                                                Ok(None)
                                            } else {
                                                Ok(Some(bytes.to_vec()))
                                            }
                                        }
                                        Err(e) => Err(QueryError::Other(e.to_string())),
                                    }
                                } else {
                                    Err(QueryError::Other(format!("status {}", resp.status())))
                                }
                            }
                            Err(e) => Err(QueryError::Other(e.to_string())),
                        }
                    }
                }
            })
            .collect();

        for resp in join_all(tasks).await {
            match resp {
                Ok(Some(bytes)) => {
                    if is_write {
                        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                            if let Some(c) = v.get("count").and_then(|c| c.as_u64()) {
                                row_count = row_count.max(c);
                            }
                        }
                    } else {
                        let text = String::from_utf8_lossy(&bytes);
                        for line in text.lines() {
                            let parts: Vec<&str> = line.splitn(3, '\t').collect();
                            if parts.len() == 3 {
                                let key = parts[0].to_string();
                                let ts: u64 = parts[1].parse().unwrap_or(0);
                                let val = parts[2].to_string();
                                match rows.get(&key) {
                                    Some((cur_ts, _)) if *cur_ts >= ts => {}
                                    _ => {
                                        rows.insert(key, (ts, val));
                                    }
                                }
                            } else if !line.is_empty() {
                                others.insert(line.to_string());
                            }
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => last_err = Some(e),
            }
        }

        if is_write {
            let count = match first_stmt {
                Some(Statement::CreateTable(_))
                | Some(Statement::Drop {
                    object_type: ObjectType::Table,
                    ..
                }) => 1,
                _ => row_count as usize,
            };
            let obj = match first_stmt {
                Some(Statement::Insert(_)) => json!({"op":"INSERT","unit":"row","count":count}),
                Some(Statement::Update { .. }) => json!({"op":"UPDATE","unit":"row","count":count}),
                Some(Statement::Delete(_)) => json!({"op":"DELETE","unit":"row","count":count}),
                Some(Statement::CreateTable(_)) => {
                    json!({"op":"CREATE TABLE","unit":"table","count":count})
                }
                Some(Statement::Drop {
                    object_type: ObjectType::Table,
                    ..
                }) => json!({"op":"DROP TABLE","unit":"table","count":count}),
                _ => json!({"op":"UNKNOWN","unit":"","count":count}),
            };
            let bytes = serde_json::to_vec(&obj).map_err(|e| QueryError::Other(e.to_string()))?;
            Ok(Some(bytes))
        } else if !rows.is_empty() {
            let mut arr: Vec<Value> = Vec::new();
            for (_k, (_ts, val)) in rows {
                if !val.is_empty() {
                    if let Ok(v) = serde_json::from_str::<Value>(&val) {
                        arr.push(v);
                    }
                }
            }
            let bytes = serde_json::to_vec(&arr).map_err(|e| QueryError::Other(e.to_string()))?;
            Ok(Some(bytes))
        } else if !others.is_empty() {
            if let Some(line) = others.into_iter().next() {
                Ok(Some(line.into_bytes()))
            } else {
                Ok(Some(b"[]".to_vec()))
            }
        } else if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(Some(b"[]".to_vec()))
        }
    }
}
