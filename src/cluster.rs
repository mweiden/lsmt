use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    io::Cursor,
    sync::Arc,
};

use murmur3::murmur3_32;
use reqwest::Client;

use crate::{Database, SqlEngine, query::QueryError};
use sqlparser::ast::{ObjectType, Statement};

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
                let token = murmur3_32(&mut cursor, 0).unwrap();
                ring.insert(token, node.clone());
            }
        }
        Self {
            db,
            ring,
            rf: rf.max(1),
            client: Client::new(),
            self_addr,
        }
    }

    fn replicas_for(&self, key: &str) -> Vec<String> {
        let mut cursor = Cursor::new(key.as_bytes());
        let token = murmur3_32(&mut cursor, 0).unwrap();
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
            return engine.execute(&self.db, sql).await;
        }
        // Determine if the statement is a schema mutation that should be
        // broadcast to every node.
        let mut broadcast = false;
        if let Ok(stmts) = engine.parse(sql) {
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
        }

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

        // Collect results from all replicas, unioning unique responses.
        let mut results: BTreeSet<String> = BTreeSet::new();
        let mut last_err: Option<QueryError> = None;
        for node in replicas {
            let resp = if node == self.self_addr {
                engine.execute(&self.db, sql).await
            } else {
                let resp = self
                    .client
                    .post(format!("{}/internal", node))
                    .body(sql.to_string())
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
            };

            match resp {
                Ok(Some(bytes)) => {
                    results.insert(String::from_utf8_lossy(&bytes).to_string());
                }
                Ok(None) => {}
                Err(e) => last_err = Some(e),
            }
        }

        if !results.is_empty() {
            let joined = results.into_iter().collect::<Vec<_>>().join("\n");
            Ok(Some(joined.into_bytes()))
        } else if let Some(err) = last_err {
            Err(err)
        } else {
            Ok(None)
        }
    }
}
