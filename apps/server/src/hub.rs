use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{Mutex, mpsc, watch};
use tracing::warn;

use crate::protocol::ServerEvent;

#[derive(Clone)]
pub struct TopicHub {
    inner: std::sync::Arc<Mutex<HubState>>,
    next_connection_id: std::sync::Arc<AtomicU64>,
}

struct ConnectionHandle {
    sender: mpsc::Sender<ServerEvent>,
    close_tx: watch::Sender<bool>,
    topics: HashSet<String>,
}

struct HubState {
    connections: HashMap<u64, ConnectionHandle>,
    topics: HashMap<String, HashSet<u64>>,
}

impl TopicHub {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(Mutex::new(HubState {
                connections: HashMap::new(),
                topics: HashMap::new(),
            })),
            next_connection_id: std::sync::Arc::new(AtomicU64::new(1)),
        }
    }

    pub async fn register(
        &self,
        sender: mpsc::Sender<ServerEvent>,
        close_tx: watch::Sender<bool>,
    ) -> u64 {
        let id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock().await;
        inner.connections.insert(
            id,
            ConnectionHandle {
                sender,
                close_tx,
                topics: HashSet::new(),
            },
        );
        id
    }

    pub async fn subscribe(&self, connection_id: u64, topic: &str) {
        let mut inner = self.inner.lock().await;
        if let Some(connection) = inner.connections.get_mut(&connection_id) {
            connection.topics.insert(topic.to_owned());
            inner
                .topics
                .entry(topic.to_owned())
                .or_default()
                .insert(connection_id);
        }
    }

    pub async fn unregister(&self, connection_id: u64) {
        let mut inner = self.inner.lock().await;
        if let Some(connection) = inner.connections.remove(&connection_id) {
            for topic in connection.topics {
                if let Some(members) = inner.topics.get_mut(&topic) {
                    members.remove(&connection_id);
                    if members.is_empty() {
                        inner.topics.remove(&topic);
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn topic_member_count(&self, topic: &str) -> usize {
        let inner = self.inner.lock().await;
        inner
            .topics
            .get(topic)
            .map(|members| members.len())
            .unwrap_or(0)
    }

    pub async fn broadcast(&self, topic: &str, event: ServerEvent) {
        let targets = {
            let inner = self.inner.lock().await;
            inner
                .topics
                .get(topic)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|connection_id| {
                    inner.connections.get(&connection_id).map(|handle| {
                        (
                            connection_id,
                            handle.sender.clone(),
                            handle.close_tx.clone(),
                        )
                    })
                })
                .collect::<Vec<_>>()
        };

        let mut dropped = Vec::new();
        for (connection_id, sender, close_tx) in targets {
            match sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    let _ = sender.try_send(ServerEvent::error(
                        "queue_overflow",
                        "connection outbound queue overflowed; closing socket",
                    ));
                    let _ = close_tx.send(true);
                    warn!(
                        connection_id,
                        topic, "closing slow subscriber due to queue overflow"
                    );
                    dropped.push(connection_id);
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    let _ = close_tx.send(true);
                    dropped.push(connection_id);
                }
            }
        }

        for connection_id in dropped {
            self.unregister(connection_id).await;
        }
    }
}

impl Default for TopicHub {
    fn default() -> Self {
        Self::new()
    }
}
