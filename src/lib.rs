pub mod app;
pub mod config;
pub mod domain;
pub mod hub;
pub mod protocol;
pub mod storage;

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, time::Duration};

    use futures_util::{SinkExt, StreamExt};
    use tempfile::{TempDir, tempdir};
    use tokio::{
        net::TcpListener,
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
        time::{sleep, timeout},
    };
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientMessage};

    use crate::{
        app::{build_state, serve},
        config::AppConfig,
        domain::{clamp_history_limit, validate_text, validate_topic},
        hub::TopicHub,
        protocol::{ClientCommand, ServerEvent},
    };

    #[test]
    fn topic_validation_rejects_invalid_values() {
        assert!(validate_topic("alerts").is_ok());
        assert!(validate_topic("alerts.ops").is_ok());
        assert!(validate_topic("").is_err());
        assert!(validate_topic("not ok").is_err());
    }

    #[test]
    fn text_validation_rejects_empty_or_oversized_values() {
        assert!(validate_text("hello").is_ok());
        assert!(validate_text("   ").is_err());
        assert!(validate_text(&"x".repeat(2_001)).is_err());
    }

    #[test]
    fn history_limit_defaults_and_caps() {
        assert_eq!(clamp_history_limit(None, 50, 200), 50);
        assert_eq!(clamp_history_limit(Some(999), 50, 200), 200);
        assert_eq!(clamp_history_limit(Some(3), 50, 200), 3);
    }

    #[test]
    fn protocol_roundtrip_serializes_expected_fields() {
        let cmd = serde_json::to_string(&serde_json::json!({
            "type": "history",
            "topic": "alerts",
            "since_id": 12,
            "limit": 50
        }))
        .unwrap();
        let parsed: ClientCommand = serde_json::from_str(&cmd).unwrap();
        match parsed {
            ClientCommand::History {
                topic,
                since_id,
                limit,
            } => {
                assert_eq!(topic, "alerts");
                assert_eq!(since_id, Some(12));
                assert_eq!(limit, Some(50));
            }
            _ => panic!("expected history command"),
        }

        let event = ServerEvent::Message {
            id: 1,
            topic: "alerts".into(),
            text: "hi".into(),
            ts: "2026-05-14T00:00:00Z".into(),
        };
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(encoded.contains("\"id\":1"));
        assert!(encoded.contains("\"type\":\"message\""));
    }

    #[tokio::test]
    async fn ws_flow() {
        let harness = TestHarness::spawn(32).await;
        let (mut subscriber, _) = connect_async(harness.ws_url()).await.unwrap();
        let (mut publisher, _) = connect_async(harness.ws_url()).await.unwrap();

        subscriber
            .send(ClientMessage::Text(
                serde_json::to_string(&serde_json::json!({"type":"subscribe","topic":"alerts"}))
                    .unwrap()
                    .into(),
            ))
            .await
            .unwrap();

        let subscribed = next_event(&mut subscriber).await;
        assert!(matches!(subscribed, ServerEvent::Subscribed { topic } if topic == "alerts"));

        publisher
            .send(ClientMessage::Text(
                serde_json::to_string(&serde_json::json!({
                    "type":"publish",
                    "topic":"alerts",
                    "text":"hello world"
                }))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();

        let event = next_event(&mut subscriber).await;
        let message_id = match event {
            ServerEvent::Message {
                id, topic, text, ..
            } => {
                assert_eq!(topic, "alerts");
                assert_eq!(text, "hello world");
                id
            }
            other => panic!("expected message event, got {other:?}"),
        };
        assert!(message_id > 0);

        let rows: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM messages WHERE topic = ?1")
            .bind("alerts")
            .fetch_one(harness.pool())
            .await
            .unwrap();
        assert_eq!(rows.0, 1);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn history_reconnect() {
        let mut harness = TestHarness::spawn(32).await;
        let (mut publisher, _) = connect_async(harness.ws_url()).await.unwrap();

        for text in ["one", "two", "three"] {
            publisher
                .send(ClientMessage::Text(
                    serde_json::to_string(&serde_json::json!({
                        "type":"publish",
                        "topic":"alerts",
                        "text": text
                    }))
                    .unwrap()
                    .into(),
                ))
                .await
                .unwrap();
        }
        drop(publisher);
        sleep(Duration::from_millis(50)).await;

        let (mut reader, _) = connect_async(harness.ws_url()).await.unwrap();
        reader
            .send(ClientMessage::Text(
                serde_json::to_string(&serde_json::json!({
                    "type":"history",
                    "topic":"alerts"
                }))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();

        let first_history = next_event(&mut reader).await;
        let last_seen = match first_history {
            ServerEvent::History {
                items,
                oldest_first,
                ..
            } => {
                assert!(oldest_first);
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].text, "one");
                assert_eq!(items[2].text, "three");
                items[1].id
            }
            other => panic!("expected history event, got {other:?}"),
        };
        drop(reader);

        harness.restart().await;

        let (mut after_restart, _) = connect_async(harness.ws_url()).await.unwrap();
        after_restart
            .send(ClientMessage::Text(
                serde_json::to_string(&serde_json::json!({
                    "type":"history",
                    "topic":"alerts",
                    "since_id": last_seen,
                    "limit": 999
                }))
                .unwrap()
                .into(),
            ))
            .await
            .unwrap();

        let second_history = next_event(&mut after_restart).await;
        match second_history {
            ServerEvent::History {
                items,
                oldest_first,
                ..
            } => {
                assert!(oldest_first);
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].text, "three");
            }
            other => panic!("expected history event, got {other:?}"),
        }

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn queue_overflow() {
        let hub = TopicHub::new();
        let (tx, mut rx) = mpsc::channel(1);
        let (close_tx, mut close_rx) = watch::channel(false);
        let connection_id = hub.register(tx, close_tx).await;
        hub.subscribe(connection_id, "alerts").await;
        assert_eq!(hub.topic_member_count("alerts").await, 1);

        hub.broadcast(
            "alerts",
            ServerEvent::Message {
                id: 1,
                topic: "alerts".into(),
                text: "first".into(),
                ts: "t1".into(),
            },
        )
        .await;
        hub.broadcast(
            "alerts",
            ServerEvent::Message {
                id: 2,
                topic: "alerts".into(),
                text: "second".into(),
                ts: "t2".into(),
            },
        )
        .await;

        let first = rx.recv().await.expect("first queued event");
        assert!(matches!(first, ServerEvent::Message { id: 1, .. }));
        close_rx
            .changed()
            .await
            .expect("overflow should trigger close signal");
        assert!(
            *close_rx.borrow(),
            "overflow should mark the connection for closure"
        );
        assert_eq!(
            hub.topic_member_count("alerts").await,
            0,
            "overflow should unregister slow subscriber"
        );
    }

    async fn next_event(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> ServerEvent {
        let frame = timeout(Duration::from_secs(5), socket.next())
            .await
            .expect("timed out waiting for websocket frame")
            .expect("socket ended before event arrived")
            .expect("websocket frame error");
        match frame {
            ClientMessage::Text(text) => serde_json::from_str(&text).unwrap(),
            ClientMessage::Close(_) => panic!("socket closed before event arrived"),
            other => panic!("unexpected websocket frame: {other:?}"),
        }
    }

    struct TestHarness {
        _temp_dir: TempDir,
        config: AppConfig,
        addr: SocketAddr,
        pool: sqlx::SqlitePool,
        shutdown: Option<oneshot::Sender<()>>,
        task: Option<JoinHandle<()>>,
    }

    impl TestHarness {
        async fn spawn(queue_capacity: usize) -> Self {
            let temp_dir = tempdir().unwrap();
            let db_path = temp_dir.path().join("notify.sqlite3");
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let config = AppConfig {
                database_url: format!("sqlite://{}", db_path.display()),
                bind_addr: addr,
                outbound_queue_capacity: queue_capacity,
                ..AppConfig::default()
            };
            let (pool, shutdown, task) = spawn_server(config.clone(), listener).await;
            Self {
                _temp_dir: temp_dir,
                config,
                addr,
                pool,
                shutdown: Some(shutdown),
                task: Some(task),
            }
        }

        fn pool(&self) -> &sqlx::SqlitePool {
            &self.pool
        }

        fn ws_url(&self) -> String {
            format!("ws://{}/ws", self.addr)
        }

        async fn restart(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(task) = self.task.take() {
                let _ = task.await;
            }

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            self.addr = listener.local_addr().unwrap();
            self.config.bind_addr = self.addr;
            let (pool, shutdown, task) = spawn_server(self.config.clone(), listener).await;
            self.pool = pool;
            self.shutdown = Some(shutdown);
            self.task = Some(task);
        }

        async fn shutdown(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(task) = self.task.take() {
                let _ = task.await;
            }
        }
    }

    async fn spawn_server(
        config: AppConfig,
        listener: TcpListener,
    ) -> (sqlx::SqlitePool, oneshot::Sender<()>, JoinHandle<()>) {
        let state = build_state(config).await.unwrap();
        let pool = state.storage.pool().clone();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            serve(listener, state, shutdown_rx).await.unwrap();
        });
        sleep(Duration::from_millis(50)).await;
        (pool, shutdown_tx, task)
    }
}
