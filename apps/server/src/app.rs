use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{
        ConnectInfo, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::{sink::SinkExt, stream::StreamExt};
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
};
use tracing::{error, info, warn};

use crate::{
    config::AppConfig,
    domain::{ValidationError, clamp_history_limit, validate_text, validate_topic},
    hub::TopicHub,
    protocol::{ClientCommand, ServerEvent},
    storage::Storage,
};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub storage: Storage,
    pub hub: TopicHub,
}

pub async fn build_state(config: AppConfig) -> Result<AppState, sqlx::Error> {
    let storage = Storage::connect(&config.database_url).await?;
    Ok(AppState {
        config,
        storage,
        hub: TopicHub::new(),
    })
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_handler))
        .with_state(Arc::new(state))
}

pub async fn serve(
    listener: TcpListener,
    state: AppState,
    shutdown: oneshot::Receiver<()>,
) -> std::io::Result<()> {
    let local_addr = listener.local_addr()?;
    info!(%local_addr, "notify server starting");
    axum::serve(
        listener,
        router(state).into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        let _ = shutdown.await;
    })
    .await
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({"status": "ok"}))
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr, state))
}

async fn handle_socket(socket: WebSocket, addr: SocketAddr, state: Arc<AppState>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (outbound_tx, mut outbound_rx) =
        mpsc::channel::<ServerEvent>(state.config.outbound_queue_capacity);
    let (close_tx, mut close_rx) = watch::channel(false);
    let connection_id = state.hub.register(outbound_tx.clone(), close_tx).await;

    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                changed = close_rx.changed() => {
                    if changed.is_ok() && *close_rx.borrow() {
                        break;
                    }
                }
                maybe_event = outbound_rx.recv() => {
                    match maybe_event {
                        Some(event) => match serde_json::to_string(&event) {
                            Ok(payload) => {
                                if ws_sender.send(Message::Text(payload.into())).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                error!(?err, "failed to serialize server event");
                                break;
                            }
                        },
                        None => break,
                    }
                }
            }
        }
        let _ = ws_sender.close().await;
    });

    while let Some(frame) = ws_receiver.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                if let Err(event) =
                    process_command(connection_id, &state, &outbound_tx, &text).await
                {
                    let _ = outbound_tx.send(event).await;
                }
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(_) => {
                let _ = outbound_tx
                    .send(ServerEvent::error(
                        "bad_request",
                        "only text websocket frames are supported",
                    ))
                    .await;
            }
            Err(err) => {
                warn!(connection_id, peer = %addr, ?err, "websocket receive error");
                break;
            }
        }
    }

    state.hub.unregister(connection_id).await;
    drop(outbound_tx);
    let _ = writer.await;
    info!(connection_id, peer = %addr, "websocket connection closed");
}

async fn process_command(
    connection_id: u64,
    state: &Arc<AppState>,
    outbound_tx: &mpsc::Sender<ServerEvent>,
    payload: &str,
) -> Result<(), ServerEvent> {
    let command = serde_json::from_str::<ClientCommand>(payload).map_err(|err| {
        ServerEvent::error("bad_request", format!("invalid command payload: {err}"))
    })?;

    match command {
        ClientCommand::Subscribe { topic } => {
            validate_topic(&topic).map_err(validation_to_event)?;
            state.hub.subscribe(connection_id, &topic).await;
            outbound_tx
                .send(ServerEvent::Subscribed { topic })
                .await
                .map_err(|_| {
                    ServerEvent::error("storage_failure", "failed to acknowledge subscription")
                })?;
        }
        ClientCommand::Publish { topic, text } => {
            validate_topic(&topic).map_err(validation_to_event)?;
            validate_text(&text).map_err(validation_to_event)?;
            let persisted = state
                .storage
                .insert_message(&topic, &text)
                .await
                .map_err(|err| {
                    ServerEvent::error(
                        "storage_failure",
                        format!("failed to persist message: {err}"),
                    )
                })?;
            state
                .hub
                .broadcast(
                    &topic,
                    ServerEvent::Message {
                        id: persisted.id,
                        topic: persisted.topic,
                        text: persisted.text,
                        ts: persisted.ts,
                    },
                )
                .await;
        }
        ClientCommand::History {
            topic,
            since_id,
            limit,
        } => {
            validate_topic(&topic).map_err(validation_to_event)?;
            let effective_limit = clamp_history_limit(
                limit,
                state.config.default_history_limit,
                state.config.max_history_limit,
            );
            let items = state
                .storage
                .fetch_history(&topic, since_id, effective_limit)
                .await
                .map_err(|err| {
                    ServerEvent::error("storage_failure", format!("failed to read history: {err}"))
                })?;
            outbound_tx
                .send(ServerEvent::History {
                    topic,
                    items,
                    oldest_first: true,
                })
                .await
                .map_err(|_| {
                    ServerEvent::error("storage_failure", "failed to deliver history response")
                })?;
        }
    }

    Ok(())
}

fn validation_to_event(err: ValidationError) -> ServerEvent {
    match err {
        ValidationError::InvalidTopic => ServerEvent::error(
            "invalid_topic",
            "topic must be 1-64 chars using [A-Za-z0-9_.:-]",
        ),
        ValidationError::BadRequest => ServerEvent::error(
            "bad_request",
            "text payload must be non-empty and at most 2000 characters",
        ),
    }
}
