use std::process;

use notify::{app, config::AppConfig};
use tokio::{net::TcpListener, sync::oneshot};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder().with_target(false).finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let config = AppConfig::from_env();
    let bind_addr = config.bind_addr;

    let state = match app::build_state(config.clone()).await {
        Ok(state) => state,
        Err(error) => {
            eprintln!("failed to initialize app state: {error}");
            process::exit(1);
        }
    };

    let listener = match TcpListener::bind(bind_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("failed to bind {bind_addr}: {error}");
            process::exit(1);
        }
    };

    let (_shutdown_tx, shutdown_rx) = oneshot::channel();
    if let Err(error) = app::serve(listener, state, shutdown_rx).await {
        eprintln!("server exited with error: {error}");
        process::exit(1);
    }
}
