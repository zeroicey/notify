use std::{env, net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub default_history_limit: u32,
    pub max_history_limit: u32,
    pub outbound_queue_capacity: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        let db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("notify.sqlite3");
        Self {
            bind_addr: "127.0.0.1:3000".parse().expect("valid default bind"),
            database_url: format!("sqlite://{}", db_path.display()),
            default_history_limit: 50,
            max_history_limit: 200,
            outbound_queue_capacity: 32,
        }
    }
}

impl AppConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        let bind_addr = env::var("NOTIFY_BIND_ADDR")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(defaults.bind_addr);
        let database_url = env::var("NOTIFY_DATABASE_URL").unwrap_or(defaults.database_url);
        let default_history_limit = env::var("NOTIFY_DEFAULT_HISTORY_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(defaults.default_history_limit);
        let max_history_limit = env::var("NOTIFY_MAX_HISTORY_LIMIT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(defaults.max_history_limit);
        let outbound_queue_capacity = env::var("NOTIFY_OUTBOUND_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(defaults.outbound_queue_capacity);

        Self {
            bind_addr,
            database_url,
            default_history_limit,
            max_history_limit,
            outbound_queue_capacity,
        }
    }
}
