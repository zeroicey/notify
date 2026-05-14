use std::str::FromStr;

use chrono::Utc;
use sqlx::{
    FromRow, SqlitePool,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};

use crate::protocol::MessagePayload;

const MIGRATION_SQL: &str = include_str!("../migrations/0001_init.sql");

#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
}

#[derive(Debug, FromRow)]
struct MessageRow {
    id: i64,
    topic: String,
    text: String,
    created_at: String,
}

impl From<MessageRow> for MessagePayload {
    fn from(value: MessageRow) -> Self {
        Self {
            id: value.id,
            topic: value.topic,
            text: value.text,
            ts: value.created_at,
        }
    }
}

impl Storage {
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let options = SqliteConnectOptions::from_str(database_url)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let storage = Self { pool };
        storage.migrate().await?;
        Ok(storage)
    }

    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        for statement in MIGRATION_SQL
            .split(';')
            .map(str::trim)
            .filter(|stmt| !stmt.is_empty())
        {
            sqlx::query(statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn insert_message(
        &self,
        topic: &str,
        text: &str,
    ) -> Result<MessagePayload, sqlx::Error> {
        let timestamp = Utc::now().to_rfc3339();
        let result =
            sqlx::query(r#"INSERT INTO messages (topic, text, created_at) VALUES (?1, ?2, ?3)"#)
                .bind(topic)
                .bind(text)
                .bind(&timestamp)
                .execute(&self.pool)
                .await?;

        Ok(MessagePayload {
            id: result.last_insert_rowid(),
            topic: topic.to_owned(),
            text: text.to_owned(),
            ts: timestamp,
        })
    }

    pub async fn fetch_history(
        &self,
        topic: &str,
        since_id: Option<i64>,
        limit: u32,
    ) -> Result<Vec<MessagePayload>, sqlx::Error> {
        let rows = if let Some(since_id) = since_id {
            sqlx::query_as::<_, MessageRow>(
                r#"
                SELECT id, topic, text, created_at
                FROM messages
                WHERE topic = ?1 AND id > ?2
                ORDER BY id ASC
                LIMIT ?3
                "#,
            )
            .bind(topic)
            .bind(since_id)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, MessageRow>(
                r#"
                SELECT id, topic, text, created_at
                FROM (
                    SELECT id, topic, text, created_at
                    FROM messages
                    WHERE topic = ?1
                    ORDER BY id DESC
                    LIMIT ?2
                ) recent
                ORDER BY id ASC
                "#,
            )
            .bind(topic)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().map(Into::into).collect())
    }
}
