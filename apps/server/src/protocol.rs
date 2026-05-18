use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientCommand {
    Subscribe {
        topic: String,
    },
    Publish {
        topic: String,
        text: String,
    },
    History {
        topic: String,
        since_id: Option<i64>,
        limit: Option<u32>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessagePayload {
    pub id: i64,
    pub topic: String,
    pub text: String,
    pub ts: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    Subscribed {
        topic: String,
    },
    Message {
        id: i64,
        topic: String,
        text: String,
        ts: String,
    },
    History {
        topic: String,
        items: Vec<MessagePayload>,
        oldest_first: bool,
    },
    Error {
        code: String,
        message: String,
    },
}

impl ServerEvent {
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error {
            code: code.into(),
            message: message.into(),
        }
    }
}
