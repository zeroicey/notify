use thiserror::Error;

pub const MAX_TOPIC_LENGTH: usize = 64;
pub const MAX_TEXT_LENGTH: usize = 2_000;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid_topic")]
    InvalidTopic,
    #[error("bad_request")]
    BadRequest,
}

pub fn validate_topic(topic: &str) -> Result<(), ValidationError> {
    let valid = !topic.is_empty()
        && topic.len() <= MAX_TOPIC_LENGTH
        && topic
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'));

    if valid {
        Ok(())
    } else {
        Err(ValidationError::InvalidTopic)
    }
}

pub fn validate_text(text: &str) -> Result<(), ValidationError> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_TEXT_LENGTH {
        Err(ValidationError::BadRequest)
    } else {
        Ok(())
    }
}

pub fn clamp_history_limit(limit: Option<u32>, default_limit: u32, max_limit: u32) -> u32 {
    limit.unwrap_or(default_limit).min(max_limit).max(1)
}
