
use serde::Serialize;

use crate::models::message::MessageWithUsername;

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub message_type: &'static str,
    pub messages: Vec<HistoryMessage>,
    pub has_more: bool,
    pub oldest_id: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct HistoryMessage {
    pub id: i32,
    pub username: String,
    pub content: String,
    pub created_at: String,
}