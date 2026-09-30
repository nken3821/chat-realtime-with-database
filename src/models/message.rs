use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone)]
pub struct ChatMessage {
    pub message_type: String,
    pub content: String,
    pub username: String,
    pub users: Vec<String>,
    pub to: Option<String>
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    Message,
    PrivateMessage
}

#[derive(Debug, Deserialize)]
pub struct IncomingMessage {
    pub message_type: MessageType,
    pub content: String,
    pub to: String
}