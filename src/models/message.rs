use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChatMessage {
    pub message_type: ServerMessageType,
    pub content: String,
    pub username: String,
    pub users: Vec<String>,
    pub to: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ServerMessageType {
    Message,
    PrivateMessage,
    Join,
    Leave,
    Users
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ClientMessageTye {
    Message,
    PrivateMessage
}

#[derive(Debug, Deserialize)]
pub struct IncomingMessage {
    pub message_type: ClientMessageTye,
    pub content: String,
    pub to: Option<String>,
}
