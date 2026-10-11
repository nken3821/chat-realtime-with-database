use crate::schema::messages;
use chrono::NaiveDateTime;
use diesel::{
    Insertable, Selectable,
    deserialize::{Queryable},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = messages)]
#[diesel(check_for_backend(diesel::mysql::Mysql))]
pub struct Message {
    pub id: i32,
    pub room_id: String,
    pub user_id: i32,
    pub content: String,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = messages)]
pub struct NewMessage<'a> {
    pub room_id: &'a str,
    pub user_id: i32,
    pub content: &'a str,
}

#[derive(Debug, Queryable, Serialize)]
pub struct MessageWithUsername {
    pub id: i32,
    pub room_id: String,
    pub user_id: i32,
    pub username: String,
    pub content: String,
    pub created_at: NaiveDateTime,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChatMessage {
    pub id: Option<i32>,
    pub message_type: ServerMessageType,
    pub content: String,
    pub username: String,
    pub users: Vec<String>,
    pub to: Option<i32>,
    pub created_at: Option<String>,
    pub has_more: Option<bool>
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ServerMessageType {
    Message,
    PrivateMessage,
    Join,
    Leave,
    Users,
    Error,
    History
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ClientMessageTye {
    Message,
    PrivateMessage,
    LoadHistory
}

#[derive(Debug, Deserialize)]
pub struct IncomingMessage {
    pub message_type: ClientMessageTye,
    pub content: String,
    pub to: Option<i32>,
    pub before_id: Option<i32>
}
