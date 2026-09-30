use std::collections::HashSet;
use std::sync::Arc;
use rocket::{get, State};
use rocket::futures::SinkExt;
use rocket::serde::json::serde_json;
use tokio::sync::{mpsc, RwLock};
use ws::{Channel, WebSocket};
use uuid::Uuid;
use crate::chat::room::{Client, RoomManager};
use crate::models::message::ChatMessage;

#[get("/ws/<room_id>/<user_name>")]
pub fn websocket(ws: WebSocket, room_id: &str, user_name: &str, manager: &State<Arc<RwLock<RoomManager>>>) -> Channel<'static> {
    let connection_id = Uuid::new_v4().to_string();
    let manager = manager.inner().clone();
    let room_id = room_id.to_string();
    let user_name = user_name.to_string();

    ws.channel(move |mut stream| {
        Box::pin(async move {
            // =========================================
            // Create private channel for this client
            // =========================================
            let (private_sender, mut private_receiver) = mpsc::unbounded_channel::<ChatMessage>();

            // =========================================
            // Join room
            // =========================================
            let (sender, users, history) = {
                let mut manager = manager.write().await;
                let room = manager.get_or_create_room(&room_id);

                room.clients.insert(
                    connection_id.clone(),
                    Client {
                        username: user_name.clone(),
                        sender: private_sender
                    }
                );

                // =========================================
                // Online user
                // =========================================
                let users = room.clients
                    .values()
                    .map(|client| client.username.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();

                let history = room.history.iter().cloned().collect::<Vec<_>>();

                (room.sender.clone(), users, history)
            };

            // =========================================
            // Subscribe to room broadcast
            // =========================================
            let mut receiver = sender.subscribe();

            for history_manager in history {
                match serde_json::to_string(&history_manager) {
                    Ok(json) => {
                        if let Err(error) = stream.send(json.into()).await {
                            println!("[Room: {}] History send error: {}", room_id, error);
                            break;
                        }
                    }
                    Err(error) => {
                        println!("[Room: {}] History serialization error: {}", room_id, error);
                    }
                }
            }

            // =========================================
            // Notify room that user joined
            // =========================================
            let _ = sender.send(
                ChatMessage {
                    message_type: "join".to_string(),
                    username: user_name.clone(),
                    content: String::new(),
                    users: users.clone(),
                    to: None
                }
            );

            // =========================================
            // Send current users only to this client
            // =========================================
            let user_manager = ChatMessage {
                message_type: "users".to_string(),
                username: String::new(),
                content: String::new(),
                users,
                to: None
            };

            if let Ok(json) = serde_json::to_string(&user_manager) {
                let _ = stream.send(json.into()).await;
            }

            // =========================================
            // Main loop
            // =========================================
            loop {
                tokio::select! {

                }
            }
        })
    })
}