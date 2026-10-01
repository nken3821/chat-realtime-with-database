use crate::chat::room::{RoomManager};
use crate::models::message::{ChatMessage, IncomingMessage, ServerMessageType, ClientMessageTye};
use rocket::futures::{SinkExt, StreamExt};
use rocket::serde::json::serde_json;
use rocket::{State, get};
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;
use ws::{Channel, WebSocket};

#[get("/ws/<room_id>/<user_name>")]
pub fn websocket(
    ws: WebSocket,
    room_id: &str,
    user_name: &str,
    manager: &State<Arc<RwLock<RoomManager>>>,
) -> Channel<'static> {
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
                let sender = {
                    let room = manager.get_or_create_room(&room_id);
                    room.sender.clone()
                };

                manager.add_client(&room_id, connection_id.clone(), user_name.clone(), private_sender);

                // =========================================
                // Online user
                // =========================================
                let users = manager.get_online_users(&room_id);

                let history = manager.get_history(&room_id);

                (sender, users, history)
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
                    message_type: ServerMessageType::Join,
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
                message_type: ServerMessageType::Users,
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
                    // =================================
                    // 1. Client -> Server
                    // =================================
                    message = stream.next() => {
                        match message {
                            Some(Ok(message)) => {
                                println!(
                                        "[Room:{}] [{}] Received: {:?}",
                                        room_id,
                                        user_name,
                                        message
                                );
                                if let Ok(text) = message.to_text() {
                                    match serde_json::from_str::<IncomingMessage>(text) {
                                        Ok(incoming) => {
                                            // =================================
                                            // Validate content
                                            // =================================
                                            if incoming.content.trim().is_empty() {
                                                println!(
                                                        "[Room: {}] Empty message rejected",
                                                        room_id
                                                );
                                                continue;
                                            }

                                            // =================================
                                            // Handle message type
                                            // =================================
                                            match incoming.message_type {
                                                // =================================
                                                // Normal room message
                                                // =================================
                                                ClientMessageTye::Message => {
                                                    let chat_message = ChatMessage {
                                                        message_type: ServerMessageType::Message,
                                                        content: incoming.content,
                                                        username: user_name.clone(),
                                                        users: Vec::new(),
                                                        to: None,
                                                    };

                                                    // =================================
                                                    // Save to history
                                                    // =================================
                                                    {
                                                        let mut manager = manager.write().await;
                                                        manager.add_history(&room_id, chat_message.clone());
                                                    }   

                                                    // =================================
                                                    // Broadcast to room
                                                    // =================================
                                                    let _ = sender.send(chat_message);
                                                }
                                                // =================================
                                                // Private message
                                                // =================================
                                                ClientMessageTye::PrivateMessage => {
                                                    let target_username = match incoming.to {
                                                        Some(username) => username,
                                                        None => {
                                                            println!(
                                                                "[Room: {}] Private message missing target",
                                                                room_id
                                                            );
                                                            continue;
                                                        }
                                                    };

                                                    // =================================
                                                    // Find ALL connections of target user
                                                    // =================================
                                                    let target_senders = {
                                                        let manager = manager.read().await;
                                                        manager.get_client_sender(&room_id, &target_username)
                                                    };

                                                    // =================================
                                                    // Create private message
                                                    // =================================
                                                    let private_message = ChatMessage {
                                                        message_type: ServerMessageType::PrivateMessage,
                                                        username: user_name.clone(),
                                                        content: incoming.content,
                                                        users: Vec::new(),
                                                        to: Some(target_username.clone())
                                                    };

                                                    // =================================
                                                    // Send to all target connections
                                                    // =================================
                                                    for target_sender in target_senders {
                                                        let _ = target_sender.send(private_message.clone());
                                                    }
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            println!(
                                                "[Room: {}] Invalid JSON: {}",
                                                room_id,
                                                error
                                            );
                                        }
                                    }
                                }
                            }
                            Some(Err(error)) => {
                                println!(
                                    "[Room: {}] WebSocket error: {}",
                                    room_id,
                                    error
                                );
                                break;
                            }
                            None => {
                                println!(
                                    "[Room: {}] [{}] Client disconnected",
                                    room_id,
                                    user_name
                                );
                                break;
                            }
                        }
                    }

                    // =================================
                    // 2. Room broadcast -> Client
                    // =================================
                    message = receiver.recv() => {
                        match message {
                            Ok(chat_message) => {
                                match serde_json::to_string(&chat_message) {
                                    Ok(json) => {
                                        if let Err(error) = stream.send(json.into()).await {
                                            println!("[Room: {}] Send error: {}", room_id, error);
                                            break;
                                        }
                                    }
                                    Err(error) => {
                                        println!("[Room: {}] Serialization error: {}", room_id, error);
                                    }
                                }
                            }
                            Err(error) => {
                                println!("[Room: {}] Broadcast error: {}", room_id, error);
                                break;
                            }
                        }
                    }

                    // =================================
                    // 3. Private message -> Client
                    // =================================
                    Some(private_message) = private_receiver.recv() => {
                        match serde_json::to_string(&private_message) {
                            Ok(json) => {
                                if let Err(error) = stream.send(json.into()).await {
                                    println!("[Room: {}] Private send error: {}", room_id, error);
                                    break;
                                }
                            }
                            Err(error) => {
                                println!("[Room: {}] Private serialization error: {}", room_id, error);
                            }
                        }
                    }

                }
            }
            // =========================================
            // Remove client from room
            // =========================================
            let users = {
                let mut manager = manager.write().await;
                manager.remove_client_and_cleanup(&room_id, &connection_id)
            };

            // =========================================
            // Notify room about LEAVE
            // =========================================
            let _ = sender.send(ChatMessage {
                message_type: ServerMessageType::Leave,
                username: user_name.clone(),
                content: String::new(),
                users,
                to: None
            });

            Ok(())
        })
    })
}
