use crate::auth::user_identity::find_user_identity;
use crate::chat::room::{ RoomManager };
use crate::db::DbPool;
use crate::models::{
    ChatMessage, 
    IncomingMessage, 
    ServerMessageType, 
    ClientMessageTye, 
    AuthenticationMessage 
};
use crate::auth::{user_identity, verify_token};
use rocket::futures::{ SinkExt, StreamExt };
use rocket::serde::json::serde_json;
use rocket::{ State, get };
use std::env;
use std::sync::Arc;
use tokio::sync::{ RwLock, mpsc };
use uuid::Uuid;
use ws::{ Channel, WebSocket };

fn error_message(content: impl Into<String>) -> ChatMessage {
    ChatMessage {
        message_type: ServerMessageType::Error,
        content: content.into(),
        username: String::new(),
        users: Vec::new(),
        to: None,
    }
}

#[get("/ws/<room_id>")]
pub fn websocket(
    ws: WebSocket,
    room_id: &str,
    db: &State<DbPool>,
    manager: &State<Arc<RwLock<RoomManager>>>
) -> Channel<'static> {
    let connection_id = Uuid::new_v4().to_string();
    let manager = manager.inner().clone();
    let room_id = room_id.to_string();
    let db = db.inner().clone();

    ws.channel(move |mut stream| {
        Box::pin(async move {

            // =========================================
            // AUTHENTICATION
            // =========================================
            let authentication = match stream.next().await {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    println!(
                        "[Room: {}] Authentication WebSocket error: {}",
                        room_id,
                        error
                    );
                    return Ok(());
                }
                None => {
                    println!(
                        "[Room: {}] Client disconnected before authentication",
                        room_id
                    );
                    return Ok(());
                }
            };

            let text = match authentication.to_text() {
                Ok(text) => text,
                Err(_) => {
                    println!(
                        "[Room: {}] Authentication must be text",
                        room_id
                    );
                    return Ok(());
                }
            };

            let auth = match serde_json::from_str::<AuthenticationMessage>(text) {
                Ok(auth) => auth,
                Err(error) => {
                    println!(
                        "[Room: {}] Invalid authentication: {}",
                        room_id,
                        error
                    );
                    return Ok(());
                }
            };

            if auth.token.trim().is_empty() {
                println!(
                    "[Room: {}] Empty token",
                    room_id
                );
                return Ok(());
            }

            let secret = match env::var("JWT_SECRET") {
                Ok(secret) => secret,
                Err(_) => {
                    println!("JWT_SECRET is not configured");
                    return Ok(());
                }
            };

            let claims = match verify_token(&auth.token, &secret) {
                Ok(claims) => claims,
                Err(error) => {
                    println!(
                        "[Room: {}] Invalid JWT: {}",
                        room_id,
                        error
                    );

                    return Ok(());
                }
            };

            let user_id = claims.sub;

            println!(
                "[Room: {}] JWT authenticated user_id: {}",
                room_id,
                user_id
            );

            let user_identity = {
                let mut connection = match db.get() {
                    Ok(connection) => connection,
                    Err(error) => {
                        println!(
                            "[Room: {}] Database connection error: {}",
                            room_id,
                            error
                        );
                        return Ok(());
                    }
                };

                match find_user_identity(user_id, &mut connection) {
                    Ok(user) => user,
                    Err(diesel::result::Error::NotFound) => {
                        println!(
                            "[Room: {}] User {} not found",
                            room_id,
                            user_id
                        );
                        return Ok(());
                    }
                    Err(error) => {
                        println!(
                            "[Room: {}] Database query error: {}",
                            room_id,
                            error
                        );
                        return Ok(());
                    }
                }
            };

            let user_name = user_identity.username;

            println!(
                "[Room: {}] User authenticated: {}",
                room_id,
                user_name
            );

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

                manager.add_client(
                    &room_id,
                    connection_id.clone(),
                    user_id,
                    user_name.clone(),
                    private_sender
                );

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
            let _ = sender.send(ChatMessage {
                message_type: ServerMessageType::Join,
                username: user_name.clone(),
                content: String::new(),
                users: users.clone(),
                to: None,
            });

            // =========================================
            // Send current users only to this client
            // =========================================
            let user_manager = ChatMessage {
                message_type: ServerMessageType::Users,
                username: String::new(),
                content: String::new(),
                users,
                to: None,
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
                                                
                                                let response = error_message("Message content cannot be empty");
                                                
                                                if let Ok(json) = serde_json::to_string(&response) {
                                                    let _ = stream.send(json.into()).await;
                                                }

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
                                                    let target_user_id = match incoming.to {
                                                        Some(user_id) => user_id,
                                                        None => {
                                                            println!(
                                                                "[Room: {}] Private message missing target",
                                                                room_id
                                                            );
                                                            
                                                            let response = error_message("Private message requires a target");

                                                            if let Ok(json) = serde_json::to_string(&response) {
                                                                let _ = stream.send(json.into()).await;
                                                            }
                                                            continue;
                                                        }
                                                    };

                                                    // =================================
                                                    // Find ALL connections of target user
                                                    // =================================
                                                    let target_senders = {
                                                        let manager = manager.read().await;
                                                        manager.get_client_sender(&room_id, target_user_id)
                                                    };

                                                    if target_senders.is_empty() {
                                                        let response = error_message(format!("User '{}' is not online", target_user_id));
                                                        if let Ok(json) = serde_json::to_string(&response) {
                                                            let _ = stream.send(json.into()).await;
                                                        } 
                                                        continue;
                                                    }

                                                    // =================================
                                                    // Create private message
                                                    // =================================
                                                    let private_message = ChatMessage {
                                                        message_type: ServerMessageType::PrivateMessage,
                                                        username: user_name.clone(),
                                                        content: incoming.content,
                                                        users: Vec::new(),
                                                        to: Some(target_user_id)
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

                                            let response = error_message("Invalid message format");

                                            if let Ok(json) = serde_json::to_string(&response) {
                                                let _ = stream.send(json.into()).await;
                                            }
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
            let should_notify_leave = {
                let mut manager = manager.write().await;
                manager.remove_client_and_cleanup(&room_id, &connection_id, user_id)
            };

            // =========================================
            // Notify room about LEAVE
            // =========================================
            if should_notify_leave.user_left {
                let _ = sender.send(ChatMessage {
                message_type: ServerMessageType::Leave,
                username: user_name.clone(),
                content: String::new(),
                users: should_notify_leave.users,
                to: None,
                });
            }

            Ok(())
        })
    })
}
