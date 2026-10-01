use crate::models::message::ChatMessage;
use std::{collections::{ HashMap, HashSet, VecDeque }, str};
use tokio::sync::{ broadcast, mpsc };

pub struct Client {
    pub user_id: i32,
    pub username: String,
    pub sender: mpsc::UnboundedSender<ChatMessage>,
}

pub struct Room {
    pub sender: broadcast::Sender<ChatMessage>,
    pub clients: HashMap<String, Client>,
    pub history: VecDeque<ChatMessage>,
}

pub struct RoomManager {
    pub rooms: HashMap<String, Room>,
}

pub struct RemoveClientResult {
    pub users: Vec<String>,
    pub user_left: bool,
    pub room_removed: bool 
}

impl RoomManager {
    pub fn new() -> Self {
        Self {
            rooms: HashMap::new(),
        }
    }

    pub fn get_or_create_room(&mut self, room_id: &str) -> &mut Room {
        self.rooms.entry(room_id.to_string()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(100);
            Room {
                sender,
                clients: HashMap::new(),
                history: VecDeque::new(),
            }
        })
    }

    pub fn add_history(&mut self, room_id: &str, message: ChatMessage) {
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.history.push_back(message);

            if room.history.len() > 50 {
                room.history.pop_front();
            }
        }
    }

    pub fn get_online_users(&self, room_id: &str) -> Vec<String> {
        self.rooms
            .get(room_id)
            .map(|room| {
                room.clients
                    .values()
                    .map(|client| client.username.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn add_client(
        &mut self,
        room_id: &str,
        connection_id: String,
        user_id: i32,
        username: String,
        sender: mpsc::UnboundedSender<ChatMessage>
    ) {
        let room = self.get_or_create_room(&room_id);
        room.clients.insert(connection_id, Client {
            user_id,
            username: username,
            sender: sender,
        });
    }

    pub fn get_history(&mut self, room_id: &str) -> Vec<ChatMessage> {
        self.rooms
            .get(room_id)
            .map(|room| room.history.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_client_sender(
        &self,
        room_id: &str,
        user_id: i32
    ) -> Vec<mpsc::UnboundedSender<ChatMessage>> {
        self.rooms
            .get(room_id)
            .map(|room| {
                room.clients
                    .values()
                    .filter(|client| client.user_id == user_id)
                    .map(|client| client.sender.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn remove_client_and_cleanup(&mut self, room_id: &str, connection_id: &str, user_id: i32) -> RemoveClientResult {

        let (still_online, room_empty) = {

            let Some(room) = self.rooms.get_mut(room_id) else {
            return RemoveClientResult { 
                users: Vec::new(), 
                user_left: false, 
                room_removed: false 
                }
            };

            println!(
                "[Room: {}] Client {} removed",
                room_id,
                connection_id
            );

            // Remove this WebSocket connection
            room.clients.remove(connection_id);

                
            // Check whether this user still has another connection
            let still_online = room.clients.values().any(|client| client.user_id == user_id);

            let room_empty = room.clients.is_empty();    
            (still_online, room_empty)
        };

        let users = self.get_online_users(room_id);

        if room_empty {
            println!(
                "[Room: {}] Empty room removed",
                room_id
            );

            self.rooms.remove(room_id);
        }

        RemoveClientResult {
            users,
            user_left: !still_online,
            room_removed: room_empty
        }

    }
}
